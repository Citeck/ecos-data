package ru.citeck.ecos.data.sql.test.records

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import ru.citeck.ecos.data.sql.batch.DbBatchTaskEngine
import ru.citeck.ecos.data.sql.domain.DbDomainFactory
import ru.citeck.ecos.data.sql.modelchange.DbModelChangeQueue
import ru.citeck.ecos.data.sql.props.DbEcosDataProps
import ru.citeck.ecos.data.sql.records.perms.DefaultDbPermsComponent
import ru.citeck.ecos.webapp.api.EcosWebAppApi
import ru.citeck.ecos.webapp.api.func.UncheckedConsumer
import ru.citeck.ecos.webapp.api.func.UncheckedRunnable
import ru.citeck.ecos.webapp.api.promise.Promise
import ru.citeck.ecos.webapp.api.task.EcosTasksApi
import ru.citeck.ecos.webapp.api.task.executor.EcosTaskExecutorApi
import ru.citeck.ecos.webapp.api.task.scheduler.EcosScheduledTask
import ru.citeck.ecos.webapp.api.task.scheduler.EcosTaskSchedulerApi
import ru.citeck.ecos.webapp.api.task.scheduler.schedule.Schedule
import java.io.ByteArrayOutputStream
import java.io.PrintStream
import java.time.Duration
import java.util.Collections

/**
 * The composition root of this library's background work: which tasks a [DbDomainFactory] registers,
 * when, and how they are turned off.
 *
 * This is the first test in the repository that builds a [DbDomainFactory] at all - every other one
 * reaches its records DAOs through `DataMockFactory`, which constructs them directly. That is why
 * the switch asserted in [backgroundTasksCanBeTurnedOffTest] has to exist: without it, the day a
 * suite builds a factory is the day a real scheduler starts changing its schema underneath it.
 */
class DbDomainFactoryBackgroundTasksTest : DbRecordsTestBase() {

    private val activeTaskIds: MutableSet<String> = Collections.synchronizedSet(LinkedHashSet())
    private val builtFactories = ArrayList<DbDomainFactory>()

    @AfterEach
    fun stopBackgroundTasksOfEveryFactory() {
        builtFactories.forEach { it.stopBackgroundTasks() }
    }

    private fun reconcileTaskIds() = activeTaskIds.filter {
        it.startsWith(DbModelChangeQueue.RECONCILE_TASK_ID_PREFIX)
    }

    private fun drainTaskIds() = activeTaskIds.filter {
        it.startsWith(DbBatchTaskEngine.DRAIN_TASK_ID_PREFIX)
    }

    /**
     * slf4j-simple (the test binding) writes to stderr and this project has no in-JVM log capture
     * utility - the same approach [DbSchemaReconcilerTest] uses.
     */
    private fun captureStderr(action: () -> Unit): String {
        val original = System.err
        val captured = ByteArrayOutputStream()
        System.setErr(PrintStream(captured, true))
        try {
            action()
        } finally {
            System.setErr(original)
        }
        return captured.toString()
    }

    /**
     * The factory's own view of the application, with two differences from the plain mock, both of
     * them the point of a test in here:
     *
     *  - every scheduled task id is recorded, and forgotten again when the task is cancelled;
     *  - `doBeforeAppReady` behaves as **production** does rather than as the mock does: it throws
     *    once the application is ready. The mock runs it happily, so without this a factory that
     *    used the wrong hook would pass every test here and fail on the one production path that
     *    builds a factory lazily, after start-up - `DbRecSrcFactory.withDataSource`.
     */
    private fun factoryWebAppApi(): EcosWebAppApi {
        val delegate = webAppApi
        return object : EcosWebAppApi by delegate {
            override fun doBeforeAppReady(order: Float, action: () -> Unit) {
                if (delegate.isReady()) {
                    error("You should not call doBeforeAppReady after application become ready")
                }
                delegate.doBeforeAppReady(order, action)
            }

            override fun getTasksApi(): EcosTasksApi {
                return RecordingTasksApi(delegate.getTasksApi())
            }
        }
    }

    private fun buildFactory(backgroundTasksEnabled: Boolean): DbDomainFactory {
        val factory = DbDomainFactory(
            dbDataSource,
            modelServiceFactory,
            DefaultDbPermsComponent(records, modelServiceFactory.workspaceService),
            computedAttsComponent,
            emptyList(),
            backend.dataServiceFactory,
            factoryWebAppApi(),
            ecosContext,
            null,
            // An interval long enough that nothing can fire while the test runs: what is asserted
            // here is that the schedules were registered, not what they do when they run.
            DbEcosDataProps(
                columns = DbEcosDataProps.ColumnsProps(reconcileInterval = Duration.ofHours(1)),
                batch = DbEcosDataProps.BatchProps(drainInterval = Duration.ofHours(1)),
                backgroundTasksEnabled = backgroundTasksEnabled
            )
        )
        builtFactories.add(factory)
        return factory
    }

    @Test
    fun backgroundTasksCanBeTurnedOffTest() {

        val log = captureStderr { buildFactory(backgroundTasksEnabled = false) }

        assertThat(drainTaskIds()).isEmpty()
        assertThat(reconcileTaskIds()).isEmpty()
        assertThat(log)
            .describedAs("turning it off stops the feature, not just the load - that has to be said out loud")
            .contains("ecos.webapp.data.background-tasks-enabled")
    }

    /**
     * Both halves of the same job: the schema change (the reconcile tick) and the values that
     * follow it into the new column (the batch drain). The drain in particular had no caller at all
     * until now - the engine was built but never started.
     */
    @Test
    fun bothBackgroundTasksAreStartedByDefaultTest() {

        buildFactory(backgroundTasksEnabled = true)

        assertThat(drainTaskIds()).hasSize(1)
        assertThat(reconcileTaskIds()).hasSize(1)
    }

    /**
     * The case that decides where the start lives. `withDataSource` builds a new factory with a new
     * data source context, and therefore a new engine and a new queue; a single bean calling
     * `start()` once at boot would never reach them, and the external data sources of
     * `ecos-integrations` would have their migrations queued and never drained.
     *
     * It is also what forces the task ids to carry a counter: the scheduler rejects an id that is
     * already registered and active, so a constant one would make this throw.
     */
    @Test
    fun aDerivedFactoryStartsItsOwnPairOfTasksTest() {

        val first = buildFactory(backgroundTasksEnabled = true)
        val derived = first.withDataSource(dbDataSource)
        builtFactories.add(derived)

        assertThat(derived.getDataSourceContext())
            .describedAs("the derived factory really does have a context of its own to run for")
            .isNotSameAs(first.getDataSourceContext())
        assertThat(drainTaskIds()).hasSize(2)
        assertThat(reconcileTaskIds()).hasSize(2)
        assertThat(activeTaskIds).describedAs("and no id was reused").hasSize(4)
    }

    /**
     * The hook, asserted rather than argued. The one external caller of `withDataSource` builds its
     * factory lazily, on the first request for a records source, i.e. long after the application
     * became ready - and `doBeforeAppReady` throws at that point. [factoryWebAppApi] reproduces
     * that, so this test fails with the production message if the hook is ever changed back.
     */
    @Test
    fun aFactoryBuiltAfterTheApplicationIsReadyDoesNotThrowTest() {

        assertThat(webAppApi.isReady())
            .describedAs("the mock is always ready, which is exactly the situation under test")
            .isTrue()

        val derived = buildFactory(backgroundTasksEnabled = true).withDataSource(dbDataSource)
        builtFactories.add(derived)

        assertThat(reconcileTaskIds())
            .describedAs("built after ready, and still scheduled - so the hook was doWhenAppReady")
            .hasSize(2)
    }

    @Test
    fun stoppingBackgroundTasksCancelsBothTest() {

        val factory = buildFactory(backgroundTasksEnabled = true)
        assertThat(activeTaskIds).hasSize(2)

        factory.stopBackgroundTasks()

        assertThat(drainTaskIds()).isEmpty()
        assertThat(reconcileTaskIds()).isEmpty()
    }

    private inner class RecordingTasksApi(private val delegate: EcosTasksApi) : EcosTasksApi {

        override fun getExecutor(key: String): EcosTaskExecutorApi {
            return delegate.getExecutor(key)
        }

        override fun getScheduler(key: String): EcosTaskSchedulerApi {
            return RecordingScheduler(delegate.getScheduler(key))
        }
    }

    /**
     * Records ids, and delegates everything else to the real scheduler on purpose: a fake would
     * accept the same task id twice, which is precisely the mistake
     * [aDerivedFactoryStartsItsOwnPairOfTasksTest] exists to catch.
     */
    private inner class RecordingScheduler(private val delegate: EcosTaskSchedulerApi) : EcosTaskSchedulerApi {

        override fun schedule(
            taskId: String,
            schedule: Schedule,
            task: (EcosScheduledTask) -> Unit
        ): EcosScheduledTask {
            val scheduled = delegate.schedule(taskId, schedule, task)
            activeTaskIds.add(taskId)
            return object : EcosScheduledTask by scheduled {
                override fun cancel(): Promise<Boolean> {
                    activeTaskIds.remove(taskId)
                    return scheduled.cancel()
                }

                override fun cancel(timeout: Duration): Promise<Boolean> {
                    activeTaskIds.remove(taskId)
                    return scheduled.cancel(timeout)
                }
            }
        }

        override fun scheduleJ(taskId: String, schedule: Schedule, task: UncheckedRunnable): EcosScheduledTask {
            return schedule(taskId, schedule) { task.run() }
        }

        override fun scheduleJ(
            taskId: String,
            schedule: Schedule,
            task: UncheckedConsumer<EcosScheduledTask>
        ): EcosScheduledTask {
            return schedule(taskId, schedule) { task.accept(it) }
        }

        override fun getAsJavaScheduledExecutor() = delegate.getAsJavaScheduledExecutor()
    }
}
