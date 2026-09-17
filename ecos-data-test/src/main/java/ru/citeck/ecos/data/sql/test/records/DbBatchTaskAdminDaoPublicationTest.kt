package ru.citeck.ecos.data.sql.test.records

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import ru.citeck.ecos.context.lib.auth.AuthContext
import ru.citeck.ecos.data.sql.batch.DbBatchTaskAdminDao
import ru.citeck.ecos.data.sql.batch.DbBatchTaskCancelAction
import ru.citeck.ecos.data.sql.batch.DbBatchTaskRestartAction
import ru.citeck.ecos.data.sql.domain.DbDomainFactory
import ru.citeck.ecos.data.sql.props.DbEcosDataProps
import ru.citeck.ecos.data.sql.records.perms.DefaultDbPermsComponent
import ru.citeck.ecos.records3.record.dao.RecordsDao
import ru.citeck.ecos.webapp.api.entity.EntityRef
import java.io.ByteArrayOutputStream
import java.io.PrintStream
import java.time.Duration
import java.util.Collections

/**
 * Who publishes this library's own administrator views of `ed_batch_task`, and when.
 *
 * The question is not rhetorical: `ecos-data` builds those DAOs but cannot register them - that
 * takes a `RecordsService`, which a [DbDomainFactory] is never given - while the thing they
 * describe, a schema, comes into existence lazily, whenever something first touches it. So the two
 * orders an application can produce are both real, and both are asserted here: the registrar
 * arriving after the schema, and the schema arriving after the registrar.
 *
 * Without this the views have no publisher at all: the DAO classes exist (they are built and
 * tested them directly), and no records service in the platform has ever been handed one, so an
 * administrator has no way to reach the queue of migrations this feature fills.
 */
class DbBatchTaskAdminDaoPublicationTest : DbRecordsTestBase() {

    private val builtFactories = ArrayList<DbDomainFactory>()

    @AfterEach
    fun stopBackgroundTasksOfEveryFactory() {
        builtFactories.forEach { it.stopBackgroundTasks() }
    }

    /**
     * Collects what a registrar is handed, in order. Stands in for the records service, which is
     * what the production registrar of `ecos-webapp-commons` hands them to.
     */
    private class RecordingRegistrar {

        val registered: MutableList<RecordsDao> = Collections.synchronizedList(ArrayList())

        val ids: List<String> get() = registered.map { it.getId() }
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

    private fun buildFactory(): DbDomainFactory {
        val factory = DbDomainFactory(
            dbDataSource,
            modelServiceFactory,
            DefaultDbPermsComponent(records, modelServiceFactory.workspaceService),
            computedAttsComponent,
            emptyList(),
            backend.dataServiceFactory,
            webAppApi,
            ecosContext,
            null,
            // nothing here is about the background work, and an interval this long keeps a tick
            // from firing while the test runs
            DbEcosDataProps(
                columns = DbEcosDataProps.ColumnsProps(reconcileInterval = Duration.ofHours(1)),
                batch = DbEcosDataProps.BatchProps(drainInterval = Duration.ofHours(1))
            )
        )
        builtFactories.add(factory)
        return factory
    }

    /**
     * The fixture's schema, already built by `DataMockFactory`. Touching it through a factory of
     * this test's own gives a second data source context over the same physical schema - which is
     * the situation an application is in anyway, and which keeps this test from creating a schema
     * that the suite's cleanup would then have to find.
     */
    private val schema: String get() = tableRef.schema

    private fun expectedIds() = listOf(
        "${DbBatchTaskAdminDao.ID}-$schema",
        "${DbBatchTaskCancelAction.ID}-$schema",
        "${DbBatchTaskRestartAction.ID}-$schema"
    )

    @Test
    fun aSchemaThatAlreadyExistsIsPublishedOnRegistrationTest() {

        val factory = buildFactory()
        factory.getDataSourceContext().getSchemaContext(schema)

        val registrar = RecordingRegistrar()
        factory.setRecordsDaoRegistrar { registrar.registered.add(it) }

        assertThat(registrar.ids)
            .describedAs("the schema was there first, so the replay is the only thing that can find it")
            .containsExactlyElementsOf(expectedIds())
    }

    @Test
    fun aSchemaCreatedAfterRegistrationIsPublishedTooTest() {

        val factory = buildFactory()
        val registrar = RecordingRegistrar()
        factory.setRecordsDaoRegistrar { registrar.registered.add(it) }

        assertThat(registrar.ids)
            .describedAs("this factory has touched no schema yet")
            .isEmpty()

        factory.getDataSourceContext().getSchemaContext(schema)

        assertThat(registrar.ids).containsExactlyElementsOf(expectedIds())
    }

    /**
     * One set of DAOs per schema, however many times the schema is asked for and however many times a
     * registrar is installed. The replay of [ru.citeck.ecos.data.sql.context.DbDataSourceContext]
     * makes a double announcement reachable by construction, so the deduplication is not belt and
     * braces: without it the records service would be handed the same source id twice.
     */
    @Test
    fun oneSchemaIsPublishedOnlyOnceTest() {

        val factory = buildFactory()
        val registrar = RecordingRegistrar()
        factory.setRecordsDaoRegistrar { registrar.registered.add(it) }

        factory.getDataSourceContext().getSchemaContext(schema)
        factory.getDataSourceContext().getSchemaContext(schema)
        factory.setRecordsDaoRegistrar { registrar.registered.add(it) }

        assertThat(registrar.ids).containsExactlyElementsOf(expectedIds())
    }

    /**
     * The actions are published pointing at **their own schema's** list, not at the bare default.
     *
     * Ids in `ed_batch_task` are bare `Long`s with no schema in them, so an action paired with the
     * wrong list would happily cancel the same-numbered task of another schema - which is why both
     * actions take a `listSourceId` at all. Asserted from both sides: the right list gets past the
     * pairing check and fails later, on the task that does not exist; the default list does not get
     * past it.
     */
    @Test
    fun theActionsArePairedWithTheirOwnSchemasListTest() {

        val factory = buildFactory()
        val registrar = RecordingRegistrar()
        factory.setRecordsDaoRegistrar { registrar.registered.add(it) }
        factory.getDataSourceContext().getSchemaContext(schema)

        val listSourceId = "${DbBatchTaskAdminDao.ID}-$schema"
        val cancel = registrar.registered
            .single { it.getId().startsWith(DbBatchTaskCancelAction.ID) } as DbBatchTaskCancelAction
        val restart = registrar.registered
            .single { it.getId().startsWith(DbBatchTaskRestartAction.ID) } as DbBatchTaskRestartAction

        AuthContext.runAsSystem {
            assertThat(errorOf { cancel.mutate(DbBatchTaskCancelAction.ActionDto(taskRef(listSourceId))) })
                .describedAs("accepted as this schema's, and then failed on the task itself")
                .contains("not found")
            assertThat(errorOf { restart.mutate(DbBatchTaskRestartAction.ActionDto(taskRef(listSourceId))) })
                .contains("not found")

            assertThat(errorOf { cancel.mutate(DbBatchTaskCancelAction.ActionDto(taskRef(DbBatchTaskAdminDao.ID))) })
                .describedAs("a ref of the unqualified list belongs to another schema's view")
                .contains("Not a valid batch task id")
            assertThat(
                errorOf { restart.mutate(DbBatchTaskRestartAction.ActionDto(taskRef(DbBatchTaskAdminDao.ID))) }
            ).contains("Not a valid batch task id")
        }
    }

    private fun taskRef(listSourceId: String): EntityRef {
        return EntityRef.create(APP_NAME, listSourceId, "1")
    }

    private fun errorOf(action: () -> Unit): String {
        return runCatching(action).exceptionOrNull()?.message
            ?: error("the call was expected to fail and did not")
    }

    /**
     * A registrar that throws must not take the schema with it. The schema exists to hold the
     * application's data; an administrator's view of a queue is not a reason to fail it.
     */
    @Test
    fun aFailingRegistrarDoesNotBreakSchemaCreationTest() {

        val factory = buildFactory()
        factory.setRecordsDaoRegistrar { error("this registrar is broken on purpose") }

        val log = captureStderr {
            val schemaCtx = factory.getDataSourceContext().getSchemaContext(schema)
            assertThat(schemaCtx.schema).isEqualTo(schema)
            assertThat(schemaCtx.batchTaskService)
                .describedAs("and the schema is usable, not half-built")
                .isNotNull()
        }

        assertThat(log)
            .describedAs("swallowed, but never silently")
            .contains("Schema ready listener failed")
    }

    /**
     * A factory made by `withDataSource` owns a different data source context and must not inherit
     * the registration. Schemas of two data sources can be named alike - `public` above all - and
     * publishing both would hand the records service one source id twice.
     */
    @Test
    fun aDerivedFactoryDoesNotInheritTheRegistrarTest() {

        val factory = buildFactory()
        val registrar = RecordingRegistrar()
        factory.setRecordsDaoRegistrar { registrar.registered.add(it) }
        factory.getDataSourceContext().getSchemaContext(schema)
        val publishedByTheOriginal = registrar.ids.size

        val derived = factory.withDataSource(dbDataSource)
        builtFactories.add(derived)
        derived.getDataSourceContext().getSchemaContext(schema)

        assertThat(registrar.ids)
            .describedAs("the derived factory publishes nothing until it is given a registrar of its own")
            .hasSize(publishedByTheOriginal)
    }
}
