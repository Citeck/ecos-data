package ru.citeck.ecos.data.sql.batch

import io.github.oshai.kotlinlogging.KotlinLogging
import ru.citeck.ecos.commons.task.schedule.Schedules
import ru.citeck.ecos.context.lib.auth.AuthContext
import ru.citeck.ecos.data.sql.context.DbDataSourceContext
import ru.citeck.ecos.data.sql.context.DbSchemaContext
import ru.citeck.ecos.data.sql.dto.DbTableRef
import ru.citeck.ecos.data.sql.repo.entity.DbEntity
import ru.citeck.ecos.data.sql.repo.find.DbFindPage
import ru.citeck.ecos.data.sql.repo.find.DbFindSort
import ru.citeck.ecos.data.sql.service.DbDataService
import ru.citeck.ecos.data.sql.service.DbDataServiceConfig
import ru.citeck.ecos.data.sql.service.DbDataServiceImpl
import ru.citeck.ecos.records2.predicate.model.Predicates
import ru.citeck.ecos.txn.lib.TxnContext
import ru.citeck.ecos.webapp.api.task.scheduler.EcosScheduledTask
import java.time.Duration
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger

/**
 * Runs the background jobs of [DbBatchTaskService].
 *
 * The engine owns everything about *running* a task - the id window, the cursor, the transaction
 * boundaries, the retries and the throttle; a [DbBatchTaskHandler] owns only what happens to the
 * rows inside one window.
 *
 * Two transaction boundaries carry the whole correctness argument and are called out again where
 * they are written:
 *  - [runOneBatch] puts `processBatch` and the cursor write in **one** transaction, so a batch that
 *    fails cannot leave the cursor past work that never happened;
 *  - [registerBatchFailure] writes the attempt counter in a **different** transaction, so the
 *    counter survives the rollback of the batch it counts.
 *
 * A third rule carries the rest of it: **every** write this engine makes to `ed_batch_task` is a
 * status-conditioned compare-and-set ([DbBatchTaskService.saveProgressIfStatusMatches] for a
 * batch's progress, [DbBatchTaskService.saveIfStatusMatches] for everything that changes the
 * status), never a whole-row `save`. The table has no `__upd_version`, so a whole-row save built
 * from a snapshot taken before an arbitrarily long handler callback silently carries away anything
 * committed during it - a cancel above all. When the compare-and-set loses, the engine re-reads the
 * row and acts on its **real** status; it never falls back to an unconditional write.
 *
 * Mutual exclusion between instances is [ru.citeck.ecos.webapp.api.lock.EcosLockApi]'s job, wired
 * into [drainSchemaOnce] as a per-table lock (two tasks against the same table must serialise; two
 * tasks against different tables need not wait on each other). [start] registers the periodic
 * drain on [ru.citeck.ecos.webapp.api.task.EcosTasksApi]'s scheduler; nothing here starts itself -
 * see [start]'s own doc for why.
 */
class DbBatchTaskEngine(
    private val dataSourceCtx: DbDataSourceContext,
    /**
     * Resolves the [DbBatchTaskService] of a schema. Defaults to the schema's own instance - the
     * only thing every production call site ever wants - but is a constructor parameter rather
     * than a hardcoded `schemaCtx.batchTaskService` so that a test can substitute a
     * [DbBatchTaskService] subclass without turning `DbSchemaContext.batchTaskService` itself into
     * mutable state on an object shared by every consumer of the schema. See
     * `DbBatchTaskEngineTest`'s `CancellingBatchTaskService` / `FlappingStatusBatchTaskService` for
     * the seam this exists for.
     */
    private val taskServiceProvider: (DbSchemaContext) -> DbBatchTaskService = { it.batchTaskService }
) {

    companion object {
        private val log = KotlinLogging.logger {}

        /**
         * Public so that a test asserting which background tasks a composition root registered can
         * name them without copying the literal.
         */
        const val DRAIN_TASK_ID_PREFIX = "ecos-data-batch-task-drain"

        /**
         * Makes every engine instance's drain task id unique within this JVM.
         *
         * [ru.citeck.ecos.data.sql.domain.DbDomainFactory.withDataSource] builds a **new**
         * [DbDataSourceContext] - and therefore a new engine - per data source, while every one of
         * them schedules onto the one shared scheduler. A constant task id made the second
         * [start] throw `IllegalStateException("Task '<id>' already registered and active")`, so an
         * application with two data sources could not start its drain at all. Uniqueness is only
         * ever needed inside one JVM's scheduler, which is exactly what a process-wide counter
         * gives.
         */
        private val drainTaskCounter = AtomicInteger()

        /**
         * The distributed lock key that serialises every instance's attempts against one table of
         * one schema. Per table rather than per task: two tasks queued against the same table would
         * otherwise be free to run concurrently and race each other's writes the same way two
         * instances of this engine would.
         */
        fun lockKey(schema: String, table: String): String {
            return "ecos-data-batch-task-$schema-$table"
        }

        /**
         * `ed_batch_task.__error` is free text; a stack-trace-sized message helps nobody.
         */
        private const val MAX_ERROR_LENGTH = 4000

        /**
         * Bound on [runOneBatch]'s compare-and-set retry: the only writer this engine can race
         * against outside its own table lock is an administrator's [DbBatchTaskService.cancel],
         * and a final status never moves again, so the retry conditioned on a freshly re-read
         * final status is guaranteed to land on the very next attempt. This ceiling only guards
         * against a scenario the engine's own concurrency model does not produce - the row
         * changing status a suspicious number of times in a row - so it fails loudly rather than
         * spin forever.
         */
        private const val MAX_PROGRESS_WRITE_ATTEMPTS = 5

        /**
         * Ceiling on [logOnce]'s memory. The set is keyed by task, and a queue big enough to reach
         * this many undrainable tasks has a problem of its own - but a set that grows for the
         * process lifetime would be a leak, so it is emptied instead of grown. The warnings then
         * come back once each, which is bounded noise rather than silence.
         */
        private const val MAX_ONE_TIME_WARNINGS = 1000
    }

    /**
     * One reader per table, only ever used for "what is the largest id in this table right now".
     * [DbEntity] is the entity of every domain table of this platform and the engine reads the
     * rows raw, so nothing here depends on the shape of the table beyond it having an `id`.
     */
    private val maxIdReaders = ConcurrentHashMap<DbTableRef, DbDataService<DbEntity>>()

    /**
     * Guards [start]/[stop] against being run twice - see [start]'s doc for why nobody is allowed
     * to call it from a constructor, which is the only reason a race here would ever matter: every
     * legitimate caller is the single composition root.
     */
    private val started = AtomicBoolean(false)

    private var scheduledTask: EcosScheduledTask? = null

    /**
     * Unique per engine instance - see [drainTaskCounter].
     */
    private val drainTaskId = "$DRAIN_TASK_ID_PREFIX-${drainTaskCounter.incrementAndGet()}"

    /**
     * Keys of the "this task cannot be taken on" warnings already emitted by this process, so that
     * a task with an unknown handler type or a missing target table does not print the same WARN on
     * every single drain tick, for ever. Bounded - see [MAX_ONE_TIME_WARNINGS].
     */
    private val oneTimeWarnings = ConcurrentHashMap.newKeySet<String>()

    /**
     * Registers [drainOnce] on a fixed-delay schedule of
     * [ru.citeck.ecos.data.sql.props.DbEcosDataProps.BatchProps.drainInterval], so a data source
     * that nobody explicitly drives still makes progress on its own.
     *
     * **Never call this from a constructor.** [DbDataSourceContext] constructs its engine eagerly,
     * because doing so is cheap and the engine does nothing until [start] runs - but the test mock
     * behind [ru.citeck.ecos.webapp.api.task.EcosTasksApi] hands back a **real**
     * thread-pool-backed scheduler, so a self-starting engine would put background threads into
     * every one of this repository's ~940 tests, mutating schemas out from under assertions that
     * never asked for a background drain. The composition root calls this explicitly once it is
     * actually ready to run background work; [drainOnce]/[drainSchemaOnce] stay public and
     * synchronous so tests can still drive a tick deterministically without ever calling this.
     *
     * Idempotent: a second call is a no-op, so a composition root that runs its startup twice (a
     * retried bootstrap, a re-applied Spring context) does not end up with two competing schedules
     * racing the same drain interval against each other.
     */
    fun start() {
        if (!started.compareAndSet(false, true)) {
            return
        }
        val drainInterval = dataSourceCtx.props.batch.drainInterval
        log.info { "Starting the batch task engine drain every $drainInterval" }
        scheduledTask = dataSourceCtx.webAppApiForBackgroundTasks.getTasksApi()
            // The MAIN scheduler, not a key of this library's own invention:
            // EcosTasksManager.createScheduler throws when `ecos.webapp.task.schedulers.<key>` is
            // absent from the environment, and ecos-webapp-lib-spring's defaults declare only
            // `main` and `records`. ecos-data is a library - a key it invented would have to be
            // declared by every consuming application's yml, and `start()` would throw in every
            // application that forgot. Isolating the drain onto a pool of its own, if it is ever
            // wanted, is a composition-root concern.
            .getMainScheduler()
            .schedule(drainTaskId, Schedules.fixedDelay(drainInterval)) {
                drainOnce()
            }
    }

    /**
     * Cancels the schedule [start] registered. A no-op if [start] was never called or [stop] has
     * already run.
     */
    fun stop() {
        if (!started.compareAndSet(true, false)) {
            return
        }
        scheduledTask?.cancel()
        scheduledTask = null
    }

    /**
     * @return how many tasks were attempted across every schema this data source has touched.
     */
    fun drainOnce(): Int {
        return dataSourceCtx.getSchemaContexts().sumOf { drainSchemaOnce(it) }
    }

    /**
     * Attempts every drainable task of one schema whose table lock this instance can take.
     *
     * Each task is run under [ru.citeck.ecos.webapp.api.lock.EcosLockApi.doInSyncOrSkip], keyed by
     * [lockKey] of the task's schema and table, with a **zero** timeout: if another instance already
     * holds that table's lock, this tick skips the task rather than queuing up behind it. Waiting
     * would mean two instances both blocked on the same lock every drain interval - a pile-up, not
     * mutual exclusion. The lock is per table rather than per task, so two tasks queued against the
     * same table serialise, but tasks against different tables never wait on each other.
     *
     * @return how many tasks this call took the lock for and attempted - **not** how many reached a
     *         final state. A task can end a tick still [DbBatchTaskStatus.RUNNING] and waiting out
     *         its backoff, and "attempted" is the only number that means the same thing in both
     *         cases. A task this instance declined to take on - unknown handler type, unknown target
     *         table, a row that has gone or is already final, or whose table lock was held elsewhere
     *         - is not counted.
     */
    fun drainSchemaOnce(schemaCtx: DbSchemaContext): Int {
        var attempted = 0
        val lockApi = dataSourceCtx.webAppApiForBackgroundTasks.getAppLockApi()
        // findActive already excludes the tasks inside their retry backoff window, so a backing-off
        // task does not hide the tasks queued behind it.
        for (task in taskServiceProvider(schemaCtx).findActive()) {
            // One task must never be able to end the tick. Everything inside runTask that can fail
            // is already routed into the retry bookkeeping, but this is the last line of defence:
            // an escape here would leave every task queued behind this one unreachable, once per
            // drain interval, for ever.
            try {
                var result = RunResult.notAttempted()
                lockApi.doInSyncOrSkip(lockKey(schemaCtx.schema, task.table), Duration.ZERO) {
                    result = runTaskAttempt(schemaCtx, task.id)
                }
                if (result.attempted) {
                    attempted++
                }
            } catch (e: Throwable) {
                if (e is InterruptedException) {
                    Thread.currentThread().interrupt()
                    throw e
                }
                log.error(e) {
                    "Batch task ${task.id} ('${task.handler}') escaped the engine in schema " +
                        "'${schemaCtx.schema}'. The drain continues with the next task"
                }
            }
        }
        return attempted
    }

    /**
     * Runs one task now, ignoring the retry backoff window: [findActive][DbBatchTaskService.findActive]
     * honours `nextAttemptAt`, `runTask` means "run this one now".
     *
     * Takes the **same per-table distributed lock** [drainSchemaOnce] takes, for the same reason:
     * without it two instances calling this concurrently would run the same task against live
     * customer data at the same time and both write the cursor. The lock is taken here rather than
     * inside [runTaskAttempt] so that [drainSchemaOnce], which already holds it, does not take it
     * twice - [ru.citeck.ecos.webapp.api.lock.EcosLockApi]'s locks are explicitly non-reentrant.
     *
     * @return the status the task ended this call in, or null when this instance did not take the
     *         task on at all - including when **another instance holds the table's lock**, in which
     *         case nothing is run and nothing is written.
     */
    fun runTask(schemaCtx: DbSchemaContext, taskId: Long): DbBatchTaskStatus? {
        // the lock is keyed by the task's target table, so the snapshot has to be read first
        val snapshot = taskServiceProvider(schemaCtx).getById(taskId)
        if (snapshot == null) {
            log.warn { "Batch task $taskId doesn't exist in schema ${schemaCtx.schema}. Nothing to run" }
            return null
        }
        var result = RunResult.notAttempted()
        val executed = dataSourceCtx.webAppApiForBackgroundTasks.getAppLockApi()
            .doInSyncOrSkip(lockKey(schemaCtx.schema, snapshot.table), Duration.ZERO) {
                result = runTaskAttempt(schemaCtx, taskId)
            }
        if (!executed) {
            log.info {
                "Batch task $taskId ('${snapshot.handler}') was not run: the lock on table " +
                    "'${snapshot.table}' of schema '${schemaCtx.schema}' is held elsewhere"
            }
            return null
        }
        return result.status
    }

    private fun runTaskAttempt(schemaCtx: DbSchemaContext, taskId: Long): RunResult {
        // handlers that go through mutation need the system context for
        // DbRecordsControlAtts.DISABLE_AUDIT / DISABLE_EVENTS.
        return AuthContext.runAsSystem {
            runTaskAsSystem(schemaCtx, taskId)
        }
    }

    private fun runTaskAsSystem(schemaCtx: DbSchemaContext, taskId: Long): RunResult {

        val taskService = taskServiceProvider(schemaCtx)
        val snapshot = taskService.getById(taskId)
        if (snapshot == null) {
            log.warn { "Batch task $taskId doesn't exist in schema ${schemaCtx.schema}. Nothing to run" }
            return RunResult.notAttempted()
        }
        val handler = dataSourceCtx.batchTaskHandlers.getHandler(snapshot.handler)
        if (handler == null) {
            // During a rolling upgrade one instance may not know a type yet. Failing the task here
            // would destroy work another instance is perfectly able to do, so the row is left alone.
            // Logged once per task per process: this branch is reached on every drain tick for as
            // long as the row exists, and an unthrottled WARN here is a log line every ten seconds
            // for ever, per task, in production.
            logOnce("${schemaCtx.schema}:$taskId:unknown-handler") {
                "Batch task $taskId has unknown handler type '${snapshot.handler}'. " +
                    "Known types: ${dataSourceCtx.batchTaskHandlers.getTypes()}. Task is left " +
                    "untouched. This is logged once per task per process"
            }
            return RunResult.notAttempted()
        }
        if (snapshot.status.isFinal()) {
            // nothing was run, so this does not count as an attempt
            return RunResult.notAttempted(snapshot.status)
        }
        if (!isTargetTableKnown(schemaCtx, snapshot.table)) {
            // Throttled for the same reason as the unknown-handler branch above.
            logOnce("${schemaCtx.schema}:$taskId:unknown-table") {
                "Batch task $taskId ('${snapshot.handler}') targets table '${snapshot.table}', " +
                    "which has no columns in schema '${schemaCtx.schema}' - it doesn't exist here. " +
                    "Task is left untouched rather than silently reported as finished. This is " +
                    "logged once per task per process"
            }
            return RunResult.notAttempted()
        }

        // From here on the task has been taken on, and every failure - not only one thrown by
        // processBatch - belongs in the retry bookkeeping. A handler whose prepare() throws on a
        // malformed __params would otherwise get no errorCount, no backoff and no terminal state,
        // and its exception would propagate out through the drain loop.
        val phase = PhaseTracker()
        return try {
            RunResult.attempted(runPasses(schemaCtx, handler, snapshot.table, taskId, phase))
        } catch (e: InterruptedException) {
            // A shutdown is not this task's fault and must not consume one of its attempts:
            // recording it would raise errorCount and push nextAttemptAt minutes out, and an
            // operator who has configured a finite `batch.maxAttempts` budget would then see a few
            // redeploys mark a perfectly healthy task FAILED. Restore the flag and let it travel as
            // the shutdown signal it is.
            Thread.currentThread().interrupt()
            throw e
        } catch (e: Throwable) {
            RunResult.attempted(registerBatchFailure(schemaCtx, taskId, phase.current, e))
        }
    }

    /**
     * The batch loop itself: passes over the table until the cursor catches up with a maximum id
     * that has stopped moving.
     */
    private fun runPasses(
        schemaCtx: DbSchemaContext,
        handler: DbBatchTaskHandler,
        table: String,
        taskId: Long,
        phase: PhaseTracker
    ): DbBatchTaskStatus? {

        phase.current = PhaseTracker.PREPARE
        val preparation = prepareRun(schemaCtx, handler, taskId)
        if (preparation != null) {
            // The row went final while prepare() ran - an administrator's cancel is the only way
            // that happens. The whole run aborts here rather than walking a single batch of work
            // nobody wants any more, and it aborts through the same path a mid-batch cancel takes,
            // so onCancel is invoked and the status reported is the row's real one.
            phase.current = PhaseTracker.CANCEL
            return finishStoppedRun(schemaCtx, handler, taskId, preparation.status)
        }

        // Once per run, for the same reason DbColumnMigrationHandler.prepare drops its own: this
        // service is cached per table and survives between ticks, while the handler that queued the
        // task is usually a schema change - so the very first read of a run is the one most likely
        // to meet a table that no longer looks the way this reader remembers it. Measured on a live
        // installation: after a column returned to its previous type, this reader failed with
        // "Can't convert String to Double" and the restore's value transfer stalled on it. The read
        // path recovers from that by itself now (DbDataServiceImpl.isStaleSchemaError), at the cost
        // of one failed attempt and one error recorded on the task; this removes the cost.
        getMaxIdReader(schemaCtx, table).resetColumnsCache()

        // The outer loop is one pass over the table. The maximum id is read once per pass and
        // again at the end of it, never per batch: it costs a query, and inside a pass the cursor
        // can only walk towards it.
        var passes = 0
        val maxPasses = dataSourceCtx.props.batch.maxPassesPerRun
        while (true) {
            passes++
            phase.current = PhaseTracker.READ_MAX_ID
            val maxId = readMaxId(schemaCtx, table)
            while (true) {
                phase.current = PhaseTracker.BATCH
                when (val outcome = runOneBatch(schemaCtx, handler, taskId, maxId)) {
                    is BatchOutcome.Stopped -> {
                        phase.current = PhaseTracker.CANCEL
                        return finishStoppedRun(schemaCtx, handler, taskId, outcome.status)
                    }
                    is BatchOutcome.EndOfWindow -> break
                    is BatchOutcome.Processed -> {
                        // The throttle, between successful batches and outside any
                        // transaction - sleeping inside one would hold its locks for the pause.
                        pauseBetweenBatches()
                    }
                }
            }
            // the pass ended because the cursor reached the maximum id *this pass was given*.
            // Rows inserted while the pass ran land PAST that id, so no window of this pass could
            // ever have offered them - which is why the only way to find them is to read the
            // maximum id again and, while it is still ahead of the cursor, run another pass.
            // (Asking whether the last window did any work answers a different question entirely:
            // a pass whose tail happened to be empty would stop and drop everything appended
            // behind it.)
            phase.current = PhaseTracker.READ_MAX_ID
            val fresh = taskServiceProvider(schemaCtx).getById(taskId)
            if (fresh == null) {
                // Same situation runOneBatch reports on its own vanished-row path, and it deserves
                // the same visibility: nothing deletes ed_batch_task rows today, so reaching this
                // means something outside the engine removed one mid-run.
                log.warn {
                    "Batch task $taskId ('${handler.getType()}') disappeared from schema " +
                        "'${schemaCtx.schema}' between two passes. Ending the run here"
                }
                break
            }
            if (readMaxId(schemaCtx, table) <= fresh.cursor) {
                break
            }
            if (passes >= maxPasses) {
                // The table is still growing ahead of the cursor. Yield rather than keep the lock:
                // the row stays RUNNING with its cursor intact, so the next drain tick resumes
                // exactly here, and meanwhile every other task of this schema gets its turn.
                log.info {
                    "Batch task $taskId ('${handler.getType()}') yielding after $passes passes: " +
                        "table '$table' is still growing ahead of cursor ${fresh.cursor}. " +
                        "The task stays RUNNING and the next drain continues it"
                }
                return DbBatchTaskStatus.RUNNING
            }
        }

        phase.current = PhaseTracker.FINISH
        val finished = finishTask(schemaCtx, handler, taskId)
        if (finished == DbBatchTaskStatus.CANCELLED) {
            // A cancel that landed after the last window - or during onFinish itself - won the
            // compare-and-set in finishTask, so the row is CANCELLED and not DONE. The handler is
            // told the same way it would have been told mid-batch.
            phase.current = PhaseTracker.CANCEL
            return finishStoppedRun(schemaCtx, handler, taskId, finished)
        }
        return finished
    }

    /**
     * `prepare` runs once per call, in its own transaction, and publishes
     * [DbBatchTaskStatus.RUNNING] before the first batch so that an administrator watching the
     * queue sees the task move even if the first batch is slow.
     *
     * `prepare` is an arbitrarily long handler callback, so the write below is
     * [DbBatchTaskService.saveIfStatusMatches] and not a whole-row `save`: a cancel committed while
     * it ran would otherwise be carried away and the task would run to completion with the
     * administrator believing they stopped it.
     *
     * @return null when the run may proceed; a [BatchOutcome.Stopped] carrying the row's real
     *         status (or null for a row that is gone) when it may not.
     */
    private fun prepareRun(
        schemaCtx: DbSchemaContext,
        handler: DbBatchTaskHandler,
        taskId: Long
    ): BatchOutcome.Stopped? {

        val taskService = taskServiceProvider(schemaCtx)
        return TxnContext.doInNewTxn {
            var fresh = taskService.getById(taskId) ?: return@doInNewTxn BatchOutcome.Stopped(null)
            if (fresh.status.isFinal()) {
                return@doInNewTxn BatchOutcome.Stopped(fresh.status)
            }
            val ctx = DbBatchTaskContext(fresh, schemaCtx)
            val total = handler.prepare(ctx)
            var attempt = 0
            while (attempt < MAX_PROGRESS_WRITE_ATTEMPTS) {
                attempt++
                val updated = taskService.saveIfStatusMatches(
                    taskId,
                    fresh.status,
                    mapOf(
                        DbBatchTaskEntity.STATUS to DbBatchTaskStatus.RUNNING.name,
                        // "when this task first started", not "when the last tick picked it up":
                        // a task that ticks for a week would otherwise show today's timestamp in
                        // the admin column that exists to answer how long it has been going.
                        DbBatchTaskEntity.STARTED to
                            if (fresh.started == Instant.EPOCH) Instant.now() else fresh.started,
                        // null means "no cheap estimate"; it must not overwrite an estimate an
                        // earlier run already produced, and -1 keeps meaning "unknown" rather
                        // than "empty"
                        DbBatchTaskEntity.TOTAL to (total ?: fresh.total),
                        DbBatchTaskEntity.PROCESSED to ctx.processed,
                        DbBatchTaskEntity.SKIPPED to ctx.skipped,
                        DbBatchTaskEntity.FAILED to ctx.failed
                    )
                )
                if (updated) {
                    return@doInNewTxn null
                }
                val afterRace = taskService.getById(taskId)
                    ?: return@doInNewTxn BatchOutcome.Stopped(null)
                if (afterRace.status.isFinal()) {
                    return@doInNewTxn BatchOutcome.Stopped(afterRace.status)
                }
                fresh = afterRace
            }
            error(
                "Batch task $taskId: compare-and-set on prepare did not land after $attempt " +
                    "attempts - the row's status kept changing underneath it"
            )
        }
    }

    /**
     * One batch = one transaction.
     *
     * **The cursor write and `processBatch` are deliberately in the same transaction**:
     * they commit or roll back together, so a batch that throws cannot leave the cursor standing
     * past rows that were never converted. Moving the cursor write out - committing it before the
     * handler runs - is exactly how a failed batch silently skips its window forever.
     */
    private fun runOneBatch(
        schemaCtx: DbSchemaContext,
        handler: DbBatchTaskHandler,
        taskId: Long,
        maxId: Long
    ): BatchOutcome {

        val taskService = taskServiceProvider(schemaCtx)

        return TxnContext.doInNewTxn {

            // a real re-read, inside the batch transaction. ed_batch_task carries no
            // __upd_version, so there is no optimistic check to lean on: every write this engine
            // makes goes through a status-conditioned compare-and-set
            // (DbBatchTaskService.saveProgressIfStatusMatches / .saveIfStatusMatches) built from a
            // row read *now* rather than from a snapshot taken earlier.
            val fresh = taskService.getById(taskId)
                ?: return@doInNewTxn BatchOutcome.Stopped(null)
            if (fresh.status.isFinal()) {
                return@doInNewTxn BatchOutcome.Stopped(fresh.status)
            }
            if (fresh.cursor >= maxId) {
                return@doInNewTxn BatchOutcome.EndOfWindow
            }

            // Clamped to the table's largest id, because __cursor is "the id of the last
            // processed record" and not an arithmetic offset. Unclamped, a table of 7
            // rows would persist a cursor of 500: ids 8..500 would then sit in front of no future
            // window for ever, and DbBatchTaskService.restart deliberately keeps the cursor, so
            // even the administrator's restart could not reach them.
            val batchSize = dataSourceCtx.props.batch.batchSize.toLong()
            val batch = DbBatchTaskBatch(fresh.cursor, minOf(fresh.cursor + batchSize, maxId))
            // A fresh context per batch. DbBatchTaskContext's counters are plain in-memory vars
            // seeded from the row, and a rolled-back batch does not roll them back - reusing one
            // context across batches would double-count everything a retried batch already counted.
            val ctx = DbBatchTaskContext(fresh, schemaCtx)
            handler.processBatch(ctx, batch)

            // Read the row once more before writing it back. processBatch can run for a long time,
            // and a cancel committed by another instance during it would otherwise be overwritten
            // by the write below.
            val atWriteTime = taskService.getById(taskId)
                ?: return@doInNewTxn rowVanishedMidBatch(taskId, handler, batch.toInclusive)

            // write only through the id-keyed compare-and-set, conditioned on the
            // status this call just read - including when that status is already final. This
            // batch's work already committed inside processBatch, __cursor is "the id of the last
            // processed record" and restart() deliberately keeps it, so a cancel
            // mid-batch must not cause a restart to reprocess ids this pass already converted -
            // the progress write below is retried against the row's real status until it lands.
            // If it never lands within the bound, the batch is not abandoned silently either: it
            // throws (below), which rolls this whole transaction back - the cursor and counters
            // together with processBatch's own work - and is routed by runTaskAsSystem's catch
            // into registerBatchFailure, so error/errorCount/nextAttemptAt land on the row where an
            // administrator can see them and the existing retry machinery takes over. What changes
            // with the status on a successful attempt is only whether the loop above is told to
            // keep going afterwards.
            //
            // ed_batch_task has no __upd_version, so a plain `save` here would write the whole row
            // unconditionally - if a cancel committed in the gap between the read above and this
            // write, that whole-row write would carry the pre-cancel status right back over it, and
            // the task would finish normally through onFinish instead of onCancel. The
            // compare-and-set makes that gap unreachable: either the status is still what was just
            // read and the write lands, or it changed and the next attempt is conditioned on the
            // row's real current status instead. A status this engine did not just read can only
            // be a [DbBatchTaskStatus.CANCELLED] committed by an administrator outside this
            // engine's own table lock - and a final status never moves again - so the retry is
            // guaranteed to land on its very next attempt; the bound only guards against a
            // scenario this engine's concurrency model does not otherwise produce.
            var statusForWrite = atWriteTime.status
            var updated = false
            var rowStatus = atWriteTime.status
            var attempt = 0
            while (attempt < MAX_PROGRESS_WRITE_ATTEMPTS) {
                attempt++
                updated = taskService.saveProgressIfStatusMatches(
                    taskId,
                    statusForWrite,
                    batch.toInclusive,
                    ctx.processed,
                    ctx.skipped,
                    ctx.failed
                )
                if (updated) {
                    rowStatus = statusForWrite
                    break
                }
                val afterRace = taskService.getById(taskId)
                    ?: return@doInNewTxn rowVanishedMidBatch(taskId, handler, batch.toInclusive)
                rowStatus = afterRace.status
                statusForWrite = afterRace.status
            }

            if (!updated) {
                // The row kept changing status underneath every attempt - not a scenario this
                // engine's own concurrency model produces (the only other writer is an
                // administrator's cancel, and a final status never moves again). Logged here with
                // the cursor this batch was trying to persist - detail the thrown exception's
                // message does not carry, since ed_batch_task.__error is free text with no room for
                // it - and then thrown, so this batch's transaction rolls back instead of
                // committing the handler's domain work while silently losing its own bookkeeping.
                log.error {
                    "Batch task $taskId ('${handler.getType()}') could not persist batch progress " +
                        "up to cursor ${batch.toInclusive} after $attempt compare-and-set attempts " +
                        "- the row kept changing status underneath it. Rolling this batch back " +
                        "instead of committing its progress; the retry machinery takes over from " +
                        "the exception below"
                }
                error(
                    "Batch task $taskId: compare-and-set on batch progress did not land after " +
                        "$attempt attempts - the row's status kept changing underneath it"
                )
            }

            if (rowStatus.isFinal()) {
                // Cancelled (or otherwise finalised) while processBatch ran, or already final at
                // the read above. This batch's progress is durably persisted either way (the CAS
                // above just succeeded), but this call must not report Processed: that would let
                // the loop above keep going as if nothing happened, once per remaining batch of
                // this pass, instead of routing through onCancel now.
                return@doInNewTxn BatchOutcome.Stopped(rowStatus)
            }

            BatchOutcome.Processed
        }
    }

    /**
     * The row disappeared between `processBatch` and the cursor write. The handler's own work has
     * already been done inside this transaction and the cursor cannot be persisted against a row
     * that no longer exists, so this batch ends silently as far as the queue is concerned - which
     * is exactly why it is worth a log line naming the cursor that was lost. Nothing else records
     * it: the transaction commits normally and `drainSchemaOnce` only logs what escaped.
     */
    private fun rowVanishedMidBatch(
        taskId: Long,
        handler: DbBatchTaskHandler,
        cursor: Long
    ): BatchOutcome {
        log.warn {
            "Batch task $taskId ('${handler.getType()}') disappeared while its batch was running. " +
                "The batch's own work is committed, but the cursor $cursor was not persisted - a " +
                "restart of an equivalent task would offer those ids to the handler again"
        }
        return BatchOutcome.Stopped(null)
    }

    /**
     * The failure bookkeeping.
     *
     * **This runs in a different transaction from the batch it describes** - the batch transaction
     * has already rolled back by the time we get here. Written inside the batch, the increment
     * would roll back together with the work it counts: `errorCount` would stay 0, the backoff
     * would never grow, and a permanently broken batch would retry at full speed forever.
     *
     * The engine then returns instead of sleeping: the backoff is minutes, and a tick
     * holds a distributed lock, so sleeping it out here would block every other instance for the
     * whole window. The task simply becomes drainable again once `nextAttemptAt` passes.
     */
    private fun registerBatchFailure(
        schemaCtx: DbSchemaContext,
        taskId: Long,
        phase: String,
        error: Throwable
    ): DbBatchTaskStatus? {

        val taskService = taskServiceProvider(schemaCtx)

        val status = TxnContext.doInNewTxn {
            var fresh = taskService.getById(taskId) ?: return@doInNewTxn null
            if (fresh.status.isFinal()) {
                logAlreadyFinal(taskId, fresh.handler, fresh.status, phase, error)
                return@doInNewTxn fresh.status
            }
            val errorCount = fresh.errorCount + 1
            val maxAttempts = dataSourceCtx.props.batch.maxAttempts
            val giveUp = maxAttempts > 0 && errorCount >= maxAttempts

            // Logged BEFORE the write, not after it. The most likely reason a batch failed at all
            // is that the database is unavailable - which is also the most likely reason the write
            // below throws - and `drainSchemaOnce` only logs what escapes it. Written the other way
            // round, the exception that started all this would be replaced by the bookkeeping
            // write's own failure and lost.
            logFailure(taskId, fresh.handler, phase, errorCount, giveUp, error)

            var attempt = 0
            while (attempt < MAX_PROGRESS_WRITE_ATTEMPTS) {
                attempt++
                val newStatus = if (giveUp) DbBatchTaskStatus.FAILED else fresh.status
                val now = Instant.now()
                // The `isFinal` check above and this write are two separate statements, so a cancel
                // can still commit between them - the compare-and-set is what makes that gap
                // harmless, exactly as in every other writer of this row.
                val updated = taskService.saveIfStatusMatches(
                    taskId,
                    fresh.status,
                    mapOf(
                        DbBatchTaskEntity.STATUS to newStatus.name,
                        DbBatchTaskEntity.ERROR to describeError(error),
                        DbBatchTaskEntity.ERROR_COUNT to errorCount,
                        DbBatchTaskEntity.NEXT_ATTEMPT_AT to now.plus(backoffDelay(errorCount)),
                        DbBatchTaskEntity.FINISHED to if (giveUp) now else fresh.finished
                    )
                )
                if (updated) {
                    return@doInNewTxn newStatus
                }
                val afterRace = taskService.getById(taskId) ?: return@doInNewTxn null
                if (afterRace.status.isFinal()) {
                    // an administrator cancelled while this failure was being recorded: the cancel
                    // wins, nothing is retried, and the failure is still reported
                    logAlreadyFinal(taskId, afterRace.handler, afterRace.status, phase, error)
                    return@doInNewTxn afterRace.status
                }
                fresh = afterRace
            }
            error(
                "Batch task $taskId: compare-and-set on the failure bookkeeping did not land " +
                    "after $attempt attempts - the row's status kept changing underneath it"
            )
        }
        return status
    }

    /**
     * Cancelled (or finished by another instance) while this run was failing: the failure of work
     * nobody wants any more must not reopen or re-fail the row.
     *
     * It must still be *visible*, though. This is the only place such an exception is ever seen -
     * the caller has already unwound and nothing above logs it - so a handler whose onCancel
     * cleanup throws would otherwise disappear without a trace in any log. A silent failure is
     * precisely what this project's plan-B story cannot afford, so it is reported here even though
     * nothing is retried.
     */
    private fun logAlreadyFinal(
        taskId: Long,
        handlerType: String,
        status: DbBatchTaskStatus,
        phase: String,
        error: Throwable
    ) {
        log.error(error) {
            "Batch task $taskId ('$handlerType') failed during $phase, but its row is " +
                "already $status. Nothing will be retried; this is the only record " +
                "of the failure"
        }
    }

    private fun logFailure(
        taskId: Long,
        handlerType: String,
        phase: String,
        errorCount: Int,
        giveUp: Boolean,
        error: Throwable
    ) {
        // named for the phase that actually failed: "failed a batch" was a lie for a prepare or an
        // onFinish, and the phase is the first thing anyone diagnosing this wants to know
        val message = "Batch task $taskId ('$handlerType') failed during $phase. " +
            "Consecutive failures: $errorCount" +
            if (giveUp) ", giving up: status is FAILED" else ""
        // an endlessly retrying task has to be visible to monitoring, not buried in WARN noise
        if (giveUp || errorCount >= dataSourceCtx.props.batch.retryErrorEscalationAfter) {
            log.error(error) { message }
        } else {
            log.warn(error) { message }
        }
    }

    /**
     * The batch loop stopped because the re-read found a final status. A cancel is the case the
     * handler has to hear about.
     */
    private fun finishStoppedRun(
        schemaCtx: DbSchemaContext,
        handler: DbBatchTaskHandler,
        taskId: Long,
        status: DbBatchTaskStatus?
    ): DbBatchTaskStatus? {
        invokeOnCancelIfCancelled(schemaCtx, handler, taskId, status)
        return status
    }

    private fun invokeOnCancelIfCancelled(
        schemaCtx: DbSchemaContext,
        handler: DbBatchTaskHandler,
        taskId: Long,
        status: DbBatchTaskStatus?
    ) {
        if (status != DbBatchTaskStatus.CANCELLED) {
            return
        }
        val taskService = taskServiceProvider(schemaCtx)
        TxnContext.doInNewTxn {
            val fresh = taskService.getById(taskId) ?: return@doInNewTxn
            val ctx = DbBatchTaskContext(fresh, schemaCtx)
            handler.onCancel(ctx)
            // The status stays whatever the row says - onCancel is a notification, not a
            // transition - so this write names only the three counters and is conditioned on the
            // status read above. onCancel is another arbitrarily long handler callback, and a
            // whole-row `save` here would push the pre-callback snapshot of every other column
            // (the cursor above all) back over anything committed in the meantime.
            var expected = fresh.status
            var attempt = 0
            while (attempt < MAX_PROGRESS_WRITE_ATTEMPTS) {
                attempt++
                val updated = taskService.saveIfStatusMatches(
                    taskId,
                    expected,
                    mapOf(
                        DbBatchTaskEntity.PROCESSED to ctx.processed,
                        DbBatchTaskEntity.SKIPPED to ctx.skipped,
                        DbBatchTaskEntity.FAILED to ctx.failed
                    )
                )
                if (updated) {
                    return@doInNewTxn
                }
                expected = (taskService.getById(taskId) ?: return@doInNewTxn).status
            }
            // Not thrown: the task is already cancelled, nothing will retry it, and failing the
            // run over three counters would replace a cancelled task with a spurious error entry.
            log.error {
                "Batch task $taskId ('${handler.getType()}') could not persist the counters its " +
                    "onCancel produced after $attempt compare-and-set attempts. The task stays " +
                    "cancelled; only processed/skipped/failed may be stale"
            }
        }
    }

    /**
     * Normal completion.
     */
    private fun finishTask(
        schemaCtx: DbSchemaContext,
        handler: DbBatchTaskHandler,
        taskId: Long
    ): DbBatchTaskStatus? {
        val taskService = taskServiceProvider(schemaCtx)
        return TxnContext.doInNewTxn {
            var fresh = taskService.getById(taskId) ?: return@doInNewTxn null
            if (fresh.status.isFinal()) {
                return@doInNewTxn fresh.status
            }
            val ctx = DbBatchTaskContext(fresh, schemaCtx)
            handler.onFinish(ctx)
            // onFinish is an arbitrarily long handler callback, so DONE is published through the
            // compare-and-set too: a cancel committed while it ran must not be overwritten with a
            // cheerful DONE. If the compare-and-set loses, the row's real status is returned and
            // runPasses routes it through onCancel.
            var attempt = 0
            while (attempt < MAX_PROGRESS_WRITE_ATTEMPTS) {
                attempt++
                val updated = taskService.saveIfStatusMatches(
                    taskId,
                    fresh.status,
                    mapOf(
                        DbBatchTaskEntity.STATUS to DbBatchTaskStatus.DONE.name,
                        DbBatchTaskEntity.FINISHED to Instant.now(),
                        DbBatchTaskEntity.PROCESSED to ctx.processed,
                        DbBatchTaskEntity.SKIPPED to ctx.skipped,
                        DbBatchTaskEntity.FAILED to ctx.failed,
                        // as in runOneBatch: the counters are reset, the diagnosis is kept
                        DbBatchTaskEntity.ERROR_COUNT to 0,
                        DbBatchTaskEntity.NEXT_ATTEMPT_AT to Instant.EPOCH
                    )
                )
                if (updated) {
                    return@doInNewTxn DbBatchTaskStatus.DONE
                }
                val afterRace = taskService.getById(taskId) ?: return@doInNewTxn null
                if (afterRace.status.isFinal()) {
                    return@doInNewTxn afterRace.status
                }
                fresh = afterRace
            }
            error(
                "Batch task $taskId: compare-and-set on completion did not land after $attempt " +
                    "attempts - the row's status kept changing underneath it"
            )
        }
    }

    /**
     * Is the task's target table something this schema actually has?
     *
     * A table that does not exist has no columns, and [readMaxId] would answer 0 for it - which
     * would end the loop before the first batch and mark the task DONE with no error and no log.
     * An operator who typed the table name wrong would read that as "migration succeeded". A table
     * that exists and is genuinely empty is a different thing and *is* a legitimate DONE; only the
     * "no columns at all" case is refused here, the same way design point 1 refuses an unknown
     * handler: WARN, leave the row alone.
     */
    private fun isTargetTableKnown(schemaCtx: DbSchemaContext, table: String): Boolean {
        val dataService = getMaxIdReader(schemaCtx, table)
        val exists = TxnContext.doInNewTxn(readOnly = true) { dataService.isTableExists() }
        if (!exists) {
            // the cached empty column set would otherwise outlive the table being created a moment
            // later, and a task queued slightly too early would be refused for the process lifetime
            dataService.resetColumnsCache()
        }
        return exists
    }

    /**
     * The table's current maximum id, `find` with a descending sort on `id` and page
     * [DbFindPage.FIRST]. Read raw, so nothing here depends on the table being mappable to an
     * entity, and 0 for an empty table - which simply ends the loop.
     */
    private fun readMaxId(schemaCtx: DbSchemaContext, table: String): Long {
        val dataService = getMaxIdReader(schemaCtx, table)
        return TxnContext.doInNewTxn(readOnly = true) {
            dataService.findRaw(
                Predicates.alwaysTrue(),
                listOf(DbFindSort(DbEntity.ID, false)),
                DbFindPage.FIRST,
                emptyList(),
                emptyList(),
                emptyList(),
                false
            ).entities.firstOrNull()?.get(DbEntity.ID) as? Long ?: 0L
        }
    }

    private fun getMaxIdReader(schemaCtx: DbSchemaContext, table: String): DbDataService<DbEntity> {
        return maxIdReaders.computeIfAbsent(schemaCtx.getTableRef(table)) {
            DbDataServiceImpl(
                DbEntity::class.java,
                DbDataServiceConfig.create {
                    withTable(table)
                },
                schemaCtx
            )
        }
    }

    /**
     * WARN, but only the first time this process sees [key]. The two branches that use it are
     * re-evaluated on every drain tick for as long as the offending row exists, so an unthrottled
     * log line there never stops.
     */
    private fun logOnce(key: String, message: () -> String) {
        if (oneTimeWarnings.size >= MAX_ONE_TIME_WARNINGS) {
            oneTimeWarnings.clear()
        }
        if (oneTimeWarnings.add(key)) {
            log.warn(message)
        }
    }

    private fun pauseBetweenBatches() {
        val batchPauseMs = dataSourceCtx.props.batch.batchPause.toMillis()
        if (batchPauseMs <= 0) {
            return
        }
        try {
            Thread.sleep(batchPauseMs)
        } catch (e: InterruptedException) {
            Thread.currentThread().interrupt()
            throw e
        }
    }

    /**
     * `min(retryInitialDelay * 2^(errorCount - 1), retryMaxDelay)`. The shift is clamped so that a
     * task which has failed thousands of times still computes a delay instead of overflowing.
     */
    private fun backoffDelay(errorCount: Int): Duration {
        val retryInitialDelay = dataSourceCtx.props.batch.retryInitialDelay
        val retryMaxDelay = dataSourceCtx.props.batch.retryMaxDelay
        val shift = (errorCount - 1).coerceIn(0, 32)
        val delayMs = retryInitialDelay.toMillis() shl shift
        val maxDelayMs = retryMaxDelay.toMillis()
        return if (delayMs <= 0 || delayMs >= maxDelayMs) {
            retryMaxDelay
        } else {
            Duration.ofMillis(delayMs)
        }
    }

    private fun describeError(error: Throwable): String {
        val message = "${error::class.java.simpleName}: ${error.message ?: ""}"
        return if (message.length > MAX_ERROR_LENGTH) {
            message.substring(0, MAX_ERROR_LENGTH)
        } else {
            message
        }
    }

    /**
     * Which stage of a run is currently executing, so that a failure can be reported against the
     * phase that actually failed. A plain mutable holder rather than a return value because the
     * whole point is to survive the stack unwinding that brought the exception to the handler.
     */
    private class PhaseTracker(var current: String = PREPARE) {
        companion object {
            const val PREPARE = "prepare"
            const val READ_MAX_ID = "reading the target table's maximum id"
            const val BATCH = "a batch"
            const val CANCEL = "onCancel"
            const val FINISH = "onFinish"
        }
    }

    /**
     * What one [runTaskAttempt] did. [attempted] is what [drainSchemaOnce] counts: it is true
     * exactly when this instance took the task on and ran (or tried to run) it, which is not the
     * same question as whether [status] came back non-null.
     */
    private class RunResult(val status: DbBatchTaskStatus?, val attempted: Boolean) {
        companion object {
            fun attempted(status: DbBatchTaskStatus?) = RunResult(status, true)
            fun notAttempted(status: DbBatchTaskStatus? = null) = RunResult(status, false)
        }
    }

    private sealed interface BatchOutcome {

        /**
         * The re-read found a final status (or the row is gone) - the run is over.
         */
        class Stopped(val status: DbBatchTaskStatus?) : BatchOutcome

        /**
         * The cursor has passed the maximum id this pass was given.
         */
        object EndOfWindow : BatchOutcome

        /**
         * A batch ran and committed.
         */
        object Processed : BatchOutcome
    }
}
