package ru.citeck.ecos.data.sql.test.records

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatCode
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import ru.citeck.ecos.commons.data.ObjectData
import ru.citeck.ecos.data.sql.batch.DbBatchTaskAdminDao
import ru.citeck.ecos.data.sql.batch.DbBatchTaskBatch
import ru.citeck.ecos.data.sql.batch.DbBatchTaskContext
import ru.citeck.ecos.data.sql.batch.DbBatchTaskDto
import ru.citeck.ecos.data.sql.batch.DbBatchTaskEngine
import ru.citeck.ecos.data.sql.batch.DbBatchTaskEntity
import ru.citeck.ecos.data.sql.batch.DbBatchTaskHandler
import ru.citeck.ecos.data.sql.batch.DbBatchTaskService
import ru.citeck.ecos.data.sql.batch.DbBatchTaskStatus
import ru.citeck.ecos.data.sql.context.DbSchemaContext
import ru.citeck.ecos.data.sql.props.DbEcosDataProps
import ru.citeck.ecos.data.sql.repo.entity.DbEntity
import ru.citeck.ecos.data.sql.service.DbDataServiceConfig
import ru.citeck.ecos.data.sql.service.DbDataServiceImpl
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.txn.lib.TxnContext
import ru.citeck.ecos.webapp.api.entity.EntityRef
import java.io.ByteArrayOutputStream
import java.io.PrintStream
import java.time.Duration
import java.time.Instant

class DbBatchTaskEngineTest : DbRecordsTestBase() {

    @Test
    fun registryIsOpenAndRejectsDuplicateTypesTest() {

        registerAtts(listOf())

        val registry = getTableCtx().getSchemaCtx().dataSourceCtx.batchTaskHandlers
        val handler = DbBatchTaskTestHandler("registry-test-handler")

        registry.register(handler)

        assertThat(registry.getHandler("registry-test-handler")).isSameAs(handler)
        assertThat(registry.getHandler("nobody-registered-this")).isNull()

        assertThatThrownBy { registry.register(DbBatchTaskTestHandler("registry-test-handler")) }
            .describedAs(
                "a silently replaced handler is how a task ends up processed by the wrong code; " +
                    "the registry is open but not overwritable"
            )
            .isInstanceOf(IllegalStateException::class.java)
            .hasMessageContaining("registry-test-handler")
    }

    @Test
    fun theCursorAdvancesWithTheBatchAndAResumeDoesNotReprocessTest() {

        registerAtts(listOf(AttributeDef.create { withId("someAtt") }))
        repeat(7) { createRecord("someAtt" to "value-$it") }

        val handler = DbBatchTaskTestHandler("resume-handler", recordsPerBatch = 2)
        val schemaCtx = getTableCtx().getSchemaCtx()
        schemaCtx.dataSourceCtx.batchTaskHandlers.register(handler)

        val task = TxnContext.doInTxn {
            schemaCtx.batchTaskService.queue("resume-handler", tableRef.table, ObjectData.create())
        }

        val engine = DbBatchTaskEngine(schemaCtx.dataSourceCtx)
        engine.runTask(schemaCtx, task.id)

        // Continuity *between* windows is not asserted with a zipWithNext loop here: the default
        // batch size is 500 and every wave in this test is smaller, so each run produces exactly
        // one window and such a loop would iterate zero times - a dead assertion that reads like
        // coverage it does not provide. What continuity there is to check across a resume is
        // pinned explicitly below, by comparing the second run's first window against the cursor
        // the first run persisted. Multi-window continuity within a single run is covered
        // separately by DbBatchTaskEngineBatchSizePropsTest.theBatchSizeComesFromPropsTest, which
        // configures a batch size of 3 and asserts the second window picks up where the first left
        // off.
        val firstRunWindows = handler.seenBatches.toList()
        assertThat(firstRunWindows)
            .describedAs("a task over a non-empty table must offer the handler at least one window")
            .isNotEmpty
        assertThat(firstRunWindows.first().fromExclusive)
            .describedAs("the first window of a fresh task starts at the initial cursor")
            .isEqualTo(0)

        val afterFirstRun = TxnContext.doInTxn { schemaCtx.batchTaskService.getById(task.id) }!!
        assertThat(afterFirstRun.status).isEqualTo(DbBatchTaskStatus.DONE)
        assertThat(afterFirstRun.cursor)
            .describedAs("the persisted cursor is the end of the last window the handler saw")
            .isEqualTo(firstRunWindows.last().toInclusive)
        assertThat(handler.finishCalls.get()).isEqualTo(1)
        assertThat(handler.cancelCalls.get()).isEqualTo(0)

        val maxIdAfterFirstWave = maxRecordId()
        assertThat(afterFirstRun.cursor)
            .describedAs(
                "__cursor is 'the id of the last processed record', not an arithmetic " +
                    "offset. A cursor parked past the largest id that exists puts every id between " +
                    "them in front of no future window, and restart keeps the cursor, so those rows " +
                    "are unreachable without a manual UPDATE"
            )
            .isLessThanOrEqualTo(maxIdAfterFirstWave)

        // the resume: more rows arrive after the task finished, and an administrator restarts it
        repeat(5) { createRecord("someAtt" to "second-wave-$it") }
        assertThat(TxnContext.doInTxn { schemaCtx.batchTaskService.restart(task.id) }).isTrue

        engine.runTask(schemaCtx, task.id)

        val secondRunWindows = handler.seenBatches.toList().drop(firstRunWindows.size)
        assertThat(secondRunWindows)
            .describedAs(
                "a restart resumes rather than rewinds, so the rows created after the first run " +
                    "must still be offered to the handler - zero windows here means they were lost"
            )
            .isNotEmpty
        assertThat(secondRunWindows.first().fromExclusive)
            .describedAs(
                "the resume continues exactly at the cursor the first run persisted: earlier would " +
                    "reprocess rows, later would skip them"
            )
            .isEqualTo(afterFirstRun.cursor)

        val finished = TxnContext.doInTxn { schemaCtx.batchTaskService.getById(task.id) }!!
        assertThat(finished.status).isEqualTo(DbBatchTaskStatus.DONE)
        assertThat(handler.finishCalls.get()).isEqualTo(2)
        assertThat(finished.cursor)
            .describedAs("every row that existed when the second run ended has been walked past")
            .isGreaterThanOrEqualTo(maxRecordId())
    }

    /**
     * The largest id in the records table, read independently of the engine: [findAll] plus a
     * Kotlin `maxOf`, so this cannot agree with a bug in the engine's own descending-sort query.
     */
    private fun maxRecordId(): Long {
        return TxnContext.doInTxn(readOnly = true) {
            DbDataServiceImpl(
                DbEntity::class.java,
                DbDataServiceConfig.create { withTable(tableRef.table) },
                getTableCtx().getSchemaCtx()
            ).findAll().maxOf { it.id }
        }
    }

    @Test
    fun aCancelledTaskStopsBeforeTheNextBatchTest() {

        registerAtts(listOf(AttributeDef.create { withId("someAtt") }))
        repeat(20) { createRecord("someAtt" to "value-$it") }

        val schemaCtx = getTableCtx().getSchemaCtx()
        val handler = DbBatchTaskTestHandler("cancel-handler", recordsPerBatch = 1)
        schemaCtx.dataSourceCtx.batchTaskHandlers.register(handler)

        val task = TxnContext.doInTxn {
            schemaCtx.batchTaskService.queue("cancel-handler", tableRef.table, ObjectData.create())
        }

        // cancel from "another instance" in the middle of the first batch
        handler.beforeBatch = { ctx, _ ->
            if (handler.seenBatches.isEmpty()) {
                TxnContext.doInNewTxn { schemaCtx.batchTaskService.cancel(ctx.task.id) }
            }
        }

        val engine = DbBatchTaskEngine(schemaCtx.dataSourceCtx)
        engine.runTask(schemaCtx, task.id)

        // 20 records fit in a single window, so "one batch" is arithmetic, not evidence of
        // cancellation - it is the three assertions below that discriminate.
        assertThat(handler.seenBatches).hasSize(1)
        assertThat(handler.cancelCalls.get())
            .describedAs(
                "the status is re-read before every batch, so the cancel that landed during the " +
                    "batch already in flight is seen before the next one is offered: the handler " +
                    "is told the task was cancelled"
            )
            .isEqualTo(1)
        assertThat(handler.finishCalls.get())
            .describedAs("a cancelled task must not also be reported as finished")
            .isEqualTo(0)
        assertThat(TxnContext.doInTxn { schemaCtx.batchTaskService.getById(task.id) }!!.status)
            .isEqualTo(DbBatchTaskStatus.CANCELLED)
    }

    @Test
    fun aFailedBatchBacksOffKeepsTheTaskAliveAndDoesNotLoseTheCursorTest() {

        registerAtts(listOf(AttributeDef.create { withId("someAtt") }))
        repeat(5) { createRecord("someAtt" to "value-$it") }

        val schemaCtx = getTableCtx().getSchemaCtx()
        // fails far more often than any tick will try, so the task must still be alive afterwards
        val handler = DbBatchTaskTestHandler("failing-handler", failBatchesUntilAttempt = 99, recordsPerBatch = 1)
        schemaCtx.dataSourceCtx.batchTaskHandlers.register(handler)

        val task = TxnContext.doInTxn {
            schemaCtx.batchTaskService.queue("failing-handler", tableRef.table, ObjectData.create())
        }

        val before = Instant.now()
        DbBatchTaskEngine(schemaCtx.dataSourceCtx).runTask(schemaCtx, task.id)

        val afterFirst = TxnContext.doInTxn { schemaCtx.batchTaskService.getById(task.id) }!!
        assertThat(afterFirst.status)
            .describedAs(
                "the default is unlimited retries: a transient fault must not leave a half-migrated " +
                    "column waiting for a human to notice"
            )
            .isNotEqualTo(DbBatchTaskStatus.FAILED)
        assertThat(afterFirst.errorCount)
            .describedAs(
                "the attempt counter must survive the batch rollback - written inside the batch " +
                    "transaction it would roll back too, and the backoff would never grow"
            )
            .isEqualTo(1)
        assertThat(afterFirst.error).contains("injected batch failure")
        assertThat(afterFirst.cursor)
            .describedAs("no batch ever committed, so the cursor never moved")
            .isEqualTo(0)
        assertThat(afterFirst.nextAttemptAt)
            .describedAs("a failed batch schedules its own next attempt instead of waiting for an admin")
            .isAfter(before)

        assertThat(handler.seenBatches)
            .describedAs(
                "the tick ends on the failed batch and releases the lock - it must not sleep the " +
                    "backoff out while holding it"
            )
            .hasSize(1)

        assertThat(TxnContext.doInTxn { schemaCtx.batchTaskService.findActive(Instant.now()) }.map { it.id })
            .describedAs("while inside its backoff window the task is not drainable")
            .doesNotContain(task.id)
        assertThat(
            TxnContext.doInTxn { schemaCtx.batchTaskService.findActive(Instant.now().plus(Duration.ofDays(1))) }
                .map { it.id }
        )
            .describedAs("once the window has passed it comes back on its own")
            .contains(task.id)
    }

    @Test
    fun aFailedBatchBacksOffAndThenRecoversOnItsOwnTest() {

        registerAtts(listOf(AttributeDef.create { withId("someAtt") }))
        repeat(4) { createRecord("someAtt" to "value-$it") }

        val schemaCtx = getTableCtx().getSchemaCtx()
        // the first attempt fails, every later one succeeds - a transient fault
        val handler = DbBatchTaskTestHandler("recovering-handler", failBatchesUntilAttempt = 1, recordsPerBatch = 1)
        schemaCtx.dataSourceCtx.batchTaskHandlers.register(handler)

        val task = TxnContext.doInTxn {
            schemaCtx.batchTaskService.queue("recovering-handler", tableRef.table, ObjectData.create())
        }

        val engine = DbBatchTaskEngine(schemaCtx.dataSourceCtx)
        engine.runTask(schemaCtx, task.id)
        assertThat(TxnContext.doInTxn { schemaCtx.batchTaskService.getById(task.id) }!!.status)
            .describedAs("after the injected failure the task is merely waiting, not dead")
            .isEqualTo(DbBatchTaskStatus.RUNNING)

        // the next drain tick, once the backoff window has passed
        engine.runTask(schemaCtx, task.id)

        val finished = TxnContext.doInTxn { schemaCtx.batchTaskService.getById(task.id) }!!
        assertThat(finished.status)
            .describedAs("no administrator touched anything - the task came back by itself")
            .isEqualTo(DbBatchTaskStatus.DONE)
        assertThat(handler.finishCalls.get()).isEqualTo(1)
    }

    @Test
    fun aBrokenRecordIsCountedAndTheTaskKeepsGoingTest() {

        registerAtts(listOf(AttributeDef.create { withId("someAtt") }))
        repeat(6) { createRecord("someAtt" to "value-$it") }

        val schemaCtx = getTableCtx().getSchemaCtx()
        val handler = DbBatchTaskTestHandler(
            "broken-record-handler",
            recordsPerBatch = 2,
            failRecordIndexes = setOf(1)
        )
        schemaCtx.dataSourceCtx.batchTaskHandlers.register(handler)

        val task = TxnContext.doInTxn {
            schemaCtx.batchTaskService.queue("broken-record-handler", tableRef.table, ObjectData.create())
        }

        val engine = DbBatchTaskEngine(schemaCtx.dataSourceCtx)
        engine.runTask(schemaCtx, task.id)

        val finished = TxnContext.doInTxn { schemaCtx.batchTaskService.getById(task.id) }!!
        assertThat(finished.status)
            .describedAs("one bad record must not fail the task")
            .isEqualTo(DbBatchTaskStatus.DONE)
        assertThat(finished.failed).isEqualTo(1)
        assertThat(finished.processed).isGreaterThan(0)
    }

    @Test
    fun anUnknownHandlerTypeLeavesTheTaskAloneTest() {

        registerAtts(listOf())

        val schemaCtx = getTableCtx().getSchemaCtx()
        val task = TxnContext.doInTxn {
            schemaCtx.batchTaskService.queue("nobody-implements-this", tableRef.table, ObjectData.create())
        }

        val engine = DbBatchTaskEngine(schemaCtx.dataSourceCtx)
        assertThat(engine.runTask(schemaCtx, task.id))
            .describedAs("an instance that does not know the type reports that it did nothing")
            .isNull()

        assertThat(TxnContext.doInTxn { schemaCtx.batchTaskService.getById(task.id) }!!.status)
            .describedAs(
                "during a rolling upgrade one instance may not know a type yet; failing the task " +
                    "would destroy work another instance is able to do"
            )
            .isEqualTo(DbBatchTaskStatus.PENDING)
    }

    @Test
    fun aTaskAgainstAMissingTableIsLeftAloneRatherThanReportedFinishedTest() {

        registerAtts(listOf(AttributeDef.create { withId("someAtt") }))
        repeat(3) { createRecord("someAtt" to "value-$it") }

        val schemaCtx = getTableCtx().getSchemaCtx()
        val handler = DbBatchTaskTestHandler("missing-table-handler", recordsPerBatch = 1)
        schemaCtx.dataSourceCtx.batchTaskHandlers.register(handler)

        val task = TxnContext.doInTxn {
            schemaCtx.batchTaskService.queue(
                "missing-table-handler",
                tableRef.table + "-this-table-does-not-exist",
                ObjectData.create()
            )
        }

        val engine = DbBatchTaskEngine(schemaCtx.dataSourceCtx)
        assertThat(engine.runTask(schemaCtx, task.id))
            .describedAs("an instance that cannot find the target table reports that it did nothing")
            .isNull()

        val afterRun = TxnContext.doInTxn { schemaCtx.batchTaskService.getById(task.id) }!!
        assertThat(afterRun.status)
            .describedAs(
                "a table that does not exist has no rows to walk, so the loop would end before the " +
                    "first batch and report DONE - telling an operator who mistyped the table name " +
                    "that the migration succeeded. The row is left alone instead, exactly the way " +
                    "an unknown handler type leaves it alone"
            )
            .isEqualTo(DbBatchTaskStatus.PENDING)
        assertThat(handler.prepareCalls.get())
            .describedAs("the task is refused before it is taken on, so the handler is never started")
            .isEqualTo(0)
        assertThat(handler.seenBatches).isEmpty()
        assertThat(afterRun.errorCount)
            .describedAs("declining a task is not a failed attempt: no backoff, no attempt consumed")
            .isEqualTo(0)
    }

    @Test
    fun aTaskAgainstAnExistingButEmptyTableStillFinishesTest() {

        registerAtts(listOf(AttributeDef.create { withId("someAtt") }))
        // create one record to bring the physical table into existence, then truncate it: the
        // table exists and holds no rows, so readMaxId answers 0 - the very same answer it gives
        // for a table that does not exist. This is the half that makes the test above
        // discriminating: without it, an engine that declined every task would pass it too.
        createRecord("someAtt" to "seed")
        cleanRecords()

        val schemaCtx = getTableCtx().getSchemaCtx()
        val handler = DbBatchTaskTestHandler("empty-table-handler", recordsPerBatch = 1)
        schemaCtx.dataSourceCtx.batchTaskHandlers.register(handler)

        val task = TxnContext.doInTxn {
            schemaCtx.batchTaskService.queue("empty-table-handler", tableRef.table, ObjectData.create())
        }

        assertThat(DbBatchTaskEngine(schemaCtx.dataSourceCtx).runTask(schemaCtx, task.id))
            .describedAs(
                "an empty table is a legitimate, completely migrated table - the engine must tell " +
                    "it apart from a missing one by whether the table exists, not by its row count"
            )
            .isEqualTo(DbBatchTaskStatus.DONE)

        val finished = TxnContext.doInTxn { schemaCtx.batchTaskService.getById(task.id) }!!
        assertThat(finished.status).isEqualTo(DbBatchTaskStatus.DONE)
        assertThat(handler.prepareCalls.get())
            .describedAs("the task was taken on, unlike the missing-table case")
            .isEqualTo(1)
        assertThat(handler.finishCalls.get()).isEqualTo(1)
        assertThat(handler.seenBatches)
            .describedAs("there is nothing to walk, so no window is offered")
            .isEmpty()
    }

    @Test
    fun aPrepareThatThrowsIsRetriedInsteadOfEscapingTest() {

        registerAtts(listOf(AttributeDef.create { withId("someAtt") }))
        repeat(3) { createRecord("someAtt" to "value-$it") }

        val schemaCtx = getTableCtx().getSchemaCtx()
        val handler = DbBatchTaskTestHandler("broken-prepare-handler", recordsPerBatch = 1)
        // the realistic case is malformed __params: the handler cannot even start, so the failure
        // happens outside processBatch, where the engine's batch-level try does not reach
        handler.beforePrepare = { error("injected prepare failure") }
        schemaCtx.dataSourceCtx.batchTaskHandlers.register(handler)

        val task = TxnContext.doInTxn {
            schemaCtx.batchTaskService.queue("broken-prepare-handler", tableRef.table, ObjectData.create())
        }

        val before = Instant.now()
        val engine = DbBatchTaskEngine(schemaCtx.dataSourceCtx)
        assertThatCode { engine.runTask(schemaCtx, task.id) }
            .describedAs(
                "the exception must not escape runTask: propagating it would end the whole drain " +
                    "tick and starve every task queued behind this one, once per tick, for ever"
            )
            .doesNotThrowAnyException()

        val afterRun = TxnContext.doInTxn { schemaCtx.batchTaskService.getById(task.id) }!!
        assertThat(afterRun.errorCount)
            .describedAs("a failure outside processBatch is still a failed attempt and must be counted")
            .isEqualTo(1)
        assertThat(afterRun.error).contains("injected prepare failure")
        assertThat(afterRun.status.isFinal())
            .describedAs("the default attempt budget is unlimited, so the task stays alive and retries")
            .isFalse
        assertThat(afterRun.nextAttemptAt)
            .describedAs("it schedules its own next attempt instead of spinning at full speed")
            .isAfter(before)
        assertThat(handler.seenBatches)
            .describedAs("prepare never returned, so no batch was ever offered")
            .isEmpty()
    }

    @Test
    fun aFailingOnCancelIsStillReportedEvenThoughNothingIsRetriedTest() {

        registerAtts(listOf(AttributeDef.create { withId("someAtt") }))
        repeat(4) { createRecord("someAtt" to "value-$it") }

        val schemaCtx = getTableCtx().getSchemaCtx()
        // An anonymous handler rather than DbBatchTaskTestHandler: this test needs onCancel to
        // throw, and a throwing cleanup is not a behaviour the shared fixture should offer by
        // default to every future test that happens to register it.
        val handler = object : DbBatchTaskHandler {

            override fun getType(): String = "cancel-cleanup-fails-handler"

            override fun prepare(ctx: DbBatchTaskContext): Long? = null

            override fun processBatch(ctx: DbBatchTaskContext, batch: DbBatchTaskBatch) {
                // cancel from "another instance" while this batch is in flight, so that the
                // re-read before the next batch finds a final row and the engine calls onCancel
                TxnContext.doInNewTxn { schemaCtx.batchTaskService.cancel(ctx.task.id) }
            }

            override fun onCancel(ctx: DbBatchTaskContext) {
                error("injected onCancel cleanup failure")
            }
        }
        schemaCtx.dataSourceCtx.batchTaskHandlers.register(handler)

        val task = TxnContext.doInTxn {
            schemaCtx.batchTaskService.queue("cancel-cleanup-fails-handler", tableRef.table, ObjectData.create())
        }

        val engine = DbBatchTaskEngine(schemaCtx.dataSourceCtx)
        var logged = ""
        assertThatCode {
            logged = captureStderr { engine.runTask(schemaCtx, task.id) }
        }
            .describedAs("a handler whose cleanup fails must not take the drain tick down with it")
            .doesNotThrowAnyException()

        assertThat(logged)
            .describedAs(
                "this log line is the *only* trace an onCancel failure leaves anywhere: the row is " +
                    "already final, so nothing is retried, no counter moves and no status changes. " +
                    "Drop it and a handler whose cancellation cleanup is broken fails silently in " +
                    "production and in every test alike"
            )
            .contains("failed during onCancel, but its row is already CANCELLED")

        val afterRun = TxnContext.doInTxn { schemaCtx.batchTaskService.getById(task.id) }!!
        assertThat(afterRun.status)
            .describedAs("reporting the failure must not reopen a row an administrator cancelled")
            .isEqualTo(DbBatchTaskStatus.CANCELLED)
        assertThat(afterRun.errorCount)
            .describedAs(
                "nothing will retry a cancelled task, so the failure is reported rather than " +
                    "counted - an attempt budget must not be spent on work nobody wants any more"
            )
            .isEqualTo(0)
    }

    @Test
    fun aSecondDrainSkipsWhileTheFirstHoldsTheTableLockTest() {

        registerAtts(listOf(AttributeDef.create { withId("someAtt") }))
        repeat(4) { createRecord("someAtt" to "value-$it") }

        val schemaCtx = getTableCtx().getSchemaCtx()
        val lockApi = schemaCtx.webAppApi.getAppLockApi()
        val handler = DbBatchTaskTestHandler("lock-handler", recordsPerBatch = 1)
        schemaCtx.dataSourceCtx.batchTaskHandlers.register(handler)

        TxnContext.doInTxn {
            schemaCtx.batchTaskService.queue("lock-handler", tableRef.table, ObjectData.create())
        }

        val engine = DbBatchTaskEngine(schemaCtx.dataSourceCtx)
        val key = DbBatchTaskEngine.lockKey(schemaCtx.schema, tableRef.table)

        // hold the lock the way another instance would, then drain
        val lock = lockApi.getLock(key)
        assertThat(lock.acquire(Duration.ZERO)).isTrue()
        try {
            assertThat(engine.drainSchemaOnce(schemaCtx))
                .describedAs(
                    "with the table lock held elsewhere the drain must skip this tick, not queue " +
                        "up behind it - two instances both waiting is how a 10-second tick turns " +
                        "into a pile-up"
                )
                .isEqualTo(0)
            assertThat(handler.seenBatches).isEmpty()
        } finally {
            lock.release()
        }

        assertThat(engine.drainSchemaOnce(schemaCtx))
            .describedAs("once the lock is free the very next tick picks the task up")
            .isEqualTo(1)
        assertThat(handler.seenBatches).isNotEmpty
    }

    // Deliberately no "the lock is released after the task finishes" test here: release comes
    // from EcosLockApi.doInSyncOrSkip's own `finally`, which is platform code this test cannot get
    // wrong, and every attempt at asserting it discriminates nothing this engine controls - a
    // handler that throws inside processBatch is already caught and routed into the retry
    // bookkeeping before doInSyncOrSkip's lambda returns, so it exercises the exact same release
    // path as a normal run. A version of this test used to exist and passed even with the lock
    // removed entirely from drainSchemaOnce - it could not fail, which is worse than not having it.

    @Test
    fun drainOnceVisitsEveryKnownSchemaTest() {

        registerAtts(listOf(AttributeDef.create { withId("someAtt") }))
        createRecord("someAtt" to "value")

        val schemaCtx = getTableCtx().getSchemaCtx()
        val handler = DbBatchTaskTestHandler("drain-all-handler", recordsPerBatch = 1)
        schemaCtx.dataSourceCtx.batchTaskHandlers.register(handler)
        TxnContext.doInTxn {
            schemaCtx.batchTaskService.queue("drain-all-handler", tableRef.table, ObjectData.create())
        }

        assertThat(DbBatchTaskEngine(schemaCtx.dataSourceCtx).drainOnce())
            .describedAs("the data-source level drain must reach the schema the task was queued in")
            .isEqualTo(1)
    }

    @Test
    fun aCancelBetweenTheReadAndTheProgressWriteIsNotClobberedTest() {

        registerAtts(listOf(AttributeDef.create { withId("someAtt") }))
        createRecord("someAtt" to "value")

        val schemaCtx = getTableCtx().getSchemaCtx()
        val task = TxnContext.doInTxn {
            schemaCtx.batchTaskService.queue("cas-race-handler", tableRef.table, ObjectData.create())
        }

        // The exact gap runOneBatch's compare-and-set closes: a status is read (this is what the
        // engine's `atWriteTime.status` would be), "another instance" commits a cancel, and only
        // then does the batch attempt to write its progress conditioned on the status it read.
        val statusAtReadTime = task.status
        assertThat(TxnContext.doInNewTxn { schemaCtx.batchTaskService.cancel(task.id) })
            .isTrue()

        val updated = TxnContext.doInTxn {
            schemaCtx.batchTaskService.saveProgressIfStatusMatches(
                task.id,
                statusAtReadTime,
                500L,
                3L,
                0L,
                0L
            )
        }

        assertThat(updated)
            .describedAs(
                "the compare-and-set must see the row no longer has the status this write was " +
                    "conditioned on, and refuse rather than blindly apply the progress over the cancel"
            )
            .isFalse()

        val afterRace = TxnContext.doInTxn { schemaCtx.batchTaskService.getById(task.id) }!!
        assertThat(afterRace.status)
            .describedAs(
                "closing the race means a cancel that wins it is never silently reverted - the " +
                    "whole-row `save` this replaces could not tell the difference and would have " +
                    "written RUNNING (or whatever was read) right back over the cancel"
            )
            .isEqualTo(DbBatchTaskStatus.CANCELLED)
        assertThat(afterRace.cursor)
            .describedAs("a failed compare-and-set must not partially apply the progress it was conditioned on")
            .isEqualTo(0L)
    }

    /**
     * The engine-level pin for the cancel-clobber fix: [aCancelBetweenTheReadAndTheProgressWriteIsNotClobberedTest]
     * proves the compare-and-set primitive is correct on its own, but never enters
     * `DbBatchTaskEngine.runOneBatch` - a naive revert of the engine's write path back to the old
     * whole-row `save` would leave that test green. [CancellingBatchTaskService] injects a real,
     * independently-committed cancel from inside `saveProgressIfStatusMatches` itself - the exact
     * method the engine calls right before its progress write - so the cancel lands in the real gap
     * between the engine's re-read and its write, not merely in a hand-simulated one.
     */
    @Test
    fun aCancelInsideTheEngineSProgressWriteIsNotClobberedTest() {

        registerAtts(listOf(AttributeDef.create { withId("someAtt") }))
        createRecord("someAtt" to "value")

        val schemaCtx = getTableCtx().getSchemaCtx()
        val handler = DbBatchTaskTestHandler("engine-cas-race-handler", recordsPerBatch = 1)
        schemaCtx.dataSourceCtx.batchTaskHandlers.register(handler)

        val task = TxnContext.doInTxn {
            schemaCtx.batchTaskService.queue("engine-cas-race-handler", tableRef.table, ObjectData.create())
        }

        val realService = schemaCtx.batchTaskService
        val decorator = CancellingBatchTaskService(schemaCtx) {
            TxnContext.doInNewTxn { realService.cancel(task.id) }
        }
        val engine = DbBatchTaskEngine(schemaCtx.dataSourceCtx) { decorator }
        engine.runTask(schemaCtx, task.id)

        val afterRun = TxnContext.doInTxn { realService.getById(task.id) }!!
        assertThat(afterRun.status)
            .describedAs(
                "the cancel committed inside the engine's own write call must not be silently " +
                    "reverted - this is the same failure aCancelBetweenTheReadAndTheProgressWriteIsNotClobberedTest " +
                    "pins on the primitive, now pinned on the engine's actual call site"
            )
            .isEqualTo(DbBatchTaskStatus.CANCELLED)
        assertThat(afterRun.processed)
            .describedAs(
                "the batch that was already in flight when the cancel landed did real work, and a " +
                    "cancel racing the write must not throw that work away"
            )
            .isEqualTo(1L)
        assertThat(handler.cancelCalls.get())
            .describedAs("a cancel that actually took effect must still reach onCancel")
            .isEqualTo(1)
    }

    /**
     * Subclasses [DbBatchTaskService] purely as a test seam (see the KDoc on the base class): runs
     * [onFirstCall] exactly once, synchronously and in its own committed transaction, before
     * delegating to the real compare-and-set. Used to land a cancel in the narrow window between
     * `DbBatchTaskEngine.runOneBatch`'s re-read and its write - the one gap nothing else in this
     * file can reach without modifying production code with a one-off hook.
     */
    private class CancellingBatchTaskService(
        schemaCtx: DbSchemaContext,
        /**
         * Which write to land the cancel in front of. `null` means the batch-progress write
         * ([saveProgressIfStatusMatches]); a column name means the next
         * [saveIfStatusMatches] whose `newValues` names that column - which is how a test picks out
         * one of the engine's several status-conditioned writers (only `registerBatchFailure`
         * writes `__error`, only `finishTask` publishes `DONE`, and so on) without a one-off hook in
         * production code.
         */
        private val triggerColumn: String? = null,
        private val onFirstCall: () -> Unit
    ) : DbBatchTaskService(schemaCtx) {

        private var called = false

        private fun fireOnce() {
            if (!called) {
                called = true
                onFirstCall()
            }
        }

        override fun saveProgressIfStatusMatches(
            id: Long,
            expectedStatus: DbBatchTaskStatus,
            cursor: Long,
            processed: Long,
            skipped: Long,
            failed: Long
        ): Boolean {
            if (triggerColumn == null) {
                fireOnce()
            }
            return super.saveProgressIfStatusMatches(id, expectedStatus, cursor, processed, skipped, failed)
        }

        override fun saveIfStatusMatches(
            id: Long,
            expectedStatus: DbBatchTaskStatus,
            newValues: Map<String, Any?>
        ): Boolean {
            if (triggerColumn != null && newValues.containsKey(triggerColumn)) {
                fireOnce()
            }
            return super.saveIfStatusMatches(id, expectedStatus, newValues)
        }
    }

    /**
     * The other half of [CancellingBatchTaskService]: instead of landing a cancel in front of a
     * write, it lands a **committed batch** in front of a *read*. That is the gap
     * [DbBatchTaskService.cancel] has to survive - it reads the row, and anything it then writes
     * back from that snapshot rewinds whatever committed in between.
     */
    private class ProgressInjectingBatchTaskService(
        schemaCtx: DbSchemaContext,
        private val onFirstRead: () -> Unit
    ) : DbBatchTaskService(schemaCtx) {

        private var called = false

        override fun getById(id: Long): DbBatchTaskDto? {
            val result = super.getById(id)
            if (!called) {
                called = true
                onFirstRead()
            }
            return result
        }
    }

    /**
     * What happens when the compare-and-set never lands within [DbBatchTaskEngine]'s retry
     * bound. [FlappingStatusBatchTaskService] forces every attempt to miss - deterministically,
     * not by timing - by changing the row's real status to something other than whatever the
     * engine is about to compare against, right before delegating to the real compare-and-set.
     * The bound must then roll the batch back rather than commit its progress while leaving the
     * row silent about the failure - `error`/`errorCount` are the only trace an administrator has.
     */
    @Test
    fun aCompareAndSetThatNeverLandsRollsBackInsteadOfSilentlyDroppingProgressTest() {

        registerAtts(listOf(AttributeDef.create { withId("someAtt") }))
        createRecord("someAtt" to "value")

        val schemaCtx = getTableCtx().getSchemaCtx()
        val handler = DbBatchTaskTestHandler("cas-exhausted-handler", recordsPerBatch = 1)
        schemaCtx.dataSourceCtx.batchTaskHandlers.register(handler)

        val task = TxnContext.doInTxn {
            schemaCtx.batchTaskService.queue("cas-exhausted-handler", tableRef.table, ObjectData.create())
        }

        val decorator = FlappingStatusBatchTaskService(schemaCtx)
        val engine = DbBatchTaskEngine(schemaCtx.dataSourceCtx) { decorator }
        engine.runTask(schemaCtx, task.id)

        val afterRun = TxnContext.doInTxn { schemaCtx.batchTaskService.getById(task.id) }!!
        assertThat(afterRun.errorCount)
            .describedAs("an exhausted compare-and-set must be recorded as a failure, not silently forgotten")
            .isGreaterThan(0)
        assertThat(afterRun.error).isNotEmpty
        assertThat(afterRun.cursor)
            .describedAs("a batch whose progress write never landed must roll back its cursor together with it")
            .isEqualTo(0L)
    }

    /**
     * Subclasses [DbBatchTaskService] purely as a test seam: forces every
     * [saveProgressIfStatusMatches] call to lose its compare-and-set, deterministically, by
     * changing the row's real status to something other than [expectedStatus] immediately before
     * delegating. Used to exhaust `DbBatchTaskEngine.runOneBatch`'s retry bound on demand instead
     * of relying on unlikely timing.
     */
    private class FlappingStatusBatchTaskService(
        schemaCtx: DbSchemaContext
    ) : DbBatchTaskService(schemaCtx) {

        private val flapTo = listOf(DbBatchTaskStatus.CANCELLED, DbBatchTaskStatus.PENDING)
        private var callIndex = 0

        override fun saveProgressIfStatusMatches(
            id: Long,
            expectedStatus: DbBatchTaskStatus,
            cursor: Long,
            processed: Long,
            skipped: Long,
            failed: Long
        ): Boolean {
            var forced = flapTo[callIndex % flapTo.size]
            if (forced == expectedStatus) {
                forced = flapTo[(callIndex + 1) % flapTo.size]
            }
            callIndex++
            val current = getById(id) ?: return false
            save(current.copy(status = forced))
            return super.saveProgressIfStatusMatches(id, expectedStatus, cursor, processed, skipped, failed)
        }
    }

    /**
     * F1/1: `prepare` is an arbitrarily long handler callback, and the write that publishes
     * `RUNNING` used to be an unconditional whole-row `save` built from the row as it looked
     * *before* it. A cancel committed inside that window was carried away, the administrator's stop
     * button did nothing, and the task walked the whole table.
     */
    @Test
    fun aCancelCommittedWhilePrepareRunsAbortsTheWholeRunTest() {

        registerAtts(listOf(AttributeDef.create { withId("someAtt") }))
        repeat(5) { createRecord("someAtt" to "value-$it") }

        val schemaCtx = getTableCtx().getSchemaCtx()
        val handler = DbBatchTaskTestHandler("cancel-in-prepare-handler", recordsPerBatch = 1)
        schemaCtx.dataSourceCtx.batchTaskHandlers.register(handler)

        val task = TxnContext.doInTxn {
            schemaCtx.batchTaskService.queue("cancel-in-prepare-handler", tableRef.table, ObjectData.create())
        }

        // "another instance" cancels while prepare() is still running
        handler.beforePrepare = { ctx ->
            TxnContext.doInNewTxn { schemaCtx.batchTaskService.cancel(ctx.task.id) }
        }

        val engine = DbBatchTaskEngine(schemaCtx.dataSourceCtx)
        engine.runTask(schemaCtx, task.id)

        val afterRun = TxnContext.doInTxn { schemaCtx.batchTaskService.getById(task.id) }!!
        assertThat(afterRun.status)
            .describedAs(
                "the cancel was committed before the engine published RUNNING, so it must stick - " +
                    "a whole-row save built from the pre-prepare snapshot would write RUNNING back " +
                    "over it and the task would run to completion"
            )
            .isEqualTo(DbBatchTaskStatus.CANCELLED)
        assertThat(handler.seenBatches)
            .describedAs(
                "aborting means aborting: not one batch of work nobody wants any more may be " +
                    "offered to the handler, let alone committed against live data"
            )
            .isEmpty()
        assertThat(handler.cancelCalls.get())
            .describedAs("a cancel honoured at prepare time must still reach onCancel")
            .isEqualTo(1)
        assertThat(handler.finishCalls.get()).isEqualTo(0)
    }

    /**
     * F1/2: the same gap, at the other end of the run. `onFinish` is a handler callback too, and
     * `DONE` used to be published by a whole-row `save` built from the row as it was before it.
     */
    @Test
    fun aCancelCommittedWhileOnFinishRunsIsNotOverwrittenWithDoneTest() {

        registerAtts(listOf(AttributeDef.create { withId("someAtt") }))
        repeat(3) { createRecord("someAtt" to "value-$it") }

        val schemaCtx = getTableCtx().getSchemaCtx()
        val handler = DbBatchTaskTestHandler("cancel-in-finish-handler", recordsPerBatch = 1)
        schemaCtx.dataSourceCtx.batchTaskHandlers.register(handler)

        val task = TxnContext.doInTxn {
            schemaCtx.batchTaskService.queue("cancel-in-finish-handler", tableRef.table, ObjectData.create())
        }

        handler.beforeFinish = { ctx ->
            TxnContext.doInNewTxn { schemaCtx.batchTaskService.cancel(ctx.task.id) }
        }

        DbBatchTaskEngine(schemaCtx.dataSourceCtx).runTask(schemaCtx, task.id)

        val afterRun = TxnContext.doInTxn { schemaCtx.batchTaskService.getById(task.id) }!!
        assertThat(afterRun.status)
            .describedAs(
                "an administrator who cancelled must not be told the task completed normally: the " +
                    "cancel was committed before DONE was published, so the row stays CANCELLED"
            )
            .isEqualTo(DbBatchTaskStatus.CANCELLED)
        assertThat(handler.finishCalls.get())
            .describedAs("onFinish did run - the cancel landed inside it, not before it")
            .isEqualTo(1)
        assertThat(handler.cancelCalls.get())
            .describedAs("the status that actually won is reported to the handler as well")
            .isEqualTo(1)
    }

    /**
     * F1/3: `registerBatchFailure` already re-reads the row and gives up if it is final - but the
     * read and the write are two statements, and a cancel committed between them was written back
     * to RUNNING by the whole-row save, resurrecting a task an administrator had stopped.
     */
    @Test
    fun aCancelInsideTheFailureBookkeepingIsNotRevertedToRunningTest() {

        registerAtts(listOf(AttributeDef.create { withId("someAtt") }))
        repeat(3) { createRecord("someAtt" to "value-$it") }

        val schemaCtx = getTableCtx().getSchemaCtx()
        val handler = DbBatchTaskTestHandler(
            "failure-race-handler",
            failBatchesUntilAttempt = 99,
            recordsPerBatch = 1
        )
        schemaCtx.dataSourceCtx.batchTaskHandlers.register(handler)

        val realService = schemaCtx.batchTaskService
        val task = TxnContext.doInTxn {
            realService.queue("failure-race-handler", tableRef.table, ObjectData.create())
        }

        // __error is written by registerBatchFailure and by nothing else in the engine, so this
        // lands the cancel in exactly that writer's window and in no other
        val decorator = CancellingBatchTaskService(schemaCtx, DbBatchTaskEntity.ERROR) {
            TxnContext.doInNewTxn { realService.cancel(task.id) }
        }
        DbBatchTaskEngine(schemaCtx.dataSourceCtx) { decorator }.runTask(schemaCtx, task.id)

        val afterRun = TxnContext.doInTxn { realService.getById(task.id) }!!
        assertThat(afterRun.status)
            .describedAs(
                "the failure bookkeeping must not resurrect a task cancelled between its own read " +
                    "and its own write - recording a retry for work nobody wants would put the row " +
                    "back in the drain queue for ever"
            )
            .isEqualTo(DbBatchTaskStatus.CANCELLED)
    }

    /**
     * F1/4: `cancel` reads the row and then writes. A batch that commits in between used to be
     * written back out of existence by the whole-row save - and because `restart` deliberately
     * keeps the cursor, the rows that batch had already converted became unreachable without a
     * manual `UPDATE`.
     */
    @Test
    fun cancelDoesNotRewindTheCursorOfABatchThatCommittedWhileItRanTest() {

        registerAtts(listOf())

        val schemaCtx = getTableCtx().getSchemaCtx()
        val realService = schemaCtx.batchTaskService
        val task = TxnContext.doInTxn {
            realService.queue("cursor-rewind-handler", tableRef.table, ObjectData.create())
        }

        val racing = ProgressInjectingBatchTaskService(schemaCtx) {
            TxnContext.doInNewTxn {
                realService.saveProgressIfStatusMatches(task.id, DbBatchTaskStatus.PENDING, 42L, 17L, 3L, 1L)
            }
        }

        assertThat(TxnContext.doInTxn { racing.cancel(task.id) })
            .describedAs("the task was active, so the cancel itself must still report that it did something")
            .isTrue()

        val afterRace = TxnContext.doInTxn { realService.getById(task.id) }!!
        assertThat(afterRace.status).isEqualTo(DbBatchTaskStatus.CANCELLED)
        assertThat(afterRace.cursor)
            .describedAs(
                "cancel writes only __status and __finished. Rewinding __cursor would hide every " +
                    "id up to 42 from every future window, and restart keeps the cursor, so an " +
                    "administrator could not get them back"
            )
            .isEqualTo(42L)
        assertThat(afterRace.processed).isEqualTo(17L)
        assertThat(afterRace.skipped).isEqualTo(3L)
        assertThat(afterRace.failed).isEqualTo(1L)
    }

    /**
     * F4: the public `runTask` used to call straight through to the run, with no lock at all, while
     * the drain wrapped the identical call in the per-table distributed lock. Two instances calling
     * it therefore ran the same task against the same live rows at the same time.
     */
    @Test
    fun runTaskDoesNothingWhileTheTablesLockIsHeldElsewhereTest() {

        registerAtts(listOf(AttributeDef.create { withId("someAtt") }))
        repeat(4) { createRecord("someAtt" to "value-$it") }

        val schemaCtx = getTableCtx().getSchemaCtx()
        val handler = DbBatchTaskTestHandler("run-task-lock-handler", recordsPerBatch = 1)
        schemaCtx.dataSourceCtx.batchTaskHandlers.register(handler)

        val task = TxnContext.doInTxn {
            schemaCtx.batchTaskService.queue("run-task-lock-handler", tableRef.table, ObjectData.create())
        }

        val engine = DbBatchTaskEngine(schemaCtx.dataSourceCtx)
        val lock = schemaCtx.webAppApi.getAppLockApi()
            .getLock(DbBatchTaskEngine.lockKey(schemaCtx.schema, tableRef.table))
        assertThat(lock.acquire(Duration.ZERO)).isTrue()
        try {
            assertThat(engine.runTask(schemaCtx, task.id))
                .describedAs(
                    "another instance holds the table lock, so this one must report that it took " +
                        "the task on at all"
                )
                .isNull()
            assertThat(handler.prepareCalls.get()).isEqualTo(0)
            assertThat(handler.seenBatches).isEmpty()
            assertThat(TxnContext.doInTxn { schemaCtx.batchTaskService.getById(task.id) }!!.status)
                .describedAs("nothing ran, so nothing was written either")
                .isEqualTo(DbBatchTaskStatus.PENDING)
        } finally {
            lock.release()
        }

        assertThat(engine.runTask(schemaCtx, task.id))
            .describedAs("once the lock is free the very same call runs the task to completion")
            .isEqualTo(DbBatchTaskStatus.DONE)
    }

    /**
     * F5: rows appended while a pass runs land **past** that pass's maximum id, so no window of
     * that pass can ever offer them. The old termination condition asked whether the last window
     * did any work, which answers a different question entirely - a pass whose tail happened to be
     * empty stopped and dropped everything appended behind it.
     */
    @Test
    fun aRowAppendedWhileThePassRunsIsStillProcessedTest() {

        registerAtts(listOf(AttributeDef.create { withId("someAtt") }))
        repeat(3) { createRecord("someAtt" to "value-$it") }

        val schemaCtx = getTableCtx().getSchemaCtx()
        // recordsPerBatch = 0: the handler reports no progress at all, which is exactly the case
        // the old "did the last window do work" condition got wrong
        val handler = DbBatchTaskTestHandler("appended-row-handler", recordsPerBatch = 0)
        schemaCtx.dataSourceCtx.batchTaskHandlers.register(handler)

        val task = TxnContext.doInTxn {
            schemaCtx.batchTaskService.queue("appended-row-handler", tableRef.table, ObjectData.create())
        }

        val idsBeforeTheRun = maxRecordId()
        handler.beforeBatch = { _, _ ->
            if (handler.seenBatches.isEmpty()) {
                TxnContext.doInNewTxn { createRecord("someAtt" to "appended-while-the-pass-ran") }
            }
        }

        DbBatchTaskEngine(schemaCtx.dataSourceCtx).runTask(schemaCtx, task.id)

        val appendedId = maxRecordId()
        assertThat(appendedId)
            .describedAs("the fixture really did append a row past the first pass's maximum id")
            .isGreaterThan(idsBeforeTheRun)
        assertThat(handler.seenBatches.map { it.toInclusive })
            .describedAs(
                "the appended row is past the first pass's maximum id, so it can only be reached " +
                    "by a second pass over a freshly read maximum - not by any window of the first"
            )
            .contains(appendedId)

        val finished = TxnContext.doInTxn { schemaCtx.batchTaskService.getById(task.id) }!!
        assertThat(finished.status).isEqualTo(DbBatchTaskStatus.DONE)
        assertThat(finished.cursor)
            .describedAs("a task reported DONE must have walked past every row that existed when it ended")
            .isGreaterThanOrEqualTo(appendedId)
    }

    /**
     * `__started` answers "when did this task first start", and an operator reads it to see how
     * long a long task has been going. Rewriting it on every tick turned it into "when did the last
     * tick pick it up", which is what `nextAttemptAt` already says.
     */
    @Test
    fun startedIsTheFirstStartNotTheLastTickTest() {

        registerAtts(listOf(AttributeDef.create { withId("someAtt") }))
        repeat(3) { createRecord("someAtt" to "value-$it") }

        val schemaCtx = getTableCtx().getSchemaCtx()
        // fails every batch, so the task stays alive and is picked up again by the next tick
        val handler = DbBatchTaskTestHandler("started-handler", failBatchesUntilAttempt = 99, recordsPerBatch = 1)
        schemaCtx.dataSourceCtx.batchTaskHandlers.register(handler)

        val task = TxnContext.doInTxn {
            schemaCtx.batchTaskService.queue("started-handler", tableRef.table, ObjectData.create())
        }

        val engine = DbBatchTaskEngine(schemaCtx.dataSourceCtx)
        engine.runTask(schemaCtx, task.id)
        val afterFirstTick = TxnContext.doInTxn { schemaCtx.batchTaskService.getById(task.id) }!!
        assertThat(afterFirstTick.started)
            .describedAs("the first tick is what starts the task")
            .isAfter(Instant.EPOCH)

        engine.runTask(schemaCtx, task.id)

        assertThat(TxnContext.doInTxn { schemaCtx.batchTaskService.getById(task.id) }!!.started)
            .describedAs("a later tick resumes a task that is already started; it does not restart it")
            .isEqualTo(afterFirstTick.started)
    }

    /**
     * F9: `skipped` is one of the three per-record outcomes and had no coverage at all - not in the
     * context, not through the compare-and-set that persists it, not in the DTO, not in the admin
     * view. This walks the whole round trip.
     */
    @Test
    fun skippedRecordsAreCountedPersistedAndVisibleToTheAdministratorTest() {

        registerAtts(listOf(AttributeDef.create { withId("someAtt") }))
        repeat(4) { createRecord("someAtt" to "value-$it") }

        val schemaCtx = getTableCtx().getSchemaCtx()
        val handler = DbBatchTaskTestHandler(
            "skipping-handler",
            recordsPerBatch = 4,
            skipRecordIndexes = setOf(0, 2)
        )
        schemaCtx.dataSourceCtx.batchTaskHandlers.register(handler)

        val adminDao = DbBatchTaskAdminDao(schemaCtx.batchTaskService)
        records.register(adminDao)

        val task = TxnContext.doInTxn {
            schemaCtx.batchTaskService.queue("skipping-handler", tableRef.table, ObjectData.create())
        }

        DbBatchTaskEngine(schemaCtx.dataSourceCtx).runTask(schemaCtx, task.id)

        val finished = TxnContext.doInTxn { schemaCtx.batchTaskService.getById(task.id) }!!
        assertThat(finished.status).isEqualTo(DbBatchTaskStatus.DONE)
        assertThat(finished.skipped)
            .describedAs("a record the handler decided needed no work is skipped, not processed")
            .isEqualTo(2)
        assertThat(finished.processed).isEqualTo(2)
        assertThat(finished.failed).isEqualTo(0)

        val atts = records.getAtts(
            EntityRef.create(APP_NAME, adminDao.getId(), task.id.toString()),
            listOf("skipped", "processed")
        )
        assertThat(atts["skipped"].asLong())
            .describedAs("the administrator's view is the only place these three counters are ever shown")
            .isEqualTo(2)
        assertThat(atts["processed"].asLong()).isEqualTo(2)
    }

    /**
     * F3: the drain task id used to be a constant while
     * [ru.citeck.ecos.data.sql.domain.DbDomainFactory.withDataSource] builds one engine per data
     * source, all of them scheduling onto the one shared scheduler - so the second `start()` threw
     * `Task '<id>' already registered and active` and the application could not start its drain.
     */
    @Test
    fun twoEnginesSharingOneSchedulerCanBothStartTest() {

        registerAtts(listOf())

        val schemaCtx = getTableCtx().getSchemaCtx()
        val first = DbBatchTaskEngine(schemaCtx.dataSourceCtx)
        val second = DbBatchTaskEngine(schemaCtx.dataSourceCtx)
        try {
            assertThatCode {
                first.start()
                second.start()
            }
                .describedAs(
                    "one engine per data source is what DbDomainFactory.withDataSource builds, and " +
                        "they all share one scheduler - a constant task id makes the second start throw"
                )
                .doesNotThrowAnyException()
        } finally {
            first.stop()
            second.stop()
        }
    }

    @Test
    fun anEngineCanBeStartedStoppedAndStartedAgainTest() {

        registerAtts(listOf())

        val engine = DbBatchTaskEngine(getTableCtx().getSchemaCtx().dataSourceCtx)
        try {
            assertThatCode {
                engine.start()
                engine.stop()
                engine.start()
            }
                .describedAs(
                    "a composition root that restarts - a retried bootstrap, a re-applied Spring " +
                        "context - must be able to schedule the drain again after stopping it"
                )
                .doesNotThrowAnyException()
            assertThatCode { engine.start() }
                .describedAs(
                    "and a second start with no stop in between stays a no-op rather than " +
                        "registering a second competing schedule"
                )
                .doesNotThrowAnyException()
        } finally {
            engine.stop()
        }
    }

    /**
     * Captures stderr around a block of code and returns what was logged to it. slf4j-simple (the
     * test binding) writes there by default and resolves `System.err` per write, so replacing it
     * for the duration of the block is enough; this project has no in-JVM log-capture utility.
     *
     * Copied from [DbLargeTableMigrationTest] rather than shared: one more duplicate is cheaper
     * here than a cross-file extraction, and the duplication is deliberate.
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
}

/**
 * `batchSize` from [DbEcosDataProps.BatchProps], kept out of [DbBatchTaskEngineTest] proper: `dataProps`
 * (see `DataMockFactory.dataProps`) is read by `DataMockFactory.setUp` while building
 * `DbDataSourceContext`, which runs in `@BeforeEach` - before any `@Test` method body gets a chance to
 * run. Setting it from inside a test method would therefore configure nothing: the context has
 * already been built with whatever `dataProps` held at construction time. `DbLargeTableMigrationTest`
 * and `DbRecordsAssocsCustomFullValuesLimitTest` both set it from an `init` block for exactly this
 * reason, and a dedicated class here is what lets this one test have a `batchSize` different from
 * every other test in [DbBatchTaskEngineTest], which relies on the 500 default.
 */
class DbBatchTaskEngineBatchSizePropsTest : DbRecordsTestBase() {

    init {
        dataProps = DbEcosDataProps(batch = DbEcosDataProps.BatchProps(batchSize = 3))
    }

    @Test
    fun theBatchSizeComesFromPropsTest() {

        registerAtts(listOf(AttributeDef.create { withId("someAtt") }))
        repeat(4) { createRecord("someAtt" to "value-$it") }

        val schemaCtx = getTableCtx().getSchemaCtx()
        val handler = DbBatchTaskTestHandler("props-handler", recordsPerBatch = 1)
        schemaCtx.dataSourceCtx.batchTaskHandlers.register(handler)
        val task = TxnContext.doInTxn {
            schemaCtx.batchTaskService.queue("props-handler", tableRef.table, ObjectData.create())
        }

        DbBatchTaskEngine(schemaCtx.dataSourceCtx).runTask(schemaCtx, task.id)

        assertThat(handler.seenBatches.first().toInclusive - handler.seenBatches.first().fromExclusive)
            .describedAs("the configured batch size is the width of the id window the handler sees")
            .isEqualTo(3)
        assertThat(handler.seenBatches)
            .describedAs(
                "4 records over a batch size of 3 must not fit in one window - this is the only " +
                    "test in the suite that drives the engine through more than one window in a " +
                    "single pass"
            )
            .hasSize(2)
        assertThat(handler.seenBatches[1].fromExclusive)
            .describedAs("the second window must pick up exactly where the first window's cursor left off")
            .isEqualTo(handler.seenBatches[0].toInclusive)
    }
}

/**
 * `maxPassesPerRun` from [DbEcosDataProps.BatchProps], in its own class for the same reason as
 * [DbBatchTaskEngineBatchSizePropsTest]: `dataProps` is read while the context is built, in
 * `@BeforeEach`, so it has to be set from an `init` block.
 */
class DbBatchTaskEngineMaxPassesPropsTest : DbRecordsTestBase() {

    init {
        dataProps = DbEcosDataProps(batch = DbEcosDataProps.BatchProps(maxPassesPerRun = 2))
    }

    /**
     * A pass ends at the maximum id it was given, and rows inserted while it ran land past that id,
     * so a table under steady insert load always has one more pass waiting. Without a ceiling the
     * run never returns - it keeps the table's distributed lock and, because a drain tick walks a
     * schema's tasks one after another, starves every other task of the data source for as long as
     * it lasts. The fixture below appends a row on each of the first few batches, so the run does
     * terminate either way: the point of the assertion is *which* way.
     */
    @Test
    fun aRunYieldsInsteadOfPassingForeverOverAGrowingTableTest() {

        registerAtts(listOf(AttributeDef.create { withId("someAtt") }))
        repeat(3) { createRecord("someAtt" to "value-$it") }

        val schemaCtx = getTableCtx().getSchemaCtx()
        val handler = DbBatchTaskTestHandler("growing-table-handler", recordsPerBatch = 1)
        schemaCtx.dataSourceCtx.batchTaskHandlers.register(handler)

        val task = TxnContext.doInTxn {
            schemaCtx.batchTaskService.queue("growing-table-handler", tableRef.table, ObjectData.create())
        }

        // bounded, so that a regression shows up as a wrong status rather than as a hanging build
        handler.beforeBatch = { _, _ ->
            if (handler.seenBatches.size < 5) {
                TxnContext.doInNewTxn { createRecord("someAtt" to "appended-during-the-run") }
            }
        }

        val status = DbBatchTaskEngine(schemaCtx.dataSourceCtx).runTask(schemaCtx, task.id)

        assertThat(status)
            .describedAs(
                "the table was still growing ahead of the cursor when the pass budget ran out, " +
                    "so the run must hand the lock back rather than keep passing - without the " +
                    "budget it stays in the loop until the inserts happen to stop and reports DONE"
            )
            .isEqualTo(DbBatchTaskStatus.RUNNING)

        val yielded = TxnContext.doInTxn { schemaCtx.batchTaskService.getById(task.id) }!!
        assertThat(yielded.status)
            .describedAs("yielding is not finishing: the row stays RUNNING so the next drain resumes it")
            .isEqualTo(DbBatchTaskStatus.RUNNING)
        assertThat(yielded.cursor)
            .describedAs("and it resumes from the work already done, not from the beginning")
            .isGreaterThan(0)
        assertThat(yielded.finished)
            .describedAs("a yielded task has not finished, so nothing may have been written to __finished")
            .isEqualTo(Instant.EPOCH)
    }
}

/**
 * `maxAttempts` from [DbEcosDataProps.BatchProps], kept out of [DbBatchTaskEngineTest] proper for the
 * same reason as [DbBatchTaskEngineBatchSizePropsTest]: `dataProps` has to be set before
 * `DataMockFactory.setUp` builds the context, so it belongs in an `init` block of its own dedicated
 * class rather than at the top of a `@Test` method, where it would run too late to have any effect.
 */
class DbBatchTaskEngineMaxAttemptsPropsTest : DbRecordsTestBase() {

    init {
        dataProps = DbEcosDataProps(batch = DbEcosDataProps.BatchProps(maxAttempts = 2))
    }

    @Test
    fun aPositiveMaxAttemptsStillGivesATerminalFailedStateTest() {

        registerAtts(listOf(AttributeDef.create { withId("someAtt") }))
        repeat(3) { createRecord("someAtt" to "value-$it") }

        val schemaCtx = getTableCtx().getSchemaCtx()
        val handler = DbBatchTaskTestHandler("always-failing-handler", failBatchesUntilAttempt = 99, recordsPerBatch = 1)
        schemaCtx.dataSourceCtx.batchTaskHandlers.register(handler)

        val task = TxnContext.doInTxn {
            schemaCtx.batchTaskService.queue("always-failing-handler", tableRef.table, ObjectData.create())
        }

        val engine = DbBatchTaskEngine(schemaCtx.dataSourceCtx)
        engine.runTask(schemaCtx, task.id)
        engine.runTask(schemaCtx, task.id)

        assertThat(TxnContext.doInTxn { schemaCtx.batchTaskService.getById(task.id) }!!.status)
            .describedAs(
                "an operator who deliberately configures a finite attempt budget still gets a " +
                    "terminal state to alert on"
            )
            .isEqualTo(DbBatchTaskStatus.FAILED)
    }
}
