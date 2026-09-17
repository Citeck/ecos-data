package ru.citeck.ecos.data.sql.test.records

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import ru.citeck.ecos.commons.data.ObjectData
import ru.citeck.ecos.context.lib.auth.AuthContext
import ru.citeck.ecos.context.lib.auth.AuthRole
import ru.citeck.ecos.data.sql.batch.DbBatchTaskAdminDao
import ru.citeck.ecos.data.sql.batch.DbBatchTaskCancelAction
import ru.citeck.ecos.data.sql.batch.DbBatchTaskRestartAction
import ru.citeck.ecos.data.sql.batch.DbBatchTaskStatus
import ru.citeck.ecos.records2.predicate.PredicateService
import ru.citeck.ecos.records2.predicate.model.Predicates
import ru.citeck.ecos.records3.record.dao.query.dto.query.QueryPage
import ru.citeck.ecos.records3.record.dao.query.dto.query.RecordsQuery
import ru.citeck.ecos.records3.record.dao.query.dto.query.SortBy
import ru.citeck.ecos.txn.lib.TxnContext
import ru.citeck.ecos.webapp.api.entity.EntityRef
import java.time.Instant
import java.time.temporal.ChronoUnit

/**
 * The admin surface: [DbBatchTaskAdminDao] lists `ed_batch_task` rows with the atts the
 * design doc asks for (plus `errorCount`/`nextAttemptAt`, per the reviews), and
 * [DbBatchTaskCancelAction] / [DbBatchTaskRestartAction] delegate the two row actions to
 * [ru.citeck.ecos.data.sql.batch.DbBatchTaskService.cancel] / `.restart`, which already
 * built and tested. This test does not re-derive any lifecycle rule - it only checks that the DAO
 * layer reaches the service correctly and that a refusal surfaces as a real failure rather than a
 * silent no-op.
 */
class DbBatchTaskAdminDaoTest : DbRecordsTestBase() {

    private fun service() = getTableCtx().getSchemaCtx().batchTaskService

    private fun registerAdminDao(): DbBatchTaskAdminDao {
        val dao = DbBatchTaskAdminDao(service())
        records.register(dao)
        return dao
    }

    private fun registerCancelAction(): DbBatchTaskCancelAction {
        val dao = DbBatchTaskCancelAction(service())
        records.register(dao)
        return dao
    }

    private fun registerRestartAction(): DbBatchTaskRestartAction {
        val dao = DbBatchTaskRestartAction(service())
        records.register(dao)
        return dao
    }

    private fun taskRef(adminDao: DbBatchTaskAdminDao, id: Long): EntityRef {
        return EntityRef.create(APP_NAME, adminDao.getId(), id.toString())
    }

    private fun queryAllOf(sourceId: String, action: RecordsQuery.Builder.() -> Unit = {}): RecordsQuery {
        return RecordsQuery.create()
            .withSourceId(sourceId)
            .withLanguage(PredicateService.LANGUAGE_PREDICATE)
            .withQuery(Predicates.alwaysTrue())
            .also(action)
            .build()
    }

    @Test
    fun queryReturnsQueuedTasksWithProgressAndErrorStateTest() {

        registerAtts(listOf())
        val adminDao = registerAdminDao()

        val queued = TxnContext.doInTxn {
            service().queue("test-handler", tableRef.table, ObjectData.create())
        }
        val nextAttempt = Instant.now().plusSeconds(600).truncatedTo(ChronoUnit.MILLIS)
        TxnContext.doInTxn {
            service().save(
                queued.copy(
                    status = DbBatchTaskStatus.RUNNING,
                    cursor = 5,
                    total = 100,
                    processed = 42,
                    skipped = 1,
                    failed = 2,
                    error = "some batch failed",
                    errorCount = 3,
                    nextAttemptAt = nextAttempt
                )
            )
        }

        val queryRes = records.query(queryAllOf(adminDao.getId()))

        assertThat(queryRes.getRecords()).hasSize(1)
        val ref = queryRes.getRecords()[0]
        assertThat(ref.getLocalId()).isEqualTo(queued.id.toString())

        val atts = records.getAtts(
            ref,
            listOf(
                "handler",
                "table",
                "status",
                "processed",
                "total",
                "skipped",
                "failed",
                "error",
                "errorCount",
                "nextAttemptAt"
            )
        )
        assertThat(atts["handler"].asText()).isEqualTo("test-handler")
        assertThat(atts["table"].asText()).isEqualTo(tableRef.table)
        assertThat(atts["status"].asText()).isEqualTo(DbBatchTaskStatus.RUNNING.name)
        assertThat(atts["processed"].asLong()).isEqualTo(42)
        assertThat(atts["total"].asLong()).isEqualTo(100)
        assertThat(atts["skipped"].asLong()).isEqualTo(1)
        assertThat(atts["failed"].asLong()).isEqualTo(2)
        assertThat(atts["error"].asText())
            .describedAs("the last error must be visible - there is no other display for it")
            .isEqualTo("some batch failed")
        assertThat(atts["errorCount"].asInt())
            .describedAs("D6's unlimited retries make errorCount the thing an administrator needs")
            .isEqualTo(3)
        assertThat(atts["nextAttemptAt"].getAsInstant())
            .describedAs("and nextAttemptAt tells them when it will try again")
            .isEqualTo(nextAttempt)
    }

    @Test
    fun queryFiltersByThePublishedAttributeNamesNotRawColumnsTest() {

        registerAtts(listOf())
        val adminDao = registerAdminDao()

        val failed = TxnContext.doInTxn {
            service().queue("test-handler", tableRef.table, ObjectData.create())
        }
        TxnContext.doInTxn { service().save(failed.copy(status = DbBatchTaskStatus.FAILED)) }
        TxnContext.doInTxn {
            service().queue("test-handler", tableRef.table, ObjectData.create())
        }

        val res = records.query(
            queryAllOf(adminDao.getId()) {
                withQuery(Predicates.eq("status", DbBatchTaskStatus.FAILED.name))
            }
        )

        assertThat(res.getRecords().map { it.getLocalId() })
            .describedAs(
                "filtering by 'status' - the attribute this DAO itself publishes, and the first " +
                    "thing an admin screen would filter the queue by - must reach the underlying " +
                    "'__status' column rather than silently matching nothing"
            )
            .containsExactly(failed.id.toString())
    }

    @Test
    fun queryHonoursTheRequestedSortOverThePublishedAttributeNamesTest() {

        registerAtts(listOf())
        val adminDao = registerAdminDao()

        val tasks = (0 until 3).map {
            TxnContext.doInTxn { service().queue("test-handler", tableRef.table, ObjectData.create()) }
        }
        val errorCounts = listOf(5, 0, 2)
        tasks.zip(errorCounts).forEach { (task, errorCount) ->
            TxnContext.doInTxn { service().save(task.copy(errorCount = errorCount)) }
        }

        val res = records.query(
            queryAllOf(adminDao.getId()) {
                withSortBy(SortBy("errorCount", true))
            }
        )

        assertThat(res.getRecords().map { it.getLocalId() })
            .describedAs(
                "errorCount is the attribute the brief calls out as the state an administrator " +
                    "most needs, and sorting by it is the very next click after seeing it"
            )
            .containsExactly(
                tasks[1].id.toString(),
                tasks[2].id.toString(),
                tasks[0].id.toString()
            )
    }

    @Test
    fun queryReportsTheRealTotalCountNotThePageSizeTest() {

        registerAtts(listOf())
        val adminDao = registerAdminDao()

        repeat(3) {
            TxnContext.doInTxn { service().queue("test-handler", tableRef.table, ObjectData.create()) }
        }

        val res = records.query(
            queryAllOf(adminDao.getId()) {
                withPage(QueryPage.create().withMaxItems(1).build())
            }
        )

        assertThat(res.getRecords()).hasSize(1)
        assertThat(res.getTotalCount())
            .describedAs("a paged admin view over a queue of hundreds must not show '1 of 1'")
            .isEqualTo(3)
    }

    /**
     * F2: the read half of the guard the two action DAOs already apply to the write half. `handler`
     * and `table` describe the internal schema and `error` is free text lifted straight out of a
     * database exception - SQL fragments and column names included - so this list is not something
     * any principal who can reach `records.query` may read.
     */
    @Test
    fun queryingWithoutAdminOrSystemPermissionsIsRefusedTest() {

        registerAtts(listOf())
        val adminDao = registerAdminDao()

        TxnContext.doInTxn { service().queue("test-handler", tableRef.table, ObjectData.create()) }

        val ex = assertThrows<Exception> {
            AuthContext.runAs("a-regular-user") {
                records.query(queryAllOf(adminDao.getId()))
            }
        }
        assertThat(ex.message)
            .describedAs(
                "a regular user must not be able to read the queue - the free-text 'error' column " +
                    "alone leaks SQL fragments and column names out of PostgreSQL exceptions"
            )
            .contains("Permission denied")

        assertThat(AuthContext.runAsSystem { records.query(queryAllOf(adminDao.getId())) }.getRecords())
            .describedAs("system is exactly the context the engine and the migration machinery run in")
            .hasSize(1)
        assertThat(
            AuthContext.runAs("an-admin", listOf(AuthRole.ADMIN)) {
                records.query(queryAllOf(adminDao.getId()))
            }.getRecords()
        )
            .describedAs("and an administrator is who this view exists for")
            .hasSize(1)
    }

    @Test
    fun readingOneTasksAttsWithoutAdminOrSystemPermissionsIsRefusedTest() {

        registerAtts(listOf())
        val adminDao = registerAdminDao()

        val task = TxnContext.doInTxn {
            service().queue("test-handler", tableRef.table, ObjectData.create())
        }

        val ex = assertThrows<Exception> {
            AuthContext.runAs("a-regular-user") {
                records.getAtts(taskRef(adminDao, task.id), listOf("handler", "error"))
            }
        }
        assertThat(ex.message)
            .describedAs("guarding only the list would leave the same data readable one row at a time")
            .contains("Permission denied")

        assertThat(
            AuthContext.runAsSystem {
                records.getAtts(taskRef(adminDao, task.id), listOf("handler"))
            }["handler"].asText()
        ).isEqualTo("test-handler")
        assertThat(
            AuthContext.runAs("an-admin", listOf(AuthRole.ADMIN)) {
                records.getAtts(taskRef(adminDao, task.id), listOf("handler"))
            }["handler"].asText()
        ).isEqualTo("test-handler")
    }

    /**
     * F13: an attribute this DAO does not publish degrades instead of failing the whole query, and
     * the degrade has to stay correct under composition. `PredicateUtils` folds a composite the
     * moment one branch is a *constant* predicate, so a constant branch does not drop out of an
     * `OR` - it decides it. Expressing the degrade as a real value predicate keeps that folding out
     * of the picture entirely; these three tests pin each shape the folding used to get wrong.
     */
    @Test
    fun anUnresolvableValueClauseInsideAnOrDoesNotWidenTheResultTest() {

        registerAtts(listOf())
        val adminDao = registerAdminDao()

        val failed = TxnContext.doInTxn {
            service().queue("test-handler", tableRef.table, ObjectData.create())
        }
        TxnContext.doInTxn { service().save(failed.copy(status = DbBatchTaskStatus.FAILED)) }
        repeat(2) {
            TxnContext.doInTxn { service().queue("test-handler", tableRef.table, ObjectData.create()) }
        }

        val res = records.query(
            queryAllOf(adminDao.getId()) {
                withQuery(
                    Predicates.or(
                        Predicates.eq("status", DbBatchTaskStatus.FAILED.name),
                        Predicates.eq("noSuchAttributeExists", "whatever")
                    )
                )
            }
        )

        assertThat(res.getRecords().map { it.getLocalId() })
            .describedAs(
                "no row has 'noSuchAttributeExists', so that branch is false everywhere and the " +
                    "OR must come down to the status filter - degrading it to a constant made the " +
                    "optimizer fold the whole OR and list the entire queue"
            )
            .containsExactly(failed.id.toString())
    }

    @Test
    fun anUnresolvableValueClauseUnderANotDoesNotEmptyTheResultTest() {

        registerAtts(listOf())
        val adminDao = registerAdminDao()

        val failed = TxnContext.doInTxn {
            service().queue("test-handler", tableRef.table, ObjectData.create())
        }
        TxnContext.doInTxn { service().save(failed.copy(status = DbBatchTaskStatus.FAILED)) }
        TxnContext.doInTxn { service().queue("test-handler", tableRef.table, ObjectData.create()) }

        val res = records.query(
            queryAllOf(adminDao.getId()) {
                withQuery(
                    Predicates.and(
                        Predicates.eq("status", DbBatchTaskStatus.FAILED.name),
                        Predicates.not(Predicates.eq("noSuchAttributeExists", "whatever"))
                    )
                )
            }
        )

        assertThat(res.getRecords().map { it.getLocalId() })
            .describedAs(
                "'not (an attribute no row has equals X)' is true on every row, so the AND must " +
                    "come down to the status filter - a constant degrade collapsed it to 'match " +
                    "nothing' instead"
            )
            .containsExactly(failed.id.toString())
    }

    @Test
    fun anIsEmptyCheckOnAnUnpublishedAttributeDoesNotWipeTheAndTest() {

        registerAtts(listOf())
        val adminDao = registerAdminDao()

        val failed = TxnContext.doInTxn {
            service().queue("test-handler", tableRef.table, ObjectData.create())
        }
        TxnContext.doInTxn { service().save(failed.copy(status = DbBatchTaskStatus.FAILED)) }
        TxnContext.doInTxn { service().queue("test-handler", tableRef.table, ObjectData.create()) }

        val res = records.query(
            queryAllOf(adminDao.getId()) {
                withQuery(
                    Predicates.and(
                        Predicates.eq("status", DbBatchTaskStatus.FAILED.name),
                        Predicates.empty("noSuchAttributeExists")
                    )
                )
            }
        )

        assertThat(res.getRecords().map { it.getLocalId() })
            .describedAs(
                "an attribute no row has IS empty on every row, so this clause adds nothing and " +
                    "the status filter must still decide - reading it as 'always false' folded " +
                    "the AND and returned an empty list with no explanation"
            )
            .containsExactly(failed.id.toString())
    }

    @Test
    fun anIsEmptyCheckOnAnUnpublishedAttributeIsTrueOnEveryRowTest() {

        registerAtts(listOf())
        val adminDao = registerAdminDao()

        val failed = TxnContext.doInTxn {
            service().queue("test-handler", tableRef.table, ObjectData.create())
        }
        TxnContext.doInTxn { service().save(failed.copy(status = DbBatchTaskStatus.FAILED)) }
        val queued = TxnContext.doInTxn {
            service().queue("test-handler", tableRef.table, ObjectData.create())
        }

        val res = records.query(
            queryAllOf(adminDao.getId()) {
                withQuery(
                    Predicates.or(
                        Predicates.eq("status", DbBatchTaskStatus.FAILED.name),
                        Predicates.empty("noSuchAttributeExists")
                    )
                )
            }
        )

        assertThat(res.getRecords().map { it.getLocalId() })
            .describedAs(
                "the other side of the same truth: no row has the attribute, so 'is empty' holds " +
                    "for all of them and this OR legitimately matches everything. Reading the " +
                    "clause as 'always false' would hide the queued task behind a filter the " +
                    "caller never asked for"
            )
            .containsExactlyInAnyOrder(failed.id.toString(), queued.id.toString())
    }

    @Test
    fun anOrOfNothingButUnresolvableClausesMatchesNothingTest() {

        registerAtts(listOf())
        val adminDao = registerAdminDao()

        val failed = TxnContext.doInTxn {
            service().queue("test-handler", tableRef.table, ObjectData.create())
        }
        TxnContext.doInTxn { service().save(failed.copy(status = DbBatchTaskStatus.FAILED)) }
        TxnContext.doInTxn { service().queue("test-handler", tableRef.table, ObjectData.create()) }

        val res = records.query(
            queryAllOf(adminDao.getId()) {
                withQuery(
                    Predicates.or(
                        Predicates.eq("noSuchAttributeA", "x"),
                        Predicates.eq("noSuchAttributeB", "y")
                    )
                )
            }
        )

        assertThat(res.getRecords())
            .describedAs(
                "neither branch can match any row, so the OR matches nothing. Degrading to " +
                    "alwaysFalse() made the whole composite fold away to null, which this DAO " +
                    "then read as 'no filter at all' and listed the entire queue"
            )
            .isEmpty()
    }

    @Test
    fun anUnresolvableOrNestedInAnAndDoesNotVanishFromItTest() {

        registerAtts(listOf())
        val adminDao = registerAdminDao()

        val failed = TxnContext.doInTxn {
            service().queue("test-handler", tableRef.table, ObjectData.create())
        }
        TxnContext.doInTxn { service().save(failed.copy(status = DbBatchTaskStatus.FAILED)) }

        val res = records.query(
            queryAllOf(adminDao.getId()) {
                withQuery(
                    Predicates.and(
                        Predicates.eq("status", DbBatchTaskStatus.FAILED.name),
                        Predicates.or(
                            Predicates.eq("noSuchAttributeA", "x"),
                            Predicates.eq("noSuchAttributeB", "y")
                        )
                    )
                )
            }
        )

        assertThat(res.getRecords())
            .describedAs(
                "the nested OR can match nothing, so the AND must match nothing either - with a " +
                    "constant degrade the OR folded to null and was silently dropped from the " +
                    "AND, leaving only the status filter"
            )
            .isEmpty()
    }

    @Test
    fun anUnknownAttributeInAFlatAndStillMatchesNothingTest() {

        registerAtts(listOf())
        val adminDao = registerAdminDao()

        val failed = TxnContext.doInTxn {
            service().queue("test-handler", tableRef.table, ObjectData.create())
        }
        TxnContext.doInTxn { service().save(failed.copy(status = DbBatchTaskStatus.FAILED)) }

        val res = records.query(
            queryAllOf(adminDao.getId()) {
                withQuery(
                    Predicates.and(
                        Predicates.eq("status", DbBatchTaskStatus.FAILED.name),
                        Predicates.eq("noSuchAttributeExists", "whatever")
                    )
                )
            }
        )

        assertThat(res.getRecords())
            .describedAs(
                "a flat AND of value predicates is what an admin journal actually sends, and a " +
                    "clause that can never match must keep narrowing it to nothing - fixing the " +
                    "composed cases must not turn this into 'match everything'"
            )
            .isEmpty()
    }

    @Test
    fun cancellingThroughTheDaoMovesAnActiveTaskToCancelledTest() {

        registerAtts(listOf())
        val adminDao = registerAdminDao()
        val cancelAction = registerCancelAction()

        val task = TxnContext.doInTxn {
            service().queue("test-handler", tableRef.table, ObjectData.create())
        }
        assertThat(TxnContext.doInTxn { service().getById(task.id) }!!.status)
            .isEqualTo(DbBatchTaskStatus.PENDING)

        records.mutate(
            EntityRef.create(APP_NAME, cancelAction.getId(), ""),
            mapOf("recordRef" to taskRef(adminDao, task.id))
        )

        assertThat(TxnContext.doInTxn { service().getById(task.id) }!!.status)
            .isEqualTo(DbBatchTaskStatus.CANCELLED)
    }

    @Test
    fun restartingThroughTheDaoReturnsAFailedTaskToPendingWithTheCursorUnchangedTest() {

        registerAtts(listOf())
        val adminDao = registerAdminDao()
        val restartAction = registerRestartAction()

        val queued = TxnContext.doInTxn {
            service().queue("test-handler", tableRef.table, ObjectData.create())
        }
        TxnContext.doInTxn {
            service().save(
                queued.copy(
                    status = DbBatchTaskStatus.FAILED,
                    error = "boom",
                    errorCount = 5,
                    processed = 17,
                    cursor = 42,
                    nextAttemptAt = Instant.now().plusSeconds(600),
                    finished = Instant.now()
                )
            )
        }

        records.mutate(
            EntityRef.create(APP_NAME, restartAction.getId(), ""),
            mapOf("recordRef" to taskRef(adminDao, queued.id))
        )

        val restarted = TxnContext.doInTxn { service().getById(queued.id) }!!
        assertThat(restarted.status).isEqualTo(DbBatchTaskStatus.PENDING)
        assertThat(restarted.errorCount).isEqualTo(0)
        assertThat(restarted.error).isEmpty()
        assertThat(restarted.cursor)
            .describedAs("a restart resumes, it does not rewind - the cursor must survive it")
            .isEqualTo(42)
        assertThat(restarted.processed)
            .describedAs("only the error bookkeeping is forgiven, not the work already counted")
            .isEqualTo(17)
    }

    @Test
    fun cancellingAnAlreadyFinalTaskIsRefusedRatherThanSilentlySucceedingTest() {

        registerAtts(listOf())
        val adminDao = registerAdminDao()
        val cancelAction = registerCancelAction()

        val queued = TxnContext.doInTxn {
            service().queue("test-handler", tableRef.table, ObjectData.create())
        }
        TxnContext.doInTxn {
            service().save(queued.copy(status = DbBatchTaskStatus.DONE, finished = Instant.now()))
        }

        val ex = assertThrows<Exception> {
            records.mutate(
                EntityRef.create(APP_NAME, cancelAction.getId(), ""),
                mapOf("recordRef" to taskRef(adminDao, queued.id))
            )
        }
        assertThat(ex.message).contains("already in a final state")

        assertThat(TxnContext.doInTxn { service().getById(queued.id) }!!.status)
            .describedAs("a refused cancel must not have moved the already-final task")
            .isEqualTo(DbBatchTaskStatus.DONE)
    }

    @Test
    fun restartingAStillActiveTaskIsRefusedRatherThanSilentlySucceedingTest() {

        registerAtts(listOf())
        val adminDao = registerAdminDao()
        val restartAction = registerRestartAction()

        val task = TxnContext.doInTxn {
            service().queue("test-handler", tableRef.table, ObjectData.create())
        }
        assertThat(task.status).isEqualTo(DbBatchTaskStatus.PENDING)

        val ex = assertThrows<Exception> {
            records.mutate(
                EntityRef.create(APP_NAME, restartAction.getId(), ""),
                mapOf("recordRef" to taskRef(adminDao, task.id))
            )
        }
        assertThat(ex.message).contains("still active")

        assertThat(TxnContext.doInTxn { service().getById(task.id) }!!.status)
            .describedAs("a refused restart must not have moved the still-active task")
            .isEqualTo(DbBatchTaskStatus.PENDING)
    }

    @Test
    fun cancellingATaskThatDoesNotExistReportsNotFoundRatherThanAlreadyFinalTest() {

        registerAtts(listOf())
        val adminDao = registerAdminDao()
        val cancelAction = registerCancelAction()

        val ex = assertThrows<Exception> {
            records.mutate(
                EntityRef.create(APP_NAME, cancelAction.getId(), ""),
                mapOf("recordRef" to taskRef(adminDao, 999999L))
            )
        }
        assertThat(ex.message)
            .describedAs(
                "DbBatchTaskService.cancel() returns false for both 'not found' and 'refused' - " +
                    "an administrator chasing a stale link must not be told the false 'already in " +
                    "a final state', which implies a task that isn't there"
            )
            .contains("not found")
    }

    @Test
    fun restartingATaskThatDoesNotExistReportsNotFoundRatherThanStillActiveTest() {

        registerAtts(listOf())
        val adminDao = registerAdminDao()
        val restartAction = registerRestartAction()

        val ex = assertThrows<Exception> {
            records.mutate(
                EntityRef.create(APP_NAME, restartAction.getId(), ""),
                mapOf("recordRef" to taskRef(adminDao, 999999L))
            )
        }
        assertThat(ex.message).contains("not found")
    }

    @Test
    fun cancellingWithoutAdminOrSystemPermissionsIsRefusedTest() {

        registerAtts(listOf())
        val adminDao = registerAdminDao()
        val cancelAction = registerCancelAction()

        val task = TxnContext.doInTxn {
            service().queue("test-handler", tableRef.table, ObjectData.create())
        }

        val ex = assertThrows<Exception> {
            AuthContext.runAs("a-regular-user") {
                records.mutate(
                    EntityRef.create(APP_NAME, cancelAction.getId(), ""),
                    mapOf("recordRef" to taskRef(adminDao, task.id))
                )
            }
        }
        assertThat(ex.message)
            .describedAs(
                "any principal reaching records.mutate on the cancel action must not be able to " +
                    "cancel a running task on live customer data without being an admin or system"
            )
            .isEqualTo("Permission denied")

        assertThat(TxnContext.doInTxn { service().getById(task.id) }!!.status)
            .isEqualTo(DbBatchTaskStatus.PENDING)
    }

    @Test
    fun restartingWithoutAdminOrSystemPermissionsIsRefusedTest() {

        registerAtts(listOf())
        val adminDao = registerAdminDao()
        val restartAction = registerRestartAction()

        val queued = TxnContext.doInTxn {
            service().queue("test-handler", tableRef.table, ObjectData.create())
        }
        TxnContext.doInTxn {
            service().save(queued.copy(status = DbBatchTaskStatus.FAILED, finished = Instant.now()))
        }

        val ex = assertThrows<Exception> {
            AuthContext.runAs("a-regular-user") {
                records.mutate(
                    EntityRef.create(APP_NAME, restartAction.getId(), ""),
                    mapOf("recordRef" to taskRef(adminDao, queued.id))
                )
            }
        }
        assertThat(ex.message).isEqualTo("Permission denied")

        assertThat(TxnContext.doInTxn { service().getById(queued.id) }!!.status)
            .isEqualTo(DbBatchTaskStatus.FAILED)
    }

    @Test
    fun cancellingWithARefFromAnotherSourceIsRefusedTest() {

        registerAtts(listOf())
        registerAdminDao()
        val cancelAction = registerCancelAction()

        val task = TxnContext.doInTxn {
            service().queue("test-handler", tableRef.table, ObjectData.create())
        }

        assertThrows<Exception> {
            records.mutate(
                EntityRef.create(APP_NAME, cancelAction.getId(), ""),
                mapOf(
                    "recordRef" to EntityRef.create(APP_NAME, "some-other-schemas-list", task.id.toString())
                )
            )
        }

        assertThat(TxnContext.doInTxn { service().getById(task.id) }!!.status)
            .describedAs(
                "ids are bare Longs with no schema tag of their own - a ref built against a " +
                    "different schema's list must not be accepted just because the number matches"
            )
            .isEqualTo(DbBatchTaskStatus.PENDING)
    }

    @Test
    fun restartingWithARefFromAnotherSourceIsRefusedTest() {

        registerAtts(listOf())
        registerAdminDao()
        val restartAction = registerRestartAction()

        val queued = TxnContext.doInTxn {
            service().queue("test-handler", tableRef.table, ObjectData.create())
        }
        TxnContext.doInTxn {
            service().save(queued.copy(status = DbBatchTaskStatus.FAILED, finished = Instant.now()))
        }

        assertThrows<Exception> {
            records.mutate(
                EntityRef.create(APP_NAME, restartAction.getId(), ""),
                mapOf(
                    "recordRef" to EntityRef.create(APP_NAME, "some-other-schemas-list", queued.id.toString())
                )
            )
        }

        assertThat(TxnContext.doInTxn { service().getById(queued.id) }!!.status)
            .isEqualTo(DbBatchTaskStatus.FAILED)
    }
}
