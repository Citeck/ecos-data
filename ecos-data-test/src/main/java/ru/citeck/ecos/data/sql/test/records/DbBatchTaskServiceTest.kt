package ru.citeck.ecos.data.sql.test.records

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import ru.citeck.ecos.commons.data.ObjectData
import ru.citeck.ecos.data.sql.batch.DbBatchTaskDto
import ru.citeck.ecos.data.sql.batch.DbBatchTaskEntity
import ru.citeck.ecos.data.sql.batch.DbBatchTaskStatus
import ru.citeck.ecos.data.sql.repo.find.DbFindPage
import ru.citeck.ecos.data.sql.repo.find.DbFindQuery
import ru.citeck.ecos.data.sql.service.DbDataServiceConfig
import ru.citeck.ecos.data.sql.service.DbDataServiceImpl
import ru.citeck.ecos.data.sql.service.expression.token.ColumnToken
import ru.citeck.ecos.data.sql.service.expression.token.FunctionToken
import ru.citeck.ecos.data.sql.service.expression.token.ScalarToken
import ru.citeck.ecos.records2.predicate.model.Predicates
import ru.citeck.ecos.txn.lib.TxnContext
import java.time.Instant

class DbBatchTaskServiceTest : DbRecordsTestBase() {

    private fun service() = getTableCtx().getSchemaCtx().batchTaskService

    private fun rawService() = DbDataServiceImpl(
        DbBatchTaskEntity::class.java,
        DbDataServiceConfig.create { withTable(DbBatchTaskEntity.TABLE) },
        getTableCtx().getSchemaCtx()
    )

    @Test
    fun latestAttributeTasksRespectGenerationBoundariesWithoutLoadingHistory() {
        registerAtts(listOf())
        val handler = "column-migration"
        fun task(
            attribute: String,
            status: DbBatchTaskStatus,
            taskHandler: String = handler,
            table: String = tableRef.table
        ): DbBatchTaskDto = TxnContext.doInTxn {
            service().save(
                DbBatchTaskDto(
                    handler = taskHandler,
                    table = table,
                    params = ObjectData.create().set("attId", attribute),
                    status = status
                )
            )
        }

        // Old-format tasks have no separate attribute column, just the key in params.
        repeat(200) { task("pending", DbBatchTaskStatus.FAILED) }
        task("closed", DbBatchTaskStatus.PENDING)
        task("closed", DbBatchTaskStatus.DONE)
        task("cancelled", DbBatchTaskStatus.FAILED)
        task("cancelled", DbBatchTaskStatus.CANCELLED)
        task("reopened", DbBatchTaskStatus.DONE)
        val failed = task("reopened", DbBatchTaskStatus.FAILED)
        val pending = task("pending", DbBatchTaskStatus.PENDING)
        val running = task("running", DbBatchTaskStatus.RUNNING)
        task("pending", DbBatchTaskStatus.DONE, taskHandler = "other-handler")
        task("pending", DbBatchTaskStatus.DONE, table = "other-table")

        // Warm schema setup so the observation covers only the lookup itself.
        service().findLatestUnfinishedByAttribute(tableRef.table, handler)
        val reads = ArrayList<Pair<DbFindQuery, Int>>()
        afterStorageRead = { table, query, rows ->
            if (table.table == DbBatchTaskEntity.TABLE) reads.add(query to rows.size)
        }
        val found = try {
            service().findLatestUnfinishedByAttribute(tableRef.table, handler)
        } finally {
            afterStorageRead = null
        }
        assertThat(found.map { it.id }).containsExactly(running.id, pending.id, failed.id)
        assertThat(reads).hasSize(2)
        assertThat(reads[0].first.groupBy).hasSize(1)
        assertThat(reads.map { it.second }).containsExactly(5, 3)
    }

    @Test
    fun latestAttributeLookupWithNoHistoryDoesNotFetchPayloads() {
        registerAtts(listOf())
        service().findLatestUnfinishedByAttribute(tableRef.table, "column-migration")
        val sizes = ArrayList<Int>()
        afterStorageRead = { table, _, rows ->
            if (table.table == DbBatchTaskEntity.TABLE) sizes.add(rows.size)
        }
        try {
            assertThat(service().findLatestUnfinishedByAttribute(tableRef.table, "column-migration")).isEmpty()
        } finally {
            afterStorageRead = null
        }
        assertThat(sizes).containsExactly(0)
    }

    @Test
    fun jsonPathExpressionsReadNestedValuesAndDistinguishMissingAndNull() {
        registerAtts(listOf())
        val task = TxnContext.doInTxn {
            service().queue(
                "json-test",
                tableRef.table,
                ObjectData.create()
                    .set("nested", mapOf("text" to "quoted \"value\""))
                    .set("items", listOf("first", "last"))
                    .set("nil", null)
            )
        }
        fun extract(vararg path: String) = FunctionToken(
            "jsonb_extract_path_text",
            listOf(ColumnToken(DbBatchTaskEntity.PARAMS)) + path.map { ScalarToken(it) }
        )
        val values = TxnContext.doInTxn {
            rawService().findRaw(
                DbFindQuery.create {
                    withPredicate(Predicates.eq(DbBatchTaskEntity.ID, task.id))
                    withExpressions(
                        mapOf(
                            "nestedText" to extract("nested", "text"),
                            "lastItem" to extract("items", "-1"),
                            "missing" to extract("absent"),
                            "jsonNull" to extract("nil"),
                            "wrongShape" to extract("nested", "text", "extra")
                        )
                    )
                },
                DbFindPage.ALL,
                false
            ).entities.single()
        }
        assertThat(values["nestedText"]).isEqualTo("quoted \"value\"")
        assertThat(values["lastItem"]).isEqualTo("last")
        assertThat(values["missing"]).isNull()
        assertThat(values["jsonNull"]).isNull()
        assertThat(values["wrongShape"]).isNull()
    }

    @Test
    fun integralExtremaDoNotRoundAdjacentLongValues() {
        registerAtts(listOf())
        val low = 9_007_199_254_740_992L
        TxnContext.doInTxn {
            for ((attribute, values) in listOf("ascending" to listOf(low, low + 1), "descending" to listOf(low + 1, low))) {
                for (value in values) {
                    service().save(
                        DbBatchTaskDto(
                            handler = "precision-test",
                            table = tableRef.table,
                            params = ObjectData.create().set("attId", attribute),
                            cursor = value
                        )
                    )
                }
            }
        }
        val rows = TxnContext.doInTxn {
            rawService().findRaw(
                DbFindQuery.create {
                    withExpressions(
                        mapOf(
                            "attribute" to FunctionToken(
                                "jsonb_extract_path_text",
                                listOf(ColumnToken(DbBatchTaskEntity.PARAMS), ScalarToken("attId"))
                            ),
                            "largest" to FunctionToken("max", listOf(ColumnToken(DbBatchTaskEntity.CURSOR))),
                            "smallest" to FunctionToken("min", listOf(ColumnToken(DbBatchTaskEntity.CURSOR)))
                        )
                    )
                    withGroupBy(listOf("attribute"))
                },
                DbFindPage.ALL,
                false
            ).entities
        }
        assertThat(rows).hasSize(2)
        for (row in rows) {
            assertThat(row["largest"]).isEqualTo(low + 1)
            assertThat(row["smallest"]).isEqualTo(low)
        }
    }

    @Test
    fun queuedTaskIsPendingWithAZeroCursorTest() {

        registerAtts(listOf())

        val task = TxnContext.doInTxn {
            service().queue("test-handler", tableRef.table, ObjectData.create().set("attId", "someAtt"))
        }

        assertThat(task.id).describedAs("the row must be persisted, so it must carry a real id").isGreaterThan(0)
        assertThat(task.status).isEqualTo(DbBatchTaskStatus.PENDING)
        assertThat(task.cursor).describedAs("a fresh task has processed nothing").isEqualTo(0)
        assertThat(task.params.get("attId").asText()).isEqualTo("someAtt")

        val loaded = TxnContext.doInTxn { service().getById(task.id) }
        assertThat(loaded)
            .describedAs("params survive the JSON round trip through the column")
            .isNotNull
        assertThat(loaded!!.params.get("attId").asText()).isEqualTo("someAtt")
    }

    @Test
    fun cancelMovesAPendingTaskToCancelledAndIsIdempotentTest() {

        registerAtts(listOf())

        val task = TxnContext.doInTxn {
            service().queue("test-handler", tableRef.table, ObjectData.create())
        }

        assertThat(TxnContext.doInTxn { service().restart(task.id) })
            .describedAs("a task that is still PENDING is not final, so restarting it does nothing")
            .isFalse()
        assertThat(TxnContext.doInTxn { service().getById(task.id) }!!.status)
            .describedAs("the no-op restart above must not have moved it off PENDING")
            .isEqualTo(DbBatchTaskStatus.PENDING)

        assertThat(TxnContext.doInTxn { service().cancel(task.id) })
            .describedAs("cancelling an active task reports that it did something")
            .isTrue()
        assertThat(TxnContext.doInTxn { service().getById(task.id) }!!.status)
            .isEqualTo(DbBatchTaskStatus.CANCELLED)

        assertThat(TxnContext.doInTxn { service().cancel(task.id) })
            .describedAs("cancelling an already-final task changes nothing and says so")
            .isFalse()
    }

    @Test
    fun restartClearsErrorStateButKeepsTheCursorTest() {

        registerAtts(listOf())

        val task = TxnContext.doInTxn {
            service().queue("test-handler", tableRef.table, ObjectData.create())
        }
        TxnContext.doInTxn {
            service().save(
                task.copy(
                    status = DbBatchTaskStatus.FAILED,
                    error = "boom",
                    errorCount = 3,
                    processed = 17,
                    cursor = 42,
                    nextAttemptAt = Instant.now().plusSeconds(600)
                )
            )
        }

        assertThat(TxnContext.doInTxn { service().restart(task.id) }).isTrue()

        val restarted = TxnContext.doInTxn { service().getById(task.id) }!!
        assertThat(restarted.status).isEqualTo(DbBatchTaskStatus.PENDING)
        assertThat(restarted.errorCount).describedAs("a restart forgives past attempts").isEqualTo(0)
        assertThat(restarted.error).isEmpty()
        assertThat(restarted.nextAttemptAt)
            .describedAs(
                "an administrator pressing restart is an explicit \"try now\" - it must not leave " +
                    "the task waiting out a backoff window it did not set"
            )
            .isEqualTo(Instant.EPOCH)
        assertThat(restarted.cursor)
            .describedAs(
                "a restart resumes, it does not rewind - rewinding would reprocess rows the " +
                    "handler already converted, and a handler is only required to be idempotent " +
                    "within one batch"
            )
            .isEqualTo(42)
    }

    @Test
    fun findActiveIgnoresFinishedTasksTest() {

        registerAtts(listOf())

        val pending = TxnContext.doInTxn { service().queue("h", tableRef.table, ObjectData.create()) }
        val done = TxnContext.doInTxn { service().queue("h", tableRef.table, ObjectData.create()) }
        TxnContext.doInTxn { service().save(done.copy(status = DbBatchTaskStatus.DONE)) }

        val active = TxnContext.doInTxn { service().findActive() }
        assertThat(active.map { it.id })
            .describedAs("PENDING and RUNNING are both drainable; DONE/FAILED/CANCELLED are not")
            .containsExactly(pending.id)
    }

    @Test
    fun findActiveSkipsTasksInsideTheirBackoffWindowTest() {

        registerAtts(listOf())

        val ready = TxnContext.doInTxn { service().queue("h", tableRef.table, ObjectData.create()) }
        val backingOff = TxnContext.doInTxn { service().queue("h", tableRef.table, ObjectData.create()) }

        val now = Instant.now()
        TxnContext.doInTxn {
            service().save(backingOff.copy(nextAttemptAt = now.plusSeconds(600)))
        }

        assertThat(TxnContext.doInTxn { service().findActive(now) }.map { it.id })
            .describedAs(
                "a task waiting out its retry backoff is not drainable yet - without this filter " +
                    "the drain would re-run a failing batch at full speed instead of backing off"
            )
            .containsExactly(ready.id)

        assertThat(TxnContext.doInTxn { service().findActive(now.plusSeconds(601)) }.map { it.id })
            .describedAs("once the window has passed the task comes back on its own, with no admin action")
            .containsExactly(ready.id, backingOff.id)
    }
}
