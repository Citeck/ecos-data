package ru.citeck.ecos.data.sql.test.records

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import ru.citeck.ecos.commons.data.ObjectData
import ru.citeck.ecos.data.sql.batch.DbBatchTaskStatus
import ru.citeck.ecos.txn.lib.TxnContext
import java.time.Instant

class DbBatchTaskServiceTest : DbRecordsTestBase() {

    private fun service() = getTableCtx().getSchemaCtx().batchTaskService

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
