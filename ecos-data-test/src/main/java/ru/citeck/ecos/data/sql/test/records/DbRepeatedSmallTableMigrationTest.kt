package ru.citeck.ecos.data.sql.test.records

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import ru.citeck.ecos.data.sql.batch.DbBatchTaskEngine
import ru.citeck.ecos.data.sql.batch.DbBatchTaskStatus
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType
import ru.citeck.ecos.txn.lib.TxnContext

/** A pending transfer must be superseded even when the next cast would normally run in place. */
class DbRepeatedSmallTableMigrationTest : DbRecordsTestBase() {

    @Test
    fun safeSecondChangeInheritsThePendingSourceOnASmallTable() {
        fun change(type: AttributeType) {
            registerAtts(
                listOf(
                    AttributeDef.create {
                        withId("value")
                        withType(type)
                    }
                )
            )
        }
        change(AttributeType.TEXT)
        val record = createRecord("value" to "not-json")
        change(AttributeType.JSON)
        createRecord("value" to null)
        change(AttributeType.OPTIONS)
        createRecord("value" to null)
        val schema = getTableCtx().getSchemaCtx()
        DbBatchTaskEngine(schema.dataSourceCtx).drainSchemaOnce(schema)
        assertThat(records.getAtt(record, "value").asText()).isEqualTo("not-json")
        TxnContext.doInTxn {
            val task = schema.batchTaskService.findByTable(tableRef.table).maxBy { it.id }
            assertThat(task.status).isEqualTo(DbBatchTaskStatus.DONE)
            assertThat(task.failed).isZero()
            assertThat(mainCtx.dataService.findByExtId(record.getLocalId())!!.attributes["value"])
                .isEqualTo("not-json")
        }
    }
}
