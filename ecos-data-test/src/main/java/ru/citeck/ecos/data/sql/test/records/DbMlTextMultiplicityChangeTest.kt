package ru.citeck.ecos.data.sql.test.records

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import ru.citeck.ecos.commons.data.MLText
import ru.citeck.ecos.data.sql.batch.DbBatchTaskEngine
import ru.citeck.ecos.data.sql.repo.entity.DbEntity
import ru.citeck.ecos.data.sql.repo.find.DbFindPage
import ru.citeck.ecos.data.sql.service.DbDataServiceConfig
import ru.citeck.ecos.data.sql.service.DbDataServiceImpl
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType
import ru.citeck.ecos.records2.predicate.model.Predicates
import ru.citeck.ecos.txn.lib.TxnContext
import ru.citeck.ecos.webapp.api.entity.EntityRef
import java.util.Locale

/**
 * Making an MLText attribute multi-valued, and making it single-valued again.
 *
 * Both are thoroughly ordinary model changes and neither may touch the value. An MLTEXT column
 * already holds the **serialized** MLText, so anything that wraps it again turns the user's
 * `{"en":"hello","ru":"privet"}` into `{"en":"{\"en\":\"hello\",\"ru\":\"privet\"}"}` - a JSON
 * document wearing an English label. That is corruption rather than loss: the row is counted
 * `processed` and the migration reports success.
 *
 * Nothing here writes down what the stored form should look like. Each test reads the physical
 * column before the change and requires the same bytes back afterwards, so it cannot pass by
 * agreeing with a serializer that is itself wrong.
 */
class DbMlTextMultiplicityChangeTest : DbRecordsTestBase() {

    private val value = MLText(
        Locale.ENGLISH to "hello",
        Locale("ru") to "privet"
    )

    private fun registerDescr(multiple: Boolean) {
        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("descr")
                    withType(AttributeType.MLTEXT)
                    withMultiple(multiple)
                }
            )
        )
    }

    /**
     * The schema is reconciled by the next mutation of the table, and this is that mutation.
     */
    private fun triggerTheTransition() {
        createRecord("someKey" to "unrelated")
    }

    private fun drain() {
        val schemaCtx = getTableCtx().getSchemaCtx()
        DbBatchTaskEngine(schemaCtx.dataSourceCtx).drainSchemaOnce(schemaCtx)
    }

    /**
     * The `descr` column exactly as it physically is - a `String`, or an array of them.
     */
    private fun storedDescr(rec: EntityRef): Any? {
        val rawService = DbDataServiceImpl(
            DbEntity::class.java,
            DbDataServiceConfig.create {
                withTable(tableRef.table)
                withIncludeBackupColumns(true)
            },
            getTableCtx().getSchemaCtx()
        )
        rawService.resetColumnsCache()
        return TxnContext.doInNewTxn(readOnly = true) {
            rawService.findRaw(
                Predicates.eq(DbEntity.EXT_ID, rec.getLocalId()),
                emptyList(),
                DbFindPage.ALL,
                emptyList(),
                emptyList(),
                emptyList(),
                false
            ).entities.single()["descr"]
        }
    }

    private fun asElements(stored: Any?): List<Any?> {
        return when (stored) {
            is Collection<*> -> stored.toList()
            is Array<*> -> stored.toList()
            else -> listOf(stored)
        }
    }

    @Test
    fun makingAnMlTextAttributeMultipleLeavesItsValueAloneTest() {

        // The in-memory backend rewrites a column definition without converting its values, so
        // every type change there takes the shadow path and there is no in-place widening to assert.
        assumeColumnValuesConvertedInPlace()

        registerDescr(multiple = false)
        val rec = createRecord("descr" to value)
        val before = storedDescr(rec)

        registerDescr(multiple = true)
        triggerTheTransition()
        drain()

        assertThat(getTableCtx().getAllPhysicalColumns().map { it.name })
            .describedAs(
                "widening an MLTEXT column is `ALTER ... USING array[col]` - O(values), in place, " +
                    "and right. A backup column here means a whole column was copied aside to " +
                    "achieve what one cheap ALTER already did"
            )
            .noneMatch { it.startsWith("__backup_") }
        assertThat(asElements(storedDescr(rec)))
            .describedAs("the serialized MLText must be the same bytes it was before the change")
            .containsExactly(before)
    }

    @Test
    fun makingAnMlTextAttributeSingleLeavesItsValueAloneTest() {

        registerDescr(multiple = true)
        val rec = createRecord("descr" to listOf(value))
        val before = asElements(storedDescr(rec)).single()

        registerDescr(multiple = false)
        triggerTheTransition()
        drain()

        assertThat(storedDescr(rec))
            .describedAs(
                "narrowing takes the shadow path, where the converter must pass an already " +
                    "serialized MLText through rather than wrap it in a second MLText"
            )
            .isEqualTo(before)
        assertThat(records.getAtt(rec, "descr?json").getAs(MLText::class.java))
            .describedAs("and the user still reads both locales back, not one JSON document")
            .isEqualTo(value)
    }
}
