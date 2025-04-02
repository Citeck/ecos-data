package ru.citeck.ecos.data.sql.pg.records

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import ru.citeck.ecos.data.sql.pg.records.commons.DbRecordsTestBase
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.records2.predicate.model.Predicates

class DbRecordsEmptyMultiAttTest : DbRecordsTestBase() {

    companion object {
        const val ATT_TEXT = "textAtt"
    }

    @Test
    fun test() {
        registerAtts(
            listOf(
                AttributeDef.create {
                    withId(ATT_TEXT)
                    withMultiple(true)
                }
            )
        )

        fun testWithAtt(att: Any?) {
            val newRec = createRecord(ATT_TEXT to att)
            val queryRes = records.query(
                baseQuery.copy {
                    withQuery(Predicates.empty(ATT_TEXT))
                }
            )
            if (att !is List<*> || att.size == 0) {
                assertThat(queryRes.getRecords()).hasSize(1)
                assertThat(queryRes.getRecords()[0]).isEqualTo(newRec)
            } else {
                assertThat(queryRes.getRecords()).hasSize(0)
            }
            records.delete(newRec)
        }

        testWithAtt(null)
        testWithAtt(emptyList<Any>())
        testWithAtt(listOf("value"))
    }
}
