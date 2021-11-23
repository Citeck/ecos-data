package ru.citeck.ecos.data.sql.pg.records

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import ru.citeck.ecos.commons.data.ObjectData
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType
import ru.citeck.ecos.records2.RecordRef
import ru.citeck.ecos.records2.predicate.model.Predicate
import ru.citeck.ecos.records2.predicate.model.Predicates
import ru.citeck.ecos.records3.record.dao.atts.RecordAttsDao

class DbRecordsAssocTest : DbRecordsTestBase() {

    @Test
    fun test() {

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("textAtt")
                    withType(AttributeType.TEXT)
                },
                AttributeDef.create {
                    withId("assocAtt")
                    withType(AttributeType.ASSOC)
                },
                AttributeDef.create {
                    withId("multiAssocAtt")
                    withType(AttributeType.ASSOC)
                    withMultiple(true)
                }
            )
        )

        val txtValue = "txt-value"

        val rec0 = createRecord("textAtt" to txtValue)
        val rec1 = createRecord("assocAtt" to rec0)

        assertThat(records.getAtt(rec1, "assocAtt?id").asText()).isEqualTo(rec0.toString())
        assertThat(records.getAtt(rec1, "assocAtt.textAtt").asText()).isEqualTo(txtValue)

        val execQuery = { condition: Predicate ->
            records.query(
                baseQuery.copy {
                    withQuery(
                        Predicates.and(
                            condition,
                            Predicates.eq("_type", REC_TEST_TYPE_REF)
                        )
                    )
                }
            )
        }

        val queryRes = execQuery(Predicates.eq("assocAtt", rec0))
        assertThat(queryRes.getRecords()).hasSize(1)
        assertThat(queryRes.getRecords()).containsExactly(rec1)

        assertThat(execQuery(Predicates.eq("assocAtt", rec1)).getRecords()).isEmpty()

        val rec2TxtVal = "bbb"
        val rec3TxtVal = "ccc"
        val rec4TxtVal = "aaa"

        val rec2 = createRecord("textAtt" to rec2TxtVal)
        val rec3 = createRecord("textAtt" to rec3TxtVal)
        val rec4 = createRecord("textAtt" to rec4TxtVal)

        updateRecord(rec1, "multiAssocAtt" to listOf(rec2, rec3, rec4))

        val multiAssocRefValue = records.getAtt(rec1, "multiAssocAtt[]?id").asStrList()
        assertThat(multiAssocRefValue).containsExactly(rec2.toString(), rec3.toString(), rec4.toString())

        val multiAssocTxtValue = records.getAtt(rec1, "multiAssocAtt[].textAtt").asStrList()
        assertThat(multiAssocTxtValue).containsExactly(rec2TxtVal, rec3TxtVal, rec4TxtVal)

        val extIdRef = RecordRef.create("ext-src-id", "ext-record-id")
        val extRefRecValue = ObjectData.create("""{"aa":"bb"}""")
        records.register(object : RecordAttsDao {
            override fun getId() = extIdRef.sourceId
            override fun getRecordAtts(recordId: String): Any? {
                if (recordId == extIdRef.id) {
                    return extRefRecValue
                }
                return null
            }
        })

        val rec5 = createRecord("assocAtt" to extIdRef)
        assertThat(records.getAtt(rec5, "assocAtt.aa").asText()).isEqualTo("bb")

        // printQueryRes("SELECT * FROM ${tableRef.withTable("ecos_record_ref").fullName}")
        // printQueryRes("SELECT * FROM ${tableRef.fullName}")

        // todo add query by array support and test for it
    }
}