package ru.citeck.ecos.data.sql.pg.records

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import ru.citeck.ecos.context.lib.auth.AuthContext
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.type.dto.TypePermsPolicy
import ru.citeck.ecos.records2.RecordRef
import ru.citeck.ecos.records3.record.dao.delete.DelStatus
import ru.citeck.ecos.txn.lib.TxnContext

class DbRecordsDeleteTest : DbRecordsTestBase() {

    @Test
    fun test() {

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("textAtt")
                }
            )
        )
        setPermsPolicy(TypePermsPolicy.OWN)

        AuthContext.runAs("admin") {
            testAsAdmin()
        }
    }

    private fun testAsAdmin() {

        val elements = createElements()

        TxnContext.doInTxn {
            records.delete(elements)
        }

        val recordsFromDao1 = records.query(baseQuery)
        assertThat(recordsFromDao1.getRecords()).isEmpty()

        val elements2 = createElements()
        elements2.forEach {
            TxnContext.doInTxn {
                records.delete(it)
            }
        }
        val recordsFromDao2 = records.query(baseQuery)
        assertThat(recordsFromDao2.getRecords()).isEmpty()
    }

    @Test
    fun testDeleteAndCreateWithSameId() {

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("textAtt")
                }
            )
        )

        val customId = "custom-id"

        val recRef = createRecord(
            "id" to customId,
            "textAtt" to "value"
        )
        assertThat(recRef.id).isEqualTo(customId)

        val delRes = records.delete(recRef)

        assertThat(delRes).isEqualTo(DelStatus.OK)

        val recRef2 = createRecord(
            "id" to customId,
            "textAtt" to "value2"
        )
        assertThat(recRef2.id).isEqualTo(customId)
        assertThat(records.getAtt(recRef2, "textAtt").asText()).isEqualTo("value2")
    }

    private fun createElements(): List<RecordRef> {

        val recordsList = ArrayList<RecordRef>()
        for (i in 0..5) {
            recordsList.add(createRecord("textAtt" to "idx-$i"))
        }

        recordsList.forEach {
            setAuthoritiesWithReadPerms(it, "admin")
        }

        val recordsFromDao0 = records.query(baseQuery)
        assertThat(recordsFromDao0.getRecords()).containsExactlyElementsOf(recordsList)

        return recordsList
    }
}
