package ru.citeck.ecos.data.sql.pg.records

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import ru.citeck.ecos.context.lib.auth.AuthContext
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.records2.RecordRef
import ru.citeck.ecos.records3.record.request.RequestContext

class DbRecordsDeleteTest : DbRecordsTestBase() {

    @Test
    fun test() {

        initWithTable(tableRef, true)

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("textAtt")
                }
            )
        )

        AuthContext.runAs("admin") {
            testAsAdmin()
        }
    }

    private fun testAsAdmin() {

        val elements = createElements()

        RequestContext.doWithTxn {
            records.delete(elements)
        }

        val recordsFromDao1 = records.query(baseQuery)
        assertThat(recordsFromDao1.getRecords()).isEmpty()

        val elements2 = createElements()
        elements2.forEach {
            RequestContext.doWithTxn {
                records.delete(it)
            }
        }
        val recordsFromDao2 = records.query(baseQuery)
        assertThat(recordsFromDao2.getRecords()).isEmpty()
    }

    private fun createElements(): List<RecordRef> {

        val recordsList = ArrayList<RecordRef>()
        for (i in 0..5) {
            recordsList.add(createRecord("textAtt" to "idx-$i"))
        }

        recordsList.forEach {
            setPerms(it, "admin")
        }

        val recordsFromDao0 = records.query(baseQuery)
        assertThat(recordsFromDao0.getRecords()).containsExactlyElementsOf(recordsList)

        return recordsList
    }
}
