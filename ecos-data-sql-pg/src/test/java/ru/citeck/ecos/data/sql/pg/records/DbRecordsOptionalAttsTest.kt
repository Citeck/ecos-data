package ru.citeck.ecos.data.sql.pg.records

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import ru.citeck.ecos.commons.data.DataValue
import ru.citeck.ecos.data.sql.pg.records.commons.DbRecordsTestBase
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef

class DbRecordsOptionalAttsTest : DbRecordsTestBase() {

    @Test
    fun test() {

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("test-str")
                }
            )
        )

        val rec = createRecord("test-str" to "value")
        assertThat(records.getAtt(rec, "test-str").asText()).isEqualTo("value")
        assertThat(getColumns()).noneMatch { it.name == "_proc" || it.name == "_doc_num" }

        val procData = DataValue.create(
            """
            [
                {
                    "procInstanceId": "123123123",
                    "procDefId": "qqqq-wwww-ddd"
                },
                {
                    "procInstanceId": "aaa-www-dd",
                    "procDefId": "123-123-123"
                }
            ]
        """
        )

        updateRecord(rec, "_proc" to procData)

        println(selectRecFromDb(rec, "_proc"))

        assertThat(records.getAtt(rec, "_proc[]?json")).isEqualTo(procData)
        assertThat(records.getAtt(rec, "_proc?json")).isEqualTo(procData.get(0))

        updateRecord(rec, "test-str" to "value22")
        assertThat(records.getAtt(rec, "test-str").asText()).isEqualTo("value22")

        assertThat(records.getAtt(rec, "_proc[]?json")).isEqualTo(procData)
        assertThat(records.getAtt(rec, "_proc?json")).isEqualTo(procData.get(0))

        updateRecord(rec, "_state" to "draft")
        updateRecord(rec, "_proc" to procData)
    }
}
