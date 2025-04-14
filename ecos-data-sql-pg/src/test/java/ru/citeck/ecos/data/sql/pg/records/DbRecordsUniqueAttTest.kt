package ru.citeck.ecos.data.sql.pg.records

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import ru.citeck.ecos.commons.data.ObjectData
import ru.citeck.ecos.data.sql.pg.records.commons.DbRecordsTestBase
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType

class DbRecordsUniqueAttTest : DbRecordsTestBase() {

    companion object {
        private const val NOT_UNIQUE_ATT = "notUniqueAtt"
        private const val UNIQUE_ATT = "uniqueAtt"
        private const val UNIQUE_ATT_1 = "uniqueAtt_1"
        private const val UNIQUE_ATT_2 = "uniqueAtt_2"
        private const val MULTIPLE_UNIQUE_ATT = "multipleUniqueAtt"
    }

    @Test
    fun test() {
        registerAtts(
            listOf(
                AttributeDef.create {
                    withId(NOT_UNIQUE_ATT)
                    withType(AttributeType.TEXT)
                },
                AttributeDef.create {
                    withId(UNIQUE_ATT)
                    withType(AttributeType.TEXT)
                    withConfig(
                        ObjectData.create().set("unique", true)
                    )
                },
                AttributeDef.create {
                    withId(UNIQUE_ATT_1)
                    withType(AttributeType.TEXT)
                    withConfig(
                        ObjectData.create().set("unique", true)
                    )
                },
                AttributeDef.create {
                    withId(UNIQUE_ATT_2)
                    withType(AttributeType.TEXT)
                    withConfig(
                        ObjectData.create().set("unique", true)
                    )
                },
                AttributeDef.create {
                    withId(MULTIPLE_UNIQUE_ATT)
                    withType(AttributeType.TEXT)
                    withMultiple(true)
                    withConfig(
                        ObjectData.create().set("unique", true)
                    )
                }
            )
        )

        createRecord(
            NOT_UNIQUE_ATT to "not unique att",
            UNIQUE_ATT to "unique att",
        )

        var ex = assertThrows<Exception> {
            createRecord(
                NOT_UNIQUE_ATT to "not unique att",
                UNIQUE_ATT to "unique att"
            )
        }
        assertThat(ex.message).contains("has non-unique attributes {\"uniqueAtt\":\"unique att\"}")

        var record = createRecord(
            NOT_UNIQUE_ATT to "not unique att",
            UNIQUE_ATT to "unique att test"
        )
        var attValue = records.getAtt(record, UNIQUE_ATT).asText()
        assertThat(attValue).isEqualTo("unique att test")

        ex = assertThrows<Exception> {
            updateRecord(
                record,
                NOT_UNIQUE_ATT to "not unique att 11",
                UNIQUE_ATT to "unique att"
            )
        }
        assertThat(ex.message).contains("$record has non-unique attributes {\"uniqueAtt\":\"unique att\"}")

        val updatedRecord = updateRecord(
            record,
            NOT_UNIQUE_ATT to "not unique att 11",
            UNIQUE_ATT to "another unique text"
        )

        attValue = records.getAtt(updatedRecord, UNIQUE_ATT).asText()
        assertThat(attValue).isEqualTo("another unique text")

        createRecord(MULTIPLE_UNIQUE_ATT to listOf("text1", "text2"))
        record = createRecord(MULTIPLE_UNIQUE_ATT to listOf("text1", "text2"))

        val attListValue = records.getAtt(record, "$MULTIPLE_UNIQUE_ATT[]?str").asStrList()
        assertThat(attListValue).isEqualTo(listOf("text1", "text2"))

        record = createRecord(
            UNIQUE_ATT to "value 1",
            UNIQUE_ATT_1 to "value 1",
            UNIQUE_ATT_2 to "value 1"
        )

        ex = assertThrows<Exception> {
            createRecord(
                UNIQUE_ATT to "value 1",
                UNIQUE_ATT_1 to "value 2",
                UNIQUE_ATT_2 to "value 2"
            )
        }
        assertThat(ex.message).contains("has non-unique attributes {\"uniqueAtt\":\"value 1\"}")

        ex = assertThrows<Exception> {
            createRecord(
                UNIQUE_ATT to "value 1",
                UNIQUE_ATT_1 to "value 1",
                UNIQUE_ATT_2 to "value 2"
            )
        }
        assertThat(ex.message).contains("has non-unique attributes {\"uniqueAtt\":\"value 1\",\"uniqueAtt_1\":\"value 1\"}")
    }
}
