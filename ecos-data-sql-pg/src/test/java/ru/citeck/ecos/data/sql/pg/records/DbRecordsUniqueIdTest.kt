package ru.citeck.ecos.data.sql.pg.records

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import ru.citeck.ecos.context.lib.auth.AuthContext
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType
import ru.citeck.ecos.records2.RecordConstants

class DbRecordsUniqueIdTest : DbRecordsTestBase() {

    companion object {
        private const val ATT_TEST = "attTest"
    }

    @Test
    fun test() {
        registerAtts(
            listOf(
                AttributeDef.create {
                    withId(ATT_TEST)
                    withType(AttributeType.TEXT)
                }
            )
        )

        val record = AuthContext.runAs("user") {
            createRecord(
                RecordConstants.ATT_ID to "test-id",
                ATT_TEST to "testText"
            )
        }

        val updatedRecord = updateRecord(record, ATT_TEST to "testText111")
        var attValue = records.getAtt(updatedRecord, ATT_TEST).asText()
        assertThat(attValue).isEqualTo("testText111")

        val ex = assertThrows<Exception> {
            AuthContext.runAs("user") {
                createRecord(
                    RecordConstants.ATT_ID to "test-id",
                    ATT_TEST to "testText"
                )
            }
        }
        assertThat(ex.message).contains("Record with id: 'test-id' already exists. The id must be unique.")

        val record0 = createRecord(
            RecordConstants.ATT_ID to "test-id-1",
            ATT_TEST to "testText"
        )

        createRecord(
            RecordConstants.ATT_ID to "test-id-1",
            ATT_TEST to "changed testText"
        )

        attValue = records.getAtt(record0, ATT_TEST).asText()
        assertThat(attValue).isEqualTo("changed testText")
    }
}
