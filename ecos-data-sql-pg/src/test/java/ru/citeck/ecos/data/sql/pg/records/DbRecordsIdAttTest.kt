package ru.citeck.ecos.data.sql.pg.records

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import ru.citeck.ecos.context.lib.auth.AuthContext
import ru.citeck.ecos.data.sql.pg.records.commons.DbRecordsTestBase
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType
import ru.citeck.ecos.records2.RecordConstants

class DbRecordsIdAttTest : DbRecordsTestBase() {

    companion object {
        private const val ATT_TEST = "attTest"
    }

    @Test
    fun uniqueIdTest() {
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

    @Test
    fun validateIdTest() {
        registerAtts(
            listOf(
                AttributeDef.create {
                    withId(ATT_TEST)
                    withType(AttributeType.TEXT)
                }
            )
        )

        var record = AuthContext.runAs("user") {
            createRecord(
                RecordConstants.ATT_ID to "abc"
            )
        }
        var attValue = records.getAtt(record, RecordConstants.ATT_ID).asText()
        assertThat(attValue).isEqualTo("abc")

        record = AuthContext.runAs("user") {
            createRecord(
                RecordConstants.ATT_ID to "a"
            )
        }
        attValue = records.getAtt(record, RecordConstants.ATT_ID).asText()
        assertThat(attValue).isEqualTo("a")

        record = AuthContext.runAs("user") {
            createRecord(
                RecordConstants.ATT_ID to "1"
            )
        }
        attValue = records.getAtt(record, RecordConstants.ATT_ID).asText()
        assertThat(attValue).isEqualTo("1")

        record = AuthContext.runAs("user") {
            createRecord(
                RecordConstants.ATT_ID to "a-b"
            )
        }
        attValue = records.getAtt(record, RecordConstants.ATT_ID).asText()
        assertThat(attValue).isEqualTo("a-b")

        record = AuthContext.runAs("user") {
            createRecord(
                RecordConstants.ATT_ID to "a$/.-b"
            )
        }
        attValue = records.getAtt(record, RecordConstants.ATT_ID).asText()
        assertThat(attValue).isEqualTo("a$/.-b")

        record = AuthContext.runAs("user") {
            createRecord(
                RecordConstants.ATT_ID to "a.b"
            )
        }
        attValue = records.getAtt(record, RecordConstants.ATT_ID).asText()
        assertThat(attValue).isEqualTo("a.b")

        record = AuthContext.runAs("user") {
            createRecord(
                RecordConstants.ATT_ID to "a123b-123"
            )
        }
        attValue = records.getAtt(record, RecordConstants.ATT_ID).asText()
        assertThat(attValue).isEqualTo("a123b-123")

        record = AuthContext.runAs("user") {
            createRecord(
                RecordConstants.ATT_ID to "C8l1G7sXfY8258OR8MsoP8UqhKCYoCtFvbPnG79hGmhoYFP9UFEJCPbyeTYEr157fC290S2VjvwbuUFE3JQVv1zQShFrBc6hd1Kxl88nTJuxXg8A1JYKWg403oz4jz7Y"
            )
        }
        attValue = records.getAtt(record, RecordConstants.ATT_ID).asText()
        assertThat(attValue).isEqualTo("C8l1G7sXfY8258OR8MsoP8UqhKCYoCtFvbPnG79hGmhoYFP9UFEJCPbyeTYEr157fC290S2VjvwbuUFE3JQVv1zQShFrBc6hd1Kxl88nTJuxXg8A1JYKWg403oz4jz7Y")

        var ex = assertThrows<Exception> {
            AuthContext.runAs("user") {
                createRecord(
                    RecordConstants.ATT_ID to "8LChLp53383qxUpdO2KAZagSs8wmN9qDxiSc1ryxB8i34X2nPZ8QNdc31r342IhiAB5UpQFxiC9NCu6uWNSmkL8EdXMX09KQG1rRMHwW26ZWC8DLE1s3bWCC340Ngo4oz"
                )
            }
        }
        assertThat(ex.message).contains("Invalid id: '8LChLp53383qxUpdO2KAZagSs8wmN9qDxiSc1ryxB8i34X2nPZ8QNdc31r342IhiAB5UpQFxiC9NCu6uWNSmkL8EdXMX09KQG1rRMHwW26ZWC8DLE1s3bWCC340Ngo4oz'. Max length 128")

        ex = assertThrows<Exception> {
            AuthContext.runAs("user") {
                createRecord(
                    RecordConstants.ATT_ID to "...1"
                )
            }
        }
        assertThat(ex.message).contains("Invalid id: '...1'. Valid pattern: '^(\\w+|\\w[\\w$/.-]+\\w)$'")

        ex = assertThrows<Exception> {
            AuthContext.runAs("user") {
                createRecord(
                    RecordConstants.ATT_ID to "///1"
                )
            }
        }
        assertThat(ex.message).contains("Invalid id: '///1'. Valid pattern: '^(\\w+|\\w[\\w$/.-]+\\w)$'")

        ex = assertThrows<Exception> {
            AuthContext.runAs("user") {
                createRecord(
                    RecordConstants.ATT_ID to "-abc-"
                )
            }
        }
        assertThat(ex.message).contains("Invalid id: '-abc-'. Valid pattern: '^(\\w+|\\w[\\w$/.-]+\\w)$'")
    }
}
