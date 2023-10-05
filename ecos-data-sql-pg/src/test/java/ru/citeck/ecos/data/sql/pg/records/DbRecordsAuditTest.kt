package ru.citeck.ecos.data.sql.pg.records

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import ru.citeck.ecos.commons.data.ObjectData
import ru.citeck.ecos.context.lib.auth.AuthContext
import ru.citeck.ecos.data.sql.records.DbRecordsControlAtts
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.records2.RecordConstants
import ru.citeck.ecos.webapp.api.entity.EntityRef
import java.lang.Exception
import java.time.Instant

class DbRecordsAuditTest : DbRecordsTestBase() {

    @BeforeEach
    fun beforeEach() {
        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("text")
                }
            )
        )
    }

    @Test
    fun test() {
        val beforeCreation = Instant.now()

        val ref = AuthContext.runAs("admin") {
            createRecord("text" to "abc")
        }
        assertThat(getModifier(ref)).isEqualTo("admin")
        assertThat(getCreator(ref)).isEqualTo("admin")
        assertThat(getModified(ref)).isAfter(beforeCreation)
        assertThat(getCreated(ref)).isAfter(beforeCreation)
        assertThat(getCreated(ref)).isEqualTo(getModified(ref))

        val customCreator = "custom-creator"
        val customModifier = "custom-modifier"
        val customCreated = Instant.parse("2021-01-01T00:00:00Z")
        val customModified = Instant.parse("2022-01-01T00:00:00Z")
        val mutAtts = mapOf(
            DbRecordsControlAtts.DISABLE_AUDIT to true,
            RecordConstants.ATT_CREATED to customCreated,
            RecordConstants.ATT_CREATOR to customCreator,
            RecordConstants.ATT_MODIFIED to customModified,
            RecordConstants.ATT_MODIFIER to customModifier
        )
        fun assertCustomAtts(ref: EntityRef) {
            assertThat(getModifier(ref)).isEqualTo(customModifier)
            assertThat(getCreator(ref)).isEqualTo(customCreator)
            assertThat(getModified(ref)).isEqualTo(customModified)
            assertThat(getCreated(ref)).isEqualTo(customCreated)
        }

        assertThrows<Exception> {
            AuthContext.runAs("admin") {
                records.mutate(ref, mutAtts)
            }
        }

        records.mutate(ref, mutAtts)
        assertCustomAtts(ref)

        val ref2 = createRecord(ObjectData.create(mutAtts))
        assertCustomAtts(ref2)
    }

    private fun getModifier(ref: EntityRef): String {
        return records.getAtt(ref, RecordConstants.ATT_MODIFIER + "?localId").asText()
    }

    private fun getCreator(ref: EntityRef): String {
        return records.getAtt(ref, RecordConstants.ATT_CREATOR + "?localId").asText()
    }

    private fun getCreated(ref: EntityRef): Instant {
        return records.getAtt(ref, RecordConstants.ATT_CREATED).getAsInstantOrEpoch()
    }

    private fun getModified(ref: EntityRef): Instant {
        return records.getAtt(ref, RecordConstants.ATT_MODIFIED).getAsInstantOrEpoch()
    }
}
