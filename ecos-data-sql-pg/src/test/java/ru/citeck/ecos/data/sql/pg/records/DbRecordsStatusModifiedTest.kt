package ru.citeck.ecos.data.sql.pg.records

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import ru.citeck.ecos.data.sql.pg.records.commons.DbRecordsTestBase
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.status.dto.StatusDef
import ru.citeck.ecos.model.lib.type.dto.TypeInfo
import ru.citeck.ecos.model.lib.type.dto.TypeModelDef

class DbRecordsStatusModifiedTest : DbRecordsTestBase() {

    companion object {
        private const val TEST_ATT = "testAtt"

        private const val STATUS_1 = "status-1"
        private const val STATUS_2 = "status-2"
    }

    @Test
    fun test() {
        val atts = listOf(
            AttributeDef.create { withId(TEST_ATT) }
        )

        val statuses = listOf(
            StatusDef.create { withId(STATUS_1) },
            StatusDef.create { withId(STATUS_2) }
        )

        val model = TypeModelDef.create {
            withAttributes(atts)
            withStatuses(statuses)
        }

        registerType(
            TypeInfo.create {
                withId(REC_TEST_TYPE_ID)
                withModel(model)
                withDefaultStatus(STATUS_1)
            }
        )

        val newRecRef = createRecord()

        var status = records.getAtt(newRecRef, "_status?localId").asText()
        assertThat(status).isEqualTo(STATUS_1)

        val created = records.getAtt(newRecRef, "_created").getAsInstant()
        var modified = records.getAtt(newRecRef, "_modified").getAsInstant()
        val statusModified = records.getAtt(newRecRef, "_statusModified").getAsInstant()
        assertThat(created).isNotNull()
        assertThat(statusModified).isEqualTo(created)
        assertThat(modified).isEqualTo(created)

        updateRecord(newRecRef, TEST_ATT to "test")
        modified = records.getAtt(newRecRef, "_modified").getAsInstant()
        var newStatusModified = records.getAtt(newRecRef, "_statusModified").getAsInstant()
        assertThat(modified).isAfter(created)
        assertThat(newStatusModified).isEqualTo(statusModified)

        updateRecord(newRecRef, "_status" to STATUS_2)
        status = records.getAtt(newRecRef, "_status?localId").asText()
        assertThat(status).isEqualTo(STATUS_2)
        val newModified = records.getAtt(newRecRef, "_modified").getAsInstant()
        newStatusModified = records.getAtt(newRecRef, "_statusModified").getAsInstant()
        assertThat(modified).isAfter(created)
        assertThat(newModified).isAfter(modified)
        assertThat(newStatusModified).isNotEqualTo(statusModified)
        assertThat(newModified).isEqualTo(newStatusModified)
    }
}
