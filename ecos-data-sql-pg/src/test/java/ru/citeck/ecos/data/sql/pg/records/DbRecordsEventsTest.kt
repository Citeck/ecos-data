package ru.citeck.ecos.data.sql.pg.records

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import ru.citeck.ecos.data.sql.pg.records.commons.DbRecordsTestBase
import ru.citeck.ecos.data.sql.records.listener.DbRecordChangedEvent
import ru.citeck.ecos.data.sql.records.listener.DbRecordsListenerAdapter
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType
import java.util.*

class DbRecordsEventsTest : DbRecordsTestBase() {

    @Test
    fun assocsTest() {

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
            ),
            listOf(
                AttributeDef.create {
                    withId("systemTxtAtt")
                    withType(AttributeType.TEXT)
                },
                AttributeDef.create {
                    withId("systemAssocAtt")
                    withType(AttributeType.ASSOC)
                }
            )
        )
        val changedEvents = ArrayList<DbRecordChangedEvent>()

        mainCtx.dao.addListener(object : DbRecordsListenerAdapter() {
            override fun onChanged(event: DbRecordChangedEvent) {
                changedEvents.add(event)
            }
        })

        val assocRecs = (1..10).map { createRecord("textAtt" to "rec-$it") }
        val rec0 = createRecord(
            "assocAtt" to assocRecs[0],
            "multiAssocAtt" to listOf(assocRecs[1], assocRecs[2])
        )

        assertThat(changedEvents).isEmpty()

        updateRecord(rec0, "att_add_assocAtt" to assocRecs[2])

        assertThat(changedEvents).isEmpty()

        updateRecord(rec0, "att_add_multiAssocAtt" to listOf(assocRecs[2], assocRecs[3], assocRecs[4]))
        assertThat(changedEvents).hasSize(1)
        assertThat(changedEvents[0].assocs).hasSize(1)
        assertThat(changedEvents[0].assocs[0].assocId).isEqualTo("multiAssocAtt")
        assertThat(changedEvents[0].assocs[0].added).containsExactly(assocRecs[3], assocRecs[4])
        assertThat(changedEvents[0].assocs[0].removed).isEmpty()

        changedEvents.clear()

        updateRecord(rec0, "systemAssocAtt" to listOf(assocRecs[3]))
        assertThat(changedEvents).hasSize(1)
        assertThat(changedEvents[0].assocs).isEmpty()
        assertThat(changedEvents[0].systemAssocs).hasSize(1)
        assertThat(changedEvents[0].systemAssocs[0].assocId).isEqualTo("systemAssocAtt")
        assertThat(changedEvents[0].systemAssocs[0].added).containsExactly(assocRecs[3])
        assertThat(changedEvents[0].systemAssocs[0].removed).isEmpty()

        changedEvents.clear()

        updateRecord(rec0, "systemTxtAtt" to "abc")
        assertThat(changedEvents).hasSize(1)
        assertThat(changedEvents[0].systemAssocs).isEmpty()
        assertThat(changedEvents[0].systemBefore["systemTxtAtt"]).isNull()
        assertThat(changedEvents[0].systemAfter["systemTxtAtt"]).isEqualTo("abc")
    }
}
