package ru.citeck.ecos.data.sql.pg.records

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import ru.citeck.ecos.commons.data.ObjectData
import ru.citeck.ecos.data.sql.pg.records.commons.DbRecordsTestBase
import ru.citeck.ecos.data.sql.records.listener.DbRecordChangedEvent
import ru.citeck.ecos.data.sql.records.listener.DbRecordDeletedEvent
import ru.citeck.ecos.data.sql.records.listener.DbRecordParentChangedEvent
import ru.citeck.ecos.data.sql.records.listener.DbRecordsListenerAdapter
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType
import ru.citeck.ecos.records2.RecordConstants
import ru.citeck.ecos.webapp.api.entity.EntityRef
import ru.citeck.ecos.webapp.api.entity.toEntityRef
import java.util.*

class DbRecordsEventsTest : DbRecordsTestBase() {

    @Test
    fun parentChangedTest() {

        registerAtts(
            listOf(
                AttributeDef.create()
                    .withId("children")
                    .withType(AttributeType.ASSOC)
                    .withConfig(ObjectData.create().set("child", true))
                    .build(),
                AttributeDef.create()
                    .withId("children2")
                    .withType(AttributeType.ASSOC)
                    .withConfig(ObjectData.create().set("child", true))
                    .build()
            )
        )

        val parentChangedEvents = ArrayList<DbRecordParentChangedEvent>()
        recordsDao.addListener(object : DbRecordsListenerAdapter() {
            override fun onParentChanged(event: DbRecordParentChangedEvent) {
                parentChangedEvents.add(event)
            }
        })

        val parent0 = createRecord()
        val parent1 = createRecord()

        val child = createRecord("_parent" to parent0, "_parentAtt" to "children")
        fun assertParentAtts(expParent: EntityRef, expParentAtt: String) {
            assertThat(records.getAtt(child, "_parent?id").asText().toEntityRef()).isEqualTo(expParent)
            assertThat(records.getAtt(child, "_parentAtt?str").asText()).isEqualTo(expParentAtt)
        }
        assertParentAtts(parent0, "children")
        assertThat(parentChangedEvents).isEmpty()

        updateRecord(child, "_parent" to parent1)
        assertParentAtts(parent1, "children")

        assertThat(parentChangedEvents).hasSize(1)
        assertThat(parentChangedEvents[0].parentBefore).isEqualTo(parent0)
        assertThat(parentChangedEvents[0].parentAfter).isEqualTo(parent1)
        assertThat(parentChangedEvents[0].parentAttBefore).isEqualTo("children")
        assertThat(parentChangedEvents[0].parentAttAfter).isEqualTo("children")

        updateRecord(child, "_parent" to parent0, "_parentAtt" to "children2")
        assertParentAtts(parent0, "children2")

        assertThat(parentChangedEvents).hasSize(2)
        assertThat(parentChangedEvents[1].parentBefore).isEqualTo(parent1)
        assertThat(parentChangedEvents[1].parentAfter).isEqualTo(parent0)
        assertThat(parentChangedEvents[1].parentAttBefore).isEqualTo("children")
        assertThat(parentChangedEvents[1].parentAttAfter).isEqualTo("children2")

        updateRecord(child, "_parentAtt" to "children")
        assertParentAtts(parent0, "children")

        assertThat(parentChangedEvents).hasSize(3)
        assertThat(parentChangedEvents[2].parentBefore).isEqualTo(parent0)
        assertThat(parentChangedEvents[2].parentAfter).isEqualTo(parent0)
        assertThat(parentChangedEvents[2].parentAttBefore).isEqualTo("children2")
        assertThat(parentChangedEvents[2].parentAttAfter).isEqualTo("children")
    }

    @Test
    fun deletedTest() {

        registerAtts(
            listOf(
                AttributeDef.create()
                    .withId("children")
                    .withType(AttributeType.ASSOC)
                    .withConfig(ObjectData.create().set("child", true))
                    .build()
            )
        )

        val parent = createRecord()
        val child = createRecord(
            "id" to "child",
            RecordConstants.ATT_PARENT to parent,
            RecordConstants.ATT_PARENT_ATT to "children"
        )

        val deletedEvents = mutableListOf<DbRecordDeletedEvent>()

        recordsDao.addListener(object : DbRecordsListenerAdapter() {
            override fun onDeleted(event: DbRecordDeletedEvent) {
                deletedEvents.add(event)
                if (event.localRef.getLocalId() == "child") {
                    assertThat(records.getAtt(event.record, "_parent?id").toEntityRef()).isEqualTo(parent)
                }
            }
        })

        records.delete(parent)

        assertThat(deletedEvents).hasSize(2)
    }

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
