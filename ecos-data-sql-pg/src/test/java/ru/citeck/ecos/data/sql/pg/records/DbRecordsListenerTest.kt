package ru.citeck.ecos.data.sql.pg.records

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import ru.citeck.ecos.commons.data.MLText
import ru.citeck.ecos.commons.data.ObjectData
import ru.citeck.ecos.data.sql.records.listener.*
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.status.dto.StatusDef
import ru.citeck.ecos.model.lib.type.dto.TypeInfo
import ru.citeck.ecos.model.lib.type.dto.TypeModelDef
import ru.citeck.ecos.webapp.api.entity.EntityRef

class DbRecordsListenerTest : DbRecordsTestBase() {

    @Test
    fun test() {
        registerType(
            TypeInfo.create {
                withId(REC_TEST_TYPE_ID)
                withModel(
                    TypeModelDef.create {
                        withAttributes(
                            listOf(
                                AttributeDef.create {
                                    withId("textAtt")
                                }
                            )
                        )
                        withStatuses(
                            listOf(
                                StatusDef.create {
                                    withId("draft")
                                    withName(MLText("olala"))
                                },
                                StatusDef.create {
                                    withId("approve")
                                    withName(MLText("olala-approve"))
                                }
                            )
                        )
                    }
                )
            }
        )

        val attsToReq = mapOf("attKey" to "textAtt")

        val mutationEvents = mutableListOf<DbRecordChangedEvent>()
        val mutationBeforeAtts = mutableListOf<ObjectData>()
        val mutationAfterAtts = mutableListOf<ObjectData>()
        val mutationRefs = mutableListOf<EntityRef>()

        val createdEvents = mutableListOf<EntityRef>()

        val mutationLists = listOf(
            mutationEvents,
            mutationBeforeAtts,
            mutationAfterAtts,
            mutationRefs
        )

        val deletionEvents = mutableListOf<DbRecordDeletedEvent>()
        val deletionAtts = mutableListOf<ObjectData>()

        val deletionLists = listOf(
            deletionEvents,
            deletionAtts
        )

        val statusChangedEvents = mutableListOf<Pair<String, String>>()

        val listener = object : DbRecordsListenerAdapter() {
            override fun onChanged(event: DbRecordChangedEvent) {
                mutationEvents.add(event)
                val beforeAtts = records.getAtts(event.before, attsToReq)
                val afterAtts = records.getAtts(event.after, attsToReq)
                mutationBeforeAtts.add(beforeAtts.getAtts())
                mutationRefs.add(records.getAtt(event.record, "?id").getAs(EntityRef::class.java)!!)
                mutationAfterAtts.add(afterAtts.getAtts())
            }
            override fun onDeleted(event: DbRecordDeletedEvent) {
                deletionEvents.add(event)
                deletionAtts.add(records.getAtts(event.record, attsToReq).getAtts())
            }
            override fun onCreated(event: DbRecordCreatedEvent) {
                createdEvents.add(records.getAtt(event.record, "?id").getAs(EntityRef::class.java)!!)
            }
            override fun onStatusChanged(event: DbRecordStatusChangedEvent) {
                val beforeStr = event.before.id
                val afterStr = event.after.id
                statusChangedEvents.add(beforeStr to afterStr)
            }
        }
        recordsDao.addListener(listener)

        val rec = createRecord("textAtt" to "value")

        assertThat(createdEvents).hasSize(1)

        assertThat(mutationLists.all { it.isEmpty() }).isTrue

        createdEvents.clear()
        mutationLists.forEach { it.clear() }

        updateRecord(rec, "textAtt" to "afterValue")

        assertThat(createdEvents).isEmpty()

        assertThat(mutationLists.all { it.size == 1 }).isTrue
        assertThat(mutationBeforeAtts[0].get("attKey").asText()).isEqualTo("value")
        assertThat(mutationAfterAtts[0].get("attKey").asText()).isEqualTo("afterValue")
        assertThat(mutationRefs[0]).isEqualTo(rec)

        mutationLists.forEach { it.clear() }

        updateRecord(rec, "_status" to "draft")

        assertThat(mutationLists.all { it.isEmpty() }).isTrue
        assertThat(statusChangedEvents).containsExactly("" to "draft")

        statusChangedEvents.clear()

        updateRecord(rec, "_status" to "approve")

        assertThat(mutationLists.all { it.isEmpty() }).isTrue
        assertThat(statusChangedEvents).containsExactly("draft" to "approve")

        statusChangedEvents.clear()

        records.delete(rec)

        assertThat(deletionEvents).hasSize(1)
        assertThat(deletionAtts[0].get("attKey").asText()).isEqualTo("afterValue")

        mutationLists.forEach { it.clear() }
        deletionLists.forEach { it.clear() }

        recordsDao.removeListener(listener)

        val newRec = createRecord("textAtt" to "value")
        updateRecord(newRec, "textAtt" to "newVal")
        records.delete(newRec)

        assertThat(listOf(mutationLists, deletionLists, listOf(createdEvents)).flatten().all { it.isEmpty() }).isTrue
    }
}
