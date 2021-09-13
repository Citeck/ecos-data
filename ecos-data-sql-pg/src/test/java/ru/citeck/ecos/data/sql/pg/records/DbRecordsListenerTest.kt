package ru.citeck.ecos.data.sql.pg.records

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import ru.citeck.ecos.commons.data.ObjectData
import ru.citeck.ecos.data.sql.records.listener.DbRecordsListener
import ru.citeck.ecos.data.sql.records.listener.DeletionEvent
import ru.citeck.ecos.data.sql.records.listener.MutationEvent
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.records2.RecordRef

class DbRecordsListenerTest : DbRecordsTestBase() {

    @Test
    fun test() {
        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("textAtt")
                }
            )
        )

        val attsToReq = mapOf("attKey" to "textAtt")

        val mutationEvents = mutableListOf<MutationEvent>()
        val mutationBeforeAtts = mutableListOf<ObjectData>()
        val mutationAfterAtts = mutableListOf<ObjectData>()
        val mutationBeforeRefs = mutableListOf<RecordRef>()
        val mutationAfterRefs = mutableListOf<RecordRef>()

        val mutationLists = listOf(
            mutationEvents,
            mutationBeforeAtts,
            mutationAfterAtts,
            mutationBeforeRefs,
            mutationAfterRefs
        )

        val deletionEvents = mutableListOf<DeletionEvent>()
        val deletionAtts = mutableListOf<ObjectData>()

        val deletionLists = listOf(
            deletionEvents,
            deletionAtts
        )

        val listener = object : DbRecordsListener {
            override fun onMutated(event: MutationEvent) {
                mutationEvents.add(event)
                mutationBeforeAtts.add(event.recordBefore.getAtts(attsToReq))
                mutationBeforeRefs.add(event.recordBefore.getRef())
                mutationAfterAtts.add(event.recordAfter.getAtts(attsToReq))
                mutationAfterRefs.add(event.recordAfter.getRef())
            }

            override fun onDeleted(event: DeletionEvent) {
                deletionEvents.add(event)
                deletionAtts.add(event.record.getAtts(attsToReq))
            }
        }
        recordsDao.addListener(listener)

        val rec = createRecord("textAtt" to "value")

        assertThat(mutationLists.all { it.size == 1 }).isTrue
        assertThat(mutationBeforeAtts[0].get("attKey").asText()).isEmpty()
        assertThat(mutationAfterAtts[0].get("attKey").asText()).isEqualTo("value")
        assertThat(mutationBeforeRefs[0]).isEqualTo(RecordRef.create(recordsDao.getId(), ""))
        assertThat(mutationAfterRefs[0]).isEqualTo(rec)
        assertThat(mutationEvents[0].isNewRecord).isTrue

        mutationLists.forEach { it.clear() }

        updateRecord(rec, "textAtt" to "afterValue")

        assertThat(mutationLists.all { it.size == 1 }).isTrue
        assertThat(mutationBeforeAtts[0].get("attKey").asText()).isEqualTo("value")
        assertThat(mutationAfterAtts[0].get("attKey").asText()).isEqualTo("afterValue")
        assertThat(mutationBeforeRefs[0]).isEqualTo(rec)
        assertThat(mutationAfterRefs[0]).isEqualTo(rec)
        assertThat(mutationEvents[0].isNewRecord).isFalse

        records.delete(rec)

        assertThat(deletionEvents).hasSize(1)
        assertThat(deletionAtts[0].get("attKey").asText()).isEqualTo("afterValue")

        mutationLists.forEach { it.clear() }
        deletionLists.forEach { it.clear() }

        recordsDao.removeListener(listener)

        val newRec = createRecord("textAtt" to "value")
        updateRecord(newRec, "textAtt" to "newVal")
        records.delete(newRec)

        assertThat(listOf(mutationLists, deletionLists).flatten().all { it.isEmpty() }).isTrue
    }
}
