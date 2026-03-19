package ru.citeck.ecos.data.sql.pg.records

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import ru.citeck.ecos.data.sql.pg.records.commons.DbRecordsTestBase
import ru.citeck.ecos.data.sql.records.DbRecordsControlAtts
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType
import ru.citeck.ecos.webapp.api.entity.EntityRef

class DbIdMappingCacheTest : DbRecordsTestBase() {

    @Test
    fun cacheHitAfterLookupTest() {
        registerType()
            .withAttributes(AttributeDef.create().withId("text"))
            .register()

        val ref = createRecord("text" to "value")

        // First lookup populates cache
        val id = dbRecordRefService.getIdByEntityRef(ref)
        assertThat(id).isGreaterThan(0)

        // Second lookup should return same result (from cache)
        val id2 = dbRecordRefService.getIdByEntityRef(ref)
        assertThat(id2).isEqualTo(id)

        // Reverse lookup
        val resolvedRef = dbRecordRefService.getEntityRefById(id)
        assertThat(resolvedRef).isEqualTo(ref)
    }

    @Test
    fun cacheConsistencyAfterMoveRefTest() {
        registerType()
            .withAttributes(
                AttributeDef.create().withId("text"),
                AttributeDef.create().withId("assoc").withType(AttributeType.ASSOC)
            )
            .register()

        val ref = createRecord("text" to "abc")
        val newRef = records.mutateAtt(ref, DbRecordsControlAtts.UPDATE_ID, "new-id")

        // Old ref should not resolve to the record anymore
        assertThat(records.getAtt(ref, "_notExists?bool").asBoolean()).isTrue()

        // New ref should resolve correctly
        assertThat(records.getAtt(newRef, "text").asText()).isEqualTo("abc")

        // Cache: old ref's movedTo should point to the new ref's id
        val newId = dbRecordRefService.getIdByEntityRef(newRef)
        val oldId = dbRecordRefService.getIdByEntityRef(ref)
        assertThat(oldId).isEqualTo(newId)

        // Reverse: newId should resolve to newRef
        val resolvedRef = dbRecordRefService.getEntityRefById(newId)
        assertThat(resolvedRef).isEqualTo(newRef)
    }

    @Test
    fun cacheConsistencyAfterSwapMoveRefTest() {
        registerType()
            .withLocalIdTemplate("\${scope}$\${id}")
            .withAttributes(
                AttributeDef.create().withId("scope")
            )
            .register()

        val initialRef = createRecord("scope" to "uiserv", "id" to "my-id")
        assertThat(initialRef.getLocalId()).isEqualTo("uiserv\$my-id")

        // Populate cache
        val initialId = dbRecordRefService.getIdByEntityRef(initialRef)
        assertThat(initialId).isGreaterThan(0)

        // Move to new localId template
        registerType()
            .withLocalIdTemplate("\${scope}___\${id}")
            .withAttributes(
                AttributeDef.create().withId("scope")
            )
            .register()

        val newRef = records.mutateAtt(initialRef, DbRecordsControlAtts.UPDATE_ID, true)
        assertThat(newRef.getLocalId()).isEqualTo("uiserv___my-id")

        // Move back (triggers swap-ids branch in moveRef)
        registerType()
            .withLocalIdTemplate("\${scope}$\${id}")
            .withAttributes(
                AttributeDef.create().withId("scope")
            )
            .register()

        val swappedRef = records.mutateAtt(newRef, DbRecordsControlAtts.UPDATE_ID, true)
        assertThat(swappedRef).isEqualTo(initialRef)

        // After swap, cache must be consistent
        val idAfterSwap = dbRecordRefService.getIdByEntityRef(initialRef)
        assertThat(idAfterSwap).isEqualTo(initialId)

        val resolvedAfterSwap = dbRecordRefService.getEntityRefById(initialId)
        assertThat(resolvedAfterSwap).isEqualTo(initialRef)

        // newRef should point back to initialRef via movedTo
        val movedToRef = dbRecordRefService.getMovedToRef(newRef)
        assertThat(movedToRef).isEqualTo(initialRef)
    }

    @Test
    fun getOrCreateIdsPreservesOrderTest() {
        registerType()
            .withAttributes(AttributeDef.create().withId("text"))
            .register()

        val ref1 = createRecord("text" to "a")
        val ref2 = createRecord("text" to "b")
        val ref3 = createRecord("text" to "c")

        val refs = listOf(ref3, ref1, ref2)
        val ids = dbRecordRefService.getOrCreateIdByEntityRefs(refs)

        assertThat(ids).hasSize(3)
        // Verify order matches input
        for (i in refs.indices) {
            val expectedId = dbRecordRefService.getIdByEntityRef(refs[i])
            assertThat(ids[i]).isEqualTo(expectedId)
        }
    }

    @Test
    fun batchLookupCachesResultsTest() {
        registerType()
            .withAttributes(AttributeDef.create().withId("text"))
            .register()

        val ref1 = createRecord("text" to "a")
        val ref2 = createRecord("text" to "b")

        // Batch lookup
        val ids = dbRecordRefService.getIdByEntityRefs(listOf(ref1, ref2))
        assertThat(ids).hasSize(2)
        assertThat(ids[0]).isGreaterThan(0)
        assertThat(ids[1]).isGreaterThan(0)

        // Single lookups should return same results (from cache)
        assertThat(dbRecordRefService.getIdByEntityRef(ref1)).isEqualTo(ids[0])
        assertThat(dbRecordRefService.getIdByEntityRef(ref2)).isEqualTo(ids[1])

        // Reverse batch lookup
        val refs = dbRecordRefService.getEntityRefsByIds(ids)
        assertThat(refs).containsExactly(ref1, ref2)
    }

    @Test
    fun nonExistentRefReturnsMinusOneTest() {
        registerType()
            .withAttributes(AttributeDef.create().withId("text"))
            .register()

        val fakeRef = EntityRef.create(APP_NAME, RECS_DAO_ID, "does-not-exist")
        val id = dbRecordRefService.getIdByEntityRef(fakeRef)
        assertThat(id).isEqualTo(-1L)
    }
}
