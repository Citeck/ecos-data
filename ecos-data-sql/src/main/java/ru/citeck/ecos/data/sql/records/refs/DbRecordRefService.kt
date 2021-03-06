package ru.citeck.ecos.data.sql.records.refs

import ru.citeck.ecos.data.sql.repo.entity.DbEntity
import ru.citeck.ecos.data.sql.service.DbDataService
import ru.citeck.ecos.records2.RecordRef
import ru.citeck.ecos.records2.predicate.model.Predicates

class DbRecordRefService(
    private val appName: String,
    private val dataService: DbDataService<DbRecordRefEntity>
) {
    /**
     * Get or create record identifier for reference
     */
    fun getOrCreateIdByRecordRef(ref: RecordRef): Long {
        return getOrCreateIdByRecordRefs(listOf(ref))[0]
    }

    /**
     * Get or create record references
     */
    fun getOrCreateIdByRecordRefs(refs: List<RecordRef>): List<Long> {
        val refsIds = getIdByRecordRefs(refs)
        val result = LongArray(refs.size)
        for ((idx, id) in refsIds.withIndex()) {
            if (id == -1L) {
                val entity = DbRecordRefEntity()
                entity.extId = fixRecordRef(refs[idx]).toString()
                result[idx] = dataService.save(entity).id
            } else {
                result[idx] = id
            }
        }
        return result.toList()
    }

    /**
     * Get record identifiers for references or -1 if reference is not registered
     */
    fun getIdByRecordRefs(refs: List<RecordRef>): List<Long> {
        val fixedRefs = refs.map { fixRecordRef(it) }
        val predicate = Predicates.`in`(
            DbEntity.EXT_ID,
            fixedRefs.map { it.toString() }
        )
        val entities = dataService.findAll(predicate)
        val entityByRef = entities.associateBy { RecordRef.valueOf(it.extId) }
        return fixedRefs.map { entityByRef[it]?.id ?: -1 }
    }

    fun getRecordRefsByIdsMap(ids: Collection<Long>): Map<Long, RecordRef> {
        if (ids.isEmpty()) {
            return emptyMap()
        }
        val idsList = ids.toList()
        val refs = getRecordRefsByIds(idsList)
        val result = HashMap<Long, RecordRef>(ids.size)
        for (idx in ids.indices) {
            result[idsList[idx]] = refs[idx]
        }
        return result
    }

    fun getRecordRefById(id: Long): RecordRef {
        return getRecordRefsByIds(listOf(id))[0]
    }

    fun getRecordRefsByIds(ids: List<Long>): List<RecordRef> {
        val entities = dataService.findById(ids.toSet())
        if (entities.size != ids.size) {
            error("RecordRef's count doesn't match. Ids: $ids Refs: ${entities.map { it.extId }}")
        }
        val entitiesById = entities.associate { it.id to RecordRef.valueOf(it.extId) }
        return ids.map { entitiesById[it] ?: error("Ref doesn't found for id $it") }
    }

    fun runMigrations(mock: Boolean, diff: Boolean): List<String> {
        return dataService.runMigrations(mock, diff, true)
    }

    private fun fixRecordRef(recordRef: RecordRef): RecordRef {
        if (recordRef.appName.isNotBlank()) {
            return recordRef
        }
        if (recordRef.id.startsWith("workspace://SpacesStore/")) {
            return RecordRef.create("alfresco", "", recordRef.id)
        }
        return recordRef.withDefaultAppName(appName)
    }
}
