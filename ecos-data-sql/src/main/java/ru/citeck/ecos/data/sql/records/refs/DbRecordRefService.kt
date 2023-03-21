package ru.citeck.ecos.data.sql.records.refs

import ru.citeck.ecos.data.sql.context.DbSchemaContext
import ru.citeck.ecos.data.sql.repo.entity.DbEntity
import ru.citeck.ecos.data.sql.service.DbDataService
import ru.citeck.ecos.data.sql.service.DbDataServiceConfig
import ru.citeck.ecos.data.sql.service.DbDataServiceImpl
import ru.citeck.ecos.records2.predicate.model.Predicates
import ru.citeck.ecos.webapp.api.entity.EntityRef

class DbRecordRefService(
    private val appName: String,
    schemaCtx: DbSchemaContext
) {

    private val dataService: DbDataService<DbRecordRefEntity> = DbDataServiceImpl(
        DbRecordRefEntity::class.java,
        DbDataServiceConfig.create {
            withTable(DbRecordRefEntity.TABLE)
        },
        schemaCtx
    )

    /**
     * Get or create record identifier for reference
     */
    fun getOrCreateIdByEntityRef(ref: EntityRef): Long {
        return getOrCreateIdByEntityRefs(listOf(ref))[0]
    }

    /**
     * Get or create record references
     */
    fun getOrCreateIdByEntityRefs(refs: List<EntityRef>): List<Long> {
        val refsIds = getIdByEntityRefs(refs)
        val result = LongArray(refs.size)
        for ((idx, id) in refsIds.withIndex()) {
            if (id == -1L) {
                val entity = DbRecordRefEntity()
                entity.extId = fixEntityRef(refs[idx]).toString()
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
    fun getIdByEntityRefs(refs: List<EntityRef>): List<Long> {
        val fixedRefs = refs.map { fixEntityRef(it) }
        val predicate = Predicates.`in`(
            DbEntity.EXT_ID,
            fixedRefs.map { it.toString() }
        )
        val entities = dataService.findAll(predicate)
        val entityByRef = entities.associateBy { EntityRef.valueOf(it.extId) }
        return fixedRefs.map { entityByRef[it]?.id ?: -1 }
    }

    fun getEntityRefsByIdsMap(ids: Collection<Long>): Map<Long, EntityRef> {
        if (ids.isEmpty()) {
            return emptyMap()
        }
        val idsList = ids.toList()
        val refs = getEntityRefsByIds(idsList)
        val result = HashMap<Long, EntityRef>(ids.size)
        for (idx in ids.indices) {
            result[idsList[idx]] = refs[idx]
        }
        return result
    }

    fun getEntityRefById(id: Long): EntityRef {
        return getEntityRefsByIds(listOf(id))[0]
    }

    fun getEntityRefsByIds(ids: List<Long>): List<EntityRef> {
        val entities = dataService.findByIds(ids.toSet())
        if (entities.size != ids.size) {
            error("RecordRef's count doesn't match. Ids: $ids Refs: ${entities.map { it.extId }}")
        }
        val entitiesById = entities.associate { it.id to EntityRef.valueOf(it.extId) }
        return ids.map { entitiesById[it] ?: error("Ref doesn't found for id $it") }
    }

    fun runMigrations(mock: Boolean, diff: Boolean): List<String> {
        return dataService.runMigrations(mock, diff)
    }

    private fun fixEntityRef(entityRef: EntityRef): EntityRef {
        if (entityRef.getAppName().isNotBlank()) {
            return entityRef
        }
        if (entityRef.getLocalId().startsWith("workspace://SpacesStore/")) {
            return EntityRef.create("alfresco", "", entityRef.getLocalId())
        }
        return entityRef.withDefaultAppName(appName)
    }
}
