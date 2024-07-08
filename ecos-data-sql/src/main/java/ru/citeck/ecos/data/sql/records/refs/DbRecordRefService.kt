package ru.citeck.ecos.data.sql.records.refs

import ru.citeck.ecos.data.sql.context.DbSchemaContext
import ru.citeck.ecos.data.sql.repo.entity.DbEntity
import ru.citeck.ecos.data.sql.service.DbDataService
import ru.citeck.ecos.data.sql.service.DbDataServiceConfig
import ru.citeck.ecos.data.sql.service.DbDataServiceImpl
import ru.citeck.ecos.records2.predicate.model.Predicates
import ru.citeck.ecos.txn.lib.TxnContext
import ru.citeck.ecos.webapp.api.entity.EntityRef
import ru.citeck.ecos.webapp.api.entity.toEntityRef

class DbRecordRefService(
    private val appName: String,
    schemaCtx: DbSchemaContext
) {

    companion object {
        private const val TXN_CACHE_MAX_SIZE = 1000
    }

    private val idsByRefsCacheTxnKey = Any()
    private val refsByIdsCacheTxnKey = Any()

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
    fun getOrCreateIdByEntityRefsMap(refs: Collection<EntityRef>): Map<EntityRef, Long> {
        val refsList = if (refs is List<EntityRef>) {
            refs
        } else {
            refs.toList()
        }
        val ids = getOrCreateIdByEntityRefs(refsList)
        val result = LinkedHashMap<EntityRef, Long>()
        for ((idx, ref) in refsList.withIndex()) {
            result[ref] = ids[idx]
        }
        return result
    }

    /**
     * Get or create record references
     */
    fun getOrCreateIdByEntityRefs(refs: List<EntityRef>): List<Long> {
        val txnCache = getIdsByRefsTxnCache()
        val refsIds = getIdByEntityRefs(refs)
        val result = LongArray(refs.size)
        for ((idx, id) in refsIds.withIndex()) {
            if (id == -1L) {
                val entity = DbRecordRefEntity()
                val ref = fixEntityRef(refs[idx])
                entity.extId = ref.toString()
                val createdId = dataService.save(entity).id
                result[idx] = createdId
                if (txnCache.size < TXN_CACHE_MAX_SIZE) {
                    txnCache[ref] = createdId
                }
            } else {
                result[idx] = id
            }
        }
        return result.toList()
    }

    fun getIdByEntityRef(ref: EntityRef): Long {
        return getIdByEntityRefs(listOf(ref))[0]
    }

    /**
     * Get record identifiers for references
     */
    fun getIdByEntityRefsMap(refs: Collection<EntityRef>): Map<EntityRef, Long> {
        val refsList = if (refs is List<EntityRef>) {
            refs
        } else {
            refs.toList()
        }
        val ids = getIdByEntityRefs(refsList)
        val result = LinkedHashMap<EntityRef, Long>()
        for (i in refs.indices) {
            val id = ids[i]
            if (id != -1L) {
                result[refsList[i]] = id
            }
        }
        return result
    }

    /**
     * Get record identifiers for references or -1 if reference is not registered
     */
    fun getIdByEntityRefs(refs: List<EntityRef>): List<Long> {

        val fixedRefs = refs.map { fixEntityRef(it) }

        val refsToQuery = ArrayList<EntityRef>()
        val idsByRefs = HashMap<EntityRef, Long>()

        val txnCache = getIdsByRefsTxnCache()
        if (txnCache.isEmpty()) {
            refsToQuery.addAll(fixedRefs)
        } else {
            for (ref in fixedRefs) {
                val idFromCache = txnCache[ref]
                if (idFromCache != null) {
                    idsByRefs[ref] = idFromCache
                } else {
                    refsToQuery.add(ref)
                }
            }
        }

        val predicate = Predicates.`in`(
            DbEntity.EXT_ID,
            refsToQuery.map { it.toString() }
        )
        val entities = dataService.findAll(predicate)
        for (entity in entities) {
            val ref = EntityRef.valueOf(entity.extId)
            idsByRefs[ref] = entity.id
            if (txnCache.size < TXN_CACHE_MAX_SIZE) {
                txnCache[ref] = entity.id
            }
        }
        return fixedRefs.map { idsByRefs[it] ?: -1 }
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

        val txnCache = getRefsByIdsTxnCache()

        val idsToQuery = ArrayList<Long>()
        val refsByIds = HashMap<Long, EntityRef>()
        if (txnCache.isEmpty()) {
            idsToQuery.addAll(ids)
        } else {
            for (id in ids) {
                val ref = txnCache[id]
                if (ref != null) {
                    refsByIds[id] = ref
                } else {
                    idsToQuery.add(id)
                }
            }
        }

        val entities = dataService.findByIds(idsToQuery.toSet())
        if (entities.size != idsToQuery.size) {
            error("RecordRef's count doesn't match. Ids: $ids Refs: ${entities.map { it.extId }}")
        }

        for (entity in entities) {
            val ref = EntityRef.valueOf(entity.extId.toEntityRef())
            refsByIds[entity.id] = ref
            if (txnCache.size < TXN_CACHE_MAX_SIZE) {
                txnCache[entity.id] = ref
            }
        }
        return ids.map { refsByIds[it] ?: error("Ref doesn't found for id $it") }
    }

    fun createTableIfNotExists() {
        TxnContext.doInTxn {
            dataService.runMigrations(mock = false, diff = true)
        }
    }

    fun runMigrations(mock: Boolean, diff: Boolean): List<String> {
        return dataService.runMigrations(mock, diff)
    }

    fun resetColumnsCache() {
        dataService.resetColumnsCache()
    }

    fun getDataService(): DbDataService<DbRecordRefEntity> {
        return dataService
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

    private fun getRefsByIdsTxnCache(): MutableMap<Long, EntityRef> {
        return TxnContext.getTxnOrNull()?.getData(refsByIdsCacheTxnKey) { HashMap() } ?: HashMap()
    }

    private fun getIdsByRefsTxnCache(): MutableMap<EntityRef, Long> {
        return TxnContext.getTxnOrNull()?.getData(idsByRefsCacheTxnKey) { HashMap() } ?: HashMap()
    }
}
