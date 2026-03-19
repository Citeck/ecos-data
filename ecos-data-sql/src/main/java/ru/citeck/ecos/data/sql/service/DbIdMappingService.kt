package ru.citeck.ecos.data.sql.service

import com.github.benmanes.caffeine.cache.Cache
import com.github.benmanes.caffeine.cache.Caffeine
import ru.citeck.ecos.data.sql.context.DbTableContext
import ru.citeck.ecos.data.sql.repo.entity.DbEntity
import ru.citeck.ecos.records2.predicate.model.Predicates
import ru.citeck.ecos.records2.predicate.model.ValuePredicate

/**
 * Base service for Long <-> String mapping tables with non-transactional Caffeine cache.
 *
 * All mapping entities must have `id: Long` and `extId: String` properties.
 * If the entity has a `movedTo` column, it is used to resolve the effective id
 * in the extId->id direction (e.g. for record ref redirects).
 */
open class DbIdMappingService<T : Any>(
    private val dataService: DbDataService<T>,
    cacheMaxSize: Long = DEFAULT_CACHE_MAX_SIZE
) {

    companion object {
        const val DEFAULT_CACHE_MAX_SIZE = 10_000L

        private const val MOVED_TO_COLUMN = "__moved_to"
    }

    private val entityMapper = dataService.getEntityMapper()

    private val idGetter = entityMapper.getEntityColumnByColumnName(DbEntity.ID)!!.getter
    private val movedToGetter = entityMapper.getEntityColumnByColumnName(MOVED_TO_COLUMN)?.getter

    /**
     * extId -> CachedId(effectiveId, physicalId).
     * physicalId is stored alongside effectiveId so that [invalidate] can clean
     * [extIdByIdCache] without requiring the caller to pass both keys.
     */
    private val idByExtIdCache: Cache<String, CachedId> = Caffeine.newBuilder()
        .maximumSize(cacheMaxSize)
        .build()

    /** physicalId -> extId */
    private val extIdByIdCache: Cache<Long, String> = Caffeine.newBuilder()
        .maximumSize(cacheMaxSize)
        .build()

    fun getExtIdById(id: Long): String {
        extIdByIdCache.getIfPresent(id)?.let { return it }
        val entity = dataService.findById(id) ?: return ""
        val extId = entityMapper.getExtId(entity)
        if (extId.isNotEmpty()) {
            cacheEntity(entity, extId)
        }
        return extId
    }

    fun getIdByExtId(extId: String): Long {
        idByExtIdCache.getIfPresent(extId)?.let { return it.effectiveId }
        val entity = dataService.findByExtId(extId) ?: return -1L
        val resolvedExtId = entityMapper.getExtId(entity)
        cacheEntity(entity, resolvedExtId)
        return resolveId(entity)
    }

    fun getIdsByExtIds(extIds: Collection<String>): Map<String, Long> {
        if (extIds.isEmpty()) {
            return emptyMap()
        }

        val resolved = HashMap<String, Long>(extIds.size)
        val toQuery = mutableListOf<String>()

        for (extId in extIds) {
            val cached = idByExtIdCache.getIfPresent(extId)
            if (cached != null) {
                resolved[extId] = cached.effectiveId
            } else {
                toQuery.add(extId)
            }
        }

        if (toQuery.isNotEmpty()) {
            val entities = dataService.findAll(Predicates.`in`(DbEntity.EXT_ID, toQuery))
            for (entity in entities) {
                val extId = entityMapper.getExtId(entity)
                resolved[extId] = resolveId(entity)
                cacheEntity(entity, extId)
            }
        }

        // preserve input order
        val result = LinkedHashMap<String, Long>(extIds.size)
        for (extId in extIds) {
            resolved[extId]?.let { result[extId] = it }
        }
        return result
    }

    fun getExtIdsByIds(ids: Collection<Long>): Map<Long, String> {
        if (ids.isEmpty()) {
            return emptyMap()
        }

        val resolved = HashMap<Long, String>(ids.size)
        val toQuery = mutableListOf<Long>()

        for (id in ids) {
            val cached = extIdByIdCache.getIfPresent(id)
            if (cached != null) {
                resolved[id] = cached
            } else {
                toQuery.add(id)
            }
        }

        if (toQuery.isNotEmpty()) {
            val entities = dataService.findAll(
                ValuePredicate(DbEntity.ID, ValuePredicate.Type.IN, toQuery)
            )
            for (entity in entities) {
                val extId = entityMapper.getExtId(entity)
                resolved[getId(entity)] = extId
                cacheEntity(entity, extId)
            }
        }

        // preserve input order
        val result = LinkedHashMap<Long, String>(ids.size)
        for (id in ids) {
            resolved[id]?.let { result[id] = it }
        }
        return result
    }

    fun getOrCreateId(extId: String): Long {
        idByExtIdCache.getIfPresent(extId)?.let { return it.effectiveId }
        val entity = entityMapper.convertToEntity(mapOf(DbEntity.EXT_ID to extId))
        val id = dataService.saveAtomicallyOrGetExistingByExtId(entity)
        idByExtIdCache.put(extId, CachedId(id, id))
        extIdByIdCache.put(id, extId)
        return id
    }

    /**
     * Get or create ids for extIds.
     * Each missing entry is created via [DbDataService.saveAtomicallyOrGetExistingByExtId]
     * (one new transaction per item) for concurrent safety.
     * For entities with `movedTo`, a tombstone redirect is treated as "already existing"
     * and the effective (redirected) id is returned.
     */
    fun getOrCreateIds(extIds: Collection<String>): Map<String, Long> {
        if (extIds.isEmpty()) {
            return emptyMap()
        }

        // getIdsByExtIds already preserves input order
        val result = LinkedHashMap(getIdsByExtIds(extIds))

        for (extId in extIds) {
            if (!result.containsKey(extId)) {
                val entity = entityMapper.convertToEntity(mapOf(DbEntity.EXT_ID to extId))
                val id = dataService.saveAtomicallyOrGetExistingByExtId(entity)
                result[extId] = id
                idByExtIdCache.put(extId, CachedId(id, id))
                extIdByIdCache.put(id, extId)
            }
        }

        return result
    }

    fun getExistingIdsInAnyOrder(extIds: Collection<String>, filter: (String) -> Boolean = { true }): List<Long> {
        val filtered = extIds.filter(filter)
        if (filtered.isEmpty()) {
            return emptyList()
        }
        val result = mutableListOf<Long>()
        val toQuery = mutableListOf<String>()

        for (extId in filtered) {
            val cached = idByExtIdCache.getIfPresent(extId)
            if (cached != null) {
                result.add(cached.effectiveId)
            } else {
                toQuery.add(extId)
            }
        }

        if (toQuery.isNotEmpty()) {
            val entities = dataService.findAll(Predicates.inVals(DbEntity.EXT_ID, toQuery))
            for (entity in entities) {
                val extId = entityMapper.getExtId(entity)
                result.add(resolveId(entity))
                cacheEntity(entity, extId)
            }
        }

        return result
    }

    /**
     * Invalidate cache entries by extId.
     * Cleans both caches consistently using the stored physicalId.
     */
    fun invalidate(extId: String) {
        val cached = idByExtIdCache.getIfPresent(extId)
        idByExtIdCache.invalidate(extId)
        if (cached != null) {
            extIdByIdCache.invalidate(cached.physicalId)
        }
    }

    /**
     * Invalidate cache entries by physical id.
     * Cleans both caches consistently using the stored extId.
     */
    fun invalidateById(physicalId: Long) {
        val extId = extIdByIdCache.getIfPresent(physicalId)
        extIdByIdCache.invalidate(physicalId)
        if (extId != null) {
            idByExtIdCache.invalidate(extId)
        }
    }

    fun findByExtId(extId: String): T? {
        return dataService.findByExtId(extId)
    }

    fun findById(id: Long): T? {
        return dataService.findById(id)
    }

    fun save(entity: T): T {
        return dataService.save(entity)
    }

    fun getTableContext(): DbTableContext {
        return dataService.getTableContext()
    }

    fun resetColumnsCache() {
        dataService.resetColumnsCache()
    }

    fun runMigrations(mock: Boolean, diff: Boolean): List<String> {
        return dataService.runMigrations(mock, diff)
    }

    private fun getId(entity: T): Long {
        return idGetter(entity) as Long
    }

    private fun resolveId(entity: T): Long {
        val id = getId(entity)
        val movedTo = movedToGetter?.invoke(entity) as? Long
        return movedTo ?: id
    }

    private fun cacheEntity(entity: T, extId: String) {
        val physicalId = getId(entity)
        val effectiveId = resolveId(entity)
        idByExtIdCache.put(extId, CachedId(effectiveId, physicalId))
        extIdByIdCache.put(physicalId, extId)
    }

    private data class CachedId(val effectiveId: Long, val physicalId: Long)
}
