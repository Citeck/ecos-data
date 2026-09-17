package ru.citeck.ecos.data.sql.modelchange

import io.github.oshai.kotlinlogging.KotlinLogging
import ru.citeck.ecos.data.sql.records.DbRecordsDao
import ru.citeck.ecos.records3.RecordsService
import ru.citeck.ecos.webapp.api.entity.EntityRef
import java.lang.ref.WeakReference
import java.util.concurrent.ConcurrentHashMap

/**
 * "Type id -> the live [DbRecordsDao] of that type" for one data source. A model change arrives as
 * a type id and has to be turned into the tables that store it; this is the only place that knows
 * the mapping, because nothing else in ecos-data keeps a list of the DAOs built over a data source.
 *
 * Populated from [DbRecordsDao.setRecordsServiceFactory], which is the single path both production
 * and the tests of this repository go through.
 */
class DbRecordsDaoIndex {

    companion object {
        private val log = KotlinLogging.logger {}
    }

    private val entriesByTypeId = ConcurrentHashMap<String, MutableList<Entry>>()

    fun register(dao: DbRecordsDao, recordsService: RecordsService) {

        val typeRef = dao.getTypeRef()
        if (EntityRef.isEmpty(typeRef)) {
            // Indexing it under "" would glue every type-less DAO into one group, and a model
            // change could never reach it anyway: the event carries a type id.
            log.debug { "Records DAO '${dao.getId()}' has no type and won't be indexed" }
            return
        }
        val sourceId = dao.getId()
        val entries = entriesByTypeId.computeIfAbsent(typeRef.getLocalId()) { ArrayList() }
        synchronized(entries) {
            // A re-registration under the same sourceId replaces the previous entry instead of
            // adding a second one - the old DAO is unreachable through the records service anyway.
            entries.removeIf { it.sourceId == sourceId || it.resolve() == null }
            entries.add(Entry(dao, sourceId, recordsService))
        }
    }

    fun getByTypeId(typeId: String): List<DbRecordsDao> {
        return resolveAll(entriesByTypeId[typeId] ?: return emptyList())
    }

    fun getAll(): List<DbRecordsDao> {
        return entriesByTypeId.values.flatMap { resolveAll(it) }
    }

    fun getTypeIds(): Set<String> {
        val result = LinkedHashSet<String>()
        entriesByTypeId.forEach { (typeId, entries) ->
            if (resolveAll(entries).isNotEmpty()) {
                result.add(typeId)
            }
        }
        return result
    }

    private fun resolveAll(entries: MutableList<Entry>): List<DbRecordsDao> {
        synchronized(entries) {
            val result = ArrayList<DbRecordsDao>(entries.size)
            val iterator = entries.iterator()
            while (iterator.hasNext()) {
                val dao = iterator.next().resolve()
                if (dao == null) {
                    iterator.remove()
                } else {
                    result.add(dao)
                }
            }
            return result
        }
    }

    private class Entry(
        dao: DbRecordsDao,
        val sourceId: String,
        private val recordsService: RecordsService
    ) {
        private val ref = WeakReference(dao)

        /**
         * The weak reference is not enough on its own. `ecos-model` unregisters a DAO when a type
         * changes its sourceId or is deleted (`TypesConfig`), and ecos-data is never told; meanwhile
         * `DbDomainFactory` keeps every DAO it built in a list it never prunes, so the referent
         * stays reachable forever. Asking the records service who answers for this sourceId is what
         * actually detects the drop.
         */
        fun resolve(): DbRecordsDao? {
            val dao = ref.get() ?: return null
            return dao.takeIf { recordsService.getRecordsDao(sourceId) === it }
        }
    }
}
