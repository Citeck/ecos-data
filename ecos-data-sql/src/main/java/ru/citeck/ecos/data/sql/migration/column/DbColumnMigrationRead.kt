package ru.citeck.ecos.data.sql.migration.column

import io.github.oshai.kotlinlogging.KotlinLogging
import ru.citeck.ecos.data.sql.columnmeta.DbBackupColumnNames
import ru.citeck.ecos.data.sql.columnmeta.DbColumnConversions
import ru.citeck.ecos.data.sql.context.DbTableContext
import ru.citeck.ecos.data.sql.records.utils.DbReadToleranceLog
import ru.citeck.ecos.data.sql.repo.entity.DbEntity
import ru.citeck.ecos.data.sql.repo.find.DbFindPage
import ru.citeck.ecos.data.sql.repo.find.DbFindQuery
import ru.citeck.ecos.data.sql.service.DbDataServiceConfig
import ru.citeck.ecos.data.sql.service.DbDataServiceImpl
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType
import ru.citeck.ecos.records2.predicate.model.Predicates

/** Query-scoped view of pending scalar transfers. It never writes converted values or exposes backups. */
class DbColumnMigrationRead(private val tableCtx: DbTableContext) {

    companion object {
        private val log = KotlinLogging.logger {}
    }

    private val values = HashMap<Long, Map<String, Any?>>()
    private val pending by lazy {
        if (tableCtx.getAllPhysicalColumns().none { DbBackupColumnNames.isBackupName(it.name) }) {
            emptyList()
        } else {
            DbColumnMigrations.findUnfinished(tableCtx.getSchemaCtx(), tableCtx.getTableRef().table)
                // Reference conversion can register new references. Reads must have no such effects.
                .filter { !AttributeType.isAssocLike(it.targetType) && it.targetType != AttributeType.ENTITY_REF }
        }
    }

    fun getValues(entity: DbEntity): Map<String, Any?> {
        if (!values.containsKey(entity.id)) prepare(listOf(entity))
        return values[entity.id].orEmpty()
    }

    fun prepare(entities: List<DbEntity>) {
        val missing = entities.filter { it.id != DbEntity.NEW_REC_ID && !values.containsKey(it.id) }
        missing.forEach { values[it.id] = emptyMap() }
        if (missing.isEmpty() || pending.isEmpty()) return
        val relevant = pending.filter { task -> missing.any { it.attributes[task.attId] == null } }
        if (relevant.isEmpty()) return
        val raw = DbDataServiceImpl(
            DbEntity::class.java,
            DbDataServiceConfig.create {
                withTable(tableCtx.getTableRef().table)
                withIncludeBackupColumns(true)
            },
            tableCtx.getSchemaCtx()
        )
        val rows = raw.findRaw(
            DbFindQuery.create {
                withPredicate(Predicates.inVals(DbEntity.ID, missing.map { it.id }))
            },
            DbFindPage.ALL,
            false
        ).entities
        val entitiesById = missing.associateBy { it.id }
        val sources = relevant.associateWith { DbColumnMigrationSources(it, raw.getTableContext(), rows, readLiveAssocs = true) }
        val selected = LinkedHashMap<Long, Map<String, Pair<DbColumnMigrationParams, Any>>>()
        for (row in rows) {
            val id = row[DbEntity.ID] as? Long ?: continue
            val entity = entitiesById[id] ?: continue
            val attributes = LinkedHashMap<String, Pair<DbColumnMigrationParams, Any>>()
            for ((params, reader) in sources) {
                if (entity.attributes[params.attId] != null) continue
                val source = reader.select(row) ?: continue
                if (DbColumnConversions.isTransferable(source.first.conversionClass)) attributes[params.attId] = source
            }
            selected[id] = attributes
        }
        val refsById = DbColumnValueConverter.resolveSourceReferences(selected.values.flatMap { it.values }, tableCtx)
        for ((id, attributes) in selected) {
            val overrides = LinkedHashMap<String, Any?>()
            for ((attId, source) in attributes) {
                try {
                    overrides[attId] = DbColumnValueConverter.convert(source.second, source.first, tableCtx, refsById)
                } catch (e: Exception) {
                    if (e is InterruptedException) {
                        Thread.currentThread().interrupt()
                        throw e
                    }
                    DbReadToleranceLog.warnOnce(log, "migration-read:${tableCtx.getTableRef()}:$attId", e) {
                        "Cannot display a pending converted value for '$attId' in ${tableCtx.getTableRef()}"
                    }
                }
            }
            values[id] = overrides
        }
    }
}
