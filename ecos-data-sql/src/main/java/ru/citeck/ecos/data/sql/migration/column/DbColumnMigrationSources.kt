package ru.citeck.ecos.data.sql.migration.column

import ru.citeck.ecos.data.sql.context.DbTableContext
import ru.citeck.ecos.data.sql.repo.entity.DbEntity

/** The newest non-null value wins, even when it cannot be converted to the requested type. */
internal class DbColumnMigrationSources(
    private val params: DbColumnMigrationParams,
    tableCtx: DbTableContext,
    rows: List<Map<String, Any?>>,
    readLiveAssocs: Boolean = false
) {
    private val sources = params.sources()
    private val sourceIds = rows.mapNotNullTo(LinkedHashSet()) { it[DbEntity.REF_ID] as? Long }

    // Read live links first: if prepare commits between these queries, the backup read below
    // still finds them. Inherited sources belong to earlier departures and may only read backups.
    private val live = if (readLiveAssocs && sources.first().sourceAssocBackup) {
        tableCtx.getAssocsService().getTargetAssocs(sourceIds, params.attId)
    } else {
        emptyMap()
    }
    private val parked = sources.filter { it.sourceAssocBackup }
        .associate { source ->
            source.backupColumnMetaId to tableCtx.getSchemaCtx().assocBackupService.findByColumnMeta(
                source.backupColumnMetaId,
                sourceIds
            )
        }
    private val conversions = sources.associateWith { params.forSource(it) }

    fun select(row: Map<String, Any?>): Pair<DbColumnMigrationParams, Any>? {
        for (source in sources) {
            val value = if (source.sourceAssocBackup) {
                val sourceId = row[DbEntity.REF_ID] as? Long
                parked[source.backupColumnMetaId]?.get(sourceId)?.map { it.targetId }
                    ?: if (source.backupColumnMetaId == params.backupColumnMetaId) {
                        live[sourceId]?.map { it.targetId }
                    } else {
                        null
                    }
            } else {
                row[source.backupColumn]
            }
            if (value != null) return conversions.getValue(source) to value
        }
        return null
    }
}
