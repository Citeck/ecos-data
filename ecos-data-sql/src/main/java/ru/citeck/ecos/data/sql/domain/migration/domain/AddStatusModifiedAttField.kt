package ru.citeck.ecos.data.sql.domain.migration.domain

import io.github.oshai.kotlinlogging.KotlinLogging
import ru.citeck.ecos.data.sql.domain.migration.DbDomainMigrationContext
import ru.citeck.ecos.data.sql.dto.DbColumnDef
import ru.citeck.ecos.data.sql.dto.DbColumnType
import ru.citeck.ecos.data.sql.records.dao.atts.DbRecord
import ru.citeck.ecos.data.sql.repo.entity.DbEntity
import ru.citeck.ecos.data.sql.repo.find.DbFindPage
import ru.citeck.ecos.data.sql.repo.find.DbFindRes
import ru.citeck.ecos.records2.predicate.model.Predicates
import java.sql.Timestamp
import java.time.Instant

class AddStatusModifiedAttField : DbDomainMigration {

    companion object {
        private val log = KotlinLogging.logger {}

        const val TEMP_FIELD_STATUS_MODIFIED = "__temp__status_modified"

        private const val LOG_CHUNK_SIZE = 10_000
    }

    override fun run(context: DbDomainMigrationContext) {
        val tableRef = context.dataService.getTableRef()
        val dataSource = context.dataSource

        val currentColumns = context.doInNewTxn {
            context.schemaDao.getColumns(
                context.dataSource,
                tableRef
            ).associateBy {
                it.name
            }
        }

        val currentStatusColumn = currentColumns[DbEntity.STATUS]
        if (currentStatusColumn == null || currentStatusColumn.type != DbColumnType.TEXT) {
            // nothing to migrate
            return
        }

        val recordWithStatus = context.doInNewRoTxn {
            context.dataService.findRaw(
                Predicates.notEmpty(DbEntity.STATUS),
                emptyList(),
                DbFindPage.FIRST,
                emptyList(),
                emptyList(),
                emptyList(),
                false
            )
        }

        if (recordWithStatus.entities.isEmpty()) {
            // nothing to migrate
            return
        }

        log.info { "Start migration for $tableRef" }

        context.doInNewTxn {
            val newColumns = listOf(
                DbColumnDef.create {
                    withName(TEMP_FIELD_STATUS_MODIFIED)
                    withType(DbColumnType.DATETIME)
                }
            ).filter {
                !currentColumns.containsKey(it.name)
            }

            if (newColumns.isNotEmpty()) {
                context.schemaDao.addColumns(
                    context.dataSource,
                    tableRef,
                    newColumns
                )
            }
        }

        context.dataService.resetColumnsCache()

        fun findNext(withTotalCount: Boolean): DbFindRes<EntityData> {
            return context.dataService.findRaw(
                Predicates.and(
                    Predicates.empty(TEMP_FIELD_STATUS_MODIFIED),
                    Predicates.notEmpty(DbEntity.STATUS)
                ),
                emptyList(),
                DbFindPage(0, 100),
                emptyList(),
                emptyList(),
                emptyList(),
                withTotalCount
            ).mapEntities {
                val id = it[DbEntity.ID] as Long
                val modified = it[DbEntity.MODIFIED] as? Timestamp ?: Timestamp.from(Instant.now())
                EntityData(
                    id = id,
                    modified = modified,
                    raw = it
                )
            }
        }

        fun findNextValues(): List<EntityData> {
            return findNext(false).entities
        }

        context.schemaContext.recordRefService.createTableIfNotExists()

        val firstChunkRes = context.doInNewRoTxn {
            findNext(true)
        }
        log.info { "Start migration. Total count: ${firstChunkRes.totalCount}" }
        var processedCount = 0L
        var lastLoggedChunkIdx = 0L

        var recordsToMigrate = firstChunkRes.entities
        while (recordsToMigrate.isNotEmpty()) {
            context.doInNewTxn {
                recordsToMigrate.forEach {
                    val updateSql = "UPDATE ${tableRef.fullName} SET " +
                        "$TEMP_FIELD_STATUS_MODIFIED = ? WHERE id = ?;"

                    dataSource.update(updateSql, listOf(it.modified, it.id))

                    processedCount++
                    val chunkIdx = processedCount / LOG_CHUNK_SIZE
                    if (chunkIdx != lastLoggedChunkIdx) {
                        log.info { "Processed $processedCount entities" }
                        lastLoggedChunkIdx = chunkIdx
                    }
                }
                recordsToMigrate = findNextValues()
            }
        }

        log.info { "First stage of migration completed with processed count $processedCount. Let's switch columns" }

        context.doInNewTxn {
            dataSource.updateSchema(
                "ALTER TABLE ${tableRef.fullName} " +
                    "RENAME COLUMN \"$TEMP_FIELD_STATUS_MODIFIED\" TO \"${DbRecord.ATT_STATUS_MODIFIED}\";"
            )
        }

        log.info { "Migration completed" }
    }

    override fun getAppliedVersions(): Int {
        return 6
    }

    data class EntityData(
        val id: Long,
        val modified: Timestamp,
        val raw: Map<String, Any?>
    )
}
