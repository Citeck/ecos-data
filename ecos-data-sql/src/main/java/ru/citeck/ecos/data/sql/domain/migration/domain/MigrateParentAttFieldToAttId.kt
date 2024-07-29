package ru.citeck.ecos.data.sql.domain.migration.domain

import io.github.oshai.kotlinlogging.KotlinLogging
import ru.citeck.ecos.data.sql.domain.migration.DbDomainMigrationContext
import ru.citeck.ecos.data.sql.dto.DbColumnDef
import ru.citeck.ecos.data.sql.dto.DbColumnType
import ru.citeck.ecos.data.sql.repo.entity.DbEntity
import ru.citeck.ecos.data.sql.repo.find.DbFindPage
import ru.citeck.ecos.data.sql.repo.find.DbFindRes
import ru.citeck.ecos.records2.RecordConstants
import ru.citeck.ecos.records2.predicate.model.Predicates

class MigrateParentAttFieldToAttId : DbDomainMigration {

    companion object {
        private val log = KotlinLogging.logger {}

        const val TEMP_FIELD_PARENT_ATT = "__temp__parent_att"

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

        val currentParentAttColumn = currentColumns[RecordConstants.ATT_PARENT_ATT]
        if (currentParentAttColumn == null || currentParentAttColumn.type != DbColumnType.TEXT) {
            // nothing to migrate
            return
        }
        log.info { "Start migration for $tableRef" }

        context.doInNewTxn {

            val newColumns = listOf(
                DbColumnDef.create {
                    withName(TEMP_FIELD_PARENT_ATT)
                    withType(DbColumnType.LONG)
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
                    Predicates.empty(TEMP_FIELD_PARENT_ATT),
                    Predicates.notEmpty(RecordConstants.ATT_PARENT_ATT)
                ),
                emptyList(),
                DbFindPage(0, 100),
                true,
                emptyList(),
                emptyList(),
                emptyList(),
                withTotalCount
            ).mapEntities {
                val id = it[DbEntity.ID] as Long
                val parentAtt = it[RecordConstants.ATT_PARENT_ATT] as? String ?: ""
                EntityData(
                    id = id,
                    parentAtt = parentAtt,
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

                val textAtts = recordsToMigrate.mapTo(HashSet()) { it.parentAtt }
                val attIdByName = textAtts.associateWith { context.schemaContext.assocsService.getIdForAtt(it, true) }

                recordsToMigrate.forEach {

                    val parentAttId = attIdByName[it.parentAtt] ?: error("attribute id is not found for ${it.parentAtt}")

                    if (parentAttId != -1L) {

                        val updateSql = "UPDATE ${tableRef.fullName} SET " +
                            "$TEMP_FIELD_PARENT_ATT=$parentAttId WHERE id = ${it.id};"

                        dataSource.update(updateSql, emptyList())

                        processedCount++
                        val chunkIdx = processedCount / LOG_CHUNK_SIZE
                        if (chunkIdx != lastLoggedChunkIdx) {
                            log.info { "Processed $processedCount entities" }
                            lastLoggedChunkIdx = chunkIdx
                        }
                    }
                }
                recordsToMigrate = findNextValues()
            }
        }

        log.info { "First stage of migration completed with processed count $processedCount. Let's switch columns" }

        fun renameColumn(from: String, to: String) {
            dataSource.updateSchema("ALTER TABLE ${tableRef.fullName} RENAME COLUMN \"$from\" TO \"$to\";")
        }

        context.doInNewTxn {
            renameColumn(RecordConstants.ATT_PARENT_ATT, "__legacy_${RecordConstants.ATT_PARENT_ATT}")
            renameColumn(TEMP_FIELD_PARENT_ATT, RecordConstants.ATT_PARENT_ATT)
        }

        log.info { "Migration completed" }
    }

    override fun getAppliedVersions(): Int {
        return 5
    }

    data class EntityData(
        val id: Long,
        val parentAtt: String,
        val raw: Map<String, Any?>
    )
}
