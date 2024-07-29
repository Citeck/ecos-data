package ru.citeck.ecos.data.sql.domain.migration.domain

import io.github.oshai.kotlinlogging.KotlinLogging
import ru.citeck.ecos.data.sql.domain.migration.DbDomainMigrationContext
import ru.citeck.ecos.data.sql.dto.DbColumnDef
import ru.citeck.ecos.data.sql.dto.DbColumnIndexDef
import ru.citeck.ecos.data.sql.dto.DbColumnType
import ru.citeck.ecos.data.sql.repo.entity.DbEntity
import ru.citeck.ecos.data.sql.repo.find.DbFindPage
import ru.citeck.ecos.data.sql.repo.find.DbFindRes
import ru.citeck.ecos.model.lib.utils.ModelUtils
import ru.citeck.ecos.records2.predicate.model.Predicates
import ru.citeck.ecos.webapp.api.entity.EntityRef

class MigrateMetaFieldsToRefs : DbDomainMigration {

    companion object {
        private val log = KotlinLogging.logger {}

        const val TEMP_FIELD_CREATOR_REF = "__temp__creator_ref"
        const val TEMP_FIELD_MODIFIER_REF = "__temp__modifier_ref"
        const val TEMP_FIELD_TYPE_REF = "__temp__type_ref"

        private const val LOG_CHUNK_SIZE = 10_000
    }

    override fun run(context: DbDomainMigrationContext) {

        val tableRef = context.dataService.getTableRef()
        val dataSource = context.dataSource
        val recsDaoCtx = context.recordsDao.getRecordsDaoCtx()

        context.doInNewTxn {

            val currentColumns = context.schemaDao.getColumns(
                context.dataSource,
                tableRef
            ).mapTo(HashSet()) {
                it.name
            }
            val newColumns = listOf(
                DbColumnDef.create {
                    withIndex(DbColumnIndexDef(true))
                    withName(TEMP_FIELD_TYPE_REF)
                    withType(DbColumnType.LONG)
                },
                DbColumnDef.create {
                    withName(TEMP_FIELD_CREATOR_REF)
                    withType(DbColumnType.LONG)
                },
                DbColumnDef.create {
                    withName(TEMP_FIELD_MODIFIER_REF)
                    withType(DbColumnType.LONG)
                },
            ).filter {
                !currentColumns.contains(it.name)
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
                Predicates.empty(TEMP_FIELD_TYPE_REF),
                emptyList(),
                DbFindPage(0, 100),
                true,
                emptyList(),
                emptyList(),
                emptyList(),
                withTotalCount
            ).mapEntities {
                val id = it[DbEntity.ID] as Long
                val modifier = recsDaoCtx.getUserRef(it[DbEntity.MODIFIER] as? String ?: "")
                val creator = recsDaoCtx.getUserRef(it[DbEntity.CREATOR] as? String ?: "")
                val type = ModelUtils.getTypeRef((it[DbEntity.TYPE] as? String ?: "").ifBlank { "base" })
                EntityData(
                    id = id,
                    creator = creator,
                    modifier = modifier,
                    type = type,
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
                val refs = HashSet<EntityRef>()
                recordsToMigrate.forEach {
                    refs.add(it.creator)
                    refs.add(it.modifier)
                    refs.add(it.type)
                }
                val refsList = refs.toList()
                val ids = context.schemaContext.recordRefService.getOrCreateIdByEntityRefs(refsList)
                val idsByRefs = HashMap<EntityRef, Long>()
                refsList.forEachIndexed { idx, ref -> idsByRefs[ref] = ids[idx] }

                recordsToMigrate.forEach {

                    val creatorId = idsByRefs[it.creator] ?: error("Creator ref ID doesn't found")
                    val typeId = idsByRefs[it.type] ?: error("Type ref ID doesn't found")
                    val modifierId = idsByRefs[it.modifier] ?: error("Modifier ref ID doesn't found")
                    val updateSql = "UPDATE ${tableRef.fullName} SET " +
                        "$TEMP_FIELD_TYPE_REF=$typeId, " +
                        "$TEMP_FIELD_CREATOR_REF=$creatorId, " +
                        "$TEMP_FIELD_MODIFIER_REF=$modifierId WHERE id = ${it.id};"

                    dataSource.update(updateSql, emptyList())

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

        fun renameColumn(from: String, to: String, dropNotNullConstraints: Boolean = false) {
            val query = "ALTER TABLE ${tableRef.fullName} RENAME COLUMN $from TO $to;"
            dataSource.updateSchema(query)
            if (dropNotNullConstraints) {
                dataSource.updateSchema("ALTER TABLE ${tableRef.fullName} ALTER \"$to\" DROP NOT NULL;")
            }
        }

        context.doInNewTxn {
            renameColumn(DbEntity.TYPE, "__legacy_${DbEntity.TYPE}", true)
            renameColumn(DbEntity.CREATOR, "__legacy_${DbEntity.CREATOR}", true)
            renameColumn(DbEntity.MODIFIER, "__legacy_${DbEntity.MODIFIER}", true)

            renameColumn(TEMP_FIELD_TYPE_REF, DbEntity.TYPE)
            renameColumn(TEMP_FIELD_CREATOR_REF, DbEntity.CREATOR)
            renameColumn(TEMP_FIELD_MODIFIER_REF, DbEntity.MODIFIER)
        }
        log.info { "Migration completed" }
    }

    override fun getAppliedVersions(): Int {
        return 4
    }

    data class EntityData(
        val id: Long,
        val creator: EntityRef,
        val modifier: EntityRef,
        val type: EntityRef,
        val raw: Map<String, Any?>
    )
}
