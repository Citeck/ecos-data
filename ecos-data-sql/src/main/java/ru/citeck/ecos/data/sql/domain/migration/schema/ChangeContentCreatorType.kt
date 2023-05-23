package ru.citeck.ecos.data.sql.domain.migration.schema

import mu.KotlinLogging
import ru.citeck.ecos.context.lib.auth.AuthUser
import ru.citeck.ecos.data.sql.content.DbContentServiceImpl
import ru.citeck.ecos.data.sql.content.entity.DbContentEntity
import ru.citeck.ecos.data.sql.context.DbSchemaContext
import ru.citeck.ecos.data.sql.dto.DbColumnDef
import ru.citeck.ecos.data.sql.dto.DbColumnType
import ru.citeck.ecos.data.sql.repo.find.DbFindPage
import ru.citeck.ecos.records2.predicate.model.Predicates
import ru.citeck.ecos.webapp.api.constants.AppName
import ru.citeck.ecos.webapp.api.entity.EntityRef

class ChangeContentCreatorType : DbSchemaMigration {

    companion object {
        private const val TEMP_CREATOR_COLUMN_NAME = "__temp_creator_column"
        private val log = KotlinLogging.logger {}
    }

    override fun run(context: DbSchemaContext) {
        val columns = context.doInNewRoTxn {
            context.getColumns(DbContentEntity.TABLE)
        }
        val creatorColumn = columns.find { it.name == DbContentEntity.CREATOR }
        if (creatorColumn == null || creatorColumn.type != DbColumnType.TEXT) {
            log.info { "Nothing to migrate. Creator column: $creatorColumn" }
            return
        }
        val contentDataService = (context.contentService as? DbContentServiceImpl)?.getDataService()
        if (contentDataService == null) {
            log.info { "Content data service is null. Migration will be skipped" }
            return
        }
        if (columns.none { it.name == TEMP_CREATOR_COLUMN_NAME }) {
            context.doInNewTxn {
                context.addColumns(
                    DbContentEntity.TABLE,
                    listOf(
                        DbColumnDef.create()
                            .withName(TEMP_CREATOR_COLUMN_NAME)
                            .withType(DbColumnType.LONG)
                            .build()
                    )
                )
                contentDataService.resetColumnsCache()
            }
        }

        fun findNext(): List<Map<String, Any?>> {
            return contentDataService.findRaw(
                Predicates.empty(TEMP_CREATOR_COLUMN_NAME),
                emptyList(),
                DbFindPage(0, 100),
                true,
                emptyList(),
                emptyList(),
                emptyMap(),
                false
            ).entities
        }

        val tableRef = context.getTableRef(DbContentEntity.TABLE)
        var entities = findNext()
        var processed = 0
        while (entities.isNotEmpty()) {
            context.doInNewTxn {
                for (entity in entities) {
                    val creator = ((entity[DbContentEntity.CREATOR] as? String) ?: "").ifBlank {
                        AuthUser.ANONYMOUS
                    }
                    val creatorId = context.recordRefService.getOrCreateIdByEntityRef(
                        EntityRef.create(AppName.EMODEL, "person", creator)
                    )
                    val entityId = entity[DbContentEntity.ID] as Long
                    val updateSql = "UPDATE ${tableRef.fullName} SET " +
                        "$TEMP_CREATOR_COLUMN_NAME=$creatorId WHERE id = $entityId;"

                    context.dataSourceCtx.dataSource.update(updateSql, emptyList())
                    processed++
                }
                entities = findNext()
            }
        }

        log.info { "First stage of migration completed. Processed: $processed" }

        fun renameColumn(from: String, to: String) {
            val query = "ALTER TABLE ${tableRef.fullName} RENAME COLUMN $from TO $to;"
            context.dataSourceCtx.dataSource.update(query, emptyList())
        }

        renameColumn(DbContentEntity.CREATOR, "__legacy_${DbContentEntity.CREATOR}")
        renameColumn(TEMP_CREATOR_COLUMN_NAME, DbContentEntity.CREATOR)

        log.info { "Migration completed" }
    }

    override fun getAppliedVersions(): Int {
        return 1
    }
}
