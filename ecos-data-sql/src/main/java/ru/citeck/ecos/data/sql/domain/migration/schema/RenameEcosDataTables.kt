package ru.citeck.ecos.data.sql.domain.migration.schema

import io.github.oshai.kotlinlogging.KotlinLogging
import ru.citeck.ecos.data.sql.context.DbSchemaContext
import ru.citeck.ecos.data.sql.meta.table.DbTableMetaEntity

class RenameEcosDataTables {

    companion object {
        private val log = KotlinLogging.logger {}

        private val RENAMES = mapOf(
            "ecos_schema_meta" to "ed_schema_meta",
            "ecos_associations" to "ed_associations",
            "ecos_associations_deleted" to "ed_associations_deleted",
            "ecos_authorities" to "ed_authorities",
            "ecos_content_data" to "ed_content_data",
            "ecos_content" to "ed_content",
            "ecos_attributes" to "ed_attributes",
            "ecos_read_perms" to "ed_read_perms",
            "ecos_record_ref" to "ed_record_ref",
            "ecos_workspace" to "ed_workspace"
        )
    }

    fun run(context: DbSchemaContext) {

        log.info { "Migration started" }

        val dataSource = context.dataSourceCtx.dataSource

        val tableMetaRef = context.getTableRef(DbTableMetaEntity.TABLE)
        var updatedCounter = 0
        for ((renameFrom, renameTo) in RENAMES) {
            val renameFromTable = context.getTableRef(renameFrom)
            if (!context.isTableExists(renameFromTable)) {
                continue
            }
            val renameToTable = context.getTableRef(renameTo)
            if (context.isTableExists(renameToTable)) {
                log.warn { "Both source and target tables exists. Source: $renameFromTable Target: $renameToTable" }
                continue
            }
            log.warn { "Rename table $renameFromTable to $renameToTable" }
            dataSource.updateSchema(
                "ALTER TABLE ${renameFromTable.fullName} " +
                    "RENAME TO \"${renameToTable.table}\";"
            )

            if (context.isTableExists(tableMetaRef)) {
                val query = "UPDATE ${tableMetaRef.fullName} " +
                    "SET ${DbTableMetaEntity.EXT_ID} = ? " +
                    "WHERE ${DbTableMetaEntity.EXT_ID} = ?"
                log.info { "$query [${renameToTable.table}, ${renameFromTable.table}]" }
                dataSource.update(query, listOf(renameToTable.table, renameFromTable.table))
            }

            val metaSchemaVersionKeyBefore = listOf(
                "table",
                renameFromTable.schema,
                renameFromTable.table,
                "schema-version"
            )
            val metaSchemaVersionKeyAfter = listOf(
                "table",
                renameToTable.schema,
                renameToTable.table,
                "schema-version"
            )
            val schemaMetaValueBefore = context.schemaMetaService.getValue(metaSchemaVersionKeyBefore)
            if (schemaMetaValueBefore.isNotEmpty()) {
                context.schemaMetaService.setValue(metaSchemaVersionKeyAfter, schemaMetaValueBefore)
            }

            updatedCounter++
        }
        log.info { "Migration completed. Updated: $updatedCounter tables" }
    }
}
