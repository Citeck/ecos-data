package ru.citeck.ecos.data.sql.domain.migration.schema

import io.github.oshai.kotlinlogging.KotlinLogging
import ru.citeck.ecos.data.sql.context.DbSchemaContext
import ru.citeck.ecos.data.sql.records.assocs.DbAssocEntity
import ru.citeck.ecos.data.sql.records.attnames.DbEcosAttributeEntity

class MakeAttributesExtIdUnique : DbSchemaMigration {

    companion object {
        private val log = KotlinLogging.logger {}
    }

    override fun run(context: DbSchemaContext) {

        val tableRef = context.getTableRef(DbEcosAttributeEntity.TABLE)
        if (!context.isTableExists(tableRef)) {
            return
        }

        log.info { "Migration started for ${tableRef.fullName}" }

        val dataSource = context.dataSourceCtx.dataSource
        val extId = DbEcosAttributeEntity.EXT_ID
        val attCol = DbAssocEntity.ATTRIBUTE

        // Remap duplicate attribute ids to the canonical (min id) in assoc tables
        for (assocTable in listOf(DbAssocEntity.MAIN_TABLE, DbAssocEntity.DELETED_TABLE)) {
            val assocRef = context.getTableRef(assocTable)
            if (!context.isTableExists(assocRef)) {
                continue
            }
            val remapped = dataSource.update(
                "UPDATE ${assocRef.fullName} assoc SET \"$attCol\" = min_ids.min_id" +
                    " FROM ${tableRef.fullName} dup" +
                    " JOIN (SELECT \"$extId\", MIN(id) AS min_id FROM ${tableRef.fullName}" +
                    " GROUP BY \"$extId\" HAVING COUNT(*) > 1) min_ids" +
                    " ON dup.\"$extId\" = min_ids.\"$extId\"" +
                    " WHERE assoc.\"$attCol\" = dup.id AND dup.id > min_ids.min_id",
                emptyList()
            )
            log.info { "Remapped ${remapped.size} assoc rows in ${assocRef.fullName}" }
        }

        // Remove duplicates keeping the row with the smallest id
        val deleted = dataSource.update(
            "DELETE FROM ${tableRef.fullName} a USING ${tableRef.fullName} b" +
                " WHERE a.\"$extId\" = b.\"$extId\" AND a.id > b.id",
            emptyList()
        )
        log.info { "Deleted ${deleted.size} duplicate rows from ${tableRef.fullName}" }

        // Create unique index (old non-unique index is redundant but harmless)
        dataSource.updateSchema(
            "CREATE UNIQUE INDEX IF NOT EXISTS \"${tableRef.table}_${extId}_unique\"" +
                " ON ${tableRef.fullName} (\"$extId\")"
        )

        log.info { "Migration completed for ${tableRef.fullName}" }
    }

    override fun getAppliedVersions(): Int {
        return 6
    }
}
