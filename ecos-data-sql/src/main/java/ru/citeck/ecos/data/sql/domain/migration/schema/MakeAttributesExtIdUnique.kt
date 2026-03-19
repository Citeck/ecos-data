package ru.citeck.ecos.data.sql.domain.migration.schema

import ru.citeck.ecos.data.sql.context.DbSchemaContext
import ru.citeck.ecos.data.sql.records.assocs.DbAssocEntity
import ru.citeck.ecos.data.sql.records.attnames.DbEcosAttributeEntity

class MakeAttributesExtIdUnique : DbSchemaMigration {

    override fun run(context: DbSchemaContext) {

        val tableRef = context.getTableRef(DbEcosAttributeEntity.TABLE)
        if (!context.isTableExists(tableRef)) {
            return
        }

        val dataSource = context.dataSourceCtx.dataSource
        val extId = DbEcosAttributeEntity.EXT_ID
        val attCol = DbAssocEntity.ATTRIBUTE

        // Remap duplicate attribute ids to the canonical (min id) in assoc tables
        for (assocTable in listOf(DbAssocEntity.MAIN_TABLE, DbAssocEntity.DELETED_TABLE)) {
            val assocRef = context.getTableRef(assocTable)
            if (!context.isTableExists(assocRef)) {
                continue
            }
            dataSource.update(
                "UPDATE ${assocRef.fullName} assoc SET \"$attCol\" = min_ids.min_id" +
                    " FROM ${tableRef.fullName} dup" +
                    " JOIN (SELECT \"$extId\", MIN(id) AS min_id FROM ${tableRef.fullName}" +
                    " GROUP BY \"$extId\" HAVING COUNT(*) > 1) min_ids" +
                    " ON dup.\"$extId\" = min_ids.\"$extId\"" +
                    " WHERE assoc.\"$attCol\" = dup.id AND dup.id > min_ids.min_id",
                emptyList()
            )
        }

        // Remove duplicates keeping the row with the smallest id
        dataSource.update(
            "DELETE FROM ${tableRef.fullName} a USING ${tableRef.fullName} b" +
                " WHERE a.\"$extId\" = b.\"$extId\" AND a.id > b.id",
            emptyList()
        )

        // Create unique index (old non-unique index is redundant but harmless)
        dataSource.updateSchema(
            "CREATE UNIQUE INDEX IF NOT EXISTS \"${tableRef.table}_${extId}_unique\"" +
                " ON ${tableRef.fullName} (\"$extId\")"
        )
    }

    override fun getAppliedVersions(): Int {
        return 6
    }
}
