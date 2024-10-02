package ru.citeck.ecos.data.sql.domain.migration.schema

import ru.citeck.ecos.data.sql.context.DbSchemaContext
import ru.citeck.ecos.data.sql.records.assocs.DbAssocEntity

class AddDeletedAssocsTable : DbSchemaMigration {

    companion object {
        const val COLUMN_DELETED = "__deleted"
    }

    override fun run(context: DbSchemaContext) {
        if (!context.assocsService.isAssocsTableExists()) {
            return
        }
        val assocsTableRef = context.getTableRef(DbAssocEntity.MAIN_TABLE)
        val currentColumns = context.getColumns(DbAssocEntity.MAIN_TABLE)
        if (currentColumns.any { it.name == COLUMN_DELETED }) {
            val query = "ALTER TABLE ${assocsTableRef.fullName} DROP COLUMN \"$COLUMN_DELETED\";"
            context.dataSourceCtx.dataSource.updateSchema(query)
        }
        if (currentColumns.none { it.name == "id" }) {
            val query = "ALTER TABLE ${assocsTableRef.fullName} ADD COLUMN id BIGSERIAL PRIMARY KEY;"
            context.dataSourceCtx.dataSource.updateSchema(query)
        }
    }

    override fun getAppliedVersions(): Int {
        return 3
    }
}
