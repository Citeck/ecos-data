package ru.citeck.ecos.data.sql.domain.migration.schema

import ru.citeck.ecos.data.sql.context.DbSchemaContext
import ru.citeck.ecos.data.sql.perms.DbPermsEntity

class RemoveAllowedFlagFromPerms : DbSchemaMigration {

    companion object {
        const val COLUMN_ALLOWED = "__allowed"
    }

    override fun run(context: DbSchemaContext) {
        if (!context.entityPermsService.isTableExists()) {
            return
        }
        val tableRef = context.getTableRef(DbPermsEntity.TABLE)
        val query = "ALTER TABLE ${tableRef.fullName} DROP COLUMN \"$COLUMN_ALLOWED\";"
        context.dataSourceCtx.dataSource.updateSchema(query)
    }

    override fun getAppliedVersions(): Int {
        return 2
    }
}
