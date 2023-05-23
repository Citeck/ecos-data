package ru.citeck.ecos.data.sql.domain.migration.domain

import mu.KotlinLogging
import ru.citeck.ecos.data.sql.domain.migration.DbDomainMigrationContext
import ru.citeck.ecos.data.sql.perms.DbPermsEntity

class MovePermsToSchemaTable : DbDomainMigration {

    companion object {
        private val log = KotlinLogging.logger {}
    }

    override fun run(context: DbDomainMigrationContext) {

        val tableRef = context.dataService.getTableRef()
        val permsTableRef = tableRef.withTable(tableRef.table + "__perms")
        val targetTableRef = tableRef.withTable(DbPermsEntity.TABLE)

        val schemaDao = context.schemaContext.dataSourceCtx.schemaDao
        if (schemaDao.getColumns(context.schemaContext.dataSourceCtx.dataSource, permsTableRef).isEmpty()) {
            log.info { "Perms table doesn't found: '$permsTableRef'. Skip it" }
            return
        }

        context.schemaContext.entityPermsService.createTableIfNotExists()

        val migrationQuery = "INSERT INTO ${targetTableRef.fullName}" +
            "(${DbPermsEntity.ENTITY_REF_ID},${DbPermsEntity.AUTHORITY_ID},${DbPermsEntity.ALLOWED}) " +
            "SELECT " +
            "recs.__ref_id as __entity_ref_id," +
            "perms.__authority_id," +
            "perms.__allowed " +
            "FROM ${permsTableRef.fullName} perms JOIN ${tableRef.fullName} recs on perms.__record_id = recs.id;"
        log.info { "Migrate permissions from $permsTableRef to $targetTableRef. Query: $migrationQuery" }

        val dataSource = context.schemaContext.dataSourceCtx.dataSource

        val migrationResult = dataSource.update(migrationQuery, emptyList()).firstOrNull() ?: -1
        log.info { "Migration completed. Result: $migrationResult" }
    }

    override fun getAppliedVersions(): Int {
        return 1
    }
}
