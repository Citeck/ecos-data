package ru.citeck.ecos.data.sql.domain.migration.schema

import mu.KotlinLogging
import ru.citeck.ecos.data.sql.content.DbContentServiceImpl
import ru.citeck.ecos.data.sql.content.entity.DbContentEntity
import ru.citeck.ecos.data.sql.content.storage.EcosContentStorageServiceImpl
import ru.citeck.ecos.data.sql.context.DbSchemaContext

class UpdateContentTables : DbSchemaMigration {

    companion object {
        const val COLUMN_URI = "__uri"
        const val COLUMN_SHA256 = "__sha256"
        const val COLUMN_SIZE = "__size"
        private val log = KotlinLogging.logger {}
    }

    override fun run(context: DbSchemaContext) {
        migrateEcosContent(context)
        migrateLocalStorage(context)
    }

    private fun migrateLocalStorage(context: DbSchemaContext) {
        val localStorageService = (context.contentStorageService as EcosContentStorageServiceImpl).getLocalStorageService()
        val localStorageDataService = localStorageService.getDataService()
        if (!localStorageDataService.isTableExists()) {
            return
        }
        val hasShaColumn = localStorageDataService.getTableContext().getColumns().any { it.name == COLUMN_SHA256 }
        if (!hasShaColumn) {
            return
        }
        val dataSource = context.dataSourceCtx.dataSource
        val tableName = localStorageDataService.getTableRef().fullName
        dataSource.updateSchema("ALTER TABLE $tableName DROP COLUMN \"$COLUMN_SHA256\";")
        dataSource.updateSchema("ALTER TABLE $tableName DROP COLUMN \"$COLUMN_SIZE\";")
    }

    private fun migrateEcosContent(context: DbSchemaContext) {
        val contentDataService = (context.contentService as? DbContentServiceImpl)?.getDataService()
        if (contentDataService == null) {
            log.warn { "Content data service is null" }
            return
        }
        if (!contentDataService.isTableExists()) {
            return
        }
        val hasUriColumn = contentDataService.getTableContext().getColumns().any { it.name == COLUMN_URI }
        if (!hasUriColumn) {
            return
        }
        val query = "ALTER TABLE ${contentDataService.getTableRef().fullName}" +
            " RENAME COLUMN \"$COLUMN_URI\" TO \"${DbContentEntity.DATA_KEY}\";"
        context.dataSourceCtx.dataSource.updateSchema(query)
    }

    override fun getAppliedVersions(): Int {
        return 4
    }
}
