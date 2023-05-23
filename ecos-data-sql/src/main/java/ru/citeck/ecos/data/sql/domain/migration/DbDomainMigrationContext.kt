package ru.citeck.ecos.data.sql.domain.migration

import ru.citeck.ecos.data.sql.context.DbSchemaContext
import ru.citeck.ecos.data.sql.domain.DbDomainConfig
import ru.citeck.ecos.data.sql.records.DbRecordsDao
import ru.citeck.ecos.data.sql.repo.entity.DbEntity
import ru.citeck.ecos.data.sql.service.DbDataService

class DbDomainMigrationContext(
    val dataService: DbDataService<DbEntity>,
    val schemaContext: DbSchemaContext,
    val recordsDao: DbRecordsDao,
    val config: DbDomainConfig
) {

    val dataSource = schemaContext.dataSourceCtx.dataSource
    val schemaDao = schemaContext.dataSourceCtx.schemaDao

    fun <T> doInNewTxn(action: () -> T): T {
        return schemaContext.dataSourceCtx.doInNewTxn(action)
    }

    fun <T> doInNewRoTxn(action: () -> T): T {
        return schemaContext.dataSourceCtx.doInNewRoTxn(action)
    }
}
