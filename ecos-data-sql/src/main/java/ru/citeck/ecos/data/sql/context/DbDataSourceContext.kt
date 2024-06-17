package ru.citeck.ecos.data.sql.context

import ru.citeck.ecos.data.sql.datasource.DbDataSource
import ru.citeck.ecos.data.sql.domain.migration.DbMigrationService
import ru.citeck.ecos.data.sql.remote.DbRecordsRemoteActionsClient
import ru.citeck.ecos.data.sql.repo.DbEntityRepo
import ru.citeck.ecos.data.sql.schema.DbSchemaDao
import ru.citeck.ecos.data.sql.service.DbDataServiceFactory
import ru.citeck.ecos.data.sql.type.DbTypesConverter
import ru.citeck.ecos.model.lib.ModelServiceFactory
import ru.citeck.ecos.txn.lib.TxnContext
import ru.citeck.ecos.webapp.api.EcosWebAppApi
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean

class DbDataSourceContext(
    val dataSource: DbDataSource,
    dataServiceFactory: DbDataServiceFactory,
    private val migrationService: DbMigrationService,
    private val webAppApi: EcosWebAppApi,
    val modelServices: ModelServiceFactory,
    val remoteActionsClient: DbRecordsRemoteActionsClient? = null
) {
    val appName: String = webAppApi.getProperties().appName
    val converter: DbTypesConverter = DbTypesConverter()
    val entityRepo: DbEntityRepo = dataServiceFactory.createEntityRepo()
    val schemaDao: DbSchemaDao = dataServiceFactory.createSchemaDao()

    private val schemasByName = ConcurrentHashMap<String, DbSchemaContext>()

    init {
        dataServiceFactory.registerConverters(converter)
    }

    fun <T> doInNewTxn(action: () -> T): T {
        return TxnContext.doInNewTxn {
            dataSource.withTransaction(readOnly = false, requiresNew = true) {
                action.invoke()
            }
        }
    }

    fun <T> doInNewRoTxn(action: () -> T): T {
        return TxnContext.doInNewTxn {
            dataSource.withTransaction(readOnly = true, requiresNew = true) {
                action.invoke()
            }
        }
    }

    fun getSchemaContext(schema: String): DbSchemaContext {
        val newContextWasCreated = AtomicBoolean()
        val result = schemasByName.computeIfAbsent(schema) { k ->
            newContextWasCreated.set(true)
            DbSchemaContext(k, this, webAppApi)
        }
        if (newContextWasCreated.get()) {
            if (webAppApi.isReady()) {
                migrationService.runSchemaMigrations(result)
            } else {
                webAppApi.doBeforeAppReady {
                    migrationService.runSchemaMigrations(result)
                }
            }
        }
        return result
    }
}
