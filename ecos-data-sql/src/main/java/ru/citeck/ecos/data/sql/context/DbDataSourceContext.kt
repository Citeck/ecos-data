package ru.citeck.ecos.data.sql.context

import ru.citeck.ecos.context.lib.ctx.EcosContext
import ru.citeck.ecos.data.sql.content.ContentMimeTypeDetector
import ru.citeck.ecos.data.sql.content.SafeContentMimeTypeDetector
import ru.citeck.ecos.data.sql.content.storage.EcosContentStorageService
import ru.citeck.ecos.data.sql.content.storage.EcosContentStorageServiceFactory
import ru.citeck.ecos.data.sql.content.storage.EcosContentStorageServiceImpl
import ru.citeck.ecos.data.sql.datasource.DbDataSource
import ru.citeck.ecos.data.sql.domain.migration.DbMigrationService
import ru.citeck.ecos.data.sql.props.DbEcosDataProps
import ru.citeck.ecos.data.sql.remote.DbRecordsRemoteActionsClient
import ru.citeck.ecos.data.sql.repo.DbEntityRepo
import ru.citeck.ecos.data.sql.schema.DbSchemaDao
import ru.citeck.ecos.data.sql.service.DbDataServiceFactory
import ru.citeck.ecos.data.sql.type.DbTypesConverter
import ru.citeck.ecos.txn.lib.TxnContext
import ru.citeck.ecos.webapp.api.EcosWebAppApi
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean

class DbDataSourceContext(
    val dataSource: DbDataSource,
    dataServiceFactory: DbDataServiceFactory,
    private val migrationService: DbMigrationService,
    private val webAppApi: EcosWebAppApi,
    private val ecosContext: EcosContext,
    val remoteActionsClient: DbRecordsRemoteActionsClient? = null,
    val props: DbEcosDataProps = DbEcosDataProps.DEFAULT,
    mimeTypeDetector: ContentMimeTypeDetector? = null,
    /**
     * Builds the content storage service of every schema of this data source, or null to use the
     * built-in one. Supplied the same way [DbDataServiceFactory] is - see
     * [EcosContentStorageServiceFactory].
     */
    private val contentStorageServiceFactory: EcosContentStorageServiceFactory? = null
) {
    /**
     * Wrapped on the way in, so that nothing downstream has to defend itself against the
     * application's detector - see [SafeContentMimeTypeDetector].
     */
    val mimeTypeDetector: ContentMimeTypeDetector? = mimeTypeDetector?.let { SafeContentMimeTypeDetector(it) }

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
            DbSchemaContext(k, this, webAppApi, ecosContext)
        }
        if (newContextWasCreated.get()) {
            migrationService.runSchemaMigrations(result)
        }
        return result
    }

    internal fun createContentStorageService(schemaCtx: DbSchemaContext): EcosContentStorageService {
        val factory = contentStorageServiceFactory ?: return EcosContentStorageServiceImpl(webAppApi, schemaCtx)
        return factory.create(schemaCtx)
    }

    fun forEachSchema(action: (String, DbSchemaContext) -> Unit) {
        schemasByName.forEach(action)
    }

    /**
     * All schema contexts created so far. A schema that has never been touched is not listed.
     */
    fun getSchemaContexts(): List<DbSchemaContext> {
        return schemasByName.values.toList()
    }
}
