package ru.citeck.ecos.data.sql.domain

import ru.citeck.ecos.data.sql.content.DbContentService
import ru.citeck.ecos.data.sql.content.storage.EcosContentStorageConfig
import ru.citeck.ecos.data.sql.content.storage.EcosContentStorageConstants
import ru.citeck.ecos.data.sql.context.DbDataSourceContext
import ru.citeck.ecos.data.sql.context.DbSchemaContext
import ru.citeck.ecos.data.sql.datasource.DbDataSource
import ru.citeck.ecos.data.sql.domain.migration.DbDomainMigrationContext
import ru.citeck.ecos.data.sql.domain.migration.DbMigrationService
import ru.citeck.ecos.data.sql.records.DbRecordsDao
import ru.citeck.ecos.data.sql.records.computed.DbComputedAttsComponent
import ru.citeck.ecos.data.sql.records.listener.DbIntegrityCheckListener
import ru.citeck.ecos.data.sql.records.listener.DbRecordsListener
import ru.citeck.ecos.data.sql.records.perms.DbPermsComponent
import ru.citeck.ecos.data.sql.records.refs.DbGlobalRefCalculator
import ru.citeck.ecos.data.sql.remote.DbRecordsRemoteActionsClient
import ru.citeck.ecos.data.sql.repo.entity.DbEntity
import ru.citeck.ecos.data.sql.service.DbDataServiceFactory
import ru.citeck.ecos.data.sql.service.DbDataServiceImpl
import ru.citeck.ecos.model.lib.ModelServiceFactory
import ru.citeck.ecos.webapp.api.EcosWebAppApi
import java.util.*
import kotlin.collections.ArrayList

class DbDomainFactory(
    val dataSource: DbDataSource,
    val modelServices: ModelServiceFactory,
    val permsComponent: DbPermsComponent,
    val computedAttsComponent: DbComputedAttsComponent,
    val defaultListeners: List<DbRecordsListener>,
    val dataServiceFactory: DbDataServiceFactory,
    val webAppApi: EcosWebAppApi,
    val remoteActionsClient: DbRecordsRemoteActionsClient?
) {

    private val recordsDaoWithoutDefaultContentStorage = Collections.synchronizedList(ArrayList<DbRecordsDao>())

    private val migrationService = DbMigrationService()

    private val dataSourceContext = DbDataSourceContext(
        dataSource,
        dataServiceFactory,
        migrationService,
        webAppApi,
        remoteActionsClient
    )

    private var defaultContentStorage: EcosContentStorageConfig? = null

    fun withDataSource(dataSource: DbDataSource): DbDomainFactory {
        return DbDomainFactory(
            dataSource,
            modelServices,
            permsComponent,
            computedAttsComponent,
            defaultListeners,
            dataServiceFactory,
            webAppApi,
            remoteActionsClient
        )
    }

    fun setDefaultContentStorage(storage: EcosContentStorageConfig?) {
        synchronized(recordsDaoWithoutDefaultContentStorage) {
            this.defaultContentStorage = storage
            recordsDaoWithoutDefaultContentStorage.forEach {
                it.setDefaultContentStorage(storage)
            }
        }
    }

    fun create(domainConfig: DbDomainConfig): Builder {
        return Builder(domainConfig)
    }

    private fun migrateSchema(context: DbSchemaContext) {
        migrationService.runSchemaMigrations(context)
    }

    inner class Builder(val domainConfig: DbDomainConfig) {

        var schema: String = ""
        var permsComponent: DbPermsComponent? = null
        var computedAttsComponent: DbComputedAttsComponent? = null
        var dbContentService: DbContentService? = null
        var globalRefCalculator: DbGlobalRefCalculator? = null
        var listeners: List<DbRecordsListener>? = null
        var excludeDefaultListeners: Boolean = false

        fun withPermsComponent(permsComponent: DbPermsComponent): Builder {
            this.permsComponent = permsComponent
            return this
        }

        fun withGlobalRefCalculator(globalRefCalculator: DbGlobalRefCalculator?): Builder {
            this.globalRefCalculator = globalRefCalculator
            return this
        }

        fun withComputedAttsComponent(computedAttsComponent: DbComputedAttsComponent): Builder {
            this.computedAttsComponent = computedAttsComponent
            return this
        }

        fun withEcosContentService(dbContentService: DbContentService): Builder {
            this.dbContentService = dbContentService
            return this
        }

        fun withListeners(listeners: List<DbRecordsListener>?): Builder {
            this.listeners = listeners
            return this
        }

        fun withExcludeDefaultListeners(excludeDefaultListeners: Boolean?): Builder {
            this.excludeDefaultListeners = excludeDefaultListeners ?: false
            return this
        }

        fun withSchema(schema: String?): Builder {
            this.schema = schema ?: ""
            return this
        }

        fun build(): DbRecordsDao {

            val schemaContext = dataSourceContext.getSchemaContext(schema)

            val dataService = DbDataServiceImpl(
                DbEntity::class.java,
                domainConfig.dataService,
                schemaContext
            )

            var migrationContext: DbDomainMigrationContext? = null
            val recordsDao = DbRecordsDao(
                domainConfig.recordsDao,
                modelServices,
                dataService,
                permsComponent ?: this@DbDomainFactory.permsComponent,
                computedAttsComponent ?: this@DbDomainFactory.computedAttsComponent,
                globalRefCalculator
            ) {
                val ctx = migrationContext
                    ?: error("Migration context is null for '${domainConfig.recordsDao.id}'")

                if (webAppApi.isReady()) {
                    migrationService.runDomainMigrations(ctx)
                } else {
                    webAppApi.doBeforeAppReady {
                        migrationService.runDomainMigrations(ctx)
                    }
                }
            }

            if (!excludeDefaultListeners) {
                this@DbDomainFactory.defaultListeners.forEach {
                    recordsDao.addListener(it)
                }
            }
            listeners?.forEach {
                recordsDao.addListener(it)
            }

            recordsDao.addListener(DbIntegrityCheckListener())
            migrationContext = DbDomainMigrationContext(dataService, schemaContext, recordsDao, domainConfig)

            synchronized(recordsDaoWithoutDefaultContentStorage) {
                var storage = domainConfig.content.defaultContentStorage
                if (storage == null || storage.ref == EcosContentStorageConstants.DEFAULT_CONTENT_STORAGE_REF) {
                    recordsDaoWithoutDefaultContentStorage.add(recordsDao)
                    storage = this@DbDomainFactory.defaultContentStorage
                }
                recordsDao.setDefaultContentStorage(storage)
            }
            return recordsDao
        }
    }
}
