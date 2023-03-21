package ru.citeck.ecos.data.sql.domain

import ru.citeck.ecos.data.sql.content.DbContentService
import ru.citeck.ecos.data.sql.context.DbDataSourceContext
import ru.citeck.ecos.data.sql.datasource.DbDataSource
import ru.citeck.ecos.data.sql.domain.migration.DbDomainMigrationContext
import ru.citeck.ecos.data.sql.domain.migration.DbMigrationService
import ru.citeck.ecos.data.sql.records.DbRecordsDao
import ru.citeck.ecos.data.sql.records.computed.DbComputedAttsComponent
import ru.citeck.ecos.data.sql.records.listener.DbRecordsListener
import ru.citeck.ecos.data.sql.records.perms.DbPermsComponent
import ru.citeck.ecos.data.sql.repo.entity.DbEntity
import ru.citeck.ecos.data.sql.service.DbDataServiceFactory
import ru.citeck.ecos.data.sql.service.DbDataServiceImpl
import ru.citeck.ecos.model.lib.ModelServiceFactory
import ru.citeck.ecos.webapp.api.EcosWebAppApi

class DbDomainFactory(
    val dataSource: DbDataSource,
    val modelServices: ModelServiceFactory,
    val permsComponent: DbPermsComponent,
    val computedAttsComponent: DbComputedAttsComponent,
    val defaultListeners: List<DbRecordsListener>,
    dataServiceFactory: DbDataServiceFactory,
    val webAppApi: EcosWebAppApi
) {

    private val dataSourceContext = DbDataSourceContext(
        webAppApi.getProperties().appName,
        dataSource,
        dataServiceFactory
    )

    private val migrationService = DbMigrationService()

    fun create(domainConfig: DbDomainConfig): Builder {
        return Builder(domainConfig)
    }

    inner class Builder(val domainConfig: DbDomainConfig) {

        var schema: String = ""
        var permsComponent: DbPermsComponent? = null
        var computedAttsComponent: DbComputedAttsComponent? = null
        var dbContentService: DbContentService? = null
        var listeners: List<DbRecordsListener>? = null
        var excludeDefaultListeners: Boolean = false

        fun withPermsComponent(permsComponent: DbPermsComponent): Builder {
            this.permsComponent = permsComponent
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

            val recordsDao = DbRecordsDao(
                domainConfig.recordsDao,
                modelServices,
                dataService,
                permsComponent ?: this@DbDomainFactory.permsComponent,
                computedAttsComponent ?: this@DbDomainFactory.computedAttsComponent
            )

            if (!excludeDefaultListeners) {
                this@DbDomainFactory.defaultListeners.forEach {
                    recordsDao.addListener(it)
                }
            }
            listeners?.forEach {
                recordsDao.addListener(it)
            }

            val migrationContext = DbDomainMigrationContext(dataService, schemaContext)
            if (webAppApi.isReady()) {
                migrationService.runMigrations(migrationContext)
            } else {
                webAppApi.doBeforeAppReady {
                    migrationService.runMigrations(migrationContext)
                }
            }

            return recordsDao
        }
    }
}
