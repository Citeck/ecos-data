package ru.citeck.ecos.data.sql.domain

import io.github.oshai.kotlinlogging.KotlinLogging
import ru.citeck.ecos.context.lib.ctx.EcosContext
import ru.citeck.ecos.data.sql.batch.DbBatchTaskAdminDao
import ru.citeck.ecos.data.sql.batch.DbBatchTaskCancelAction
import ru.citeck.ecos.data.sql.batch.DbBatchTaskRestartAction
import ru.citeck.ecos.data.sql.content.ContentMimeTypeDetector
import ru.citeck.ecos.data.sql.content.DbContentService
import ru.citeck.ecos.data.sql.content.storage.EcosContentStorageConfig
import ru.citeck.ecos.data.sql.content.storage.EcosContentStorageConstants
import ru.citeck.ecos.data.sql.content.storage.EcosContentStorageServiceFactory
import ru.citeck.ecos.data.sql.context.DbDataSourceContext
import ru.citeck.ecos.data.sql.context.DbSchemaContext
import ru.citeck.ecos.data.sql.datasource.DbDataSource
import ru.citeck.ecos.data.sql.domain.migration.DbDomainMigrationContext
import ru.citeck.ecos.data.sql.domain.migration.DbMigrationService
import ru.citeck.ecos.data.sql.props.DbEcosDataProps
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
    val ecosContext: EcosContext,
    val remoteActionsClient: DbRecordsRemoteActionsClient?,
    val props: DbEcosDataProps = DbEcosDataProps.DEFAULT,
    val mimeTypeDetector: ContentMimeTypeDetector? = null,
    /**
     * Handed over to the data source context - see [EcosContentStorageServiceFactory].
     */
    val contentStorageServiceFactory: EcosContentStorageServiceFactory? = null
) {

    companion object {
        private val log = KotlinLogging.logger {}
    }

    private val recordsDaoWithoutDefaultContentStorage = Collections.synchronizedList(ArrayList<DbRecordsDao>())

    /**
     * Guards [setRecordsDaoRegistrar] against publishing one schema's admin views twice.
     */
    private val schemasWithPublishedAdminDaos = HashSet<String>()

    private val migrationService = DbMigrationService()

    private val dataSourceContext = DbDataSourceContext(
        dataSource,
        dataServiceFactory,
        migrationService,
        webAppApi,
        ecosContext,
        remoteActionsClient,
        props,
        mimeTypeDetector,
        contentStorageServiceFactory
    )

    private var defaultContentStorage: EcosContentStorageConfig? = null

    init {
        if (props.backgroundTasksEnabled) {
            // doWhenAppReady, never doBeforeAppReady. The one external caller of [withDataSource],
            // ecos-integrations' DbRecSrcFactory, builds its factory lazily on the first request
            // for a records source - long after the application became ready, where
            // doBeforeAppReady throws "You should not call doBeforeAppReady after application
            // become ready". doWhenAppReady runs the action straight away in that case, which is
            // exactly what is wanted: that data source's tables have been waiting since boot.
            webAppApi.doWhenAppReady {
                dataSourceContext.batchTaskEngine.start()
                dataSourceContext.modelChangeQueue.start()
                // The start-up sweep. Queued rather than executed: the tick applies the
                // per-tick ceiling to it, so an installation with hundreds of types spreads its
                // first sweep over several ticks instead of doing it all on one thread at boot.
                dataSourceContext.modelChangeQueue.requestFullSweep()
            }
        } else {
            log.info {
                "Background tasks of ecos-data are disabled by " +
                    "ecos.webapp.data.background-tasks-enabled=false. Neither the batch task drain " +
                    "nor the schema reconciliation tick will run for this data source: a type " +
                    "whose model changes will have its table repaired only by the next mutation of " +
                    "one of its records, and a column migration already queued will not carry its " +
                    "values across."
            }
        }
    }

    /**
     * Cancels the schedules the constructor registered. For the orderly shutdown of an application,
     * and for a test that built a factory and does not want its scheduler outliving it.
     *
     * Applies to **this** factory only: a factory made by [withDataSource] owns a different data
     * source context, and therefore a different engine and queue, and has to be stopped itself.
     */
    fun stopBackgroundTasks() {
        dataSourceContext.batchTaskEngine.stop()
        dataSourceContext.modelChangeQueue.stop()
    }

    /**
     * Publishes this library's own administrator views - the `ed_batch_task` list and its two row
     * actions of the batch task admin view - for every schema of this factory's data source, as each one appears.
     *
     * Per schema and not per application, because that is what the queue is: `ed_batch_task` lives
     * in a schema, `DbBatchTaskService` is built per schema, and a single global view would list
     * one schema's tasks while silently hiding the rest. The source ids therefore carry the schema
     * name, the way `record-version-$schema` already does.
     *
     * Deliberately **not** carried over by [withDataSource]. A derived factory owns a different
     * data source, but nothing stops its schemas from being named like this one's - `public` in
     * particular - and the two factories would then register the same source id twice in one
     * records service. An application that wants admin views over a second data source calls this
     * on that factory, with source ids of its own choosing if they would collide.
     *
     * Idempotent per schema: registering twice for one schema publishes once, which matters because
     * [DbDataSourceContext.onSchemaReady] can announce a schema created concurrently with the
     * registration to both the replay and the live path.
     */
    fun setRecordsDaoRegistrar(registrar: DbRecordsDaoRegistrar) {
        dataSourceContext.onSchemaReady { schemaCtx ->
            publishAdminDaos(registrar, schemaCtx)
        }
    }

    private fun publishAdminDaos(registrar: DbRecordsDaoRegistrar, schemaCtx: DbSchemaContext) {
        val schema = schemaCtx.schema
        synchronized(schemasWithPublishedAdminDaos) {
            if (!schemasWithPublishedAdminDaos.add(schema)) {
                return
            }
        }
        val listSourceId = "${DbBatchTaskAdminDao.ID}-$schema"
        val service = schemaCtx.batchTaskService
        log.info { "Publishing batch task admin views of schema '$schema' as '$listSourceId'" }
        registrar.register(DbBatchTaskAdminDao(listSourceId, service))
        registrar.register(
            DbBatchTaskCancelAction("${DbBatchTaskCancelAction.ID}-$schema", service, listSourceId)
        )
        registrar.register(
            DbBatchTaskRestartAction("${DbBatchTaskRestartAction.ID}-$schema", service, listSourceId)
        )
    }

    fun withDataSource(dataSource: DbDataSource): DbDomainFactory {
        return DbDomainFactory(
            dataSource,
            modelServices,
            permsComponent,
            computedAttsComponent,
            defaultListeners,
            dataServiceFactory,
            webAppApi,
            ecosContext,
            remoteActionsClient,
            props,
            mimeTypeDetector,
            contentStorageServiceFactory
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

    fun getSchemaContext(schema: String): DbSchemaContext {
        return dataSourceContext.getSchemaContext(schema)
    }

    fun getDataSourceContext(): DbDataSourceContext {
        return dataSourceContext
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
