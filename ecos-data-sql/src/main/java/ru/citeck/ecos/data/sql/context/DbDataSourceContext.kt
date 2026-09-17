package ru.citeck.ecos.data.sql.context

import io.github.oshai.kotlinlogging.KotlinLogging
import ru.citeck.ecos.context.lib.ctx.EcosContext
import ru.citeck.ecos.data.sql.batch.DbBatchTaskEngine
import ru.citeck.ecos.data.sql.batch.DbBatchTaskHandlerRegistry
import ru.citeck.ecos.data.sql.content.ContentMimeTypeDetector
import ru.citeck.ecos.data.sql.content.SafeContentMimeTypeDetector
import ru.citeck.ecos.data.sql.content.storage.EcosContentStorageService
import ru.citeck.ecos.data.sql.content.storage.EcosContentStorageServiceFactory
import ru.citeck.ecos.data.sql.content.storage.EcosContentStorageServiceImpl
import ru.citeck.ecos.data.sql.datasource.DbDataSource
import ru.citeck.ecos.data.sql.domain.migration.DbMigrationService
import ru.citeck.ecos.data.sql.migration.column.DbColumnMigrationHandler
import ru.citeck.ecos.data.sql.modelchange.DbModelChangeQueue
import ru.citeck.ecos.data.sql.modelchange.DbRecordsDaoIndex
import ru.citeck.ecos.data.sql.props.DbEcosDataProps
import ru.citeck.ecos.data.sql.remote.DbRecordsRemoteActionsClient
import ru.citeck.ecos.data.sql.repo.DbEntityRepo
import ru.citeck.ecos.data.sql.schema.DbSchemaDao
import ru.citeck.ecos.data.sql.service.DbDataServiceFactory
import ru.citeck.ecos.data.sql.type.DbTypesConverter
import ru.citeck.ecos.txn.lib.TxnContext
import ru.citeck.ecos.webapp.api.EcosWebAppApi
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList
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
    companion object {
        private val log = KotlinLogging.logger {}
    }

    /**
     * Wrapped on the way in, so that nothing downstream has to defend itself against the
     * application's detector - see [SafeContentMimeTypeDetector].
     */
    val mimeTypeDetector: ContentMimeTypeDetector? = mimeTypeDetector?.let { SafeContentMimeTypeDetector(it) }

    val appName: String = webAppApi.getProperties().appName
    val converter: DbTypesConverter = DbTypesConverter()
    val entityRepo: DbEntityRepo = dataServiceFactory.createEntityRepo()
    val schemaDao: DbSchemaDao = dataServiceFactory.createSchemaDao()

    /**
     * Handlers available to the batch engine of this data source. Populated by the composition
     * root (a Spring autoconfig in a webapp, the test factory in tests); empty is legal and simply
     * means every queued task is skipped with a warning.
     */
    val batchTaskHandlers: DbBatchTaskHandlerRegistry = DbBatchTaskHandlerRegistry()

    /**
     * The batch engine of this data source. Constructed eagerly because it is cheap and stateless
     * until started, but **not started here**: the test mock returns a
     * real thread-pool-backed scheduler, so a self-starting engine would put background threads
     * into every test in the repository. The composition root calls [DbBatchTaskEngine.start].
     */
    val batchTaskEngine: DbBatchTaskEngine = DbBatchTaskEngine(this)

    /**
     * "Type id -> the live records DAOs of that type" over this data source. Filled in by
     * [ru.citeck.ecos.data.sql.records.DbRecordsDao.setRecordsServiceFactory], the one path both
     * production and the tests of this repository take.
     */
    val recordsDaoIndex: DbRecordsDaoIndex = DbRecordsDaoIndex()

    /**
     * The coalescing queue of model changes over this data source. Constructed eagerly for the same
     * reason the batch engine is - it is cheap and does nothing until [DbModelChangeQueue.start] -
     * and started by the same composition root, [ru.citeck.ecos.data.sql.domain.DbDomainFactory].
     *
     * It needs no model service here: it takes one from the first DAO the index receives, because
     * `DbDomainFactory` gives every DAO over one data source the same `ModelServiceFactory` and a
     * data source context has none of its own.
     */
    val modelChangeQueue: DbModelChangeQueue = DbModelChangeQueue(this)

    /**
     * `webAppApi` is otherwise private here. The background workers of this data source - the batch
     * engine's drain and the model change queue's reconcile tick - need the lock and scheduler APIs
     * and live at data-source level, not schema level, so they cannot borrow
     * `DbSchemaContext.webAppApi` the way [ru.citeck.ecos.data.sql.service.DbDataServiceImpl] does.
     */
    internal val webAppApiForBackgroundTasks: EcosWebAppApi
        get() = webAppApi

    private val schemasByName = ConcurrentHashMap<String, DbSchemaContext>()

    /**
     * Called once per schema of this data source, after its migrations have run - see
     * [onSchemaReady].
     */
    private val schemaReadyListeners = CopyOnWriteArrayList<(DbSchemaContext) -> Unit>()

    init {
        dataServiceFactory.registerConverters(converter)
        // The one handler ecos-data ships itself. It needs nothing from the webapp, so unlike the
        // engine's start() there is no reason to defer this to a composition
        // root - and deferring it would leave every queued migration unrunnable in tests.
        batchTaskHandlers.register(DbColumnMigrationHandler(this))
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

    /**
     * The schema name is mandatory and must be non-blank.
     *
     * An empty name used to mean "whatever the connection's search_path resolves to", but nothing
     * downstream could ask the catalog about it: [DbSchemaDao.isTableExists] filters
     * `pg_tables.schemaname = ?` and [DbSchemaDao.isSchemaExists] filters `pg_namespace.nspname = ?`,
     * and neither can ever match the empty string. Every caller of those two - the `ecos_schema_meta`
     * rename detection in [DbMigrationService], `RenameEcosDataTables`, `MakeAttributesExtIdUnique` -
     * therefore degraded into a silent no-op, and a schema that already held data was classified as
     * brand new and skipped every migration. Rejecting the blank name at the single entry point is
     * what keeps that class of silent failure unreachable.
     */
    fun getSchemaContext(schema: String): DbSchemaContext {
        if (schema.isBlank()) {
            error(
                "Schema name is required and can't be blank. Declare it explicitly on the domain " +
                    "being built - DbDomainFactory.Builder.withSchema(\"<schema>\") - the same way " +
                    "the platform's own domains do (emodel uses withSchema(\"ecos_data\"), " +
                    "ecos-apps and ecos-integrations use withSchema(\"public\")). For a records " +
                    "source over an external data source, set the \"schema\" property of its config."
            )
        }
        val newContextWasCreated = AtomicBoolean()
        val result = schemasByName.computeIfAbsent(schema) { k ->
            newContextWasCreated.set(true)
            DbSchemaContext(k, this, webAppApi, ecosContext)
        }
        if (newContextWasCreated.get()) {
            migrationService.runSchemaMigrations(result)
            // after the migrations and not before: a listener that publishes an admin view of
            // `ed_batch_task` would otherwise be handed a schema in which that table does not exist
            // yet, because creating it *is* one of those migrations
            schemaReadyListeners.forEach { notifySchemaReady(result, it) }
        }
        return result
    }

    /**
     * Registers [listener] to be called for every schema of this data source, and calls it straight
     * away for the schemas that already exist.
     *
     * The replay is what makes this usable from a composition root at all: schemas are created
     * lazily, by whatever first asks for one, and an application that registers its listener after
     * that has happened would otherwise never hear about them. The cost of the replay is that a
     * schema created concurrently with the registration can be announced twice - so a listener has
     * to be idempotent, which is cheap to be and impossible to arrange from here.
     *
     * A listener never sees an exception of its own escape into schema creation: publishing an
     * administrator's view is not a reason to fail the schema that view describes.
     */
    fun onSchemaReady(listener: (DbSchemaContext) -> Unit) {
        schemaReadyListeners.add(listener)
        getSchemaContexts().forEach { notifySchemaReady(it, listener) }
    }

    private fun notifySchemaReady(schemaCtx: DbSchemaContext, listener: (DbSchemaContext) -> Unit) {
        try {
            listener.invoke(schemaCtx)
        } catch (e: InterruptedException) {
            Thread.currentThread().interrupt()
            throw e
        } catch (e: Throwable) {
            log.error(e) { "Schema ready listener failed for schema '${schemaCtx.schema}'" }
        }
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
