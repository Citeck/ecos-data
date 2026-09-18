package ru.citeck.ecos.data.sql.test.records

import io.github.oshai.kotlinlogging.KotlinLogging
import org.junit.jupiter.api.Assumptions
import ru.citeck.ecos.commons.data.MLText
import ru.citeck.ecos.commons.data.ObjectData
import ru.citeck.ecos.commons.json.Json
import ru.citeck.ecos.commons.mime.MimeTypes
import ru.citeck.ecos.commons.utils.NameUtils
import ru.citeck.ecos.commons.utils.TmplUtils
import ru.citeck.ecos.context.lib.ctx.EcosContext
import ru.citeck.ecos.context.lib.ctx.EcosContextImpl
import ru.citeck.ecos.data.sql.content.ContentMimeTypeDetector
import ru.citeck.ecos.data.sql.content.storage.EcosContentStorageService
import ru.citeck.ecos.data.sql.content.storage.EcosContentStorageServiceFactory
import ru.citeck.ecos.data.sql.content.storage.EcosContentStorageServiceImpl
import ru.citeck.ecos.data.sql.context.DbDataSourceContext
import ru.citeck.ecos.data.sql.context.DbSchemaContext
import ru.citeck.ecos.data.sql.context.DbTableContext
import ru.citeck.ecos.data.sql.datasource.DbDataSource
import ru.citeck.ecos.data.sql.domain.migration.DbMigrationService
import ru.citeck.ecos.data.sql.dto.DbColumnDef
import ru.citeck.ecos.data.sql.dto.DbTableRef
import ru.citeck.ecos.data.sql.ecostype.DbEcosModelService
import ru.citeck.ecos.data.sql.props.DbEcosDataProps
import ru.citeck.ecos.data.sql.records.DbRecordsDao
import ru.citeck.ecos.data.sql.records.DbRecordsDaoConfig
import ru.citeck.ecos.data.sql.records.assocs.DbAssocsService
import ru.citeck.ecos.data.sql.records.computed.DbComputedAttsComponent
import ru.citeck.ecos.data.sql.records.dao.atts.DbRecord
import ru.citeck.ecos.data.sql.records.listener.DbIntegrityCheckListener
import ru.citeck.ecos.data.sql.records.perms.DbPermsComponent
import ru.citeck.ecos.data.sql.records.perms.DbRecordPerms
import ru.citeck.ecos.data.sql.records.perms.DefaultDbPermsComponent
import ru.citeck.ecos.data.sql.records.refs.DbRecordRefService
import ru.citeck.ecos.data.sql.remote.DbRecordsRemoteActionsServiceImpl
import ru.citeck.ecos.data.sql.repo.entity.DbEntity
import ru.citeck.ecos.data.sql.schema.DbSchemaDao
import ru.citeck.ecos.data.sql.service.DbDataService
import ru.citeck.ecos.data.sql.service.DbDataServiceConfig
import ru.citeck.ecos.data.sql.service.DbDataServiceImpl
import ru.citeck.ecos.model.lib.ModelServiceFactory
import ru.citeck.ecos.model.lib.api.EcosModelAppApi
import ru.citeck.ecos.model.lib.aspect.dto.AspectInfo
import ru.citeck.ecos.model.lib.aspect.repo.AspectsRepo
import ru.citeck.ecos.model.lib.attributes.computed.ComputeRes
import ru.citeck.ecos.model.lib.attributes.dto.AttOptionValue
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType
import ru.citeck.ecos.model.lib.attributes.dto.computed.ComputedAttDef
import ru.citeck.ecos.model.lib.delegation.dto.AuthDelegation
import ru.citeck.ecos.model.lib.delegation.dto.PermissionDelegateData
import ru.citeck.ecos.model.lib.delegation.service.DelegationService
import ru.citeck.ecos.model.lib.num.dto.NumTemplateDef
import ru.citeck.ecos.model.lib.num.repo.NumTemplatesRepo
import ru.citeck.ecos.model.lib.permissions.dto.PermissionType
import ru.citeck.ecos.model.lib.type.dto.QueryPermsPolicy
import ru.citeck.ecos.model.lib.type.dto.TypeInfo
import ru.citeck.ecos.model.lib.type.dto.TypeModelDef
import ru.citeck.ecos.model.lib.type.repo.TypesRepo
import ru.citeck.ecos.model.lib.utils.ModelUtils
import ru.citeck.ecos.model.lib.workspace.WorkspaceService
import ru.citeck.ecos.model.lib.workspace.api.WorkspaceApi
import ru.citeck.ecos.model.lib.workspace.api.WsMembershipType
import ru.citeck.ecos.records2.RecordConstants
import ru.citeck.ecos.records2.predicate.PredicateService
import ru.citeck.ecos.records2.predicate.model.VoidPredicate
import ru.citeck.ecos.records3.RecordsService
import ru.citeck.ecos.records3.RecordsServiceFactory
import ru.citeck.ecos.records3.record.atts.schema.ScalarType
import ru.citeck.ecos.records3.record.dao.impl.mem.InMemDataRecordsDao
import ru.citeck.ecos.records3.record.dao.query.dto.query.RecordsQuery
import ru.citeck.ecos.records3.record.dao.query.dto.query.SortBy
import ru.citeck.ecos.records3.record.request.RequestContext
import ru.citeck.ecos.test.commons.EcosWebAppApiMock
import ru.citeck.ecos.txn.lib.TxnContext
import ru.citeck.ecos.txn.lib.manager.EcosTxnProps
import ru.citeck.ecos.txn.lib.manager.TransactionManagerImpl
import ru.citeck.ecos.webapp.api.EcosWebAppApi
import ru.citeck.ecos.webapp.api.content.EcosContentData
import ru.citeck.ecos.webapp.api.entity.EntityRef
import ru.citeck.ecos.webapp.api.entity.toEntityRef
import ru.citeck.ecos.webapp.api.lock.EcosLock
import ru.citeck.ecos.webapp.api.lock.EcosLockApi
import ru.citeck.ecos.webapp.api.mime.MimeType
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong

open class DataMockFactory : AutoCloseable {

    companion object {
        const val APP_NAME = "test-app"
        const val RECS_DAO_ID = "test"
        const val REC_TEST_TYPE_ID = "test-type"

        const val TEMP_FILE_TYPE_ID = "temp-file"
        const val THUMBNAIL_TYPE_ID = "thumbnail"
        const val ATTACHMENT_TYPE_ID = "attachment"

        val REC_TEST_TYPE_REF = ModelUtils.getTypeRef(REC_TEST_TYPE_ID)

        val DEFAULT_TABLE_REF = DbTableRef("records-test-schema", "test-records-table")

        private val DEFAULT_ASPECTS = listOf(
            AspectInfo.create()
                .withId(DbRecord.ASPECT_VERSIONABLE)
                .build(),
            AspectInfo.create()
                .withId("thumbnail")
                .withSystemAttributes(
                    listOf(
                        AttributeDef.create()
                            .withId("thumbnail:thumbnails")
                            .withType(AttributeType.ASSOC)
                            .withConfig(
                                ObjectData.create()
                                    .set("typeRef", ModelUtils.getTypeRef(THUMBNAIL_TYPE_ID))
                                    .set("child", true)
                            )
                            .build()
                    )
                )
                .build(),
            AspectInfo.create()
                .withId("versionable-data")
                .withAttributes(
                    listOf(
                        AttributeDef.create()
                            .withId("version:version")
                            .build(),
                        AttributeDef.create()
                            .withId("version:comment")
                            .build(),
                        AttributeDef.create()
                            .withId("version:versions")
                            .withType(AttributeType.ASSOC)
                            .withConfig(ObjectData.create().set("child", true))
                            .build()
                    )
                )
                .build()
        )
        private val DEFAULT_TYPES = listOf(
            TypeInfo.create()
                .withId("temp-file")
                .withParentRef(ModelUtils.getTypeRef("base"))
                .withSourceId(TEMP_FILE_TYPE_ID)
                .withModel(
                    TypeModelDef.create()
                        .withAttributes(
                            listOf(
                                AttributeDef.create()
                                    .withId("name")
                                    .build(),
                                AttributeDef.create()
                                    .withId("content")
                                    .withType(AttributeType.CONTENT)
                                    .build()
                            )
                        )
                        .build()
                ).build(),
            TypeInfo.create()
                .withId(THUMBNAIL_TYPE_ID)
                .withParentRef(ModelUtils.getTypeRef("base"))
                .withSourceId(THUMBNAIL_TYPE_ID)
                .withModel(
                    TypeModelDef.create()
                        .withAttributes(
                            listOf(
                                AttributeDef.create()
                                    .withId("mimeType")
                                    .build(),
                                AttributeDef.create()
                                    .withId("srcAttribute")
                                    .build(),
                                AttributeDef.create()
                                    .withId("status")
                                    .build(),
                                AttributeDef.create()
                                    .withId("content")
                                    .withType(AttributeType.CONTENT)
                                    .build()
                            )
                        )
                        .build()
                ).build(),
            TypeInfo.create()
                .withId(ATTACHMENT_TYPE_ID)
                .withParentRef(ModelUtils.getTypeRef("base"))
                .withSourceId(ATTACHMENT_TYPE_ID)
                .withModel(
                    TypeModelDef.create()
                        .withAttributes(
                            listOf(
                                AttributeDef.create()
                                    .withId("name")
                                    .withType(AttributeType.MLTEXT)
                                    .build(),
                                AttributeDef.create()
                                    .withId("content")
                                    .withType(AttributeType.CONTENT)
                                    .build()
                            )
                        )
                        .build()
                ).build(),
            TypeInfo.create()
                .withId("user-base")
                .withParentRef(ModelUtils.getTypeRef("base"))
                .build(),
            TypeInfo.create()
                .withId("base")
                .build()
        )
    }

    private val typesInfo = mutableMapOf<String, TypeInfo>()

    /**
     * Subscribers of the mock [TypesRepo], i.e. what a real registry keeps in `listenEvents`.
     *
     * Not cleared by [setUp]: the data source context - and therefore the model change queue that
     * subscribed through it - is built once per factory and survives a second `setUp`, so dropping
     * the listeners here would leave that queue subscribed to a repo that no longer publishes
     * anything. The listeners are per factory instance, and a factory instance is per test.
     */
    private val typeChangeListeners = CopyOnWriteArrayList<(String, TypeInfo?, TypeInfo?) -> Unit>()
    private val aspectsInfo = mutableMapOf<String, AspectInfo>()
    private val numTemplates = mutableMapOf<String, NumTemplateDef>()

    lateinit var records: RecordsService
    lateinit var recordsServiceFactory: RecordsServiceFactory
    lateinit var dbDataSource: DbDataSource
    lateinit var dataSourceCtx: DbDataSourceContext

    /**
     * What the platform told the other applications about association changes. Non-null for every
     * test - see [RecordingRemoteActionsClient] for why it is installed unconditionally.
     */
    val remoteActionsClient = RecordingRemoteActionsClient()

    lateinit var dbSchemaDao: DbSchemaDao
    lateinit var dbRecordRefService: DbRecordRefService
    lateinit var assocsService: DbAssocsService
    lateinit var backend: DbRecordsTestBackend

    /**
     * A barrier the test can put **inside** a transaction the production code owns: called with the
     * table and the rows immediately before the storage writes them, on the same thread, so the test
     * can run a second transaction of its own from between two statements that were written as if
     * nothing could happen between them. Null - no barrier - for every test that does not set it,
     * and reset to null before each one. See [DbStorageWriteBarrier].
     */
    var beforeStorageWrite: ((DbTableRef, List<Map<String, Any?>>) -> Unit)? = null
    lateinit var computedAttsComponent: DbComputedAttsComponent
    lateinit var modelServiceFactory: ModelServiceFactory

    lateinit var webAppApi: EcosWebAppApiMock

    lateinit var mainCtx: RecordsDaoTestCtx
    lateinit var tempCtx: RecordsDaoTestCtx
    lateinit var thumbnailCtx: RecordsDaoTestCtx
    lateinit var attachmentCtx: RecordsDaoTestCtx

    lateinit var delegationService: CustomDelegationService
    lateinit var workspaceService: CustomWorkspaceService

    lateinit var ecosContext: EcosContext

    /**
     * Global ecos-data properties. Should be changed before [setUp] call
     * (e.g. in init block of test class) to take effect.
     */
    var dataProps: DbEcosDataProps = DbEcosDataProps()

    private var mainCtxInitialized = false
    private val registeredRecordsDao = ArrayList<RecordsDaoTestCtx>()

    private var schemaContexts = ConcurrentHashMap<String, DbSchemaContext>()

    val tableRef: DbTableRef
        get() = mainCtx.tableRef

    val recordsDao: DbRecordsDao
        get() = mainCtx.dao

    val log = KotlinLogging.logger {}

    val baseQuery: RecordsQuery
        get() = mainCtx.baseQuery

    fun setUp() {

        typesInfo.clear()
        aspectsInfo.clear()
        numTemplates.clear()
        schemaContexts.clear()
        beforeStorageWrite = null

        DEFAULT_ASPECTS.forEach { aspectsInfo[it.id] = it }
        DEFAULT_TYPES.forEach { typesInfo[it.id] = it }

        if (mainCtxInitialized) {
            mainCtx.clear()

            val daoToClean = ArrayList(registeredRecordsDao)
            registeredRecordsDao.clear()
            daoToClean.forEach {
                records.unregister(it.dao.getId())
            }
        } else {
            webAppApi = object : EcosWebAppApiMock(APP_NAME) {
                private val countingLockApi = CountingLockApi(super.getAppLockApi())
                override fun getContentApi(): ContentApiMock {
                    return ContentApiTest()
                }
                override fun getAppLockApi(): EcosLockApi {
                    return countingLockApi
                }
            }

            backend = DbRecordsTestBackends.create(webAppApi)

            val remoteActions = DbRecordsRemoteActionsServiceImpl()

            val txnManager = TransactionManagerImpl()
            txnManager.init(webAppApi, EcosTxnProps())
            TxnContext.setManager(txnManager)

            recordsServiceFactory = object : RecordsServiceFactory() {
                override fun getEcosWebAppApi(): EcosWebAppApi {
                    return webAppApi
                }
            }
            delegationService = CustomDelegationService()
            workspaceService = CustomWorkspaceService()

            val numCounters = mutableMapOf<String, AtomicLong>()
            modelServiceFactory = object : ModelServiceFactory() {

                override fun createAspectsRepo(): AspectsRepo {
                    return object : AspectsRepo {
                        override fun getAspectInfo(aspectRef: EntityRef): AspectInfo? {
                            return aspectsInfo[aspectRef.getLocalId()]
                        }

                        override fun getAspectsForAtts(attributes: Set<String>): List<EntityRef> {
                            return aspectsInfo.values.filter { aspect ->
                                aspect.attributes.any { attributes.contains(it.id) } ||
                                    aspect.systemAttributes.any { attributes.contains(it.id) }
                            }.map { ModelUtils.getAspectRef(it.id) }
                        }
                    }
                }

                override fun createTypesRepo(): TypesRepo {
                    return object : TypesRepo {
                        override fun getChildren(typeRef: EntityRef): List<EntityRef> {
                            return typesInfo.values.filter {
                                it.parentRef == typeRef
                            }.map {
                                ModelUtils.getTypeRef(it.id)
                            }
                        }

                        override fun getTypeInfo(typeRef: EntityRef): TypeInfo? {
                            return typesInfo[typeRef.getLocalId()]
                        }

                        override fun listenTypeChanges(
                            listener: (typeId: String, before: TypeInfo?, after: TypeInfo?) -> Unit
                        ) {
                            typeChangeListeners.add(listener)
                        }
                    }
                }

                override fun createNumTemplatesRepo(): NumTemplatesRepo {
                    return object : NumTemplatesRepo {
                        override fun getNumTemplate(templateRef: EntityRef): NumTemplateDef? {
                            return numTemplates[templateRef.getLocalId()]
                        }
                    }
                }

                override fun createEcosModelAppApi(): EcosModelAppApi {
                    return object : EcosModelAppApi {
                        override fun getNextNumberForModel(model: ObjectData, templateRef: EntityRef): Long {
                            val numTemplateDef = numTemplates[templateRef.getLocalId()]
                                ?: error("Num template is not found: $templateRef")
                            val keyTemplate = numTemplateDef.counterKey.ifBlank { templateRef.getLocalId() }
                            val keyValue = TmplUtils.applyAtts(keyTemplate, model).asText()
                            return numCounters.computeIfAbsent(keyValue) { AtomicLong() }.incrementAndGet()
                        }
                    }
                }

                override fun createDelegationService(): DelegationService {
                    return this@DataMockFactory.delegationService
                }

                override fun createWorkspaceApi(): WorkspaceApi {
                    return this@DataMockFactory.workspaceService
                }

                override fun getEcosWebAppApi(): EcosWebAppApi {
                    return webAppApi
                }
            }
            modelServiceFactory.setRecordsServices(recordsServiceFactory)
            delegationService.ecosTypeService = DbEcosModelService(modelServiceFactory)

            workspaceService.impl = modelServiceFactory.workspaceService

            dbDataSource = backend.dbDataSource
            ecosContext = EcosContextImpl(null)

            dataSourceCtx = DbDataSourceContext(
                dbDataSource,
                DbStorageWriteBarrier(backend.dataServiceFactory) { beforeStorageWrite },
                DbMigrationService(),
                webAppApi,
                ecosContext,
                remoteActionsClient = remoteActionsClient,
                props = dataProps,
                mimeTypeDetector = mimeTypeDetectorOverride,
                contentStorageServiceFactory = EcosContentStorageServiceFactory { schemaCtx ->
                    contentStorageServiceOverride ?: EcosContentStorageServiceImpl(webAppApi, schemaCtx)
                }
            )

            records = recordsServiceFactory.recordsService
            RequestContext.setDefaultServices(recordsServiceFactory)

            remoteActions.init(dataSourceCtx, webAppApi, records)
        }

        mainCtx = createRecordsDao()

        tempCtx = createRecordsDao(
            DEFAULT_TABLE_REF.withTable(TEMP_FILE_TYPE_ID),
            ModelUtils.getTypeRef(TEMP_FILE_TYPE_ID),
            TEMP_FILE_TYPE_ID
        )

        thumbnailCtx = createRecordsDao(
            DEFAULT_TABLE_REF.withTable(THUMBNAIL_TYPE_ID),
            ModelUtils.getTypeRef(THUMBNAIL_TYPE_ID),
            THUMBNAIL_TYPE_ID
        )
        attachmentCtx = createRecordsDao(
            DEFAULT_TABLE_REF.withTable(ATTACHMENT_TYPE_ID),
            ModelUtils.getTypeRef(ATTACHMENT_TYPE_ID),
            ATTACHMENT_TYPE_ID
        )

        records.register(InMemDataRecordsDao("emodel/type"))
        records.mutate(
            "emodel/type@",
            mapOf(
                "id" to REC_TEST_TYPE_ID,
                "system" to true,
                "name" to ObjectData.create().set("ru", "Русский").set("en", "English")
            )
        )

        mainCtxInitialized = true
    }

    /**
     * Test seam only, for suites (e.g. `ChunkedUploadContractTest`) that need a fake
     * [ru.citeck.ecos.data.sql.content.storage.EcosContentStorageService] wired into the schema
     * used by [createRecordsDao] instead of the real remote-routing implementation. Set (if needed)
     * before [setUp] runs - e.g. from the test subclass's `init` block, since JUnit5 runs superclass
     * `@BeforeEach` methods (this class's [setUp]) before the subclass's own.
     */
    var contentStorageServiceOverride: EcosContentStorageService? = null

    /**
     * Test seam only, for suites (e.g. `ChunkedUploadContractTest`) that need a
     * [ru.citeck.ecos.data.sql.content.ContentMimeTypeDetector] behind the data source context this
     * factory builds; leaving it null gives the detector-less default. Set (if needed) before
     * [setUp] runs - e.g. from the test subclass's `init` block, since the context is built there and
     * never replaced afterwards.
     */
    var mimeTypeDetectorOverride: ContentMimeTypeDetector? = null

    private fun getOrCreateSchemaCtx(schema: String): DbSchemaContext {
        return schemaContexts.computeIfAbsent(schema) {
            val schemaCtx = dataSourceCtx.getSchemaContext(schema)
            // The one moment in the whole run at which this schema is provably pristine:
            // getSchemaContext has just run runSchemaMigrations, so every ed_* system table exists
            // with the structure and the metadata rows ecos-data itself gave it, and no test body
            // has executed yet. That is what the cache has to be able to restore, so it is where
            // it is captured. Only the suite's own schema is cacheable - a test which builds a
            // second schema is building it for a reason and gets it fresh every time.
            if (schema == DEFAULT_TABLE_REF.schema && DbTestSchemaCache.isSchemaReuseActive()) {
                backend.snapshotPristineSchema(schema)
            }
            schemaCtx
        }
    }

    override fun close() {
        if (!mainCtxInitialized) {
            // never set up (or already torn down before any setUp) - nothing to release, and the
            // lateinit backend isn't assigned yet; return instead of throwing on standalone misuse.
            return
        }
        RequestContext.setDefaultServices(null)
        if (DbTestSchemaCache.isSchemaReuseActive()) {
            // Same post-condition as dropAllTables(), reached the cheap way where the backend can.
            backend.resetForNextTest()
        } else {
            dropAllTables()
        }
        backend.close()
    }

    fun setAuthoritiesWithAttReadPerms(rec: EntityRef, att: String, vararg authorities: String) {
        mainCtx.setAuthoritiesWithAttReadPerms(rec, att, *authorities)
    }

    fun setAuthoritiesWithAttWritePerms(rec: EntityRef, att: String, vararg authorities: String) {
        mainCtx.setAuthoritiesWithAttWritePerms(rec, att, *authorities)
    }

    fun setAuthoritiesWithWritePerms(rec: EntityRef, vararg authorities: String) {
        setAuthoritiesWithWritePerms(rec, authorities.toList())
    }

    fun setAuthoritiesWithWritePerms(rec: EntityRef, authorities: Collection<String>) {
        mainCtx.setAuthoritiesWithWritePerms(rec, authorities)
    }

    fun setAuthoritiesWithReadPerms(rec: EntityRef, authorities: Collection<String>) {
        mainCtx.setAuthoritiesWithReadPerms(rec, authorities)
    }

    fun setAuthoritiesWithReadPerms(rec: EntityRef, vararg authorities: String) {
        mainCtx.setAuthoritiesWithReadPerms(rec, *authorities)
    }

    fun addAuthoritiesWithReadPerms(rec: EntityRef, authorities: Collection<String>) {
        mainCtx.addAuthoritiesWithReadPerms(rec, authorities)
    }

    fun addAdditionalPermission(rec: EntityRef, authority: String, permission: String) {
        mainCtx.addAdditionalPermission(rec, authority, permission)
    }

    fun addAdditionalPermission(rec: EntityRef, authorities: Collection<String>, permission: String) {
        mainCtx.addAdditionalPermission(rec, authorities, permission)
    }

    fun addAuthoritiesWithReadPerms(rec: EntityRef, vararg authorities: String) {
        mainCtx.addAuthoritiesWithReadPerms(rec, *authorities)
    }

    fun getTableCtx(): DbTableContext {
        return mainCtx.dataService.getTableContext()
    }

    fun createRecordsDao(
        tableRef: DbTableRef = DEFAULT_TABLE_REF,
        typeRef: EntityRef = ModelUtils.getTypeRef(REC_TEST_TYPE_ID),
        sourceId: String = RECS_DAO_ID,
        allowRecordIdUpdate: Boolean = true,
        enableTotalCount: Boolean = true
    ): RecordsDaoTestCtx {

        val schemaCtx = getOrCreateSchemaCtx(tableRef.schema)
        dbSchemaDao = dataSourceCtx.schemaDao

        val dataServiceConfig = DbDataServiceConfig.create {
            withTable(tableRef.table)
        }
        val dataService = DbDataServiceImpl(
            DbEntity::class.java,
            dataServiceConfig,
            schemaCtx
        )

        val defaultPermsComponent = DefaultDbPermsComponent(records, modelServiceFactory.workspaceService)

        val recAdditionalPerms: MutableMap<EntityRef, MutableMap<String, MutableSet<String>>> = mutableMapOf()
        val recReadPerms: MutableMap<EntityRef, Set<String>> = mutableMapOf()
        val recWritePerms: MutableMap<EntityRef, Set<String>> = mutableMapOf()
        val recAttReadPerms: MutableMap<Pair<EntityRef, String>, Set<String>> = mutableMapOf()
        val recAttWritePerms: MutableMap<Pair<EntityRef, String>, Set<String>> = mutableMapOf()

        val permsComponent = object : DbPermsComponent {
            override fun getRecordPerms(user: String, authorities: Set<String>, record: Any): DbRecordPerms {
                val globalRef = records.getAtt(record, ScalarType.ID_SCHEMA)
                    .toEntityRef()
                    .withDefaultAppName(APP_NAME)
                return object : DbRecordPerms {

                    override fun getAdditionalPerms(): Set<String> {
                        val perms = recAdditionalPerms[globalRef] ?: emptyMap()
                        if (perms.isEmpty()) {
                            return emptySet()
                        }
                        val additionalPerms = HashSet<String>(perms[user] ?: emptySet())
                        authorities.forEach { auth ->
                            additionalPerms.addAll(perms[auth] ?: emptySet())
                        }
                        return additionalPerms
                    }

                    override fun getAuthoritiesWithReadPermission(): Set<String> {
                        if (recReadPerms.containsKey(globalRef)) {
                            return recReadPerms[globalRef]!!
                        }
                        return defaultPermsComponent.getRecordPerms(user, authorities, globalRef)
                            .getAuthoritiesWithReadPermission()
                    }

                    override fun hasReadPerms(): Boolean {
                        val authoritiesWithReadPerms = getAuthoritiesWithReadPermission()
                        var hasReadPerms = authoritiesWithReadPerms.any { authorities.contains(it) }
                        if (!hasReadPerms) {
                            hasReadPerms = hasDelegatedPerms(authoritiesWithReadPerms)
                        }
                        return hasReadPerms
                    }

                    override fun hasWritePerms(): Boolean {
                        val perms = recWritePerms[globalRef] ?: emptySet()
                        var hasWritePerms = perms.isEmpty() || authorities.any { perms.contains(it) }
                        if (!hasWritePerms) {
                            hasWritePerms = hasDelegatedPerms(perms)
                        }
                        return hasWritePerms
                    }

                    override fun hasAttWritePerms(name: String): Boolean {
                        val writePerms = recAttWritePerms[globalRef to name]
                        return writePerms.isNullOrEmpty() ||
                            authorities.any {
                                writePerms.contains(it)
                            }
                    }

                    override fun hasAttReadPerms(name: String): Boolean {
                        val readPerms = recAttReadPerms[globalRef to name]
                        return readPerms.isNullOrEmpty() ||
                            authorities.any {
                                readPerms.contains(it)
                            }
                    }

                    private fun hasDelegatedPerms(perms: Set<String>): Boolean {
                        if (record is DbRecord) {
                            val typeId = record.type.getLocalId()
                            val delegations = delegationService.getActiveAuthDelegations(user, listOf(typeId))
                            return delegations.any { delegation ->
                                perms.any { delegation.delegatedAuthorities.contains(it) }
                            }
                        }
                        return false
                    }
                }
            }
        }

        computedAttsComponent = object : DbComputedAttsComponent {
            override fun getAttOptions(record: Any, config: ObjectData): List<AttOptionValue> {
                return modelServiceFactory.computedAttsService.getAttOptions(record, config)
            }

            override fun computeAttsToStore(
                value: Any,
                isNewRecord: Boolean,
                typeRef: EntityRef,
                preCalculatedAtts: Map<String, Any?>
            ): ObjectData {
                return modelServiceFactory.computedAttsService.computeAttsToStore(
                    value,
                    isNewRecord,
                    typeRef,
                    preCalculatedAtts
                )
            }

            override fun computeDisplayName(value: Any, typeRef: EntityRef): MLText {
                return modelServiceFactory.computedAttsService.computeDisplayName(value, typeRef)
            }

            override fun computeAtt(
                value: Any,
                attId: String,
                attType: AttributeType,
                computed: ComputedAttDef
            ): ComputeRes {
                return modelServiceFactory.computedAttsService.computeAtt(
                    value,
                    attId,
                    attType,
                    computed
                )
            }
        }

        val recordsDao = DbRecordsDao(
            DbRecordsDaoConfig.create {
                withId(sourceId)
                withTypeRef(typeRef)
                withAllowRecordIdUpdate(allowRecordIdUpdate)
                withEnableTotalCount(enableTotalCount)
            },
            modelServiceFactory,
            dataService,
            permsComponent,
            computedAttsComponent,
            null
        )
        recordsDao.addListener(DbIntegrityCheckListener())
        records.register(recordsDao)

        dbRecordRefService = schemaCtx.recordRefService
        assocsService = schemaCtx.assocsService

        val resCtx = RecordsDaoTestCtx(
            tableRef,
            recordsDao,
            typeRef,
            dataService,
            recAdditionalPerms,
            recReadPerms,
            recWritePerms,
            recAttReadPerms,
            recAttWritePerms
        )
        registeredRecordsDao.add(resCtx)
        return resCtx
    }

    fun createQuery(action: RecordsQuery.Builder.() -> Unit = {}): RecordsQuery {
        return mainCtx.createQuery(action)
    }

    fun createRef(id: String): EntityRef {
        return mainCtx.createRef(id)
    }

    fun updateRecord(rec: EntityRef, vararg atts: Pair<String, Any?>): EntityRef {
        return mainCtx.updateRecord(rec, *atts)
    }

    fun createRecord(atts: ObjectData): EntityRef {
        return mainCtx.createRecord(atts)
    }

    fun createRecord(vararg atts: Pair<String, Any?>): EntityRef {
        return mainCtx.createRecord(*atts)
    }

    fun createTempRecord(
        name: String = UUID.randomUUID().toString(),
        mimeType: MimeType = MimeTypes.APP_BIN,
        content: ByteArray
    ): EntityRef {
        return RequestContext.doWithCtx {
            tempCtx.dao.uploadFile(
                ecosType = TEMP_FILE_TYPE_ID,
                name = name,
                mimeType = mimeType.toString()
            ) { writer ->
                writer.writeBytes(content)
            }
        }
    }

    fun selectRecFromDb(rec: EntityRef, field: String): Any? {
        return mainCtx.selectRecFromDb(rec, field)
    }

    fun selectFieldFromDbTable(field: String, table: String, condition: String): Any? {
        return mainCtx.selectFieldFromDbTable(field, table, condition)
    }

    fun getColumns(): List<DbColumnDef> {
        return mainCtx.getColumns()
    }

    /**
     * Forces the main table's own [DbTableContext] to be rebuilt from the physical schema on the
     * next read. [DbSchemaContext.resetColumnsCache] alone is not enough for this: it only resets
     * the schema's *system* services (content, perms, record refs, ...), not an arbitrary domain
     * table's [ru.citeck.ecos.data.sql.service.DbDataService], which normally only learns about a
     * DDL change it did not itself run through the reactive `column ... does not exist` handler
     *. A test that renames a column straight through [DbSchemaDao] - bypassing the data
     * service entirely, as the migration machinery itself will later do from inside its own lock -
     * has to invalidate that cache explicitly instead.
     */
    fun resetColumnsCache() {
        getTableCtx().getSchemaCtx().resetColumnsCache()
        mainCtx.dataService.resetColumnsCache()
    }

    fun cleanRecords() {
        mainCtx.cleanRecords()
    }

    /**
     * How many times the current table's schema migration lock has been requested so far. Exists
     * for the cases where "no migration happened" is the property under test and there is no more
     * direct signal available - see
     * [DbColumnMetaSeedTest.aSubsetOfAnAlreadyReconciledMapNeedsNoNewMigrationTest] for why a lock
     * count is the only honest proxy there. Wraps every real key this table's [DbDataServiceImpl]
     * ever requests through [DbDataServiceImpl.schemaMigrationLockKey], including nested tables
     * like `ed_column_meta` under their own key, so counting only this table's exact key is what
     * makes the assertion mean something.
     */
    fun schemaMigrationLockCallCount(): Int {
        val lockApi = webAppApi.getAppLockApi() as CountingLockApi
        return lockApi.callCount(DbDataServiceImpl.schemaMigrationLockKey(tableRef))
    }

    /**
     * Delegates every call through to the real (mock) lock API, counting [getLock] requests per
     * key. `getLock` is the one method every [EcosLockApi] entry point (`doInSync`, `doInSyncOrSkip`,
     * ...) funnels through, so counting it counts every acquisition attempt regardless of which one
     * was used.
     */
    private class CountingLockApi(private val delegate: EcosLockApi) : EcosLockApi {

        private val callsByKey = ConcurrentHashMap<String, AtomicInteger>()

        fun callCount(key: String): Int {
            return callsByKey[key]?.get() ?: 0
        }

        override fun getLock(key: String): EcosLock {
            callsByKey.computeIfAbsent(key) { AtomicInteger() }.incrementAndGet()
            return delegate.getLock(key)
        }
    }

    /**
     * The id of the backend the SPI resolved, used in the `assume*` skip messages.
     */
    private val activeBackendId: String
        get() = System.getProperty(DbRecordsTestBackends.BACKEND_PROP, DbRecordsTestBackends.DEFAULT_BACKEND)

    /**
     * Skip the current test when the active backend has no SQL engine (in-memory). The DB-direct
     * helpers ([sqlUpdate]/[selectRecFromDb]/etc.) call this so that a raw-SQL assertion becomes a
     * documented [Assumptions] skip on a non-SQL backend rather than a false failure.
     */
    fun assumeRawSqlSupported() {
        Assumptions.assumeTrue(
            backend.supportsRawSql,
            "Skipped on the '" + activeBackendId +
                "' backend: this test asserts on the raw SQL stored representation, which only a real SQL backend exposes."
        )
    }

    /**
     * Skip the current test when the active backend cannot evaluate PostgreSQL-internal expressions
     * (text formatting via `to_char`, timezone-aware `date_trunc`/`date_part('epoch')`, `interval`
     * arithmetic). The in-memory backend implements the portable expression subset but not Postgres'
     * formatting/timezone engine, so these few assertions become a documented assumption skip.
     */
    fun assumeSqlInternalExpressionsSupported() {
        Assumptions.assumeTrue(
            backend.supportsSqlInternalExpressions,
            "Skipped on the '" + activeBackendId +
                "' backend: this test relies on PostgreSQL-internal expressions (to_char/date_trunc/" +
                "date_part('epoch')/interval) that only a real SQL backend evaluates."
        )
    }

    /**
     * Skip the current test when the active backend does not convert stored values during an
     * in-place column type change. Only a backend that does has two conversion paths for
     * require agreement between; the in-memory backend changes the column's type and leaves the
     * values as they were.
     */
    fun assumeColumnValuesConvertedInPlace() {
        Assumptions.assumeTrue(
            backend.convertsColumnValuesInPlace,
            "Skipped on the '" + activeBackendId +
                "' backend: an in-place type change there rewrites the column definition without " +
                "converting the stored values, so there is no second conversion to agree with."
        )
    }

    /**
     * Skip the current test when the active backend does not roll back mid-transaction writes on an
     * exception thrown inside `TxnContext.doInTxn`. Backends that participate in the platform
     * transaction manager (PG, and the in-memory backend — which enlists a `TransactionResource`,
     * see its README) run these tests; a backend that returns `false` has them skipped with a
     * documented assumption rather than failing.
     */
    fun assumeTransactionRollbackSupported() {
        Assumptions.assumeTrue(
            backend.supportsTransactionRollback,
            "Skipped on the '" + activeBackendId +
                "' backend: this test asserts on state after a transaction rollback, which requires the " +
                "backend to participate in the platform transaction manager's commit/rollback."
        )
    }

    /**
     * Skip the current test when the active backend cannot have two transactions open at once - see
     * [DbRecordsTestBackend.runsTransactionsConcurrently]. A test that interleaves two transactions
     * declares this; on a serializing backend the second one would wait for a lock it cannot get and
     * fail on a timeout that says nothing about what the test is for.
     */
    fun assumeConcurrentTransactionsSupported() {
        Assumptions.assumeTrue(
            backend.runsTransactionsConcurrently,
            "Skipped on the '" + activeBackendId +
                "' backend: this test needs two transactions open at the same time, and this backend " +
                "serializes them on one lock."
        )
    }

    fun dropAllTables() {
        backend.dropAllTables()
    }

    fun printAllColumns() {
        assumeRawSqlSupported()
        backend.printAllColumns()
    }

    fun printQueryRes(sql: String) {
        assumeRawSqlSupported()
        backend.printQueryRes(sql)
    }

    fun sqlUpdate(sql: String): Int {
        assumeRawSqlSupported()
        return backend.sqlUpdate(sql)
    }

    fun registerAspect(aspect: AspectInfo) {
        if (aspect.id.isBlank()) {
            error("aspect id is blank")
        }
        this.aspectsInfo[aspect.id] = aspect
    }

    fun registerNumTemplate(numTemplate: NumTemplateDef) {
        if (numTemplate.id.isBlank()) {
            error("num-template id is blank")
        }
        this.numTemplates[numTemplate.id] = numTemplate
    }

    fun registerType(): TypeRegistration {
        val mainSrcId = mainCtx.dao.getId()
        return TypeRegistration(
            REC_TEST_TYPE_ID,
            mainSrcId
        ) {
            registerType(it)
            if (it.sourceId == mainSrcId) {
                mainCtx
            } else {
                val tableRef = DEFAULT_TABLE_REF.withTable(NameUtils.escape(it.sourceId))
                registeredRecordsDao.find { testDao ->
                    testDao.dao.getId() == it.sourceId &&
                        testDao.tableRef == tableRef &&
                        testDao.typeRef.getLocalId() == it.id
                } ?: createRecordsDao(tableRef, ModelUtils.getTypeRef(it.id), it.sourceId)
            }
        }
    }

    fun registerType(typeDef: String) {
        registerType(Json.mapper.readNotNull(typeDef, TypeInfo::class.java))
    }

    fun registerType(type: TypeInfo) {
        val fixedType = if (type.sourceId.isBlank()) {
            type.copy().withSourceId(mainCtx.dao.getId()).build()
        } else {
            type
        }
        val before = this.typesInfo.put(fixedType.id, fixedType)
        fireTypeChanged(fixedType.id, before, fixedType)
    }

    /**
     * Drops a type from the mock types repo, the way `emodel` drops a deleted one: the records DAO
     * built over it stays registered and keeps answering, exactly as it does in production until
     * something unregisters it.
     */
    fun unregisterType(typeId: String) {
        val before = this.typesInfo.remove(typeId)
        fireTypeChanged(typeId, before, null)
    }

    /**
     * What `EcosRegistryImpl.fireEvent` does: calls every listener synchronously, on the thread
     * that made the change, and lets none of them stop the others.
     *
     * This is what turns the mock model into something the trigger can be tested against at all -
     * without it every test of the trigger would have to call `onTypeChanged` by hand and would
     * therefore prove nothing about the path a real registry takes.
     *
     * Both writers of [typesInfo] go through here, and they are the only two: every other helper
     * of this factory that changes a type - `updateType`, `addAttribute`, `registerAtts`,
     * `setQueryPermsPolicy` - ends in [registerType].
     */
    private fun fireTypeChanged(typeId: String, before: TypeInfo?, after: TypeInfo?) {
        typeChangeListeners.forEach { listener ->
            try {
                listener.invoke(typeId, before, after)
            } catch (e: Throwable) {
                log.error(e) { "Type change listener failed for type '$typeId'" }
            }
        }
    }

    fun addAttribute(typeId: String = REC_TEST_TYPE_ID, attribute: AttributeDef) {
        updateType(typeId) {
            it.withModel(
                it.model.copy()
                    .withAttributes(
                        listOf(
                            *it.model.attributes.toTypedArray(),
                            attribute
                        )
                    ).build()
            )
        }
    }

    fun updateType(typeId: String = REC_TEST_TYPE_ID, action: (TypeInfo.Builder) -> Unit) {
        val typeInfo = this.typesInfo[typeId] ?: error("Type '$typeId' is not found")
        val builder = typeInfo.copy()
        action.invoke(builder)
        registerType(builder.build())
    }

    fun registerAtts(atts: List<AttributeDef>, systemAtts: List<AttributeDef> = emptyList()) {
        registerAttributes(REC_TEST_TYPE_ID, atts, systemAtts)
    }

    fun registerAttributes(id: String, atts: List<AttributeDef>, systemAtts: List<AttributeDef> = emptyList()) {
        registerType(
            TypeInfo.create {
                withId(id)
                withModel(
                    TypeModelDef.create {
                        withAttributes(atts)
                        withSystemAttributes(systemAtts)
                    }
                )
            }
        )
    }

    fun setQueryPermsPolicy(policy: QueryPermsPolicy) {
        setQueryPermsPolicy(REC_TEST_TYPE_ID, policy)
    }

    fun setQueryPermsPolicy(typeId: String, policy: QueryPermsPolicy) {
        val typeInfo = typesInfo[typeId] ?: error("Type is not found by id $typeId")
        registerType(typeInfo.copy { withQueryPermsPolicy(policy) })
    }

    class CustomWorkspaceService : WorkspaceApi {

        val usersWs = ConcurrentHashMap<String, Set<String>>()
        val nestedWorkspaces = ConcurrentHashMap<String, Set<String>>()

        lateinit var impl: WorkspaceService

        override fun getNestedWorkspaces(workspaces: Collection<String>): List<Set<String>> {
            return workspaces.map { workspace ->
                nestedWorkspaces[workspace] ?: emptySet()
            }
        }

        override fun getUserWorkspaces(user: String, membershipType: WsMembershipType): Set<String> {
            return usersWs[user] ?: emptySet()
        }

        override fun isUserManagerOf(user: String, workspace: String): Boolean {
            if (workspace.startsWith("user$")) {
                return workspace.substring("user$".length) == user
            }
            return false
        }

        override fun mapIdentifiers(identifiers: List<String>, mappingType: WorkspaceApi.IdMappingType): List<String> {
            return when (mappingType) {
                WorkspaceApi.IdMappingType.WS_SYS_ID_TO_ID -> identifiers.map {
                    if (it.endsWith("-sid")) {
                        it.substring(0, it.length - "-sid".length)
                    } else {
                        it
                    }
                }
                WorkspaceApi.IdMappingType.WS_ID_TO_SYS_ID -> identifiers.map { "$it-sid" }
                else -> identifiers
            }
        }

        fun setUserWorkspaces(user: String, workspaces: Set<String>) {
            usersWs[user] = workspaces.filterTo(LinkedHashSet()) { it.isNotBlank() }
        }

        fun setNestedWorkspaces(workspace: String, nestedWorkspaces: Set<String>) {
            this.nestedWorkspaces[workspace] = nestedWorkspaces
            impl.resetNestedWorkspacesCache()
        }
    }

    class CustomDelegationService : DelegationService {

        val activeAuthDelegations = ConcurrentHashMap<String, MutableList<AuthDelegation>>()

        lateinit var ecosTypeService: DbEcosModelService

        fun addDelegationTo(user: String, delegation: AuthDelegation) {
            activeAuthDelegations.computeIfAbsent(user) {
                CopyOnWriteArrayList()
            }.add(delegation)
        }

        fun setDelegationTo(user: String, delegation: AuthDelegation) {
            activeAuthDelegations[user] = mutableListOf(delegation)
        }

        override fun getActiveAuthDelegations(user: String, types: Collection<String>): List<AuthDelegation> {
            val delegations = activeAuthDelegations[user] ?: return emptyList()
            return delegations.mapNotNull { prepareDelegation(types, it) }
        }

        private fun prepareDelegation(reqTypes: Collection<String>, delegation: AuthDelegation): AuthDelegation? {
            val delegatedTypes = delegation.delegatedTypes

            val matchedReqTypes = HashSet<String>(reqTypes.size)
            if (!isDelegatedTypesMatch(reqTypes, delegatedTypes, matchedReqTypes)) {
                return null
            }
            val delegatedAuthorities = delegation.delegatedAuthorities.mapTo(HashSet()) {
                if (it == "OWN") {
                    delegation.delegator
                } else {
                    it
                }
            }
            if (delegatedAuthorities.isEmpty()) {
                return null
            }
            val types: Set<String> = if (delegatedTypes.isEmpty()) {
                emptySet()
            } else if (reqTypes.isEmpty()) {
                val allTypes = mutableSetOf<String>()
                delegatedTypes.forEach { delegatedTypeId ->
                    allTypes.add(delegatedTypeId)
                    ecosTypeService.getAllChildrenIds(delegatedTypeId, allTypes)
                }
                allTypes
            } else {
                matchedReqTypes
            }
            return AuthDelegation(
                delegator = delegation.delegator,
                delegatedTypes = types,
                delegatedAuthorities = delegatedAuthorities
            )
        }

        private fun isDelegatedTypesMatch(
            reqTypes: Collection<String>,
            delegatedTypes: Set<String>,
            matchedReqTypes: MutableSet<String>
        ): Boolean {
            if (reqTypes.isEmpty() || delegatedTypes.isEmpty()) {
                return true
            }
            var hasTypesMatch = false
            for (delegatedType in delegatedTypes) {
                if (reqTypes.contains(delegatedType)) {
                    return true
                }
                for (reqTypeId in reqTypes) {
                    if (ecosTypeService.isSubType(delegatedType, reqTypeId)) {
                        hasTypesMatch = true
                        matchedReqTypes.add(delegatedType)
                    } else if (ecosTypeService.isSubType(reqTypeId, delegatedType)) {
                        hasTypesMatch = true
                        matchedReqTypes.add(reqTypeId)
                    }
                }
            }
            return hasTypesMatch
        }

        override fun delegatePermission(record: Any, permission: PermissionType, from: String, to: String) {
            TODO("Not yet implemented")
        }

        override fun getPermissionDelegates(record: Any, permission: PermissionType): List<PermissionDelegateData> {
            TODO("Not yet implemented")
        }
    }

    inner class ContentApiTest : EcosWebAppApiMock.ContentApiMock() {
        override fun getContent(entity: EntityRef, attribute: String, index: Int): EcosContentData? {
            if (entity.getAppName() != webAppApi.appName) {
                return null
            }
            val dao = records.getRecordsDao(entity.getSourceId(), DbRecordsDao::class.java)
                ?: error("DAO doesn't found for ref $entity")
            return dao.getContent(entity.getLocalId(), attribute, index)
        }
    }

    inner class RecordsDaoTestCtx(
        val tableRef: DbTableRef,
        val dao: DbRecordsDao,
        val typeRef: EntityRef,
        val dataService: DbDataService<DbEntity>,
        val recAdditionalPerms: MutableMap<EntityRef, MutableMap<String, MutableSet<String>>> = mutableMapOf(),
        val recReadPerms: MutableMap<EntityRef, Set<String>> = mutableMapOf(),
        val recWritePerms: MutableMap<EntityRef, Set<String>> = mutableMapOf(),
        val recAttReadPerms: MutableMap<Pair<EntityRef, String>, Set<String>> = mutableMapOf(),
        val recAttWritePerms: MutableMap<Pair<EntityRef, String>, Set<String>> = mutableMapOf()
    ) {

        val baseQuery = createQuery {}

        fun clear() {
            recAdditionalPerms.clear()
            recReadPerms.clear()
            recWritePerms.clear()
            recAttReadPerms.clear()
            recAttWritePerms.clear()
        }

        fun createQuery(action: RecordsQuery.Builder.() -> Unit = {}): RecordsQuery {
            val builder = RecordsQuery.create()
                .withQuery(VoidPredicate.INSTANCE)
                .withSourceId(dao.getId())
                .withLanguage(PredicateService.LANGUAGE_PREDICATE)
                .withSortBy(
                    SortBy.create {
                        attribute = RecordConstants.ATT_CREATED
                        ascending = true
                    }
                )
            action.invoke(builder)
            return builder.build()
        }

        fun createRef(id: String): EntityRef {
            return EntityRef.create(APP_NAME, dao.getId(), id)
        }

        fun updateRecord(rec: EntityRef, vararg atts: Pair<String, Any?>): EntityRef {
            return records.mutate(rec, mapOf(*atts))
        }

        fun createRecord(atts: ObjectData): EntityRef {
            if (!atts.has(RecordConstants.ATT_TYPE)) {
                atts[RecordConstants.ATT_TYPE] = REC_TEST_TYPE_REF
            }
            return records.create(dao.getId(), atts)
        }

        fun createRecord(vararg atts: Pair<String, Any?>): EntityRef {
            val map = linkedMapOf(*atts)
            if (!map.containsKey(RecordConstants.ATT_TYPE)) {
                map[RecordConstants.ATT_TYPE] = typeRef
            }
            return records.create(dao.getId(), map)
        }

        fun selectRecFromDb(rec: EntityRef, field: String): Any? {
            assumeRawSqlSupported()
            return backend.selectRecFromDb(tableRef, rec.getLocalId(), field)
        }

        fun selectFieldFromDbTable(field: String, table: String, condition: String): Any? {
            assumeRawSqlSupported()
            return backend.selectFieldFromDbTable(field, table, condition)
        }

        fun selectAllFromTable(table: String): List<Map<String, Any?>> {
            assumeRawSqlSupported()
            return backend.selectAllFromTable(tableRef, table)
        }

        fun getColumns(): List<DbColumnDef> {
            return dbDataSource.withTransaction(true) {
                dbSchemaDao.getColumns(dbDataSource, tableRef)
            }
        }

        fun cleanRecords() {
            backend.cleanRecords(tableRef)
        }

        fun setAuthoritiesWithAttReadPerms(rec: EntityRef, att: String, vararg authorities: String) {
            recAttReadPerms[rec.withDefaultAppName(APP_NAME) to att] = authorities.toSet()
        }

        fun setAuthoritiesWithAttWritePerms(rec: EntityRef, att: String, vararg authorities: String) {
            recAttWritePerms[rec.withDefaultAppName(APP_NAME) to att] = authorities.toSet()
        }

        fun setAuthoritiesWithWritePerms(rec: EntityRef, vararg authorities: String) {
            setAuthoritiesWithWritePerms(rec, authorities.toList())
        }

        fun setAuthoritiesWithWritePerms(rec: EntityRef, authorities: Collection<String>) {
            recWritePerms[rec.withDefaultAppName(APP_NAME)] = authorities.toSet()
            addAuthoritiesWithReadPerms(rec, authorities)
        }

        fun addAdditionalPermission(rec: EntityRef, authority: String, permission: String) {
            addAdditionalPermission(rec, listOf(authority), permission)
        }

        fun addAdditionalPermission(rec: EntityRef, authorities: Collection<String>, permission: String) {
            val recPerms = recAdditionalPerms.computeIfAbsent(rec.withDefaultAppName(APP_NAME)) { HashMap() }
            authorities.forEach { auth ->
                recPerms.computeIfAbsent(auth) { HashSet() }.add(permission)
            }
        }

        fun setAuthoritiesWithReadPerms(rec: EntityRef, authorities: Collection<String>) {
            recReadPerms[rec.withDefaultAppName(APP_NAME)] = authorities.toSet()
            dao.updatePermissions(listOf(rec.getLocalId()))
        }

        fun setAuthoritiesWithReadPerms(rec: EntityRef, vararg authorities: String) {
            setAuthoritiesWithReadPerms(rec, authorities.toSet())
        }

        fun addAuthoritiesWithReadPerms(rec: EntityRef, authorities: Collection<String>) {
            val readPerms = recReadPerms[rec.withDefaultAppName(APP_NAME)]?.toMutableSet() ?: mutableSetOf()
            readPerms.addAll(authorities)
            setAuthoritiesWithReadPerms(rec, readPerms)
        }

        fun addAuthoritiesWithReadPerms(rec: EntityRef, vararg authorities: String) {
            addAuthoritiesWithReadPerms(rec, authorities.toSet())
        }
    }
}
