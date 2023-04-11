package ru.citeck.ecos.data.sql.pg.records

import mu.KotlinLogging
import org.apache.commons.dbcp2.BasicDataSource
import org.apache.commons.dbcp2.managed.BasicManagedDataSource
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import ru.citeck.ecos.commons.data.MLText
import ru.citeck.ecos.commons.data.ObjectData
import ru.citeck.ecos.commons.json.Json
import ru.citeck.ecos.commons.mime.MimeTypes
import ru.citeck.ecos.context.lib.auth.AuthContext
import ru.citeck.ecos.data.sql.context.DbDataSourceContext
import ru.citeck.ecos.data.sql.context.DbSchemaContext
import ru.citeck.ecos.data.sql.context.DbTableContext
import ru.citeck.ecos.data.sql.datasource.DbDataSource
import ru.citeck.ecos.data.sql.datasource.DbDataSourceImpl
import ru.citeck.ecos.data.sql.dto.DbColumnDef
import ru.citeck.ecos.data.sql.dto.DbTableRef
import ru.citeck.ecos.data.sql.pg.PgDataServiceFactory
import ru.citeck.ecos.data.sql.records.DbRecordsDao
import ru.citeck.ecos.data.sql.records.DbRecordsDaoConfig
import ru.citeck.ecos.data.sql.records.computed.DbComputedAttsComponent
import ru.citeck.ecos.data.sql.records.dao.atts.DbRecord
import ru.citeck.ecos.data.sql.records.listener.DbIntegrityCheckListener
import ru.citeck.ecos.data.sql.records.perms.DbPermsComponent
import ru.citeck.ecos.data.sql.records.perms.DbRecordPerms
import ru.citeck.ecos.data.sql.records.perms.DefaultDbPermsComponent
import ru.citeck.ecos.data.sql.records.refs.DbRecordRefService
import ru.citeck.ecos.data.sql.repo.entity.DbEntity
import ru.citeck.ecos.data.sql.schema.DbSchemaDao
import ru.citeck.ecos.data.sql.service.DbDataService
import ru.citeck.ecos.data.sql.service.DbDataServiceConfig
import ru.citeck.ecos.data.sql.service.DbDataServiceImpl
import ru.citeck.ecos.model.lib.ModelServiceFactory
import ru.citeck.ecos.model.lib.api.EcosModelAppApi
import ru.citeck.ecos.model.lib.aspect.dto.AspectInfo
import ru.citeck.ecos.model.lib.aspect.repo.AspectsRepo
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType
import ru.citeck.ecos.model.lib.num.dto.NumTemplateDef
import ru.citeck.ecos.model.lib.num.repo.NumTemplatesRepo
import ru.citeck.ecos.model.lib.type.dto.QueryPermsPolicy
import ru.citeck.ecos.model.lib.type.dto.TypeInfo
import ru.citeck.ecos.model.lib.type.dto.TypeModelDef
import ru.citeck.ecos.model.lib.type.repo.TypesRepo
import ru.citeck.ecos.model.lib.utils.ModelUtils
import ru.citeck.ecos.records2.RecordConstants
import ru.citeck.ecos.records2.RecordRef
import ru.citeck.ecos.records2.predicate.PredicateService
import ru.citeck.ecos.records2.predicate.model.VoidPredicate
import ru.citeck.ecos.records3.RecordsService
import ru.citeck.ecos.records3.RecordsServiceFactory
import ru.citeck.ecos.records3.record.dao.query.dto.query.RecordsQuery
import ru.citeck.ecos.records3.record.request.RequestContext
import ru.citeck.ecos.test.commons.EcosWebAppApiMock
import ru.citeck.ecos.test.commons.containers.TestContainers
import ru.citeck.ecos.txn.lib.TxnContext
import ru.citeck.ecos.txn.lib.manager.TransactionManagerImpl
import ru.citeck.ecos.txn.lib.resource.type.xa.JavaXaTxnManagerAdapter
import ru.citeck.ecos.webapp.api.EcosWebAppApi
import ru.citeck.ecos.webapp.api.datasource.JdbcDataSource
import ru.citeck.ecos.webapp.api.entity.EntityRef
import ru.citeck.ecos.webapp.api.entity.toEntityRef
import ru.citeck.ecos.webapp.api.mime.MimeType
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

abstract class DbRecordsTestBase {

    companion object {
        const val APP_NAME = "test-app"
        const val RECS_DAO_ID = "test"
        const val REC_TEST_TYPE_ID = "test-type"

        const val TEMP_FILE_TYPE_ID = "temp-file"

        val REC_TEST_TYPE_REF = ModelUtils.getTypeRef(REC_TEST_TYPE_ID)

        const val COLUMN_TYPE_NAME = "TYPE_NAME"
        const val COLUMN_COLUMN_NAME = "COLUMN_NAME"
        const val COLUMN_TABLE_SCHEMA = "TABLE_SCHEM"
        const val COLUMN_TABLE_NAME = "TABLE_NAME"

        val DEFAULT_TABLE_REF = DbTableRef("records-test-schema", "test-records-table")

        private val DEFAULT_ASPECTS = listOf(
            AspectInfo.create()
                .withId(DbRecord.ASPECT_VERSIONABLE)
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
                .withId("user-base")
                .withParentRef(ModelUtils.getTypeRef("base"))
                .build(),
            TypeInfo.create()
                .withId("base")
                .build()
        )
    }

    private val typesInfo = mutableMapOf<String, TypeInfo>()
    private val aspectsInfo = mutableMapOf<String, AspectInfo>()
    private val numTemplates = mutableMapOf<String, NumTemplateDef>()

    lateinit var records: RecordsService
    lateinit var recordsServiceFactory: RecordsServiceFactory
    lateinit var dbDataSource: DbDataSource
    lateinit var dataSourceCtx: DbDataSourceContext

    lateinit var dbSchemaDao: DbSchemaDao
    lateinit var dbRecordRefService: DbRecordRefService
    lateinit var dataSource: BasicDataSource
    lateinit var computedAttsComponent: DbComputedAttsComponent
    lateinit var modelServiceFactory: ModelServiceFactory

    lateinit var webAppApi: EcosWebAppApiMock

    lateinit var mainCtx: RecordsDaoTestCtx
    lateinit var tempCtx: RecordsDaoTestCtx

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

    @BeforeEach
    fun beforeEachBase() {

        typesInfo.clear()
        aspectsInfo.clear()
        numTemplates.clear()
        schemaContexts.clear()

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

            webAppApi = EcosWebAppApiMock(APP_NAME)

            val dataSource = BasicManagedDataSource()
            dataSource.transactionManager = JavaXaTxnManagerAdapter(webAppApi.getProperties())
            dataSource.xaDataSourceInstance = TestContainers.getPostgres().getXaDataSource()
            dataSource.defaultAutoCommit = false
            dataSource.autoCommitOnReturn = false
            this.dataSource = dataSource

            val jdbcDataSource = object : JdbcDataSource {
                override fun getJavaDataSource() = dataSource
                override fun isManaged() = true
            }

            dbDataSource = DbDataSourceImpl(jdbcDataSource)
            dataSourceCtx = DbDataSourceContext(webAppApi.appName, dbDataSource, PgDataServiceFactory())

            TxnContext.setManager(TransactionManagerImpl(webAppApi))

            recordsServiceFactory = object : RecordsServiceFactory() {
                override fun getEcosWebAppApi(): EcosWebAppApi {
                    return webAppApi
                }
            }

            val numCounters = mutableMapOf<EntityRef, AtomicLong>()
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
                        override fun getChildren(typeRef: EntityRef) = emptyList<EntityRef>()
                        override fun getTypeInfo(typeRef: EntityRef): TypeInfo? {
                            return typesInfo[typeRef.getLocalId()]
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
                            return numCounters.computeIfAbsent(templateRef) { AtomicLong() }.incrementAndGet()
                        }
                    }
                }

                override fun getEcosWebAppApi(): EcosWebAppApi {
                    return webAppApi
                }
            }
            modelServiceFactory.setRecordsServices(recordsServiceFactory)

            records = recordsServiceFactory.recordsServiceV1
            RequestContext.setDefaultServices(recordsServiceFactory)
        }

        mainCtx = createRecordsDao()

        tempCtx = createRecordsDao(
            DEFAULT_TABLE_REF.withTable(TEMP_FILE_TYPE_ID),
            ModelUtils.getTypeRef(TEMP_FILE_TYPE_ID),
            TEMP_FILE_TYPE_ID
        )

        mainCtxInitialized = true
    }

    private fun getOrCreateSchemaCtx(schema: String): DbSchemaContext {
        return schemaContexts.computeIfAbsent(schema) {
            dataSourceCtx.getSchemaContext(schema)
        }
    }

    @AfterEach
    fun afterEachBase() {
        RequestContext.setDefaultServices(null)
        dropAllTables()
        dataSource.close()
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

    fun addAuthoritiesWithReadPerms(rec: EntityRef, vararg authorities: String) {
        mainCtx.addAuthoritiesWithReadPerms(rec, *authorities)
    }

    fun getTableCtx(): DbTableContext {
        return mainCtx.dataService.getTableContext()
    }

    fun createRecordsDao(
        tableRef: DbTableRef = DEFAULT_TABLE_REF,
        typeRef: EntityRef = ModelUtils.getTypeRef(REC_TEST_TYPE_ID),
        sourceId: String = RECS_DAO_ID
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

        val defaultPermsComponent = DefaultDbPermsComponent(records)

        val recReadPerms: MutableMap<EntityRef, Set<String>> = mutableMapOf()
        val recWritePerms: MutableMap<EntityRef, Set<String>> = mutableMapOf()
        val recAttReadPerms: MutableMap<Pair<EntityRef, String>, Set<String>> = mutableMapOf()
        val recAttWritePerms: MutableMap<Pair<EntityRef, String>, Set<String>> = mutableMapOf()

        val permsComponent = object : DbPermsComponent {
            override fun getRecordPerms(record: Any): DbRecordPerms {
                val globalRef = records.getAtt(record, "?id").toEntityRef().withDefaultAppName(APP_NAME)
                return object : DbRecordPerms {
                    override fun getAuthoritiesWithReadPermission(): Set<String> {
                        if (recReadPerms.containsKey(globalRef)) {
                            return recReadPerms[globalRef]!!
                        }
                        return defaultPermsComponent.getRecordPerms(globalRef)
                            .getAuthoritiesWithReadPermission()
                    }

                    override fun isCurrentUserHasWritePerms(): Boolean {
                        val perms = recWritePerms[globalRef] ?: emptySet()
                        return perms.isEmpty() || AuthContext.getCurrentRunAsUserWithAuthorities().any {
                            perms.contains(it)
                        }
                    }

                    override fun isCurrentUserHasAttWritePerms(name: String): Boolean {
                        val writePerms = recAttWritePerms[globalRef to name]
                        return writePerms.isNullOrEmpty() || AuthContext.getCurrentRunAsUserWithAuthorities().any {
                            writePerms.contains(it)
                        }
                    }

                    override fun isCurrentUserHasAttReadPerms(name: String): Boolean {
                        val readPerms = recAttReadPerms[globalRef to name]
                        return readPerms.isNullOrEmpty() || AuthContext.getCurrentRunAsUserWithAuthorities().any {
                            readPerms.contains(it)
                        }
                    }
                }
            }
        }

        computedAttsComponent = object : DbComputedAttsComponent {
            override fun computeAttsToStore(value: Any, isNewRecord: Boolean, typeRef: EntityRef): ObjectData {
                return modelServiceFactory.computedAttsService.computeAttsToStore(value, isNewRecord, typeRef)
            }

            override fun computeDisplayName(value: Any, typeRef: EntityRef): MLText {
                return modelServiceFactory.computedAttsService.computeDisplayName(value, typeRef)
            }
        }

        val recordsDao = DbRecordsDao(
            DbRecordsDaoConfig.create {
                withId(sourceId)
                withTypeRef(typeRef)
            },
            modelServiceFactory,
            dataService,
            permsComponent,
            computedAttsComponent
        )
        recordsDao.addListener(DbIntegrityCheckListener())
        records.register(recordsDao)

        dbRecordRefService = schemaCtx.recordRefService

        val resCtx = RecordsDaoTestCtx(
            tableRef,
            recordsDao,
            typeRef,
            dataService,
            recReadPerms,
            recWritePerms,
            recAttReadPerms,
            recAttWritePerms
        )
        registeredRecordsDao.add(resCtx)
        return resCtx
    }

    fun createQuery(): RecordsQuery {
        return mainCtx.createQuery()
    }

    fun createRef(id: String): RecordRef {
        return mainCtx.createRef(id)
    }

    fun updateRecord(rec: EntityRef, vararg atts: Pair<String, Any?>): RecordRef {
        return mainCtx.updateRecord(rec, *atts)
    }

    fun createRecord(atts: ObjectData): RecordRef {
        return mainCtx.createRecord(atts)
    }

    fun createRecord(vararg atts: Pair<String, Any?>): RecordRef {
        return mainCtx.createRecord(*atts)
    }

    fun createTempRecord(
        name: String = UUID.randomUUID().toString(),
        mimeType: MimeType = MimeTypes.APP_BIN,
        content: ByteArray
    ): EntityRef {
        return RequestContext.doWithCtx {
            tempCtx.dao.uploadFile(ecosType = TEMP_FILE_TYPE_ID, name = name, mimeType = mimeType.toString()) {
                    writer ->
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

    fun cleanRecords() {
        mainCtx.cleanRecords()
    }

    fun dropAllTables() {
        TxnContext.doInTxn {
            dataSource.connection.use { conn ->
                val tables = ArrayList<String>()
                conn.metaData.getTables(null, null, "", arrayOf("TABLE")).use { res ->
                    while (res.next()) {
                        tables.add("\"${res.getString("TABLE_SCHEM")}\".\"${res.getString("TABLE_NAME")}\"")
                    }
                }
                if (tables.isNotEmpty()) {
                    val dropCommand = "DROP TABLE " + tables.joinToString(",") + " CASCADE"
                    println("EXEC: $dropCommand")
                    conn.createStatement().use { it.executeUpdate(dropCommand) }
                    conn.createStatement().use { it.executeUpdate("DEALLOCATE ALL") }
                }
            }
        }
    }

    fun printAllColumns() {
        dataSource.connection.use { conn ->
            conn.metaData.getColumns(null, null, null, null).use {
                while (it.next()) {
                    val schema = it.getString(COLUMN_TABLE_SCHEMA)
                    if ("information_schema" != schema && "pg_catalog" != schema) {
                        println(
                            it.getObject(COLUMN_COLUMN_NAME).toString() +
                                "\t\t\t\t" + it.getObject(COLUMN_TYPE_NAME) +
                                "\t\t\t\t" + schema +
                                "\t\t\t\t" + it.getObject(COLUMN_TABLE_NAME)
                        )
                    }
                }
            }
        }
    }

    fun printQueryRes(sql: String) {
        dataSource.connection.use { conn ->
            conn.createStatement().use { stmt ->
                stmt.executeQuery(sql).use {
                    var line = ""
                    for (i in 1..it.metaData.columnCount) {
                        line += it.metaData.getColumnName(i) + "\t\t\t\t\t"
                    }
                    println(line)
                    while (it.next()) {
                        line = ""
                        for (i in 1..it.metaData.columnCount) {
                            line += (it.getObject(i) ?: "").toString() + "\t\t\t\t\t"
                        }
                        println(line)
                    }
                }
            }
        }
    }

    fun sqlUpdate(sql: String): Int {
        return TxnContext.doInTxn {
            dataSource.connection.use { conn ->
                conn.createStatement().use { stmt ->
                    stmt.executeUpdate(sql)
                }
            }
        }
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

    fun registerType(typeDef: String) {
        registerType(Json.mapper.readNotNull(typeDef, TypeInfo::class.java))
    }

    fun registerType(type: TypeInfo) {
        val fixedType = if (type.sourceId.isBlank()) {
            type.copy().withSourceId(mainCtx.dao.getId()).build()
        } else {
            type
        }
        this.typesInfo[fixedType.id] = fixedType
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

    inner class RecordsDaoTestCtx(
        val tableRef: DbTableRef,
        val dao: DbRecordsDao,
        val typeRef: EntityRef,
        val dataService: DbDataService<DbEntity>,
        val recReadPerms: MutableMap<EntityRef, Set<String>> = mutableMapOf(),
        val recWritePerms: MutableMap<EntityRef, Set<String>> = mutableMapOf(),
        val recAttReadPerms: MutableMap<Pair<EntityRef, String>, Set<String>> = mutableMapOf(),
        val recAttWritePerms: MutableMap<Pair<EntityRef, String>, Set<String>> = mutableMapOf()
    ) {

        val baseQuery = RecordsQuery.create {
            withSourceId(dao.getId())
            withLanguage(PredicateService.LANGUAGE_PREDICATE)
            withQuery(VoidPredicate.INSTANCE)
        }

        fun clear() {
            recReadPerms.clear()
            recWritePerms.clear()
            recAttReadPerms.clear()
            recAttWritePerms.clear()
        }

        fun createQuery(): RecordsQuery {
            return RecordsQuery.create()
                .withQuery(VoidPredicate.INSTANCE)
                .withSourceId(dao.getId())
                .withLanguage(PredicateService.LANGUAGE_PREDICATE)
                .build()
        }

        fun createRef(id: String): RecordRef {
            return RecordRef.create(APP_NAME, dao.getId(), id)
        }

        fun updateRecord(rec: EntityRef, vararg atts: Pair<String, Any?>): RecordRef {
            return records.mutate(rec, mapOf(*atts))
        }

        fun createRecord(atts: ObjectData): RecordRef {
            if (!atts.has(RecordConstants.ATT_TYPE)) {
                atts[RecordConstants.ATT_TYPE] = REC_TEST_TYPE_REF
            }
            val rec = records.create(dao.getId(), atts)
            log.info { "RECORD CREATED: $rec" }
            return rec
        }

        fun createRecord(vararg atts: Pair<String, Any?>): RecordRef {
            val map = linkedMapOf(*atts)
            if (!map.containsKey(RecordConstants.ATT_TYPE)) {
                map[RecordConstants.ATT_TYPE] = typeRef
            }
            val rec = records.create(dao.getId(), map)
            log.info { "RECORD CREATED: $rec" }
            return rec
        }

        fun selectRecFromDb(rec: EntityRef, field: String): Any? {
            return dbDataSource.withTransaction(true) {
                dbDataSource.query(
                    "SELECT $field as res FROM ${tableRef.fullName} " +
                        "where __ext_id='${rec.getLocalId()}'",
                    emptyList()
                ) { res ->
                    res.next()
                    res.getObject("res")
                }
            }
        }

        fun selectFieldFromDbTable(field: String, table: String, condition: String): Any? {
            return dbDataSource.withTransaction(true) {
                dbDataSource.query(
                    "SELECT \"$field\" as res FROM $table WHERE $condition",
                    emptyList()
                ) { res ->
                    res.next()
                    res.getObject("res")
                }
            }
        }

        fun getColumns(): List<DbColumnDef> {
            return dbDataSource.withTransaction(true) {
                dbSchemaDao.getColumns(dbDataSource, tableRef)
            }
        }

        fun cleanRecords() {
            dataSource.connection.use { conn ->
                val truncCommand = "TRUNCATE TABLE " + tableRef.fullName + " CASCADE"
                println("EXEC: $truncCommand")
                conn.createStatement().use { it.executeUpdate(truncCommand) }
                conn.createStatement().use { it.executeUpdate("DEALLOCATE ALL") }
            }
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
