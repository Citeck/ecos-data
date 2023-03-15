package ru.citeck.ecos.data.sql.pg.records

import mu.KotlinLogging
import org.apache.commons.dbcp2.BasicDataSource
import org.apache.commons.dbcp2.managed.BasicManagedDataSource
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import ru.citeck.ecos.commons.data.MLText
import ru.citeck.ecos.commons.data.ObjectData
import ru.citeck.ecos.commons.json.Json
import ru.citeck.ecos.context.lib.auth.AuthContext
import ru.citeck.ecos.data.sql.content.DbContentServiceImpl
import ru.citeck.ecos.data.sql.content.entity.DbContentEntity
import ru.citeck.ecos.data.sql.content.storage.EcosContentStorageServiceImpl
import ru.citeck.ecos.data.sql.content.storage.local.DbContentDataEntity
import ru.citeck.ecos.data.sql.content.storage.local.EcosContentLocalStorage
import ru.citeck.ecos.data.sql.content.writer.EcosContentWriterFactory
import ru.citeck.ecos.data.sql.content.writer.EcosContentWriterImpl
import ru.citeck.ecos.data.sql.datasource.DbDataSource
import ru.citeck.ecos.data.sql.datasource.DbDataSourceImpl
import ru.citeck.ecos.data.sql.dto.DbColumnDef
import ru.citeck.ecos.data.sql.dto.DbTableRef
import ru.citeck.ecos.data.sql.pg.PgDataServiceFactory
import ru.citeck.ecos.data.sql.records.DbRecordsDao
import ru.citeck.ecos.data.sql.records.DbRecordsDaoConfig
import ru.citeck.ecos.data.sql.records.computed.DbComputedAttsComponent
import ru.citeck.ecos.data.sql.records.perms.DbPermsComponent
import ru.citeck.ecos.data.sql.records.perms.DbRecordPerms
import ru.citeck.ecos.data.sql.records.perms.DefaultDbPermsComponent
import ru.citeck.ecos.data.sql.records.refs.DbRecordRefEntity
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
import ru.citeck.ecos.model.lib.num.dto.NumTemplateDef
import ru.citeck.ecos.model.lib.num.repo.NumTemplatesRepo
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
import ru.citeck.ecos.webapp.api.content.EcosContentWriter
import ru.citeck.ecos.webapp.api.datasource.JdbcDataSource
import ru.citeck.ecos.webapp.api.entity.EntityRef
import java.io.OutputStream
import java.util.concurrent.atomic.AtomicLong

abstract class DbRecordsTestBase {

    companion object {
        const val RECS_DAO_ID = "test"
        const val REC_TEST_TYPE_ID = "test-type"

        val REC_TEST_TYPE_REF = ModelUtils.getTypeRef(REC_TEST_TYPE_ID)

        const val COLUMN_TYPE_NAME = "TYPE_NAME"
        const val COLUMN_COLUMN_NAME = "COLUMN_NAME"
        const val COLUMN_TABLE_SCHEMA = "TABLE_SCHEM"
        const val COLUMN_TABLE_NAME = "TABLE_NAME"

        private val DEFAULT_TABLE = DbTableRef("records-test-schema", "test-records-table")
    }

    private val typesInfo = mutableMapOf<String, TypeInfo>()
    private val aspectsInfo = mutableMapOf<String, AspectInfo>()
    private val numTemplates = mutableMapOf<String, NumTemplateDef>()

    private val recReadPerms = mutableMapOf<EntityRef, Set<String>>()
    private val recWritePerms = mutableMapOf<EntityRef, Set<String>>()

    private val recAttReadPerms = mutableMapOf<Pair<EntityRef, String>, Set<String>>()
    private val recAttWritePerms = mutableMapOf<Pair<EntityRef, String>, Set<String>>()

    lateinit var recordsDao: DbRecordsDao
    lateinit var records: RecordsService
    lateinit var recordsServiceFactory: RecordsServiceFactory
    lateinit var dbDataSource: DbDataSource
    lateinit var tableRef: DbTableRef
    lateinit var dbSchemaDao: DbSchemaDao
    lateinit var dataService: DbDataService<DbEntity>
    lateinit var dbRecordRefDataService: DbDataService<DbRecordRefEntity>
    lateinit var dbRecordRefService: DbRecordRefService
    lateinit var dataSource: BasicDataSource

    val log = KotlinLogging.logger {}

    val baseQuery = RecordsQuery.create {
        withSourceId(RECS_DAO_ID)
        withLanguage(PredicateService.LANGUAGE_PREDICATE)
        withQuery(VoidPredicate.INSTANCE)
    }

    @BeforeEach
    fun beforeEachBase() {

        typesInfo.clear()
        aspectsInfo.clear()
        numTemplates.clear()
        recReadPerms.clear()
        recWritePerms.clear()
        recAttReadPerms.clear()
        recAttWritePerms.clear()

        initServices()
    }

    @AfterEach
    fun afterEachBase() {
        RequestContext.setDefaultServices(null)
        dropAllTables()
        dataSource.close()
    }

    fun setAuthoritiesWithAttReadPerms(rec: EntityRef, att: String, vararg authorities: String) {
        recAttReadPerms[rec to att] = authorities.toSet()
    }

    fun setAuthoritiesWithAttWritePerms(rec: EntityRef, att: String, vararg authorities: String) {
        recAttWritePerms[rec to att] = authorities.toSet()
    }

    fun setAuthoritiesWithWritePerms(rec: EntityRef, vararg authorities: String) {
        setAuthoritiesWithWritePerms(rec, authorities.toList())
    }

    fun setAuthoritiesWithWritePerms(rec: EntityRef, authorities: Collection<String>) {
        recWritePerms[rec] = authorities.toSet()
        addAuthoritiesWithReadPerms(rec, authorities)
    }

    fun setAuthoritiesWithReadPerms(rec: EntityRef, authorities: Collection<String>) {
        recReadPerms[rec] = authorities.toSet()
        recordsDao.updatePermissions(listOf(rec.getLocalId()))
    }

    fun setAuthoritiesWithReadPerms(rec: EntityRef, vararg authorities: String) {
        setAuthoritiesWithReadPerms(rec, authorities.toSet())
    }

    fun addAuthoritiesWithReadPerms(rec: EntityRef, authorities: Collection<String>) {
        val readPerms = recReadPerms[rec]?.toMutableSet() ?: mutableSetOf()
        readPerms.addAll(authorities)
        setAuthoritiesWithReadPerms(rec, readPerms)
    }

    fun addAuthoritiesWithReadPerms(rec: EntityRef, vararg authorities: String) {
        addAuthoritiesWithReadPerms(rec, authorities.toSet())
    }

    fun initServices(
        tableRef: DbTableRef = DEFAULT_TABLE,
        authEnabled: Boolean = false,
        typeRef: EntityRef = EntityRef.EMPTY,
        inheritParentPerms: Boolean = true,
        appName: String = ""
    ) {

        this.tableRef = tableRef
        val webAppApi = EcosWebAppApiMock(appName)

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

        val pgDataServiceFactory = PgDataServiceFactory()

        val dataServiceConfig = DbDataServiceConfig.create {
            withAuthEnabled(authEnabled)
            withTransactional(true)
            withTableRef(tableRef)
        }
        dataService = DbDataServiceImpl(
            DbEntity::class.java,
            dataServiceConfig,
            dbDataSource,
            pgDataServiceFactory
        )

        TxnContext.setManager(TransactionManagerImpl(webAppApi))

        recordsServiceFactory = object : RecordsServiceFactory() {
            override fun getEcosWebAppApi(): EcosWebAppApi {
                return webAppApi
            }
        }
        val numCounters = mutableMapOf<EntityRef, AtomicLong>()
        val modelServiceFactory = object : ModelServiceFactory() {

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

        val defaultPermsComponent = DefaultDbPermsComponent()
        val permsComponent = object : DbPermsComponent {
            override fun getRecordPerms(recordRef: EntityRef): DbRecordPerms {
                return object : DbRecordPerms {
                    override fun getAuthoritiesWithReadPermission(): Set<String> {
                        if (recReadPerms.containsKey(recordRef)) {
                            return recReadPerms[recordRef]!!
                        }
                        return defaultPermsComponent.getRecordPerms(recordRef)
                            .getAuthoritiesWithReadPermission()
                    }

                    override fun isCurrentUserHasWritePerms(): Boolean {
                        val perms = recWritePerms[recordRef] ?: emptySet()
                        return perms.isEmpty() || AuthContext.getCurrentRunAsUserWithAuthorities().any {
                            perms.contains(it)
                        }
                    }

                    override fun isCurrentUserHasAttWritePerms(name: String): Boolean {
                        val writePerms = recAttWritePerms[recordRef to name]
                        return writePerms.isNullOrEmpty() || AuthContext.getCurrentRunAsUserWithAuthorities().any {
                            writePerms.contains(it)
                        }
                    }

                    override fun isCurrentUserHasAttReadPerms(name: String): Boolean {
                        val readPerms = recAttReadPerms[recordRef to name]
                        return readPerms.isNullOrEmpty() || AuthContext.getCurrentRunAsUserWithAuthorities().any {
                            readPerms.contains(it)
                        }
                    }
                }
            }
        }

        val contentStorageService = EcosContentStorageServiceImpl(object : EcosContentWriterFactory {
            override fun createWriter(output: OutputStream): EcosContentWriter {
                return EcosContentWriterImpl(output)
            }
        })
        contentStorageService.register(
            EcosContentLocalStorage(
                DbDataServiceImpl(
                    DbContentDataEntity::class.java,
                    DbDataServiceConfig.create {
                        withTableRef(tableRef.withTable("ecos_content_data"))
                        withStoreTableMeta(true)
                    },
                    dbDataSource,
                    pgDataServiceFactory
                )
            )
        )
        val contentService = DbContentServiceImpl(
            DbDataServiceImpl(
                DbContentEntity::class.java,
                DbDataServiceConfig.create {
                    withTableRef(tableRef.withTable("ecos_content"))
                    withStoreTableMeta(true)
                },
                dbDataSource,
                pgDataServiceFactory
            ),
            contentStorageService
        )

        dbRecordRefDataService = DbDataServiceImpl(
            DbRecordRefEntity::class.java,
            DbDataServiceConfig.create {
                withTableRef(tableRef.withTable("ecos_record_ref"))
            },
            dbDataSource,
            pgDataServiceFactory
        )
        dbRecordRefService = DbRecordRefService(appName, dbRecordRefDataService)

        recordsDao = DbRecordsDao(
            DbRecordsDaoConfig.create {
                withId(RECS_DAO_ID)
                withTypeRef(typeRef)
                withInheritParentPerms(inheritParentPerms)
            },
            modelServiceFactory,
            dataService,
            dbRecordRefService,
            permsComponent,
            object : DbComputedAttsComponent {
                override fun computeAttsToStore(value: Any, isNewRecord: Boolean, typeRef: EntityRef): ObjectData {
                    return modelServiceFactory.computedAttsService.computeAttsToStore(value, isNewRecord, typeRef)
                }

                override fun computeDisplayName(value: Any, typeRef: EntityRef): MLText {
                    return modelServiceFactory.computedAttsService.computeDisplayName(value, typeRef)
                }
            },
            contentService
        )
        dbSchemaDao = pgDataServiceFactory.createSchemaDao(tableRef, dbDataSource)
        records.register(recordsDao)
    }

    fun createQuery(): RecordsQuery {
        return RecordsQuery.create()
            .withQuery(VoidPredicate.INSTANCE)
            .withSourceId(recordsDao.getId())
            .withLanguage(PredicateService.LANGUAGE_PREDICATE)
            .build()
    }

    fun createRef(id: String): RecordRef {
        return RecordRef.create(recordsDao.getId(), id)
    }

    fun updateRecord(rec: EntityRef, vararg atts: Pair<String, Any?>): RecordRef {
        return records.mutate(rec, mapOf(*atts))
    }

    fun createRecord(atts: ObjectData): RecordRef {
        if (!atts.has(RecordConstants.ATT_TYPE)) {
            atts[RecordConstants.ATT_TYPE] = REC_TEST_TYPE_REF
        }
        val rec = records.create(RECS_DAO_ID, atts)
        log.info { "RECORD CREATED: $rec" }
        return rec
    }

    fun createRecord(vararg atts: Pair<String, Any?>): RecordRef {
        val map = linkedMapOf(*atts)
        if (!map.containsKey(RecordConstants.ATT_TYPE)) {
            map[RecordConstants.ATT_TYPE] = REC_TEST_TYPE_REF
        }
        val rec = records.create(RECS_DAO_ID, map)
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
            dbSchemaDao.getColumns()
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

    fun registerType(type: String) {
        registerType(Json.mapper.readNotNull(type, TypeInfo::class.java))
    }

    fun registerType(type: TypeInfo) {
        val fixedType = if (type.sourceId.isBlank()) {
            type.copy().withSourceId(recordsDao.getId()).build()
        } else {
            type
        }
        this.typesInfo[fixedType.id] = fixedType
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
}
