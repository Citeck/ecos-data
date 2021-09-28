package ru.citeck.ecos.data.sql.pg.records

import io.zonky.test.db.postgres.embedded.EmbeddedPostgres
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import ru.citeck.ecos.commons.data.ObjectData
import ru.citeck.ecos.data.sql.datasource.DbDataSource
import ru.citeck.ecos.data.sql.datasource.DbDataSourceImpl
import ru.citeck.ecos.data.sql.dto.DbColumnDef
import ru.citeck.ecos.data.sql.dto.DbTableRef
import ru.citeck.ecos.data.sql.pg.PgDataServiceFactory
import ru.citeck.ecos.data.sql.pg.PgUtils
import ru.citeck.ecos.data.sql.records.DbRecordsDao
import ru.citeck.ecos.data.sql.records.DbRecordsDaoConfig
import ru.citeck.ecos.data.sql.records.computed.DbComputedAttsComponent
import ru.citeck.ecos.data.sql.records.perms.DbPermsComponent
import ru.citeck.ecos.data.sql.records.perms.DbRecordPerms
import ru.citeck.ecos.data.sql.records.perms.DefaultDbPermsComponent
import ru.citeck.ecos.data.sql.repo.entity.DbEntity
import ru.citeck.ecos.data.sql.schema.DbSchemaDao
import ru.citeck.ecos.data.sql.service.DbDataServiceConfig
import ru.citeck.ecos.data.sql.service.DbDataServiceImpl
import ru.citeck.ecos.data.sql.utils.use
import ru.citeck.ecos.model.lib.ModelServiceFactory
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.type.dto.TypeInfo
import ru.citeck.ecos.model.lib.type.dto.TypeModelDef
import ru.citeck.ecos.model.lib.type.repo.TypesRepo
import ru.citeck.ecos.model.lib.type.service.utils.TypeUtils
import ru.citeck.ecos.records2.RecordRef
import ru.citeck.ecos.records2.predicate.PredicateService
import ru.citeck.ecos.records2.predicate.model.VoidPredicate
import ru.citeck.ecos.records3.RecordsService
import ru.citeck.ecos.records3.RecordsServiceFactory
import ru.citeck.ecos.records3.record.dao.query.dto.query.RecordsQuery
import ru.citeck.ecos.records3.record.request.RequestContext
import javax.sql.DataSource

abstract class DbRecordsTestBase {

    companion object {
        const val RECS_DAO_ID = "test"
        const val REC_TEST_TYPE_ID = "test-type"
        val REC_TEST_TYPE_REF = TypeUtils.getTypeRef(REC_TEST_TYPE_ID)

        const val COLUMN_TYPE_NAME = "TYPE_NAME"
        const val COLUMN_COLUMN_NAME = "COLUMN_NAME"
        const val COLUMN_TABLE_SCHEMA = "TABLE_SCHEM"
        const val COLUMN_TABLE_NAME = "TABLE_NAME"

        private lateinit var pg: EmbeddedPostgres
        private lateinit var dataSource: DataSource
        private var started = false

        @BeforeAll
        @JvmStatic
        fun beforeAll() {
            if (!started) {
                pg = EmbeddedPostgres.builder().start()
                pg.postgresDatabase.connection.use { conn ->
                    conn.prepareStatement("CREATE DATABASE \"${PgUtils.TEST_DB_NAME}\"").use { it.executeUpdate() }
                }
                dataSource = pg.getDatabase("postgres", PgUtils.TEST_DB_NAME)
                started = true
            }
        }
    }

    private val typesInfo = mutableMapOf<String, TypeInfo>()

    private val recReadPerms = mutableMapOf<RecordRef, Set<String>>()

    lateinit var recordsDao: DbRecordsDao
    lateinit var records: RecordsService
    lateinit var recordsServiceFactory: RecordsServiceFactory
    lateinit var dbDataSource: DbDataSource
    lateinit var tableRef: DbTableRef
    lateinit var dbSchemaDao: DbSchemaDao
    lateinit var ecosTypeRepo: TypesRepo

    val baseQuery = RecordsQuery.create {
        withSourceId(RECS_DAO_ID)
        withLanguage(PredicateService.LANGUAGE_PREDICATE)
        withQuery(VoidPredicate.INSTANCE)
    }

    @BeforeEach
    fun beforeEachBase() {

        dropAllTables()
        typesInfo.clear()
        recReadPerms.clear()

        initWithTable(DbTableRef("records-test-schema", "test-records-table"), false)
    }

    fun setPerms(rec: RecordRef, perms: Collection<String>) {
        recReadPerms[rec] = perms.toSet()
        recordsDao.updatePermissions(listOf(rec.id))
    }

    fun setPerms(rec: RecordRef, vararg perms: String) {
        setPerms(rec, perms.toSet())
    }

    fun initWithTable(tableRef: DbTableRef, authEnabled: Boolean = false) {

        this.tableRef = tableRef

        dbDataSource = DbDataSourceImpl(dataSource)

        val pgDataServiceFactory = PgDataServiceFactory()

        val dataServiceConfig = DbDataServiceConfig.create {
            withAuthEnabled(authEnabled)
            withTransactional(true)
            withTableRef(tableRef)
        }
        val dataService = DbDataServiceImpl(
            DbEntity::class.java,
            dataServiceConfig,
            dbDataSource,
            pgDataServiceFactory
        )

        recordsServiceFactory = RecordsServiceFactory()
        val modelServiceFactory = object : ModelServiceFactory() {
            override fun createTypesRepo(): TypesRepo {
                return object : TypesRepo {
                    override fun getChildren(typeRef: RecordRef) = emptyList<RecordRef>()
                    override fun getTypeInfo(typeRef: RecordRef): TypeInfo? {
                        return typesInfo[typeRef.id]
                    }
                }
            }
        }
        modelServiceFactory.setRecordsServices(recordsServiceFactory)
        ecosTypeRepo = modelServiceFactory.typesRepo

        records = recordsServiceFactory.recordsServiceV1
        RequestContext.setDefaultServices(recordsServiceFactory)

        val defaultPermsComponent = DefaultDbPermsComponent(records)
        val permsComponent = object : DbPermsComponent {
            override fun getRecordPerms(recordRef: RecordRef): DbRecordPerms {
                return object : DbRecordPerms {
                    override fun getAuthoritiesWithReadPermission(): Set<String> {
                        if (recReadPerms.containsKey(recordRef)) {
                            return recReadPerms[recordRef]!!
                        }
                        return defaultPermsComponent.getRecordPerms(recordRef)
                            .getAuthoritiesWithReadPermission()
                    }
                    override fun isCurrentUserHasWritePerms(): Boolean {
                        return true
                    }
                }
            }
        }

        recordsDao = DbRecordsDao(
            RECS_DAO_ID,
            DbRecordsDaoConfig(
                insertable = true,
                updatable = true,
                deletable = true,
                typeRef = RecordRef.EMPTY
            ),
            ecosTypeRepo,
            dataService,
            permsComponent,
            object : DbComputedAttsComponent {
                override fun computeAttsToStore(value: Any, isNewRecord: Boolean, typeRef: RecordRef): ObjectData {
                    return modelServiceFactory.computedAttsService.computeAttsToStore(value, isNewRecord, typeRef)
                }
            }
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

    fun updateRecord(rec: RecordRef, vararg atts: Pair<String, Any>): RecordRef {
        return records.mutate(rec, mapOf(*atts))
    }

    fun createRecord(vararg atts: Pair<String, Any>): RecordRef {
        val map = hashMapOf(*atts)
        if (!map.containsKey("_type")) {
            map["_type"] = REC_TEST_TYPE_REF
        }
        return records.create(RECS_DAO_ID, map)
    }

    fun selectRecFromDb(rec: RecordRef, field: String): Any? {
        return dbDataSource.withTransaction(true) {
            dbDataSource.query(
                "SELECT $field as res FROM ${tableRef.fullName} " +
                    "where __ext_id='${rec.id}'",
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
        return dataSource.connection.use { conn ->
            conn.createStatement().use { stmt ->
                stmt.executeUpdate(sql)
            }
        }
    }

    fun registerType(type: TypeInfo) {
        this.typesInfo[type.id] = type
    }

    fun registerAtts(atts: List<AttributeDef>) {
        registerAttributes(REC_TEST_TYPE_ID, atts)
    }

    fun registerAttributes(id: String, atts: List<AttributeDef>) {
        registerType(
            TypeInfo.create {
                withId(id)
                withModel(
                    TypeModelDef.create {
                        withAttributes(atts)
                    }
                )
            }
        )
    }
}
