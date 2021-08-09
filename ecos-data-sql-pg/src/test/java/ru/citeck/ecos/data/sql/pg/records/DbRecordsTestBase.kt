package ru.citeck.ecos.data.sql.pg.records

import io.zonky.test.db.postgres.embedded.EmbeddedPostgres
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import ru.citeck.ecos.commons.data.MLText
import ru.citeck.ecos.data.sql.datasource.DbDataSourceImpl
import ru.citeck.ecos.data.sql.dto.DbTableRef
import ru.citeck.ecos.data.sql.ecostype.DbEcosTypeInfo
import ru.citeck.ecos.data.sql.ecostype.DbEcosTypeRepo
import ru.citeck.ecos.data.sql.pg.PgDataServiceFactory
import ru.citeck.ecos.data.sql.pg.PgUtils
import ru.citeck.ecos.data.sql.records.DbRecordsDao
import ru.citeck.ecos.data.sql.records.DbRecordsDaoConfig
import ru.citeck.ecos.data.sql.repo.DbContextManager
import ru.citeck.ecos.data.sql.repo.entity.DbEntity
import ru.citeck.ecos.data.sql.service.DbDataServiceConfig
import ru.citeck.ecos.data.sql.utils.use
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.type.service.utils.TypeUtils
import ru.citeck.ecos.records2.RecordRef
import ru.citeck.ecos.records3.RecordsService
import ru.citeck.ecos.records3.RecordsServiceFactory
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

    private val typesDef = mutableMapOf<String, DbEcosTypeInfo>()
    private var currentUser = "user0"

    private lateinit var recordsDao: DbRecordsDao
    private lateinit var records: RecordsService

    @BeforeEach
    fun beforeEach() {

        cleanData()
        typesDef.clear()

        initWithTable(DbTableRef("records-test-schema", "test-records-table"))
    }

    fun initWithTable(tableRef: DbTableRef) {

        val ecosTypeRepo = object : DbEcosTypeRepo {
            override fun getTypeInfo(typeId: String): DbEcosTypeInfo? {
                return typesDef[typeId]
            }
        }
        val contextManager = object : DbContextManager {
            override fun getCurrentUser() = currentUser
            override fun getCurrentUserAuthorities(): List<String> = listOf(getCurrentUser())
        }

        val dbDataSource = DbDataSourceImpl(dataSource)
        recordsDao = DbRecordsDao(
            RECS_DAO_ID,
            DbRecordsDaoConfig(
                insertable = true,
                updatable = true,
                deletable = true,
                typeRef = RecordRef.EMPTY
            ),
            ecosTypeRepo,
            PgDataServiceFactory().create(DbEntity::class.java)
                .withConfig(DbDataServiceConfig(true))
                .withDataSource(dbDataSource)
                .withDbContextManager(contextManager)
                .withTableRef(tableRef)
                .build()
        )

        val services = RecordsServiceFactory()
        records = services.recordsServiceV1
        records.register(recordsDao)
        RequestContext.setDefaultServices(services)
    }

    fun createRecord(vararg atts: Pair<String, Any>): RecordRef {
        val map = hashMapOf(*atts)
        if (!map.containsKey("_type")) {
            map["_type"] = REC_TEST_TYPE_REF
        }
        return getRecords().create(RECS_DAO_ID, map)
    }

    fun cleanData() {
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
                        line += it.metaData.getColumnName(i) + "\t\t\t\t"
                    }
                    println(line)
                    while (it.next()) {
                        line = ""
                        for (i in 1..it.metaData.columnCount) {
                            line += (it.getObject(i) ?: "").toString() + "\t\t\t\t"
                        }
                        println(line)
                    }
                }
            }
        }
    }

    fun registerType(type: DbEcosTypeInfo) {
        this.typesDef[type.id] = type
    }

    fun registerAtts(atts: List<AttributeDef>) {
        registerType(REC_TEST_TYPE_ID, atts)
    }

    fun registerType(id: String, atts: List<AttributeDef>) {
        this.typesDef[id] = DbEcosTypeInfo(id, MLText(), MLText(), RecordRef.EMPTY, atts, emptyList())
    }

    fun setCurrentUser(user: String) {
        this.currentUser = user
    }

    fun getRecords(): RecordsService {
        return this.records
    }

    fun getCurrentUser(): String {
        return this.currentUser
    }

    fun getRecordsDao(): DbRecordsDao {
        return this.recordsDao
    }
}
