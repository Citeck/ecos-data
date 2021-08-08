package ru.citeck.ecos.data.sql.pg.records

import io.zonky.test.db.postgres.embedded.EmbeddedPostgres
import org.junit.jupiter.api.AfterEach
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
import ru.citeck.ecos.records2.RecordRef
import ru.citeck.ecos.records3.RecordsService
import ru.citeck.ecos.records3.RecordsServiceFactory
import javax.sql.DataSource

abstract class DbRecordsTestBase {

    companion object {
        const val RECS_DAO_ID = "test"

        const val COLUMN_TYPE_NAME = "TYPE_NAME"
        const val COLUMN_COLUMN_NAME = "COLUMN_NAME"
        const val COLUMN_TABLE_SCHEMA = "TABLE_SCHEM"
        const val COLUMN_TABLE_NAME = "TABLE_NAME"
    }

    private val typesDef = mutableMapOf<String, DbEcosTypeInfo>()
    private var currentUser = "user0"

    private lateinit var recordsDao: DbRecordsDao
    private lateinit var records: RecordsService
    private lateinit var pg: EmbeddedPostgres
    private lateinit var dataSource: DataSource

    @BeforeEach
    fun beforeEach() {

        pg = EmbeddedPostgres.start()
        pg.postgresDatabase.connection.use { conn ->
            conn.prepareStatement("CREATE DATABASE ${PgUtils.TEST_DB_NAME}").use { it.executeUpdate() }
        }
        dataSource = pg.getDatabase("postgres", PgUtils.TEST_DB_NAME)

        initWithTable(DbTableRef("records-test-schema", "test-records-table"))

        typesDef.clear()
    }

    fun initWithTable(table: DbTableRef) {

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
                .withTableRef(table)
                .build()
        )

        records = RecordsServiceFactory().recordsServiceV1
        records.register(recordsDao)
    }

    @AfterEach
    fun afterEach() {
        pg.close()
    }

    fun registerType(type: DbEcosTypeInfo) {
        this.typesDef[type.id] = type
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

    fun printAllColumns() {
        dataSource.connection.use { conn ->
            conn.metaData.getColumns(null, "%", "%", "%").use {
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
}
