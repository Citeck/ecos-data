package ru.citeck.ecos.data.sql.pg.records

import io.zonky.test.db.postgres.embedded.EmbeddedPostgres
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
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
import ru.citeck.ecos.records2.RecordRef
import ru.citeck.ecos.records3.RecordsService
import ru.citeck.ecos.records3.RecordsServiceFactory

abstract class DbRecordsTestBase {

    companion object {
        const val RECS_DAO_ID = "test"
    }

    private val typesDef = mutableMapOf<String, DbEcosTypeInfo>()
    private var currentUser = "user0"

    private lateinit var recordsDao: DbRecordsDao
    private lateinit var records: RecordsService
    private lateinit var pg: EmbeddedPostgres

    @BeforeEach
    fun beforeEach() {

        pg = EmbeddedPostgres.start()
        pg.postgresDatabase.connection.use { conn ->
            conn.prepareStatement("CREATE DATABASE ${PgUtils.TEST_DB_NAME}").use { it.executeUpdate() }
        }

        val ecosTypeRepo = object : DbEcosTypeRepo {
            override fun getTypeInfo(typeId: String): DbEcosTypeInfo? {
                return typesDef[typeId]
            }
        }
        val contextManager = object : DbContextManager {
            override fun getCurrentUser() = currentUser
            override fun getCurrentUserAuthorities(): List<String> = listOf(getCurrentUser())
        }

        val dbDataSource = DbDataSourceImpl(pg.getDatabase("postgres", PgUtils.TEST_DB_NAME))
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
                .withTableRef(DbTableRef("records-test-schema", "test-records-table"))
                .build()
        )

        records = RecordsServiceFactory().recordsServiceV1
        records.register(recordsDao)

        typesDef.clear()
    }

    @AfterEach
    fun afterEach() {
        pg.close()
    }

    fun registerType(type: DbEcosTypeInfo) {
        this.typesDef[type.id] = type
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
