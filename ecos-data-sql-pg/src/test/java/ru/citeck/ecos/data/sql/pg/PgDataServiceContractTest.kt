package ru.citeck.ecos.data.sql.pg

import org.junit.jupiter.api.AfterEach
import ru.citeck.ecos.data.sql.datasource.DbDataSource
import ru.citeck.ecos.data.sql.datasource.DbDataSourceImpl
import ru.citeck.ecos.data.sql.service.DbDataServiceFactory
import ru.citeck.ecos.data.sql.test.DbDataServiceContractTest
import ru.citeck.ecos.test.commons.containers.TestContainers
import ru.citeck.ecos.test.commons.containers.container.postgres.PostgresContainer
import ru.citeck.ecos.webapp.api.datasource.JdbcDataSource

/**
 * Runs the shared ecos-data storage SPI [DbDataServiceContractTest] against the PostgreSQL backend
 * (via Testcontainers). The only backend-specific code is the factory + Testcontainers-backed data
 * source supplied here and the container lifecycle.
 */
class PgDataServiceContractTest : DbDataServiceContractTest() {

    private var postgres: PostgresContainer? = null

    override fun createDataServiceFactory(): DbDataServiceFactory {
        return PgDataServiceFactory()
    }

    override fun createDataSource(): DbDataSource {
        val container = TestContainers.getPostgres(PgDataServiceContractTest::class.java)
        postgres = container
        val jdbcDataSource = object : JdbcDataSource {
            override fun getKey() = "key"
            override fun getJavaDataSource() = container.getDataSource()
            override fun isManaged() = false
        }
        return DbDataSourceImpl(jdbcDataSource)
    }

    @AfterEach
    fun releaseContainer() {
        postgres?.release()
        postgres = null
    }
}
