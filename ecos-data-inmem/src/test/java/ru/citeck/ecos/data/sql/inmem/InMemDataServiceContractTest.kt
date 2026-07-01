package ru.citeck.ecos.data.sql.inmem

import ru.citeck.ecos.data.sql.datasource.DbDataSource
import ru.citeck.ecos.data.sql.inmem.datasource.InMemDataSource
import ru.citeck.ecos.data.sql.service.DbDataServiceFactory
import ru.citeck.ecos.data.sql.test.DbDataServiceContractTest

/**
 * Runs the shared ecos-data storage SPI [DbDataServiceContractTest] against the in-memory backend.
 * The only backend-specific code is the factory + data source supplied here.
 */
class InMemDataServiceContractTest : DbDataServiceContractTest() {

    override fun createDataServiceFactory(): DbDataServiceFactory {
        return InMemDataServiceFactory()
    }

    override fun createDataSource(): DbDataSource {
        return InMemDataSource()
    }
}
