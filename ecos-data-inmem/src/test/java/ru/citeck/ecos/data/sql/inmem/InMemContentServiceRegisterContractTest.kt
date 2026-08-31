package ru.citeck.ecos.data.sql.inmem

import ru.citeck.ecos.data.sql.datasource.DbDataSource
import ru.citeck.ecos.data.sql.inmem.datasource.InMemDataSource
import ru.citeck.ecos.data.sql.service.DbDataServiceFactory
import ru.citeck.ecos.data.sql.test.content.ContentServiceRegisterContractTest

/**
 * Runs the shared [ContentServiceRegisterContractTest] against the in-memory backend. The only
 * backend-specific code is the factory + data source supplied here.
 */
class InMemContentServiceRegisterContractTest : ContentServiceRegisterContractTest() {

    override fun createDataServiceFactory(): DbDataServiceFactory {
        return InMemDataServiceFactory()
    }

    override fun createDataSource(): DbDataSource {
        return InMemDataSource()
    }
}
