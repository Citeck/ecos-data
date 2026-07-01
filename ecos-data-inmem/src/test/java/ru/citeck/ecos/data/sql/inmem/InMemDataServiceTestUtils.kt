package ru.citeck.ecos.data.sql.inmem

import ru.citeck.ecos.context.lib.ctx.GlobalEcosContext
import ru.citeck.ecos.data.sql.context.DbDataSourceContext
import ru.citeck.ecos.data.sql.datasource.DbDataSource
import ru.citeck.ecos.data.sql.domain.migration.DbMigrationService
import ru.citeck.ecos.data.sql.inmem.datasource.InMemDataSource
import ru.citeck.ecos.data.sql.repo.entity.DbEntity
import ru.citeck.ecos.data.sql.service.DbDataService
import ru.citeck.ecos.data.sql.service.DbDataServiceConfig
import ru.citeck.ecos.data.sql.service.DbDataServiceImpl
import ru.citeck.ecos.test.commons.EcosWebAppApiMock
import ru.citeck.ecos.txn.lib.TxnContext
import ru.citeck.ecos.txn.lib.manager.EcosTxnProps
import ru.citeck.ecos.txn.lib.manager.TransactionManagerImpl

/**
 * No-Spring construction recipe for the in-memory backend, mirroring the PG
 * `SqlDataServiceTestUtils`. The only difference from the PG variant is the data source
 * ([InMemDataSource]) and the factory ([InMemDataServiceFactory]) - proving that the backend is
 * selected purely by these two seams.
 */
object InMemDataServiceTestUtils {

    fun createDataSource(): InMemDataSource {
        return InMemDataSource()
    }

    fun createService(dbDataSource: DbDataSource, tableName: String): DbDataService<DbEntity> {

        val webAppApi = EcosWebAppApiMock("test")
        val txnManager = TransactionManagerImpl()
        txnManager.init(webAppApi, EcosTxnProps())
        TxnContext.setManager(txnManager)

        val dsCtx = DbDataSourceContext(
            dbDataSource,
            InMemDataServiceFactory(),
            DbMigrationService(),
            webAppApi,
            GlobalEcosContext.getContext()
        )
        val schemaCtx = dsCtx.getSchemaContext("in-mem-data-service-test-schema")

        val dataServiceConfig = DbDataServiceConfig.create {
            withTable(tableName)
        }

        return DbDataServiceImpl(
            DbEntity::class.java,
            dataServiceConfig,
            schemaCtx
        )
    }
}
