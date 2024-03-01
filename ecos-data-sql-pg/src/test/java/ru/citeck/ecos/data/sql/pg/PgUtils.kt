package ru.citeck.ecos.data.sql.pg

import ru.citeck.ecos.data.sql.datasource.DbDataSource
import ru.citeck.ecos.data.sql.datasource.DbDataSourceImpl
import ru.citeck.ecos.test.commons.EcosWebAppApiMock
import ru.citeck.ecos.test.commons.containers.TestContainers
import ru.citeck.ecos.txn.lib.TxnContext
import ru.citeck.ecos.txn.lib.manager.EcosTxnProps
import ru.citeck.ecos.txn.lib.manager.TransactionManagerImpl
import ru.citeck.ecos.webapp.api.datasource.JdbcDataSource

object PgUtils {

    fun withDbDataSource(action: (DbDataSource) -> Unit): List<String> {

        val txnManager = TransactionManagerImpl()
        txnManager.init(EcosWebAppApiMock(), EcosTxnProps())
        TxnContext.setManager(txnManager)

        val postgres = TestContainers.getPostgres(PgUtils::class.java)

        val jdbcDataSource = object : JdbcDataSource {
            override fun getJavaDataSource() = postgres.getDataSource()
            override fun isManaged() = false
        }
        val dbDataSource = DbDataSourceImpl(jdbcDataSource)
        try {
            return TxnContext.doInNewTxn {
                dbDataSource.withTransaction(false) {
                    dbDataSource.watchSchemaCommands {
                        action.invoke(dbDataSource)
                    }
                }
            }
        } finally {
            postgres.release()
        }
    }
}
