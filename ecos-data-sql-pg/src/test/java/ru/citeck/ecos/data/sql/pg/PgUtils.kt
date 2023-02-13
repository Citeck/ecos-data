package ru.citeck.ecos.data.sql.pg

import ru.citeck.ecos.data.sql.datasource.DbDataSource
import ru.citeck.ecos.data.sql.datasource.DbDataSourceImpl
import ru.citeck.ecos.test.commons.containers.TestContainers
import ru.citeck.ecos.webapp.api.datasource.JdbcDataSource

object PgUtils {

    fun withDbDataSource(action: (DbDataSource) -> Unit): List<String> {
        val postgres = TestContainers.getPostgres(PgUtils::class.java)

        val jdbcDataSource = object : JdbcDataSource {
            override fun getJavaDataSource() = postgres.getDataSource()
            override fun isManaged() = false
        }
        val dbDataSource = DbDataSourceImpl(jdbcDataSource)
        try {
            return dbDataSource.withTransaction(false) {
                dbDataSource.watchSchemaCommands {
                    action.invoke(dbDataSource)
                }
            }
        } finally {
            postgres.recreate()
        }
    }
}
