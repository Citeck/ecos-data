package ru.citeck.ecos.data.sql.pg

import io.zonky.test.db.postgres.embedded.EmbeddedPostgres
import ru.citeck.ecos.data.sql.datasource.DbDataSource
import ru.citeck.ecos.data.sql.datasource.DbDataSourceImpl
import ru.citeck.ecos.data.sql.utils.use
import ru.citeck.ecos.webapp.api.datasource.JdbcDataSource

object PgUtils {

    const val TEST_DB_NAME = "test"

    fun withDbDataSource(action: (DbDataSource) -> Unit): List<String> {
        return EmbeddedPostgres.start().use { pg ->
            pg.postgresDatabase.connection.use { conn ->
                conn.prepareStatement("CREATE DATABASE $TEST_DB_NAME").use { it.executeUpdate() }
            }
            val javaDataSource = pg.getDatabase("postgres", "test")
            val jdbcDataSource = object : JdbcDataSource {
                override fun getJavaDataSource() = javaDataSource
                override fun isManaged() = false
            }
            val dbDataSource = DbDataSourceImpl(jdbcDataSource)
            dbDataSource.withTransaction(false) {
                dbDataSource.watchSchemaCommands {
                    action.invoke(dbDataSource)
                }
            }
        }
    }
}
