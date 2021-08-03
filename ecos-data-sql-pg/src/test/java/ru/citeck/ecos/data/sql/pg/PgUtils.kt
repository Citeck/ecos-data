package ru.citeck.ecos.data.sql.pg

import io.zonky.test.db.postgres.embedded.EmbeddedPostgres
import ru.citeck.ecos.data.sql.datasource.DbDataSource
import ru.citeck.ecos.data.sql.datasource.DbDataSourceImpl
import ru.citeck.ecos.data.sql.utils.use

object PgUtils {

    const val TEST_DB_NAME = "test"

    fun withDbDataSource(action: (DbDataSource) -> Unit): List<String> {
        return EmbeddedPostgres.start().use { pg ->
            pg.postgresDatabase.connection.use { conn ->
                conn.prepareStatement("CREATE DATABASE $TEST_DB_NAME").use { it.executeUpdate() }
            }
            val dbDataSource = DbDataSourceImpl(pg.getDatabase("postgres", "test"))
            dbDataSource.withTransaction(false) {
                dbDataSource.watchCommands {
                    action.invoke(dbDataSource)
                }
            }
        }
    }
}
