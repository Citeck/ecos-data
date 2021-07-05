package ru.citeck.ecos.data.sql

import io.zonky.test.db.postgres.embedded.EmbeddedPostgres
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.sql.ResultSet
import java.sql.Statement

class PostgreSQLTest {

    @Test
    fun checkPostgresVersion() {
        EmbeddedPostgres.start().use { pg ->
            pg.postgresDatabase.connection.use { c ->
                val statement: Statement = c.createStatement()
                val rs: ResultSet = statement.executeQuery("SELECT version()")
                rs.next()
                assertThat(rs.getString(1)).startsWith("PostgreSQL 12.6")
            }
        }
    }
}
