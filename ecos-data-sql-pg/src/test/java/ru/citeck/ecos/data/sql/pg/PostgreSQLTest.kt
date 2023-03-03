package ru.citeck.ecos.data.sql.pg

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import ru.citeck.ecos.test.commons.containers.TestContainers
import java.sql.ResultSet
import java.sql.Statement

class PostgreSQLTest {

    @Test
    fun checkPostgresVersion() {
        val dataSource = TestContainers.getPostgres().getDataSource()
        dataSource.connection.use { c ->
            val statement: Statement = c.createStatement()
            val rs: ResultSet = statement.executeQuery("SELECT version()")
            rs.next()
            assertThat(rs.getString(1)).startsWith("PostgreSQL 12.6")
        }
    }
}
