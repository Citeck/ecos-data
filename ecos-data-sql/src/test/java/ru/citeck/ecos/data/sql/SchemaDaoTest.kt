package ru.citeck.ecos.data.sql

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import ru.citeck.ecos.data.sql.datasource.DbDataSource
import ru.citeck.ecos.data.sql.dto.DbColumnDef
import ru.citeck.ecos.data.sql.dto.DbColumnType
import ru.citeck.ecos.data.sql.dto.DbTableRef
import ru.citeck.ecos.data.sql.schema.DbSchemaDaoPg

class SchemaDaoTest {

    @Test
    fun test() {
        val schemaCommands = PgUtils.withDbDataSource { testImpl(it) }
        schemaCommands.forEach { println(it) }
    }

    private fun testImpl(dbDataSource: DbDataSource) {

        val dbSchemaDao = DbSchemaDaoPg(dbDataSource, DbTableRef("", "test-table"))
        assertThat(dbSchemaDao.getColumns()).isEmpty()

        val singleValueColumns = DbColumnType.values().mapIndexed { idx, value ->
            DbColumnDef("column_$idx", value, false, emptyList())
        }
        dbSchemaDao.createTable(singleValueColumns)
        val columnsFromDb = dbSchemaDao.getColumns()
        assertThat(columnsFromDb).containsExactlyInAnyOrderElementsOf(singleValueColumns)

        val arrayColumns = DbColumnType.values()
            .filter { it != DbColumnType.BIGSERIAL }
            .mapIndexed { idx, value -> DbColumnDef("column_arr_$idx", value, true, emptyList()) }

        dbSchemaDao.addColumns(arrayColumns)

        val allColumnsFromDb = dbSchemaDao.getColumns()
        assertThat(allColumnsFromDb).containsExactlyInAnyOrderElementsOf(
            listOf(*singleValueColumns.toTypedArray(), *arrayColumns.toTypedArray())
        )
    }
}
