package ru.citeck.ecos.data.sql.pg

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import ru.citeck.ecos.data.sql.datasource.DbDataSource
import ru.citeck.ecos.data.sql.dto.DbColumnDef
import ru.citeck.ecos.data.sql.dto.DbColumnType
import ru.citeck.ecos.data.sql.dto.DbTableRef

class SchemaDaoTest {

    @Test
    fun test() {
        val schemaCommands = PgUtils.withDbDataSource { testImpl(it) }
        schemaCommands.forEach { println(it) }
    }

    private fun testImpl(dbDataSource: DbDataSource) {

        val dbSchemaDao = DbSchemaDaoPg(dbDataSource, DbTableRef("some-schema", "test-table"))
        assertThat(dbSchemaDao.getColumns()).isEmpty()

        val singleValueColumns = DbColumnType.values().mapIndexed { idx, value ->
            DbColumnDef("column_$idx", value, false, emptyList())
        }
        dbSchemaDao.createTable(singleValueColumns)
        val columnsFromDb = dbSchemaDao.getColumns()
        assertThat(columnsFromDb).containsExactlyInAnyOrderElementsOf(singleValueColumns)

        val arrayColumns = DbColumnType.values()
            .filter { it != DbColumnType.BIGSERIAL }
            .filter { it != DbColumnType.JSON }
            .mapIndexed { idx, value -> DbColumnDef("column_arr_$idx", value, true, emptyList()) }

        dbSchemaDao.addColumns(arrayColumns)

        val allColumnsFromDb = dbSchemaDao.getColumns()
        assertThat(allColumnsFromDb).containsExactlyInAnyOrderElementsOf(
            listOf(*singleValueColumns.toTypedArray(), *arrayColumns.toTypedArray())
        )
    }

    @Test
    fun typeUpdateTest() {
        PgUtils.withDbDataSource { typeUpdateTestImpl(it) }
    }

    private fun typeUpdateTestImpl(dbDataSource: DbDataSource) {

        val dbSchemaDao = DbSchemaDaoPg(dbDataSource, DbTableRef("some-schema", "test-table"))
        dbSchemaDao.createTable(
            listOf(
                DbColumnDef.create {
                    withName("text_column")
                    withType(DbColumnType.TEXT)
                },
                DbColumnDef.create {
                    withName("date_column")
                    withType(DbColumnType.DATE)
                }
            )
        )

        // text to json

        assertThat(dbSchemaDao.getColumns()).hasSize(2)
        assertThat(dbSchemaDao.getColumns()[0].type).isEqualTo(DbColumnType.TEXT)
        assertThat(dbSchemaDao.getColumns()[1].type).isEqualTo(DbColumnType.DATE)

        dbSchemaDao.setColumnType("text_column", false, DbColumnType.JSON)
        assertThat(dbSchemaDao.getColumns()[0].type).isEqualTo(DbColumnType.JSON)

        dbSchemaDao.setColumnType("text_column", false, DbColumnType.TEXT)
        assertThat(dbSchemaDao.getColumns()[0].type).isEqualTo(DbColumnType.TEXT)

        dbSchemaDao.setColumnType("date_column", false, DbColumnType.DATETIME)
        assertThat(dbSchemaDao.getColumns()[1].type).isEqualTo(DbColumnType.DATETIME)
    }
}
