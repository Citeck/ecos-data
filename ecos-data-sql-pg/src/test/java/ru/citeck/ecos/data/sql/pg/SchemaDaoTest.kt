package ru.citeck.ecos.data.sql.pg

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import ru.citeck.ecos.data.sql.datasource.DbDataSource
import ru.citeck.ecos.data.sql.dto.DbColumnDef
import ru.citeck.ecos.data.sql.dto.DbColumnIndexDef
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
            DbColumnDef.create {
                withName("column_$idx")
                withType(value)
                withMultiple(false)
            }
        }
        dbSchemaDao.createTable(singleValueColumns)
        val columnsFromDb = dbSchemaDao.getColumns()
        assertThat(columnsFromDb).containsExactlyInAnyOrderElementsOf(singleValueColumns)

        val arrayColumns = DbColumnType.values()
            .filter { it != DbColumnType.BIGSERIAL }
            .filter { it != DbColumnType.JSON }
            .mapIndexed { idx, value ->
                DbColumnDef.create {
                    withName("column_arr_$idx")
                    withType(value)
                    withMultiple(true)
                }
            }

        dbSchemaDao.addColumns(arrayColumns)

        val allColumnsFromDb = dbSchemaDao.getColumns()
        assertThat(allColumnsFromDb).containsExactlyInAnyOrderElementsOf(
            listOf(*singleValueColumns.toTypedArray(), *arrayColumns.toTypedArray())
        )

        val indexCommands = dbDataSource.watchSchemaCommands {
            dbSchemaDao.addColumns(
                listOf(
                    DbColumnDef.create {
                        withName("indexed_str")
                        withType(DbColumnType.TEXT)
                        withIndex(DbColumnIndexDef(true))
                    },
                    DbColumnDef.create {
                        withName("indexed_str_arr")
                        withType(DbColumnType.TEXT)
                        withIndex(DbColumnIndexDef(true))
                        withMultiple(true)
                    }
                )
            )
        }.filter { it.contains("INDEX") }

        assertThat(indexCommands).hasSize(2)
        assertThat(indexCommands).allMatch { it.contains("CREATE INDEX") }
        assertThat(indexCommands).anyMatch { it.contains("indexed_str") && !it.contains("GIN") }
        assertThat(indexCommands).anyMatch { it.contains("indexed_str_arr") && it.contains("GIN") }
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
