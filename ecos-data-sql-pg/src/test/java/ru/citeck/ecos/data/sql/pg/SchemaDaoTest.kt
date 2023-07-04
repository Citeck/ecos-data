package ru.citeck.ecos.data.sql.pg

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import ru.citeck.ecos.data.sql.context.DbDataSourceContext
import ru.citeck.ecos.data.sql.datasource.DbDataSource
import ru.citeck.ecos.data.sql.domain.migration.DbMigrationService
import ru.citeck.ecos.data.sql.dto.DbColumnDef
import ru.citeck.ecos.data.sql.dto.DbColumnIndexDef
import ru.citeck.ecos.data.sql.dto.DbColumnType
import ru.citeck.ecos.data.sql.dto.DbTableRef
import ru.citeck.ecos.test.commons.EcosWebAppApiMock

class SchemaDaoTest {

    @Test
    fun test() {
        val schemaCommands = PgUtils.withDbDataSource { testImpl(it) }
        schemaCommands.forEach { println(it) }
    }

    private fun testImpl(dataSource: DbDataSource) {

        val dsCtx = DbDataSourceContext(
            dataSource,
            PgDataServiceFactory(),
            DbMigrationService(),
            EcosWebAppApiMock("test"),
            emptyList()
        )
        val tableRef = DbTableRef("some-schema", "test-table")

        val dbSchemaDao = dsCtx.schemaDao
        assertThat(dbSchemaDao.getColumns(dataSource, tableRef)).isEmpty()

        val singleValueColumns = DbColumnType.values().mapIndexed { idx, value ->
            DbColumnDef.create {
                withName("column_$idx")
                withType(value)
                withMultiple(false)
            }
        }
        dbSchemaDao.createTable(dataSource, tableRef, singleValueColumns)
        val columnsFromDb = dbSchemaDao.getColumns(dataSource, tableRef)
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

        dbSchemaDao.addColumns(dataSource, tableRef, arrayColumns)

        val allColumnsFromDb = dbSchemaDao.getColumns(dataSource, tableRef)
        assertThat(allColumnsFromDb).containsExactlyInAnyOrderElementsOf(
            listOf(*singleValueColumns.toTypedArray(), *arrayColumns.toTypedArray())
        )

        val indexCommands = dataSource.watchSchemaCommands {
            dbSchemaDao.addColumns(
                dataSource,
                tableRef,
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

    private fun typeUpdateTestImpl(dataSource: DbDataSource) {

        val dsCtx = DbDataSourceContext(
            dataSource,
            PgDataServiceFactory(),
            DbMigrationService(),
            EcosWebAppApiMock("test"),
            emptyList()
        )
        val tableRef = DbTableRef("some-schema", "test-table")

        val dbSchemaDao = dsCtx.schemaDao
        dbSchemaDao.createTable(
            dataSource,
            tableRef,
            listOf(
                DbColumnDef.create {
                    withName("text_column")
                    withType(DbColumnType.TEXT)
                },
                DbColumnDef.create {
                    withName("date_column")
                    withType(DbColumnType.DATE)
                },
                DbColumnDef.create {
                    withName("long_column")
                    withType(DbColumnType.LONG)
                },
                DbColumnDef.create {
                    withName("int_column")
                    withType(DbColumnType.INT)
                },
                DbColumnDef.create {
                    withName("double_column")
                    withType(DbColumnType.DOUBLE)
                }
            )
        )

        // text to json

        val columns0 = dbSchemaDao.getColumns(dataSource, tableRef)
        assertThat(columns0).hasSize(5)
        assertThat(columns0.map { it.type }).containsExactly(
            DbColumnType.TEXT,
            DbColumnType.DATE,
            DbColumnType.LONG,
            DbColumnType.INT,
            DbColumnType.DOUBLE
        )

        dbSchemaDao.setColumnType(dataSource, tableRef, "text_column", false, DbColumnType.JSON)
        assertThat(dbSchemaDao.getColumns(dataSource, tableRef)[0].type).isEqualTo(DbColumnType.JSON)

        dbSchemaDao.setColumnType(dataSource, tableRef, "text_column", false, DbColumnType.TEXT)
        assertThat(dbSchemaDao.getColumns(dataSource, tableRef)[0].type).isEqualTo(DbColumnType.TEXT)

        dbSchemaDao.setColumnType(dataSource, tableRef, "date_column", false, DbColumnType.DATETIME)
        assertThat(dbSchemaDao.getColumns(dataSource, tableRef)[1].type).isEqualTo(DbColumnType.DATETIME)

        dbSchemaDao.setColumnType(dataSource, tableRef, "long_column", false, DbColumnType.TEXT)
        assertThat(dbSchemaDao.getColumns(dataSource, tableRef)[2].type).isEqualTo(DbColumnType.TEXT)
        dbSchemaDao.setColumnType(dataSource, tableRef, "int_column", false, DbColumnType.TEXT)
        assertThat(dbSchemaDao.getColumns(dataSource, tableRef)[3].type).isEqualTo(DbColumnType.TEXT)
        dbSchemaDao.setColumnType(dataSource, tableRef, "double_column", false, DbColumnType.TEXT)
        assertThat(dbSchemaDao.getColumns(dataSource, tableRef)[4].type).isEqualTo(DbColumnType.TEXT)
    }
}
