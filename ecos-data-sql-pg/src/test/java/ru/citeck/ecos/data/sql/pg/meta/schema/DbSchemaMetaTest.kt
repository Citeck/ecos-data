package ru.citeck.ecos.data.sql.pg.meta.schema

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import ru.citeck.ecos.data.sql.context.DbDataSourceContext
import ru.citeck.ecos.data.sql.datasource.DbDataSource
import ru.citeck.ecos.data.sql.datasource.DbDataSourceImpl
import ru.citeck.ecos.data.sql.meta.schema.DbSchemaMetaEntity
import ru.citeck.ecos.data.sql.meta.schema.DbSchemaMetaService
import ru.citeck.ecos.data.sql.meta.schema.DbSchemaMetaServiceImpl
import ru.citeck.ecos.data.sql.pg.PgDataServiceFactory
import ru.citeck.ecos.data.sql.service.DbDataService
import ru.citeck.ecos.data.sql.service.DbDataServiceConfig
import ru.citeck.ecos.data.sql.service.DbDataServiceImpl
import ru.citeck.ecos.test.commons.containers.TestContainers
import ru.citeck.ecos.webapp.api.datasource.JdbcDataSource

class DbSchemaMetaTest {

    @Test
    fun test() {

        val ctx = createCtx()

        ctx.dataSource.withTransaction(false) {
            val testKey = "abc.def"
            assertThat(ctx.schemaMetaService.getValue(testKey).isNull()).isTrue
            ctx.schemaMetaService.setValue("abc.def", null)
            assertThat(ctx.schemaMetaService.getValue(testKey).isNull()).isTrue
            ctx.schemaMetaService.setValue("abc.def", 123)
            assertThat(ctx.schemaMetaService.getValue(testKey, 0)).isEqualTo(123)
            ctx.schemaMetaService.setValue("abc.def", 123)
            assertThat(ctx.schemaMetaService.getScoped("abc").getValue("def", 0)).isEqualTo(123)
        }
    }

    private fun createCtx(): TestCtx {

        val postgres = TestContainers.getPostgres(this::class)
        val jdbcDataSource = object : JdbcDataSource {
            override fun getJavaDataSource() = postgres.getDataSource()
            override fun isManaged() = false
        }
        val dataSource = DbDataSourceImpl(jdbcDataSource)

        val dsCtx = DbDataSourceContext("test", dataSource, PgDataServiceFactory())
        val schemaCtx = dsCtx.getSchemaContext("")
        val dbSchemaDao = dsCtx.schemaDao

        val dataServiceConfig = DbDataServiceConfig.create {
            withTable("schema-meta-test-table")
        }

        val dataService = DbDataServiceImpl(
            DbSchemaMetaEntity::class.java,
            dataServiceConfig,
            schemaCtx,
        )

        dataSource.withTransaction(true) {
            assertThat(dbSchemaDao.getColumns(dataSource, dataService.getTableRef())).isEmpty()
        }

        return TestCtx(dataSource, dataService, DbSchemaMetaServiceImpl(schemaCtx))
    }

    private class TestCtx(
        val dataSource: DbDataSource,
        val dataService: DbDataService<DbSchemaMetaEntity>,
        val schemaMetaService: DbSchemaMetaService
    )
}
