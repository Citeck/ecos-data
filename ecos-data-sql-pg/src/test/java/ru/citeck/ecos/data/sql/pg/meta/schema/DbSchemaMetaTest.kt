package ru.citeck.ecos.data.sql.pg.meta.schema

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import ru.citeck.ecos.context.lib.ctx.GlobalEcosContext
import ru.citeck.ecos.data.sql.context.DbDataSourceContext
import ru.citeck.ecos.data.sql.datasource.DbDataSource
import ru.citeck.ecos.data.sql.datasource.DbDataSourceImpl
import ru.citeck.ecos.data.sql.domain.migration.DbMigrationService
import ru.citeck.ecos.data.sql.meta.schema.DbSchemaMetaEntity
import ru.citeck.ecos.data.sql.meta.schema.DbSchemaMetaService
import ru.citeck.ecos.data.sql.meta.schema.DbSchemaMetaServiceImpl
import ru.citeck.ecos.data.sql.pg.PgDataServiceFactory
import ru.citeck.ecos.data.sql.service.DbDataService
import ru.citeck.ecos.data.sql.service.DbDataServiceConfig
import ru.citeck.ecos.data.sql.service.DbDataServiceImpl
import ru.citeck.ecos.data.sql.test.records.RequiresFreshSchema
import ru.citeck.ecos.test.commons.EcosWebAppApiMock
import ru.citeck.ecos.test.commons.containers.TestContainers
import ru.citeck.ecos.txn.lib.TxnContext
import ru.citeck.ecos.txn.lib.manager.EcosTxnProps
import ru.citeck.ecos.txn.lib.manager.TransactionManagerImpl
import ru.citeck.ecos.webapp.api.datasource.JdbcDataSource

// Asserts on an untouched ed_schema_meta - the cache keeps that table alive across tests with
// exactly the rows ecos-data wrote into it, which is not the empty table this test expects.
@RequiresFreshSchema
class DbSchemaMetaTest {

    companion object {
        // the schema name is mandatory - DbDataSourceContext.getSchemaContext rejects a blank one,
        // because pg_tables/pg_namespace lookups can never match the empty string
        private const val SCHEMA = "schema_meta_test"
    }

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

        // a named schema means DbSchemaContext's onSchemaCreated listener fires, and that listener
        // defers setVersion() through TxnContext.doBeforeCommit - which runs the action inline when
        // no transaction manager is installed, re-entering table creation while ed_schema_meta is
        // still being created. Install the manager the same way PgUtils does.
        val txnManager = TransactionManagerImpl()
        txnManager.init(EcosWebAppApiMock(), EcosTxnProps())
        TxnContext.setManager(txnManager)

        val postgres = TestContainers.getPostgres(this::class)
        val jdbcDataSource = object : JdbcDataSource {
            override fun getKey() = "key"
            override fun getJavaDataSource() = postgres.getDataSource()
            override fun isManaged() = false
        }
        val dataSource = DbDataSourceImpl(jdbcDataSource)

        val dsCtx = DbDataSourceContext(
            dataSource,
            PgDataServiceFactory(),
            DbMigrationService(),
            EcosWebAppApiMock("test"),
            GlobalEcosContext.getContext()
        )
        val schemaCtx = dsCtx.getSchemaContext(SCHEMA)
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
