package ru.citeck.ecos.data.sql.pg.repo

import org.assertj.core.api.Assertions
import ru.citeck.ecos.data.sql.context.DbDataSourceContext
import ru.citeck.ecos.data.sql.datasource.DbDataSource
import ru.citeck.ecos.data.sql.pg.PgDataServiceFactory
import ru.citeck.ecos.data.sql.repo.entity.DbEntity
import ru.citeck.ecos.data.sql.service.DbDataServiceConfig
import ru.citeck.ecos.data.sql.service.DbDataServiceImpl

object SqlDataServiceTestUtils {

    fun createService(dbDataSource: DbDataSource, tableName: String): SqlDataServiceTestCtx {

        val dsCtx = DbDataSourceContext("test", dbDataSource, PgDataServiceFactory())
        val schemaCtx = dsCtx.getSchemaContext("sql-data-service-test-utils-schema")

        val dataServiceConfig = DbDataServiceConfig.create {
            withTable(tableName)
        }

        val dataService = DbDataServiceImpl(
            DbEntity::class.java,
            dataServiceConfig,
            schemaCtx
        )

        val dbSchemaDao = dsCtx.schemaDao
        Assertions.assertThat(dbSchemaDao.getColumns(dbDataSource, dataService.getTableRef())).isEmpty()

        return SqlDataServiceTestCtx(dataService)
    }
}
