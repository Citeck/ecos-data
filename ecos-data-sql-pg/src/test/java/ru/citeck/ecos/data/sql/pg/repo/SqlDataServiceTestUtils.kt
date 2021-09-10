package ru.citeck.ecos.data.sql.pg.repo

import org.assertj.core.api.Assertions
import ru.citeck.ecos.data.sql.datasource.DbDataSource
import ru.citeck.ecos.data.sql.dto.DbTableRef
import ru.citeck.ecos.data.sql.pg.DbSchemaDaoPg
import ru.citeck.ecos.data.sql.pg.PgDataServiceFactory
import ru.citeck.ecos.data.sql.repo.entity.DbEntity
import ru.citeck.ecos.data.sql.service.DbDataServiceConfig
import ru.citeck.ecos.data.sql.service.DbDataServiceImpl

object SqlDataServiceTestUtils {

    fun createService(dbDataSource: DbDataSource, tableName: String): SqlDataServiceTestCtx {

        val dbSchemaDao = DbSchemaDaoPg(dbDataSource, DbTableRef("", tableName))
        Assertions.assertThat(dbSchemaDao.getColumns()).isEmpty()

        val dataServiceConfig = DbDataServiceConfig.create {
            withAuthEnabled(false)
            withTableRef(DbTableRef("sql-data-service-test-utils-schema", tableName))
        }

        val pgDataServiceFactory = PgDataServiceFactory()

        val dataService = DbDataServiceImpl(
            DbEntity::class.java,
            dataServiceConfig,
            dbDataSource,
            pgDataServiceFactory
        )

        return SqlDataServiceTestCtx(dataService)
    }
}
