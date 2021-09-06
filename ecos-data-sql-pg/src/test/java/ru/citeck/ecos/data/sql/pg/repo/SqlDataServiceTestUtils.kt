package ru.citeck.ecos.data.sql.pg.repo

import org.assertj.core.api.Assertions
import ru.citeck.ecos.data.sql.datasource.DbDataSource
import ru.citeck.ecos.data.sql.dto.DbTableRef
import ru.citeck.ecos.data.sql.pg.DbSchemaDaoPg
import ru.citeck.ecos.data.sql.pg.PgDataServiceFactory
import ru.citeck.ecos.data.sql.repo.entity.DbEntity
import ru.citeck.ecos.data.sql.service.DbDataServiceConfig

object SqlDataServiceTestUtils {

    fun createService(dbDataSource: DbDataSource, tableName: String): SqlDataServiceTestCtx {

        val dbSchemaDao = DbSchemaDaoPg(dbDataSource, DbTableRef("", tableName))
        Assertions.assertThat(dbSchemaDao.getColumns()).isEmpty()

        val sqlDataService = PgDataServiceFactory().create(DbEntity::class.java)
            .withTableRef(DbTableRef("sql-data-service-test-utils-schema", tableName))
            .withConfig(DbDataServiceConfig.create { withAuthEnabled(false) })
            .withDataSource(dbDataSource)
            .build()

        return SqlDataServiceTestCtx(sqlDataService)
    }
}
