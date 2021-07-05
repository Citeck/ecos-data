package ru.citeck.ecos.data.sql.repo

import org.assertj.core.api.Assertions
import ru.citeck.ecos.data.sql.datasource.DbDataSource
import ru.citeck.ecos.data.sql.dto.DbTableRef
import ru.citeck.ecos.data.sql.repo.entity.DbEntity
import ru.citeck.ecos.data.sql.schema.DbSchemaDaoPg
import ru.citeck.ecos.data.sql.service.DbDataService
import ru.citeck.ecos.data.sql.service.DbDataServiceConfig

object SqlDataServiceTestUtils {

    fun createService(dbDataSource: DbDataSource, tableName: String): SqlDataServiceTestCtx {

        val dbSchemaDao = DbSchemaDaoPg(dbDataSource, DbTableRef("", tableName))
        Assertions.assertThat(dbSchemaDao.getColumns()).isEmpty()

        var currentUser = "user0"
        var currentTenant = "tenant"
        val ctxManager = object : DbContextManager {
            override fun getCurrentTenant() = currentTenant
            override fun getCurrentUser() = currentUser
            override fun getCurrentUserAuthorities(): List<String> = listOf(getCurrentUser())
        }

        val sqlDataService = DbDataService(
            DbDataServiceConfig(false),
            DbTableRef("", tableName),
            dbDataSource,
            DbEntity::class,
            ctxManager,
            true
        )

        return SqlDataServiceTestCtx({ currentUser = it }, { currentTenant = it }, sqlDataService, ctxManager)
    }
}
