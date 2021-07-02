package ru.citeck.ecos.data.sql.repo

import ru.citeck.ecos.data.sql.SqlDataService
import ru.citeck.ecos.data.sql.repo.entity.DbEntity

class SqlDataServiceTestCtx(
    var setCurrentUser: (String) -> Unit,
    var setCurrentTenant: (String) -> Unit,
    val service: SqlDataService<DbEntity>,
    val ctxManager: DbContextManager
)
