package ru.citeck.ecos.data.sql.repo

import ru.citeck.ecos.data.sql.repo.entity.DbEntity
import ru.citeck.ecos.data.sql.service.DbDataService

class SqlDataServiceTestCtx(
    var setCurrentUser: (String) -> Unit,
    var setCurrentTenant: (String) -> Unit,
    val service: DbDataService<DbEntity>,
    val ctxManager: DbContextManager
)
