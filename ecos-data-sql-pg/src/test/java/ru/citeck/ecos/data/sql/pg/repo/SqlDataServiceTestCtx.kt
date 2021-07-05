package ru.citeck.ecos.data.sql.pg.repo

import ru.citeck.ecos.data.sql.repo.DbContextManager
import ru.citeck.ecos.data.sql.repo.entity.DbEntity
import ru.citeck.ecos.data.sql.service.DbDataService

class SqlDataServiceTestCtx(
    var setCurrentUser: (String) -> Unit,
    val service: DbDataService<DbEntity>,
    val ctxManager: DbContextManager
)
