package ru.citeck.ecos.data.sql.pg.repo

import ru.citeck.ecos.data.sql.repo.entity.DbEntity
import ru.citeck.ecos.data.sql.service.DbDataService

class SqlDataServiceTestCtx(
    val service: DbDataService<DbEntity>
)
