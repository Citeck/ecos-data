package ru.citeck.ecos.data.sql.repo.entity.auth

import ru.citeck.ecos.data.sql.service.DbDataService
import ru.citeck.ecos.data.sql.service.DbIdMappingService

class DbAuthorityService(
    dataService: DbDataService<DbAuthorityEntity>
) : DbIdMappingService<DbAuthorityEntity>(
    dataService
)
