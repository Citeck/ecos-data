package ru.citeck.ecos.data.sql.domain.migration

import ru.citeck.ecos.data.sql.context.DbSchemaContext
import ru.citeck.ecos.data.sql.service.DbDataService

class DbDomainMigrationContext(
    val dataService: DbDataService<*>,
    val schemaContext: DbSchemaContext
)
