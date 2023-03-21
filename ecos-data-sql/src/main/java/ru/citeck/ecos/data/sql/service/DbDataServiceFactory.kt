package ru.citeck.ecos.data.sql.service

import ru.citeck.ecos.data.sql.repo.DbEntityRepo
import ru.citeck.ecos.data.sql.schema.DbSchemaDao
import ru.citeck.ecos.data.sql.type.DbTypesConverter

interface DbDataServiceFactory {

    fun registerConverters(typesConverter: DbTypesConverter)

    fun createSchemaDao(): DbSchemaDao

    fun createEntityRepo(): DbEntityRepo
}
