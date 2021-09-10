package ru.citeck.ecos.data.sql.service

import ru.citeck.ecos.data.sql.datasource.DbDataSource
import ru.citeck.ecos.data.sql.dto.DbTableRef
import ru.citeck.ecos.data.sql.repo.DbEntityRepo
import ru.citeck.ecos.data.sql.repo.DbEntityRepoConfig
import ru.citeck.ecos.data.sql.repo.entity.DbEntityMapper
import ru.citeck.ecos.data.sql.schema.DbSchemaDao
import ru.citeck.ecos.data.sql.type.DbTypesConverter

interface DbDataServiceFactory {

    fun registerConverters(typesConverter: DbTypesConverter)

    fun createSchemaDao(tableRef: DbTableRef, dataSource: DbDataSource): DbSchemaDao

    fun <T : Any> createEntityRepo(
        tableRef: DbTableRef,
        dataSource: DbDataSource,
        entityMapper: DbEntityMapper<T>,
        typesConverter: DbTypesConverter,
        config: DbEntityRepoConfig
    ): DbEntityRepo<T>
}
