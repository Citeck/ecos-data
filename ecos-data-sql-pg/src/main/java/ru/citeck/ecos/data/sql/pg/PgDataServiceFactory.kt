package ru.citeck.ecos.data.sql.pg

import org.postgresql.jdbc.PgArray
import org.postgresql.util.PGobject
import ru.citeck.ecos.data.sql.datasource.DbDataSource
import ru.citeck.ecos.data.sql.dto.DbTableRef
import ru.citeck.ecos.data.sql.repo.DbEntityRepo
import ru.citeck.ecos.data.sql.repo.DbEntityRepoConfig
import ru.citeck.ecos.data.sql.repo.entity.DbEntityMapper
import ru.citeck.ecos.data.sql.schema.DbSchemaDao
import ru.citeck.ecos.data.sql.service.DbDataServiceFactory
import ru.citeck.ecos.data.sql.type.DbTypesConverter

class PgDataServiceFactory : DbDataServiceFactory {

    override fun registerConverters(typesConverter: DbTypesConverter) {
        typesConverter.register(PgArray::class) { it.array }
        typesConverter.register(PGobject::class) { it.value }
    }

    override fun createSchemaDao(tableRef: DbTableRef, dataSource: DbDataSource): DbSchemaDao {
        return DbSchemaDaoPg(dataSource, tableRef)
    }

    override fun <T : Any> createEntityRepo(
        tableRef: DbTableRef,
        dataSource: DbDataSource,
        entityMapper: DbEntityMapper<T>,
        typesConverter: DbTypesConverter,
        config: DbEntityRepoConfig
    ): DbEntityRepo<T> {

        return DbEntityRepoPg(entityMapper, dataSource, tableRef, typesConverter, config)
    }
}
