package ru.citeck.ecos.data.sql.context

import ru.citeck.ecos.data.sql.datasource.DbDataSource
import ru.citeck.ecos.data.sql.repo.DbEntityRepo
import ru.citeck.ecos.data.sql.schema.DbSchemaDao
import ru.citeck.ecos.data.sql.service.DbDataServiceFactory
import ru.citeck.ecos.data.sql.type.DbTypesConverter
import java.util.concurrent.ConcurrentHashMap

class DbDataSourceContext(
    val appName: String,
    val dataSource: DbDataSource,
    dataServiceFactory: DbDataServiceFactory
) {
    val converter: DbTypesConverter = DbTypesConverter()
    val entityRepo: DbEntityRepo = dataServiceFactory.createEntityRepo()
    val schemaDao: DbSchemaDao = dataServiceFactory.createSchemaDao()

    private val schemasByName = ConcurrentHashMap<String, DbSchemaContext>()

    init {
        dataServiceFactory.registerConverters(converter)
    }

    fun getSchemaContext(schema: String): DbSchemaContext {
        return schemasByName.computeIfAbsent(schema) { k -> DbSchemaContext(k, this) }
    }
}
