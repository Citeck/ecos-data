package ru.citeck.ecos.data.sql.records.attnames

import ru.citeck.ecos.data.sql.context.DbSchemaContext
import ru.citeck.ecos.data.sql.service.DbDataServiceConfig
import ru.citeck.ecos.data.sql.service.DbDataServiceImpl
import ru.citeck.ecos.data.sql.service.DbIdMappingService

class DbEcosAttributesService(
    schemaCtx: DbSchemaContext
) : DbIdMappingService<DbEcosAttributeEntity>(
    DbDataServiceImpl(
        DbEcosAttributeEntity::class.java,
        DbDataServiceConfig.create {
            withTable(DbEcosAttributeEntity.TABLE)
        },
        schemaCtx
    )
) {

    fun getAttsByIds(ids: Collection<Long>): Map<Long, String> {
        return getExtIdsByIds(ids)
    }

    fun getIdsForAtts(attributes: Collection<String>, createIfNotExists: Boolean): Map<String, Long> {
        return if (createIfNotExists) {
            getOrCreateIds(attributes)
        } else {
            getIdsByExtIds(attributes)
        }
    }
}
