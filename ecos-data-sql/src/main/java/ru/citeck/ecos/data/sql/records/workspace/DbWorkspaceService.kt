package ru.citeck.ecos.data.sql.records.workspace

import ru.citeck.ecos.data.sql.context.DbSchemaContext
import ru.citeck.ecos.data.sql.service.DbDataServiceConfig
import ru.citeck.ecos.data.sql.service.DbDataServiceImpl
import ru.citeck.ecos.data.sql.service.DbIdMappingService

class DbWorkspaceService(
    schemaCtx: DbSchemaContext
) : DbIdMappingService<DbWorkspaceEntity>(
    DbDataServiceImpl(
        DbWorkspaceEntity::class.java,
        DbDataServiceConfig.create {
            withTable(DbWorkspaceEntity.TABLE)
        },
        schemaCtx
    )
) {

    fun getWorkspaceExtIdById(id: Long): String {
        return getExtIdById(id)
    }

    fun getIdsForExistingWsInAnyOrder(workspaces: Collection<String>): List<Long> {
        return getExistingIdsInAnyOrder(workspaces) { it.isNotBlank() }
    }
}
