package ru.citeck.ecos.data.sql.records.workspace

import ru.citeck.ecos.data.sql.context.DbSchemaContext
import ru.citeck.ecos.data.sql.service.DbDataService
import ru.citeck.ecos.data.sql.service.DbDataServiceConfig
import ru.citeck.ecos.data.sql.service.DbDataServiceImpl
import ru.citeck.ecos.records2.predicate.model.Predicates

class DbWorkspaceService(
    schemaCtx: DbSchemaContext
) {

    private val dataService: DbDataService<DbWorkspaceEntity> = DbDataServiceImpl(
        DbWorkspaceEntity::class.java,
        DbDataServiceConfig.create {
            withTable(DbWorkspaceEntity.TABLE)
        },
        schemaCtx
    )

    fun getWorkspaceNameById(id: Long): String {
        return dataService.findById(id)?.extId ?: ""
    }

    fun getOrCreateId(workspace: String): Long {
        val entity = dataService.findByExtId(workspace)
        return if (entity != null) {
            entity.id
        } else {
            val newEntity = DbWorkspaceEntity()
            newEntity.extId = workspace
            dataService.save(newEntity).id
        }
    }

    fun getIdsForExistingWsInAnyOrder(workspaces: Collection<String>): List<Long> {
        return dataService.findAll(
            Predicates.inVals(
                DbWorkspaceEntity.EXT_ID,
                workspaces.filter { it.isNotBlank() }
            )
        ).map { it.id }
    }
}
