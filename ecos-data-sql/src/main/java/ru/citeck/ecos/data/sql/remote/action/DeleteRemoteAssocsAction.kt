package ru.citeck.ecos.data.sql.remote.action

import ru.citeck.ecos.commons.data.DataValue
import ru.citeck.ecos.data.sql.context.DbSchemaContext
import ru.citeck.ecos.data.sql.records.DbRecordsDao
import ru.citeck.ecos.records3.RecordsService
import ru.citeck.ecos.webapp.api.EcosWebAppApi
import ru.citeck.ecos.webapp.api.entity.EntityRef

class DeleteRemoteAssocsAction : DbRemoteActionExecutor<DeleteRemoteAssocsAction.Params> {

    companion object {
        const val TYPE = "delete-remote-assocs"
    }

    private lateinit var webAppApi: EcosWebAppApi
    private lateinit var recordsService: RecordsService

    private var currentAppName = ""

    override fun init(webAppApi: EcosWebAppApi, recordsService: RecordsService) {
        this.webAppApi = webAppApi
        this.recordsService = recordsService
        currentAppName = webAppApi.getProperties().appName
    }

    override fun execute(action: Params): DataValue {

        val processedSchemaContexts = HashSet<DbSchemaContext>()

        for (sourceId in action.sourceIds) {

            val dao = recordsService.getRecordsDao(sourceId, DbRecordsDao::class.java) ?: continue

            val schemaCtx = dao.getRecordsDaoCtx().tableCtx.getSchemaCtx()
            if (!processedSchemaContexts.add(schemaCtx)) {
                continue
            }

            val srcRefId = dao.getRecordsDaoCtx().recordRefService.getIdByEntityRef(action.srcRef)
            if (srcRefId == -1L) {
                continue
            }

            dao.getRecordsDaoCtx().assocsService.removeAssocs(srcRefId, action.force)
        }

        return DataValue.createObj()
    }

    override fun getType(): String {
        return TYPE
    }

    class Params(
        val srcRef: EntityRef,
        val sourceIds: List<String>,
        val force: Boolean
    )
}
