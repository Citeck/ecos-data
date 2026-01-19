package ru.citeck.ecos.data.sql.remote.action

import ru.citeck.ecos.data.sql.context.DbDataSourceContext
import ru.citeck.ecos.records3.RecordsService
import ru.citeck.ecos.webapp.api.EcosWebAppApi
import ru.citeck.ecos.webapp.api.entity.EntityRef

class MigrateRecordRefAction : DbRemoteActionExecutor<MigrateRecordRefAction.Params> {

    companion object {
        const val TYPE = "migrate-record-ref"
    }

    private lateinit var webAppApi: EcosWebAppApi
    private lateinit var recordsService: RecordsService
    private lateinit var dbDataSourceContext: DbDataSourceContext

    private var currentAppName = ""

    override fun init(
        dbDataSourceContext: DbDataSourceContext,
        webAppApi: EcosWebAppApi,
        recordsService: RecordsService
    ) {
        this.webAppApi = webAppApi
        this.recordsService = recordsService
        this.dbDataSourceContext = dbDataSourceContext
        currentAppName = webAppApi.getProperties().appName
    }

    override fun execute(action: Params): Response {
        val schemasRes = mutableMapOf<String, Boolean>()
        dbDataSourceContext.forEachSchema { name, context ->
            val userRef = context.getUserRef(action.migratedBy)
            val migratedByUserId = context.recordRefService.getOrCreateIdByEntityRef(userRef)
            val migrationRes = context.recordRefService.migrateRefIfExists(
                fromRef = action.fromRef,
                toRef = action.toRef,
                migratedBy = migratedByUserId
            )
            schemasRes[name] = migrationRes
        }
        return Response(schemasRes)
    }

    override fun getType(): String {
        return TYPE
    }

    class Response(
        val resultBySchema: Map<String, Boolean>
    )

    class Params(
        val fromRef: EntityRef,
        val toRef: EntityRef,
        val migratedBy: String
    )
}
