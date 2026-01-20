package ru.citeck.ecos.data.sql.remote

import ru.citeck.ecos.commons.data.ObjectData
import ru.citeck.ecos.context.lib.auth.AuthContext
import ru.citeck.ecos.data.sql.context.DbSchemaContext
import ru.citeck.ecos.data.sql.context.DbTableContext
import ru.citeck.ecos.data.sql.records.DbRecordsDao
import ru.citeck.ecos.data.sql.records.assocs.DbAssocRefsDiff
import ru.citeck.ecos.data.sql.remote.action.DeleteRemoteAssocsAction
import ru.citeck.ecos.data.sql.remote.action.MigrateRecordRefAction
import ru.citeck.ecos.data.sql.remote.action.UpdateRemoteAssocsAction
import ru.citeck.ecos.data.sql.remote.api.DbExecRemoteActionsWebExecutor
import ru.citeck.ecos.records3.RecordsService
import ru.citeck.ecos.webapp.api.EcosWebAppApi
import ru.citeck.ecos.webapp.api.entity.EntityRef
import ru.citeck.ecos.webapp.api.web.client.EcosWebClientApi

class DbRecordsRemoteActionsClientImpl : DbRecordsRemoteActionsClient {

    private lateinit var remoteActionsService: DbRecordsRemoteActionsService
    private lateinit var recordsService: RecordsService
    private lateinit var webClientApi: EcosWebClientApi
    private var currentAppName = ""

    fun init(
        webAppApi: EcosWebAppApi,
        remoteActionsService: DbRecordsRemoteActionsService,
        recordsService: RecordsService
    ) {
        this.currentAppName = webAppApi.getProperties().appName
        this.webClientApi = webAppApi.getWebClientApi()
        this.remoteActionsService = remoteActionsService
        this.recordsService = recordsService
    }

    override fun migrateRemoteRef(
        targetApp: String,
        fromRef: EntityRef,
        toRef: EntityRef,
        migratedBy: String
    ) {
        execRemoteAction(
            appName = targetApp,
            actionType = MigrateRecordRefAction.TYPE,
            config = MigrateRecordRefAction.Params(
                fromRef = fromRef,
                toRef = toRef,
                migratedBy = migratedBy
            )
        )
    }

    override fun updateRemoteAssocs(
        currentCtx: DbTableContext,
        sourceRef: EntityRef,
        creator: String,
        assocsDiff: List<DbAssocRefsDiff>
    ) {

        val assocsToUpdateByApp = HashMap<String, AppAssocsToUpdate>()
        val schemaCtxBySrcId = HashMap<String, DbSchemaContext?>()

        for (assocDiff in assocsDiff) {

            val remoteToAdd = groupByRemoteAppName(currentCtx, assocDiff.added, schemaCtxBySrcId)
            val remoteToRem = groupByRemoteAppName(currentCtx, assocDiff.removed, schemaCtxBySrcId)

            if (remoteToAdd.isEmpty() && remoteToRem.isEmpty()) {
                continue
            }

            for ((appName, refsToAdd) in remoteToAdd) {
                assocsToUpdateByApp.computeIfAbsent(appName) { AppAssocsToUpdate() }.toAdd.add(
                    UpdateRemoteAssocsAction.AssocsDiff(assocDiff.assocId, refsToAdd)
                )
            }
            for ((appName, refsToRem) in remoteToRem) {
                assocsToUpdateByApp.computeIfAbsent(appName) { AppAssocsToUpdate() }.toRem.add(
                    UpdateRemoteAssocsAction.AssocsDiff(assocDiff.assocId, refsToRem)
                )
            }
        }

        for ((appName, assocsToUpdate) in assocsToUpdateByApp) {

            if (assocsToUpdate.toAdd.isEmpty() && assocsToUpdate.toRem.isEmpty()) {
                continue
            }

            val action = UpdateRemoteAssocsAction.Params(
                sourceRef,
                creator,
                assocsToUpdate.toAdd,
                assocsToUpdate.toRem
            )

            if (appName == currentAppName) {
                remoteActionsService.execute(UpdateRemoteAssocsAction.TYPE, action)
            } else {
                execRemoteAction(appName, UpdateRemoteAssocsAction.TYPE, action)
            }
        }
    }

    override fun deleteRemoteAssocs(currentCtx: DbTableContext, sourceRef: EntityRef, force: Boolean) {
        val srcRefId = currentCtx.getRecordRefsService().getIdByEntityRef(sourceRef)
        if (srcRefId == -1L) {
            return
        }
        val recordsSourceIds = currentCtx.getAssocsService().findNonChildrenTargetRecsSrcIds(srcRefId)
        if (recordsSourceIds.isEmpty()) {
            return
        }
        val recordsSourceRefs = recordsSourceIds.mapNotNull {
            val delimIdx = it.indexOf('/')
            if (delimIdx > 0) {
                EntityRef.create(it.substring(0, delimIdx), it.substring(delimIdx + 1), "")
            } else {
                null
            }
        }
        val groups = groupByRemoteAppName(currentCtx, recordsSourceRefs, HashMap())
        if (groups.isEmpty()) {
            return
        }

        for ((appName, srcRefs) in groups) {

            val action = DeleteRemoteAssocsAction.Params(sourceRef, srcRefs.map { it.getSourceId() }, force)

            if (appName == currentAppName) {
                remoteActionsService.execute(DeleteRemoteAssocsAction.TYPE, action)
            } else {
                execRemoteAction(appName, DeleteRemoteAssocsAction.TYPE, action)
            }
        }
    }

    private fun execRemoteAction(appName: String, actionType: String, config: Any) {

        val version = webClientApi.getApiVersion(
            appName,
            DbExecRemoteActionsWebExecutor.PATH,
            0
        )
        when (version) {
            EcosWebClientApi.AV_PATH_NOT_SUPPORTED -> {
                // do nothing
            }

            EcosWebClientApi.AV_APP_NOT_AVAILABLE -> {
                error("App is not available: $appName")
            }

            EcosWebClientApi.AV_VERSION_NOT_SUPPORTED -> {
                error("Unsupported API version for path ${DbExecRemoteActionsWebExecutor.PATH}")
            }

            else -> {
                AuthContext.runAsSystem {
                    webClientApi.newRequest()
                        .targetApp(appName)
                        .path(DbExecRemoteActionsWebExecutor.PATH)
                        .version(version)
                        .body {
                            it.writeDto(
                                DbExecRemoteActionsWebExecutor.Request(
                                    actionType,
                                    ObjectData.create(config)
                                )
                            )
                        }.executeSync {}
                }
            }
        }
    }

    private fun groupByRemoteAppName(
        currentCtx: DbTableContext,
        refs: List<EntityRef>,
        schemaCtxBySrcId: MutableMap<String, DbSchemaContext?>
    ): Map<String, List<EntityRef>> {
        if (refs.isEmpty()) {
            return emptyMap()
        }
        val result = HashMap<String, MutableList<EntityRef>>()
        for (ref in refs) {
            val remoteAppName = getRemoteAppName(currentCtx, ref, schemaCtxBySrcId)
            if (remoteAppName.isBlank()) {
                continue
            }
            result.computeIfAbsent(remoteAppName) { ArrayList() }.add(ref)
        }
        return result
    }

    private fun getRemoteAppName(
        currentCtx: DbTableContext,
        ref: EntityRef,
        schemaCtxBySrcId: MutableMap<String, DbSchemaContext?>
    ): String {
        val fixedRef = ref.withDefaultAppName(currentAppName)
        return if (fixedRef.getAppName() == currentAppName) {
            val schemaCtx = schemaCtxBySrcId.computeIfAbsent(fixedRef.getSourceId()) { srcId ->
                recordsService.getRecordsDao(srcId, DbRecordsDao::class.java)
                    ?.getRecordsDaoCtx()
                    ?.dataService
                    ?.getTableContext()
                    ?.getSchemaCtx()
            } ?: return ""
            if (schemaCtx != currentCtx.getSchemaCtx()) {
                currentAppName
            } else {
                ""
            }
        } else {
            fixedRef.getAppName()
        }
    }

    private class AppAssocsToUpdate(
        val toAdd: MutableList<UpdateRemoteAssocsAction.AssocsDiff> = ArrayList(),
        val toRem: MutableList<UpdateRemoteAssocsAction.AssocsDiff> = ArrayList()
    )
}
