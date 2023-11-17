package ru.citeck.ecos.data.sql.remote.api

import ru.citeck.ecos.commons.data.DataValue
import ru.citeck.ecos.commons.data.ObjectData
import ru.citeck.ecos.context.lib.auth.AuthContext
import ru.citeck.ecos.data.sql.remote.DbRecordsRemoteActionsService
import ru.citeck.ecos.webapp.api.web.executor.EcosWebExecutor
import ru.citeck.ecos.webapp.api.web.executor.EcosWebExecutorReq
import ru.citeck.ecos.webapp.api.web.executor.EcosWebExecutorResp

class DbExecRemoteActionsWebExecutor(
    private val service: DbRecordsRemoteActionsService
) : EcosWebExecutor {

    companion object {
        const val PATH = "/ecos-data/exec-remote-action"
    }

    override fun execute(request: EcosWebExecutorReq, response: EcosWebExecutorResp) {
        if (AuthContext.isNotRunAsSystem()) {
            error("Permission denied")
        }
        val reqData = request.getBodyReader().readDto(Request::class.java)
        val result = service.execute(reqData.actionType, reqData.config)
        response.getBodyWriter().writeDto(Response(result))
    }

    override fun getPath(): String {
        return PATH
    }

    override fun isReadOnly(): Boolean {
        return false
    }

    class Request(
        val actionType: String,
        val config: ObjectData
    )

    class Response(
        val result: DataValue
    )
}
