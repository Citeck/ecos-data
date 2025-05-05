package ru.citeck.ecos.data.sql.records.workspace

import ru.citeck.ecos.webapp.api.constants.AppName
import ru.citeck.ecos.webapp.api.entity.EntityRef

object DbWorkspaceDesc {

    private const val SOURCE_ID = "workspace"

    fun getRef(workspaceId: String): EntityRef {
        val srcIdDelimIdx = workspaceId.indexOf('@')
        val wsLocalId = if (srcIdDelimIdx == -1) {
            workspaceId
        } else {
            workspaceId.substring(srcIdDelimIdx + 1)
        }
        return EntityRef.create(AppName.EMODEL, SOURCE_ID, wsLocalId)
    }

    fun isWorkspaceRef(ref: EntityRef): Boolean {
        return ref.getAppName() == AppName.EMODEL && ref.getSourceId() == SOURCE_ID
    }
}
