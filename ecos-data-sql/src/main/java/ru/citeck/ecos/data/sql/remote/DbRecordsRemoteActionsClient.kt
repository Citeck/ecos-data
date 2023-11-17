package ru.citeck.ecos.data.sql.remote

import ru.citeck.ecos.data.sql.context.DbTableContext
import ru.citeck.ecos.data.sql.records.assocs.DbAssocRefsDiff
import ru.citeck.ecos.webapp.api.entity.EntityRef

interface DbRecordsRemoteActionsClient {

    fun updateRemoteAssocs(
        currentCtx: DbTableContext,
        sourceRef: EntityRef,
        creator: String,
        assocsDiff: List<DbAssocRefsDiff>
    )

    fun deleteRemoteAssocs(
        currentCtx: DbTableContext,
        sourceRef: EntityRef,
        force: Boolean
    )
}
