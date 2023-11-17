package ru.citeck.ecos.data.sql.remote

import ru.citeck.ecos.commons.data.DataValue

interface DbRecordsRemoteActionsService {

    fun execute(actionType: String, config: Any): DataValue
}
