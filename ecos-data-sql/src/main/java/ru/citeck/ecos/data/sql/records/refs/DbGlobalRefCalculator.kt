package ru.citeck.ecos.data.sql.records.refs

import ru.citeck.ecos.webapp.api.entity.EntityRef

interface DbGlobalRefCalculator {

    fun getGlobalRef(appName: String, sourceId: String, extId: String): EntityRef
}
