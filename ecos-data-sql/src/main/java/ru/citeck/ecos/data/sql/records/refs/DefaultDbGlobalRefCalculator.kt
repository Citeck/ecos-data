package ru.citeck.ecos.data.sql.records.refs

import ru.citeck.ecos.records3.record.request.RequestContext
import ru.citeck.ecos.records3.utils.RecordRefUtils
import ru.citeck.ecos.webapp.api.entity.EntityRef

class DefaultDbGlobalRefCalculator : DbGlobalRefCalculator {

    override fun getGlobalRef(appName: String, sourceId: String, extId: String): EntityRef {
        return RecordRefUtils.mapAppIdAndSourceId(
            EntityRef.create(appName, sourceId, extId),
            appName,
            RequestContext.getCurrent()?.ctxData?.sourceIdMapping
        )
    }
}
