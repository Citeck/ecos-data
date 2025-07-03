package ru.citeck.ecos.data.sql.records.dao.atts.stage

import ru.citeck.ecos.data.sql.records.dao.DbRecordsDaoCtx
import ru.citeck.ecos.records3.record.atts.value.AttEdge
import ru.citeck.ecos.records3.record.atts.value.impl.EmptyAttValue
import ru.citeck.ecos.webapp.api.entity.EntityRef

class DbStageEdge(private val ctx: DbRecordsDaoCtx, val type: EntityRef) : AttEdge {

    override fun isMultiple() = false

    override fun getOptions(): List<Any> {
        if (type.getLocalId().isBlank()) {
            return emptyList()
        }
        val typeInfo = ctx.ecosTypeService.getTypeInfo(type.getLocalId()) ?: return emptyList()
        return typeInfo.model.stages.map {
            val attValue = ctx.attValuesConverter.toAttValue(it) ?: EmptyAttValue.INSTANCE
            DbStageValue(it, attValue)
        }
    }
}
