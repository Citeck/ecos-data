package ru.citeck.ecos.data.sql.records.dao.atts

import ru.citeck.ecos.data.sql.records.dao.DbRecordsDaoCtx
import ru.citeck.ecos.records2.RecordRef
import ru.citeck.ecos.records3.record.atts.value.AttEdge

class DbStatusEdge(private val ctx: DbRecordsDaoCtx, val type: RecordRef) : AttEdge {

    override fun isMultiple() = false

    override fun getOptions(): List<Any> {
        if (type.id.isBlank()) {
            return emptyList()
        }
        val typeInfo = ctx.ecosTypeService.getTypeInfo(type.id) ?: return emptyList()
        return typeInfo.model.statuses.map { DbStatusValue(it) }
    }
}
