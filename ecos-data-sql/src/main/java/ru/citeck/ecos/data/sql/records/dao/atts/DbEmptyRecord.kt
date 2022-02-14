package ru.citeck.ecos.data.sql.records.dao.atts

import ru.citeck.ecos.data.sql.records.dao.DbRecordsDaoCtx
import ru.citeck.ecos.model.lib.status.constants.StatusConstants
import ru.citeck.ecos.records2.RecordRef
import ru.citeck.ecos.records3.record.atts.value.AttEdge
import ru.citeck.ecos.records3.record.atts.value.AttValue

class DbEmptyRecord(private val ctx: DbRecordsDaoCtx) : AttValue {

    override fun getEdge(name: String?): AttEdge? {
        if (name == StatusConstants.ATT_STATUS) {
            return DbStatusEdge(ctx, ctx.config.typeRef)
        }
        return super.getEdge(name)
    }

    override fun getDisplayName(): Any {
        if (RecordRef.isEmpty(ctx.config.typeRef)) {
            return ""
        }
        val typeInfo = ctx.ecosTypeService.getTypeInfo(ctx.config.typeRef.id)
        return typeInfo?.name ?: ""
    }
}
