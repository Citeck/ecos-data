package ru.citeck.ecos.data.sql.records.dao.atts

import ru.citeck.ecos.commons.data.MLText
import ru.citeck.ecos.data.sql.records.DbRecordsUtils
import ru.citeck.ecos.data.sql.records.dao.DbRecordsDaoCtx
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.status.constants.StatusConstants
import ru.citeck.ecos.records3.record.atts.value.AttEdge
import ru.citeck.ecos.records3.record.atts.value.AttValue
import ru.citeck.ecos.webapp.api.entity.EntityRef

class DbEmptyRecord(private val ctx: DbRecordsDaoCtx) : AttValue {

    private val attDefs: Map<String, AttributeDef> by lazy {
        val atts = mutableMapOf<String, AttributeDef>()
        val typeInfo = ctx.ecosTypeService.getTypeInfo(ctx.config.typeRef.getLocalId())
        if (typeInfo != null) {
            ctx.ecosTypeService.getAllAttributesForAspects(typeInfo.aspects.map { it.ref }).forEach { attDef ->
                atts[attDef.id] = attDef
            }
            typeInfo.model.getAllAttributes().forEach { attDef ->
                atts[attDef.id] = attDef
            }
        }
        atts
    }

    override fun getEdge(name: String): AttEdge {
        if (name == StatusConstants.ATT_STATUS) {
            return DbStatusEdge(ctx, ctx.config.typeRef)
        }
        return EmptyEdge(
            name,
            attDefs[name] ?: AttributeDef.create {
                withId(name)
                withName(MLText(name))
            }
        )
    }

    override fun getDisplayName(): Any {
        if (EntityRef.isEmpty(ctx.config.typeRef)) {
            return ""
        }
        val typeInfo = ctx.ecosTypeService.getTypeInfo(ctx.config.typeRef.getLocalId())
        return typeInfo?.name ?: ""
    }

    private inner class EmptyEdge(
        private val name: String,
        private val def: AttributeDef
    ) : AttEdge {

        override fun getName(): String {
            return name
        }

        override fun isMandatory(): Boolean {
            return def.mandatory
        }

        override fun isMultiple(): Boolean {
            return def.multiple
        }

        override fun isAssociation(): Boolean {
            return DbRecordsUtils.isAssocLikeAttribute(def)
        }

        override fun getTitle(): MLText {
            return def.name
        }
    }
}
