package ru.citeck.ecos.data.sql.records.dao.atts

import ru.citeck.ecos.commons.data.MLText
import ru.citeck.ecos.data.sql.records.DbRecordsUtils
import ru.citeck.ecos.data.sql.records.perms.DbRecordPermsContext
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType
import ru.citeck.ecos.records2.RecordConstants
import ru.citeck.ecos.records3.record.atts.value.AttEdge

class DbRecordAttEdge(
    private val rec: DbRecord,
    private val name: String,
    private val def: AttributeDef,
    private val perms: DbRecordPermsContext?
) : AttEdge {

    override fun getName(): String {
        return name
    }

    override fun getValue(): Any? {
        return rec.getAtt(name)
    }

    private fun getInnerName(): String {
        if (name == RecordConstants.ATT_CONTENT) {
            return rec.getDefaultContentAtt()
        }
        return name
    }

    override fun isProtected(): Boolean {
        return perms != null && !perms.hasAttWritePerms(getInnerName())
    }

    override fun isUnreadable(): Boolean {
        return perms != null && !perms.hasAttReadPerms(getInnerName())
    }

    override fun isMultiple(): Boolean {
        return def.multiple
    }

    override fun getOptions(): List<*>? {
        if (def.type == AttributeType.OPTIONS) {
            return rec.ctx.computedAttsComponent?.getAttOptions(rec, def.config)
        }
        return null
    }

    override fun getTitle(): MLText {
        return def.name
    }

    override fun isMandatory(): Boolean {
        return def.mandatory
    }

    override fun isAssociation(): Boolean {
        return DbRecordsUtils.isAssocLikeAttribute(def)
    }
}
