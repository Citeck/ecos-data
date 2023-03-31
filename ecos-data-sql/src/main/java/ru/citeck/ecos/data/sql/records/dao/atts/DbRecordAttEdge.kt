package ru.citeck.ecos.data.sql.records.dao.atts

import ru.citeck.ecos.commons.data.MLText
import ru.citeck.ecos.data.sql.records.DbRecordsUtils
import ru.citeck.ecos.data.sql.records.perms.DbRecordPerms
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.records2.RecordConstants
import ru.citeck.ecos.records3.record.atts.value.AttEdge

class DbRecordAttEdge(
    private val rec: DbRecord,
    private val name: String,
    private val def: AttributeDef,
    private val perms: DbRecordPerms?
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
        return perms != null && !perms.isCurrentUserHasAttWritePerms(getInnerName())
    }

    override fun isUnreadable(): Boolean {
        return perms != null && !perms.isCurrentUserHasAttReadPerms(getInnerName())
    }

    override fun isMultiple(): Boolean {
        return def.multiple
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
