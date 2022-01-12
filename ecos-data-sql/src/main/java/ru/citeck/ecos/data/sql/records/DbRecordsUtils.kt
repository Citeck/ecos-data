package ru.citeck.ecos.data.sql.records

import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType

object DbRecordsUtils {

    fun isChildAssocAttribute(def: AttributeDef?): Boolean {
        def ?: return false
        return def.type == AttributeType.ASSOC && def.config.get("child", false)
    }

    fun isAssocLikeAttribute(def: AttributeDef?): Boolean {
        return isAssocLikeAttribute(def?.type)
    }

    fun isAssocLikeAttribute(type: AttributeType?): Boolean {
        if (type == null) {
            return false
        }
        return type == AttributeType.ASSOC ||
            type == AttributeType.PERSON ||
            type == AttributeType.AUTHORITY_GROUP ||
            type == AttributeType.AUTHORITY
    }
}
