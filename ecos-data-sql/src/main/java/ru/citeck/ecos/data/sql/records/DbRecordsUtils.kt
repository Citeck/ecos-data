package ru.citeck.ecos.data.sql.records

import ru.citeck.ecos.context.lib.auth.AuthContext
import ru.citeck.ecos.context.lib.auth.AuthGroup
import ru.citeck.ecos.context.lib.auth.data.AuthData
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType
import ru.citeck.ecos.model.lib.role.constants.RoleConstants

object DbRecordsUtils {

    fun getCurrentAuthorities(): Set<String> {
        return getCurrentAuthorities(AuthContext.getCurrentRunAsAuth())
    }

    fun getCurrentAuthorities(auth: AuthData): Set<String> {
        if (auth.getUser().isBlank()) {
            return emptySet()
        }
        val authorities = linkedSetOf(auth.getUser())
        authorities.addAll(auth.getAuthorities())

        if (authorities.isEmpty()) {
            return emptySet()
        }

        // legacy "everyone" authority
        authorities.add(RoleConstants.ROLE_EVERYONE)
        authorities.add(AuthGroup.EVERYONE)

        return authorities
    }

    fun isChildAssocAttribute(def: AttributeDef?): Boolean {
        def ?: return false
        return def.type == AttributeType.ASSOC && def.config.get("child", false)
    }

    fun isAssocLikeAttribute(def: AttributeDef?): Boolean {
        return isAssocLikeAttribute(def?.type)
    }

    fun isEntityRefAttribute(type: AttributeType?): Boolean {
        type ?: return false
        // EntityRef attribute type may be added in future
        // if (type == AttributeType.ENTITY_REF) {
        //     return true
        // }
        return isAssocLikeAttribute(type)
    }

    fun isAssocLikeAttribute(type: AttributeType?): Boolean {
        type ?: return false
        return type == AttributeType.ASSOC ||
            type == AttributeType.PERSON ||
            type == AttributeType.AUTHORITY_GROUP ||
            type == AttributeType.AUTHORITY
    }
}
