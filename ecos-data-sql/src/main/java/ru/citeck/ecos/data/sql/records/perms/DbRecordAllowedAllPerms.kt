package ru.citeck.ecos.data.sql.records.perms

import ru.citeck.ecos.context.lib.auth.AuthGroup
import ru.citeck.ecos.data.sql.records.dao.atts.DbRecPermsValue

object DbRecordAllowedAllPerms : DbRecordPerms {

    private val AUTHORITIES_WITH_READ_PERMS = setOf(AuthGroup.EVERYONE)
    private val ALLOWED_ALL_PERMS = setOf(DbRecPermsValue.PERMS_ALLOWED_ALL)

    override fun getAuthoritiesWithReadPermission(): Set<String> {
        return AUTHORITIES_WITH_READ_PERMS
    }

    override fun isAllowed(permission: String) = true
    override fun getAllowedPermissions() = ALLOWED_ALL_PERMS
    override fun hasReadPerms() = true
    override fun hasWritePerms() = true
    override fun hasAttWritePerms(name: String) = true
    override fun hasAttReadPerms(name: String) = true
}
