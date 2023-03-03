package ru.citeck.ecos.data.sql.records.perms

import ru.citeck.ecos.context.lib.auth.AuthGroup

object DbRecordAllowedAllPerms : DbRecordPerms {

    private val AUTHORITIES_WITH_READ_PERMS = setOf(AuthGroup.EVERYONE)

    override fun getAuthoritiesWithReadPermission(): Set<String> {
        return AUTHORITIES_WITH_READ_PERMS
    }
    override fun isCurrentUserHasWritePerms() = true
    override fun isCurrentUserHasAttWritePerms(name: String) = true
    override fun isCurrentUserHasAttReadPerms(name: String) = true
}
