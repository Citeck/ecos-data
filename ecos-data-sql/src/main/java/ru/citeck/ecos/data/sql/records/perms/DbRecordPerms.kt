package ru.citeck.ecos.data.sql.records.perms

interface DbRecordPerms {

    fun getAuthoritiesWithReadPermission(): Set<String>

    fun isCurrentUserHasWritePerms(): Boolean
}
