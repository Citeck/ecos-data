package ru.citeck.ecos.data.sql.records.perms

interface DbRecordPerms {

    fun getAuthoritiesWithReadPermission(): Set<String>

    fun hasReadPerms(): Boolean

    fun hasWritePerms(): Boolean

    fun hasAttWritePerms(name: String): Boolean

    fun hasAttReadPerms(name: String): Boolean
}
