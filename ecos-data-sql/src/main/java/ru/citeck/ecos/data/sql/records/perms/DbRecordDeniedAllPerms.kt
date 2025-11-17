package ru.citeck.ecos.data.sql.records.perms

object DbRecordDeniedAllPerms : DbRecordPerms {

    override fun getAuthoritiesWithReadPermission() = emptySet<String>()
    override fun getAdditionalPerms() = emptySet<String>()
    override fun hasReadPerms() = false
    override fun hasWritePerms() = false
    override fun hasAttWritePerms(name: String) = false
    override fun hasAttReadPerms(name: String) = false
}
