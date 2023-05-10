package ru.citeck.ecos.data.sql.records.perms

interface DbPermsComponent {

    fun getRecordPerms(user: String, authorities: Set<String>, record: Any): DbRecordPerms
}
