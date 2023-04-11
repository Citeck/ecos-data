package ru.citeck.ecos.data.sql.records.perms

interface DbPermsComponent {

    fun getRecordPerms(record: Any): DbRecordPerms
}
