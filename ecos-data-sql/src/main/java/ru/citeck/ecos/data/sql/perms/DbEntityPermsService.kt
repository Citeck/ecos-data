package ru.citeck.ecos.data.sql.perms

interface DbEntityPermsService {

    fun setReadPerms(permissions: List<DbEntityPermsDto>)

    fun createTableIfNotExists()

    fun isTableExists(): Boolean
}
