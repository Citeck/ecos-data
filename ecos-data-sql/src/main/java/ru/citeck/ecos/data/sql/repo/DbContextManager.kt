package ru.citeck.ecos.data.sql.repo

interface DbContextManager {

    fun getCurrentTenant(): String

    fun getCurrentUser(): String

    fun getCurrentUserAuthorities(): List<String>
}
