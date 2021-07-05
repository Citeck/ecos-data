package ru.citeck.ecos.data.sql.repo

interface DbContextManager {

    fun getCurrentUser(): String

    fun getCurrentUserAuthorities(): List<String>
}
