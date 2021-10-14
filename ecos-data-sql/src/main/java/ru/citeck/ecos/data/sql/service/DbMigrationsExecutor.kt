package ru.citeck.ecos.data.sql.service

interface DbMigrationsExecutor {

    fun runMigrations(mock: Boolean, diff: Boolean): List<String>
}
