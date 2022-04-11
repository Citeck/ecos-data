package ru.citeck.ecos.data.sql.service.migration

interface DbMigration<T : Any, C : Any> {

    fun run(service: DbMigrationService<T>, mock: Boolean, config: C)

    fun getType(): String
}
