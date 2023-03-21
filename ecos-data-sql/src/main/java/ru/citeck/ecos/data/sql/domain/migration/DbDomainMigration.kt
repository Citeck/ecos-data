package ru.citeck.ecos.data.sql.domain.migration

interface DbDomainMigration {

    fun run(context: DbDomainMigrationContext)

    fun getAppliedVersions(): Pair<Int, Int>
}
