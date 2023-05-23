package ru.citeck.ecos.data.sql.domain.migration.domain

import ru.citeck.ecos.data.sql.domain.migration.DbDomainMigrationContext

interface DbDomainMigration {

    fun run(context: DbDomainMigrationContext)

    fun getAppliedVersions(): Int
}
