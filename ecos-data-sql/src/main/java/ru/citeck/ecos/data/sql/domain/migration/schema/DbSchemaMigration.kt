package ru.citeck.ecos.data.sql.domain.migration.schema

import ru.citeck.ecos.data.sql.context.DbSchemaContext

interface DbSchemaMigration {

    fun run(context: DbSchemaContext)

    fun getAppliedVersions(): Int
}
