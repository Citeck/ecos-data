package ru.citeck.ecos.data.sql.domain.migration.schema

import io.github.oshai.kotlinlogging.KotlinLogging
import ru.citeck.ecos.data.sql.context.DbSchemaContext

/**
 * Creates `ed_column_meta` for schemas initialized before the column registry existed.
 *
 * Without it the table would first appear from inside a user's mutation, i.e. DDL issued in a user
 * request, with two concurrent first mutations racing over `CREATE TABLE`. New schemas get it from
 * [ru.citeck.ecos.data.sql.domain.migration.DbMigrationService.runSchemaMigrations]'s `version == 0`
 * branch, exactly like every other system table.
 */
class EnsureColumnMetaTableExists : DbSchemaMigration {

    companion object {
        private val log = KotlinLogging.logger {}
    }

    override fun run(context: DbSchemaContext) {
        log.info { "Ensuring column meta table exists for schema '${context.schema}'" }

        context.columnMetaService.createTableIfNotExists()

        context.resetColumnsCache()
    }

    override fun getAppliedVersions(): Int {
        return 9
    }
}
