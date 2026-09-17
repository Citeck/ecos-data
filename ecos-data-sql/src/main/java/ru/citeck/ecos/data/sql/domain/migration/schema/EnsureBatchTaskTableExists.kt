package ru.citeck.ecos.data.sql.domain.migration.schema

import io.github.oshai.kotlinlogging.KotlinLogging
import ru.citeck.ecos.data.sql.context.DbSchemaContext

/**
 * Creates `ed_batch_task` for schemas initialized before the batch engine existed.
 *
 * Same reasoning as [EnsureColumnMetaTableExists]: without it the table would first appear from
 * inside a user's mutation, i.e. DDL issued in a user request, with concurrent first mutations
 * racing over `CREATE TABLE`. Brand-new schemas get it from the `version == 0` branch of
 * [ru.citeck.ecos.data.sql.domain.migration.DbMigrationService.runSchemaMigrations] instead.
 */
class EnsureBatchTaskTableExists : DbSchemaMigration {

    companion object {
        private val log = KotlinLogging.logger {}
    }

    override fun run(context: DbSchemaContext) {
        log.info { "Ensuring batch task table exists for schema '${context.schema}'" }

        context.batchTaskService.createTableIfNotExists()

        context.resetColumnsCache()
    }

    override fun getAppliedVersions(): Int {
        return 10
    }
}
