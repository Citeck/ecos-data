package ru.citeck.ecos.data.sql.domain.migration.schema

import io.github.oshai.kotlinlogging.KotlinLogging
import ru.citeck.ecos.data.sql.context.DbSchemaContext

/**
 * Creates `ed_associations_backup` for schemas initialized before the association backup existed.
 *
 * Same reasoning as [EnsureBatchTaskTableExists]: without it the table would first appear from
 * inside the first attribute to leave the association group - DDL issued while a user's mutation
 * waits, and on the one path whose whole purpose is that the links are safe before they are
 * removed. Brand-new schemas get it from the `version == 0` branch of
 * [ru.citeck.ecos.data.sql.domain.migration.DbMigrationService.runSchemaMigrations] instead.
 */
class EnsureAssocBackupTableExists : DbSchemaMigration {

    companion object {
        private val log = KotlinLogging.logger {}
    }

    override fun run(context: DbSchemaContext) {
        log.info { "Ensuring assoc backup table exists for schema '${context.schema}'" }

        context.assocBackupService.createTableIfNotExists()

        context.resetColumnsCache()
    }

    override fun getAppliedVersions(): Int {
        return 11
    }
}
