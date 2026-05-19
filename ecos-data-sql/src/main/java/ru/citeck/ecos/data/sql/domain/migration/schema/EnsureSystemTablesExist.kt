package ru.citeck.ecos.data.sql.domain.migration.schema

import io.github.oshai.kotlinlogging.KotlinLogging
import ru.citeck.ecos.data.sql.context.DbSchemaContext

/**
 * Backfill creation of supporting tables (workspace, authority, etc.)
 * for schemas that were initialized before these services existed.
 *
 * Why: previously these tables were created only inside the
 * `if (version == 0)` branch of [ru.citeck.ecos.data.sql.domain.migration.DbMigrationService.runSchemaMigrations].
 * Schemas migrated past version 0 by an older ecos-data never reached that branch,
 * so writes to e.g. `ed_workspace` produced malformed SQL on an absent table.
 */
class EnsureSystemTablesExist : DbSchemaMigration {

    companion object {
        private val log = KotlinLogging.logger {}
    }

    override fun run(context: DbSchemaContext) {
        log.info { "Ensuring system tables exist for schema '${context.schema}'" }

        context.recordRefService.createTableIfNotExists()
        context.authorityService.createTableIfNotExists()
        context.attributesService.createTableIfNotExists()
        context.workspaceService.createTableIfNotExists()
        context.assocsService.createTableIfNotExists()
        context.trashcanService.createTableIfNotExists()

        context.resetColumnsCache()
    }

    override fun getAppliedVersions(): Int {
        return 7
    }
}
