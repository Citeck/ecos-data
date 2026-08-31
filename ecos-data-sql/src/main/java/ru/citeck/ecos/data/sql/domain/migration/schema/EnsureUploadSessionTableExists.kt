package ru.citeck.ecos.data.sql.domain.migration.schema

import io.github.oshai.kotlinlogging.KotlinLogging
import ru.citeck.ecos.data.sql.context.DbSchemaContext

/**
 * Creates `ed_content_upload_session` for schemas that were initialized before chunked uploads
 * existed.
 *
 * Without it the table would only appear on the first `chunkedUploadInit` of a schema, i.e. DDL run
 * from inside a user request (and from inside a nested `requiresNew` transaction at that), with two
 * concurrent first inits racing over `CREATE TABLE` / `CREATE INDEX`. New schemas get the table from
 * [ru.citeck.ecos.data.sql.domain.migration.DbMigrationService.runSchemaMigrations]'s `version == 0`
 * branch, exactly like every other system table.
 */
class EnsureUploadSessionTableExists : DbSchemaMigration {

    companion object {
        private val log = KotlinLogging.logger {}
    }

    override fun run(context: DbSchemaContext) {
        log.info { "Ensuring upload session table exists for schema '${context.schema}'" }

        context.uploadSessionService.createTableIfNotExists()

        context.resetColumnsCache()
    }

    override fun getAppliedVersions(): Int {
        return 8
    }
}
