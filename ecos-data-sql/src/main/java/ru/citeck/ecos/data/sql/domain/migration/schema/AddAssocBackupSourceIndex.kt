package ru.citeck.ecos.data.sql.domain.migration.schema

import io.github.oshai.kotlinlogging.KotlinLogging
import ru.citeck.ecos.data.sql.context.DbSchemaContext

/**
 * Gives `ed_associations_backup` an index on `__source_id` in schemas created before there was one.
 *
 * Every deletion of a record asks that table two questions keyed on the source alone - which of my
 * children have their links parked, and drop everything I parked - and both indexes the table was
 * created with in `1.73.0` lead on `__column_meta_id`, which neither question names. Without this
 * step every delete on the installation reads the whole backup, and the backup is as large as the
 * type changes an administrator has made.
 *
 * A step of its own rather than an entity change alone, because schema reconciliation builds
 * indexes for **new columns** only: an index added to an entity whose table already exists reaches
 * nobody. Brand-new schemas get it from [DbAssocBackupEntity][
 * ru.citeck.ecos.data.sql.records.assocs.DbAssocBackupEntity]'s `@Indexes` instead, through the
 * `version == 0` branch of
 * [ru.citeck.ecos.data.sql.domain.migration.DbMigrationService.runSchemaMigrations].
 */
class AddAssocBackupSourceIndex : DbSchemaMigration {

    companion object {
        private val log = KotlinLogging.logger {}
    }

    override fun run(context: DbSchemaContext) {
        log.info { "Ensuring assoc backup source index exists for schema '${context.schema}'" }

        context.assocBackupService.createSourceIndexIfMissing()

        context.resetColumnsCache()
    }

    override fun getAppliedVersions(): Int {
        return 12
    }
}
