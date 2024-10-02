package ru.citeck.ecos.data.sql.domain.migration

import io.github.oshai.kotlinlogging.KotlinLogging
import ru.citeck.ecos.context.lib.auth.AuthContext
import ru.citeck.ecos.data.sql.context.DbSchemaContext
import ru.citeck.ecos.data.sql.domain.migration.domain.*
import ru.citeck.ecos.data.sql.domain.migration.schema.*
import ru.citeck.ecos.data.sql.meta.schema.DbSchemaMetaEntity
import ru.citeck.ecos.data.sql.service.DbDataService
import ru.citeck.ecos.txn.lib.TxnContext
import java.util.concurrent.atomic.AtomicBoolean

class DbMigrationService {

    companion object {
        private val log = KotlinLogging.logger {}
    }

    private val domainMigrations = ArrayList<DbDomainMigration>()
    private val schemaMigrations = ArrayList<DbSchemaMigration>()

    init {
        domainMigrations.add(MovePermsToSchemaTable())
        domainMigrations.add(MoveAssocsToAssocsTable())
        domainMigrations.add(MigrateMetaFieldsToRefs())
        domainMigrations.add(MigrateParentAttFieldToAttId())
        domainMigrations.add(AddStatusModifiedAttField())

        schemaMigrations.add(ChangeContentCreatorType())
        schemaMigrations.add(RemoveAllowedFlagFromPerms())
        schemaMigrations.add(AddDeletedAssocsTable())
        schemaMigrations.add(UpdateContentTables())
        schemaMigrations.add(UpdateNullContentCreator())
    }

    fun runDomainMigrations(context: DbDomainMigrationContext) {
        var version = context.dataService.getSchemaVersion()
        if (version == DbDataService.NEW_TABLE_SCHEMA_VERSION) {
            return
        }
        val dataSource = context.schemaContext.dataSourceCtx.dataSource
        AuthContext.runAsSystem {
            while (version < DbDataService.NEW_TABLE_SCHEMA_VERSION) {
                TxnContext.doInNewTxn(false) {
                    dataSource.withTransaction(false) {
                        val upgradeTo = version + 1
                        domainMigrations.forEach {
                            if (it.getAppliedVersions() == upgradeTo) {
                                log.info {
                                    "Run domain migration: ${it::class.java.simpleName} " +
                                        "for table ${context.dataService.getTableRef()}"
                                }
                                it.run(context)
                                context.dataService.resetColumnsCache()
                            }
                        }
                        context.dataService.setSchemaVersion(++version)
                    }
                }
            }
        }
    }

    fun runSchemaMigrations(context: DbSchemaContext) {

        var version = context.getVersion()
        if (version == DbSchemaContext.NEW_SCHEMA_VERSION) {
            return
        }
        val dataSource = context.dataSourceCtx.dataSource
        val isNewSchema = AtomicBoolean()
        if (version == 0) {
            context.doInNewTxn {
                if (!context.isSchemaExists()) {
                    isNewSchema.set(true)
                    context.setVersion(DbSchemaContext.NEW_SCHEMA_VERSION)
                } else {
                    val oldMetaTableRef = context.getTableRef("ecos_schema_meta")
                    val newMetaTableRef = context.getTableRef(DbSchemaMetaEntity.TABLE)
                    if (context.isTableExists(oldMetaTableRef) && !context.isTableExists(newMetaTableRef)) {
                        val query = "ALTER TABLE ${oldMetaTableRef.fullName} RENAME TO \"${DbSchemaMetaEntity.TABLE}\";"
                        log.info { query }
                        dataSource.updateSchema(query)
                        context.schemaMetaService.resetColumnsCache()
                        version = context.getVersion()
                        RenameEcosDataTables().run(context)
                        context.resetColumnsCache()
                    }
                }
                context.recordRefService.createTableIfNotExists()
                context.assocsService.createTableIfNotExists()
            }
            if (isNewSchema.get()) {
                return
            }
        }

        AuthContext.runAsSystem {
            while (version < DbSchemaContext.NEW_SCHEMA_VERSION) {
                context.doInNewTxn {
                    val upgradeTo = version + 1
                    schemaMigrations.forEach {
                        if (it.getAppliedVersions() == upgradeTo) {
                            log.info {
                                "Run schema migration: ${it::class.java.simpleName} " +
                                    "for schema '${context.schema}'"
                            }
                            it.run(context)
                            context.resetColumnsCache()
                        }
                    }
                    version = upgradeTo
                    context.setVersion(version)
                }
            }
        }
    }
}
