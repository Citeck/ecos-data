package ru.citeck.ecos.data.sql.domain.migration

import mu.KotlinLogging
import ru.citeck.ecos.context.lib.auth.AuthContext
import ru.citeck.ecos.data.sql.context.DbSchemaContext
import ru.citeck.ecos.data.sql.domain.migration.domain.DbDomainMigration
import ru.citeck.ecos.data.sql.domain.migration.domain.MigrateMetaFieldsToRefs
import ru.citeck.ecos.data.sql.domain.migration.domain.MoveAssocsToAssocsTable
import ru.citeck.ecos.data.sql.domain.migration.domain.MovePermsToSchemaTable
import ru.citeck.ecos.data.sql.domain.migration.schema.*
import ru.citeck.ecos.data.sql.service.DbDataService
import ru.citeck.ecos.txn.lib.TxnContext

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

        schemaMigrations.add(ChangeContentCreatorType())
        schemaMigrations.add(RemoveAllowedFlagFromPerms())
        schemaMigrations.add(AddDeletedAssocsTable())
        schemaMigrations.add(UpdateContentTables())
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
        AuthContext.runAsSystem {
            while (version < DbSchemaContext.NEW_SCHEMA_VERSION) {
                TxnContext.doInNewTxn(false) {
                    dataSource.withTransaction(false) {
                        val upgradeTo = version + 1
                        schemaMigrations.forEach {
                            if (it.getAppliedVersions() == upgradeTo) {
                                log.info {
                                    "Run domain migration: ${it::class.java.simpleName} " +
                                        "for schema '${context.schema}'"
                                }
                                it.run(context)
                                context.resetColumnsCache()
                            }
                        }
                        context.setVersion(++version)
                    }
                }
            }
        }
    }
}
