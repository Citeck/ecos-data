package ru.citeck.ecos.data.sql.domain.migration

import mu.KotlinLogging
import ru.citeck.ecos.context.lib.auth.AuthContext
import ru.citeck.ecos.data.sql.domain.migration.type.MovePermsToSchemaTable
import ru.citeck.ecos.data.sql.service.DbDataService
import ru.citeck.ecos.txn.lib.TxnContext

class DbMigrationService {

    companion object {
        private val log = KotlinLogging.logger {}
    }

    private val domainMigrations = ArrayList<DbDomainMigration>()

    init {
        domainMigrations.add(MovePermsToSchemaTable())
    }

    fun runMigrations(context: DbDomainMigrationContext) {
        var version = context.dataService.getSchemaVersion()
        if (version == DbDataService.NEW_TABLE_SCHEMA_VERSION) {
            return
        }
        val dataSource = context.schemaContext.dataSourceCtx.dataSource
        AuthContext.runAsSystem {
            while (version < DbDataService.NEW_TABLE_SCHEMA_VERSION) {
                TxnContext.doInNewTxn(false) {
                    dataSource.withTransaction(false) {
                        val upgradeFromTo = version to (version + 1)
                        domainMigrations.forEach {
                            if (it.getAppliedVersions() == upgradeFromTo) {
                                log.info {
                                    "Run domain migration: ${it::class.java.simpleName} " +
                                        "for table ${context.dataService.getTableRef()}"
                                }
                                it.run(context)
                            }
                        }
                        context.dataService.setSchemaVersion(++version)
                    }
                }
            }
        }
    }
}
