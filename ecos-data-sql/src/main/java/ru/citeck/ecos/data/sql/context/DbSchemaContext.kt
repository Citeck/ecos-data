package ru.citeck.ecos.data.sql.context

import ru.citeck.ecos.data.sql.content.DbContentService
import ru.citeck.ecos.data.sql.content.DbContentServiceImpl
import ru.citeck.ecos.data.sql.content.storage.EcosContentStorage
import ru.citeck.ecos.data.sql.content.storage.EcosContentStorageService
import ru.citeck.ecos.data.sql.content.storage.EcosContentStorageServiceImpl
import ru.citeck.ecos.data.sql.content.storage.local.DbContentDataEntity
import ru.citeck.ecos.data.sql.content.storage.local.EcosContentLocalStorage
import ru.citeck.ecos.data.sql.content.writer.EcosContentWriterFactory
import ru.citeck.ecos.data.sql.content.writer.EcosContentWriterImpl
import ru.citeck.ecos.data.sql.dto.DbColumnDef
import ru.citeck.ecos.data.sql.dto.DbTableRef
import ru.citeck.ecos.data.sql.meta.schema.DbSchemaMetaService
import ru.citeck.ecos.data.sql.meta.schema.DbSchemaMetaServiceImpl
import ru.citeck.ecos.data.sql.meta.table.DbTableMetaEntity
import ru.citeck.ecos.data.sql.perms.DbEntityPermsService
import ru.citeck.ecos.data.sql.perms.DbEntityPermsServiceImpl
import ru.citeck.ecos.data.sql.records.assocs.DbAssocsService
import ru.citeck.ecos.data.sql.records.refs.DbRecordRefService
import ru.citeck.ecos.data.sql.repo.entity.auth.DbAuthorityEntity
import ru.citeck.ecos.data.sql.schema.DbSchemaListener
import ru.citeck.ecos.data.sql.service.DbDataService
import ru.citeck.ecos.data.sql.service.DbDataServiceConfig
import ru.citeck.ecos.data.sql.service.DbDataServiceImpl
import ru.citeck.ecos.txn.lib.TxnContext
import ru.citeck.ecos.webapp.api.content.EcosContentWriter
import java.io.OutputStream

class DbSchemaContext(
    val schema: String,
    val dataSourceCtx: DbDataSourceContext,
    contentStorages: List<EcosContentStorage>
) {
    companion object {
        const val NEW_SCHEMA_VERSION = 3
    }

    val schemaMetaService: DbSchemaMetaService = DbSchemaMetaServiceImpl(this)

    val tableMetaService: DbDataService<DbTableMetaEntity> = DbDataServiceImpl(
        DbTableMetaEntity::class.java,
        DbDataServiceConfig.create()
            .withTable(DbTableMetaEntity.TABLE)
            .build(),
        this
    )
    val authorityDataService: DbDataService<DbAuthorityEntity> = DbDataServiceImpl(
        DbAuthorityEntity::class.java,
        DbDataServiceConfig.create()
            .withTable(DbAuthorityEntity.TABLE)
            .build(),
        this
    )
    val entityPermsService: DbEntityPermsService = DbEntityPermsServiceImpl(this)
    val recordRefService: DbRecordRefService = DbRecordRefService(dataSourceCtx.appName, this)
    val assocsService: DbAssocsService = DbAssocsService(this)

    private val metaSchemaVersionKey = listOf("schema-version")

    private val contentWriterFactory: EcosContentWriterFactory = object : EcosContentWriterFactory {
        override fun createWriter(output: OutputStream): EcosContentWriter {
            return EcosContentWriterImpl(output)
        }
    }

    val contentStorageService: EcosContentStorageService = EcosContentStorageServiceImpl(contentWriterFactory)
    val contentService: DbContentService = DbContentServiceImpl(contentStorageService, this)

    init {
        contentStorages.forEach { contentStorageService.register(it) }
        contentStorageService.register(
            EcosContentLocalStorage(
                DbDataServiceImpl(
                    DbContentDataEntity::class.java,
                    DbDataServiceConfig.create {
                        withTable(DbContentDataEntity.TABLE)
                        withStoreTableMeta(true)
                    },
                    this
                )
            )
        )
        dataSourceCtx.schemaDao.addSchemaListener(
            schema,
            object : DbSchemaListener {
                override fun onSchemaCreated() {
                    TxnContext.doBeforeCommit(0f) {
                        setVersion(NEW_SCHEMA_VERSION)
                    }
                }
            }
        )
    }

    fun getTableRef(table: String): DbTableRef {
        return DbTableRef(schema, table)
    }

    fun getColumns(table: String): List<DbColumnDef> {
        return dataSourceCtx.schemaDao.getColumns(dataSourceCtx.dataSource, DbTableRef(schema, table))
    }

    fun addColumns(table: String, columns: List<DbColumnDef>) {
        dataSourceCtx.schemaDao.addColumns(dataSourceCtx.dataSource, DbTableRef(schema, table), columns)
    }

    fun resetColumnsCache() {
        schemaMetaService.resetColumnsCache()
        contentService.resetColumnsCache()
        tableMetaService.resetColumnsCache()
        authorityDataService.resetColumnsCache()
        entityPermsService.resetColumnsCache()
        recordRefService.resetColumnsCache()
        assocsService.resetColumnsCache()
    }

    fun getVersion(): Int {
        return schemaMetaService.getValue(metaSchemaVersionKey, 0)
    }

    fun setVersion(value: Int) {
        schemaMetaService.setValue(metaSchemaVersionKey, value)
    }

    fun <T> doInNewTxn(action: () -> T): T {
        return dataSourceCtx.doInNewTxn(action)
    }

    fun <T> doInNewRoTxn(action: () -> T): T {
        return dataSourceCtx.doInNewRoTxn(action)
    }
}
