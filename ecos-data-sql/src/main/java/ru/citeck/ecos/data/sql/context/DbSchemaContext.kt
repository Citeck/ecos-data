package ru.citeck.ecos.data.sql.context

import ru.citeck.ecos.context.lib.auth.AuthUser
import ru.citeck.ecos.context.lib.ctx.EcosContext
import ru.citeck.ecos.data.sql.batch.DbBatchTaskService
import ru.citeck.ecos.data.sql.columnmeta.DbColumnMetaService
import ru.citeck.ecos.data.sql.content.DbContentService
import ru.citeck.ecos.data.sql.content.DbContentServiceImpl
import ru.citeck.ecos.data.sql.content.storage.EcosContentStorageService
import ru.citeck.ecos.data.sql.content.upload.DbChunkedUploadService
import ru.citeck.ecos.data.sql.content.upload.DbContentUploadSessionService
import ru.citeck.ecos.data.sql.content.upload.DbContentUploadSessionServiceImpl
import ru.citeck.ecos.data.sql.dto.DbColumnDef
import ru.citeck.ecos.data.sql.dto.DbTableRef
import ru.citeck.ecos.data.sql.meta.schema.DbSchemaMetaService
import ru.citeck.ecos.data.sql.meta.schema.DbSchemaMetaServiceImpl
import ru.citeck.ecos.data.sql.meta.table.DbTableMetaEntity
import ru.citeck.ecos.data.sql.perms.DbEntityPermsService
import ru.citeck.ecos.data.sql.perms.DbEntityPermsServiceImpl
import ru.citeck.ecos.data.sql.records.assocs.DbAssocBackupService
import ru.citeck.ecos.data.sql.records.assocs.DbAssocsService
import ru.citeck.ecos.data.sql.records.attnames.DbEcosAttributesService
import ru.citeck.ecos.data.sql.records.refs.DbRecordRefService
import ru.citeck.ecos.data.sql.records.workspace.DbWorkspaceService
import ru.citeck.ecos.data.sql.repo.entity.auth.DbAuthorityEntity
import ru.citeck.ecos.data.sql.repo.entity.auth.DbAuthorityService
import ru.citeck.ecos.data.sql.schema.DbSchemaListener
import ru.citeck.ecos.data.sql.service.DbDataService
import ru.citeck.ecos.data.sql.service.DbDataServiceConfig
import ru.citeck.ecos.data.sql.service.DbDataServiceImpl
import ru.citeck.ecos.data.sql.trashcan.DbTrashcanService
import ru.citeck.ecos.data.sql.trashcan.DbTrashcanServiceImpl
import ru.citeck.ecos.records3.RecordsService
import ru.citeck.ecos.txn.lib.TxnContext
import ru.citeck.ecos.webapp.api.EcosWebAppApi
import ru.citeck.ecos.webapp.api.authority.EcosAuthoritiesApi
import ru.citeck.ecos.webapp.api.entity.EntityRef
import java.util.concurrent.ConcurrentHashMap

class DbSchemaContext(
    val schema: String,
    val dataSourceCtx: DbDataSourceContext,
    val webAppApi: EcosWebAppApi,
    val ecosContext: EcosContext
) {
    companion object {
        const val NEW_SCHEMA_VERSION = 11
    }

    val schemaMetaService: DbSchemaMetaService = DbSchemaMetaServiceImpl(this)

    val tableMetaService: DbDataService<DbTableMetaEntity> = DbDataServiceImpl(
        DbTableMetaEntity::class.java,
        DbDataServiceConfig.create()
            .withTable(DbTableMetaEntity.TABLE)
            .build(),
        this
    )
    private val authorityDataService: DbDataService<DbAuthorityEntity> = DbDataServiceImpl(
        DbAuthorityEntity::class.java,
        DbDataServiceConfig.create()
            .withTable(DbAuthorityEntity.TABLE)
            .build(),
        this
    )
    val authorityService: DbAuthorityService = DbAuthorityService(authorityDataService)
    val entityPermsService: DbEntityPermsService = DbEntityPermsServiceImpl(this)
    val recordRefService: DbRecordRefService = DbRecordRefService(dataSourceCtx.appName, this)
    val attributesService: DbEcosAttributesService = DbEcosAttributesService(this)
    val assocsService: DbAssocsService = DbAssocsService(dataSourceCtx.appName, this)
    val assocBackupService: DbAssocBackupService = DbAssocBackupService(this)
    val workspaceService: DbWorkspaceService = DbWorkspaceService(this)

    private val metaSchemaVersionKey = listOf("schema-version")

    /**
     * The [RecordsService] of each domain table of this schema, published by
     * [ru.citeck.ecos.data.sql.records.DbRecordsDao] once its own context is fully wired.
     *
     * It exists for the background column migration and nothing else: a child link needs the
     * `_parent`/`_parentAtt` back-reference written through `RecordsService`, and a batch handler is
     * handed a schema and a table with no other way to reach one. A departure needs it from the
     * other end, since a released child may live in any table of any application.
     *
     * **A `RecordsService` and not a whole `DbRecordsDaoCtx`, on purpose.** A records service is one
     * per application, so "last registration wins" chooses nothing - every registration for a table
     * writes the same object. Every other member of a dao context is per-dao, and taking one out of
     * a table-keyed map would be picking a dao at random.
     *
     * **Nothing here is ever removed by the platform**, because nothing closes a records dao.
     * [unregisterRecordsService] exists for a caller that replaces one, and for tests that need a
     * table with no service registered - the state in which child links must be refused.
     */
    private val recordsServiceByTable = ConcurrentHashMap<String, RecordsService>()

    fun registerRecordsService(table: String, recordsService: RecordsService) {
        recordsServiceByTable[table] = recordsService
    }

    fun unregisterRecordsService(table: String) {
        recordsServiceByTable.remove(table)
    }

    fun getRecordsService(table: String): RecordsService? {
        return recordsServiceByTable[table]
    }

    val contentStorageService: EcosContentStorageService = dataSourceCtx.createContentStorageService(this)

    val contentService: DbContentService = DbContentServiceImpl(this)
    val uploadSessionService: DbContentUploadSessionService = DbContentUploadSessionServiceImpl(this)
    val chunkedUploadService: DbChunkedUploadService = DbChunkedUploadService(this)
    val columnMetaService: DbColumnMetaService = DbColumnMetaService(this)

    val batchTaskService: DbBatchTaskService = DbBatchTaskService(this)

    val trashcanService: DbTrashcanService = DbTrashcanServiceImpl(this)

    val authoritiesApi: EcosAuthoritiesApi = webAppApi.getAuthoritiesApi()

    init {
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
        authorityService.resetColumnsCache()
        entityPermsService.resetColumnsCache()
        recordRefService.resetColumnsCache()
        attributesService.resetColumnsCache()
        workspaceService.resetColumnsCache()
        assocsService.resetColumnsCache()
        assocBackupService.resetColumnsCache()
        contentStorageService.resetColumnsCache()
        trashcanService.resetColumnsCache()
        uploadSessionService.resetColumnsCache()
        columnMetaService.resetColumnsCache()
        batchTaskService.resetColumnsCache()
    }

    fun isSchemaExists(): Boolean {
        return dataSourceCtx.schemaDao.isSchemaExists(dataSourceCtx.dataSource, schema)
    }

    fun isTableExists(tableRef: DbTableRef): Boolean {
        return dataSourceCtx.schemaDao.isTableExists(dataSourceCtx.dataSource, tableRef)
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

    fun getUserRef(userName: String): EntityRef {
        val nonEmptyUserName = userName.ifBlank { AuthUser.ANONYMOUS }
        return authoritiesApi.getPersonRef(nonEmptyUserName)
    }

    fun forEachNeighbourSchema(action: (String, DbSchemaContext) -> Unit) {
        dataSourceCtx.forEachSchema { name, context ->
            if (name != this.schema) {
                action(name, context)
            }
        }
    }

    override fun equals(other: Any?): Boolean {
        other ?: return false
        if (this === other) {
            return true
        }
        if (other !is DbSchemaContext) {
            return false
        }
        return dataSourceCtx === other.dataSourceCtx && schema == other.schema
    }

    override fun hashCode(): Int {
        var hash = dataSourceCtx.hashCode()
        hash = 31 * hash + schema.hashCode()
        return hash
    }

    override fun toString(): String {
        return "DbSchemaContext[$schema]"
    }
}
