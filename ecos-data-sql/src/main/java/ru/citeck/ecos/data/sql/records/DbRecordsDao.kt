package ru.citeck.ecos.data.sql.records

import io.github.oshai.kotlinlogging.KotlinLogging
import ru.citeck.ecos.commons.data.ObjectData
import ru.citeck.ecos.context.lib.auth.AuthContext
import ru.citeck.ecos.data.sql.content.DbContentService
import ru.citeck.ecos.data.sql.content.storage.EcosContentStorageConfig
import ru.citeck.ecos.data.sql.context.DbTableContext
import ru.citeck.ecos.data.sql.dto.DbTableRef
import ru.citeck.ecos.data.sql.ecostype.DbEcosModelService
import ru.citeck.ecos.data.sql.meta.table.dto.DbTableMetaDto
import ru.citeck.ecos.data.sql.records.assocs.DbAssocsService
import ru.citeck.ecos.data.sql.records.computed.DbComputedAttsComponent
import ru.citeck.ecos.data.sql.records.dao.DbRecordsDaoCtx
import ru.citeck.ecos.data.sql.records.dao.DbRecordsDaoCtxAware
import ru.citeck.ecos.data.sql.records.dao.atts.DbRecord
import ru.citeck.ecos.data.sql.records.listener.*
import ru.citeck.ecos.data.sql.records.perms.DbPermsComponent
import ru.citeck.ecos.data.sql.records.perms.DbRecordPermsContext
import ru.citeck.ecos.data.sql.records.refs.DbGlobalRefCalculator
import ru.citeck.ecos.data.sql.records.refs.DbRecordRefService
import ru.citeck.ecos.data.sql.records.refs.DefaultDbGlobalRefCalculator
import ru.citeck.ecos.data.sql.remote.DbRecordsRemoteActionsClient
import ru.citeck.ecos.data.sql.repo.entity.DbEntity
import ru.citeck.ecos.data.sql.service.DbDataService
import ru.citeck.ecos.model.lib.ModelServiceFactory
import ru.citeck.ecos.model.lib.type.dto.TypeInfo
import ru.citeck.ecos.records3.RecordsServiceFactory
import ru.citeck.ecos.records3.record.atts.dto.LocalRecordAtts
import ru.citeck.ecos.records3.record.atts.value.AttValue
import ru.citeck.ecos.records3.record.dao.AbstractRecordsDao
import ru.citeck.ecos.records3.record.dao.atts.RecordsAttsDao
import ru.citeck.ecos.records3.record.dao.delete.DelStatus
import ru.citeck.ecos.records3.record.dao.delete.RecordsDeleteDao
import ru.citeck.ecos.records3.record.dao.mutate.RecordMutateDao
import ru.citeck.ecos.records3.record.dao.query.RecordsQueryDao
import ru.citeck.ecos.records3.record.dao.query.RecsGroupQueryDao
import ru.citeck.ecos.records3.record.dao.query.dto.query.RecordsQuery
import ru.citeck.ecos.records3.record.dao.query.dto.res.RecsQueryRes
import ru.citeck.ecos.txn.lib.TxnContext
import ru.citeck.ecos.webapp.api.content.EcosContentData
import ru.citeck.ecos.webapp.api.content.EcosContentWriter
import ru.citeck.ecos.webapp.api.entity.EntityRef
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.collections.ArrayList

class DbRecordsDao(
    private val config: DbRecordsDaoConfig,
    private val modelServices: ModelServiceFactory,
    private val dataService: DbDataService<DbEntity>,
    private val permsComponent: DbPermsComponent,
    private val computedAttsComponent: DbComputedAttsComponent?,
    private val globalRefCalculator: DbGlobalRefCalculator?,
    private val onInitialized: () -> Unit = {}
) : AbstractRecordsDao(),
    RecordsAttsDao,
    RecordsQueryDao,
    RecordMutateDao,
    RecordsDeleteDao,
    RecsGroupQueryDao {

    companion object {
        private val log = KotlinLogging.logger {}
    }

    private lateinit var ecosTypeService: DbEcosModelService
    private lateinit var daoCtx: DbRecordsDaoCtx
    private val daoCtxInitialized = AtomicBoolean(false)

    private val tableContext: DbTableContext = dataService.getTableContext()
    private val recordRefService: DbRecordRefService = tableContext.getRecordRefsService()
    private val assocsService: DbAssocsService = tableContext.getAssocsService()
    private val contentService: DbContentService = tableContext.getContentService()

    private val listeners: MutableList<DbRecordsListener> = CopyOnWriteArrayList()

    fun uploadFile(
        ecosType: String? = null,
        name: String? = null,
        mimeType: String? = null,
        encoding: String? = null,
        attributes: ObjectData? = null,
        writer: (EcosContentWriter) -> Unit
    ): EntityRef {
        return daoCtx.contentDao.uploadFile(ecosType, name, mimeType, encoding, attributes, writer)
    }

    @JvmOverloads
    fun getContent(recordId: String, attribute: String = "", index: Int = 0): EcosContentData? {
        return daoCtx.contentDao.getContent(recordId, attribute, index)
    }

    fun runMigrations(typeRef: EntityRef, mock: Boolean = true, diff: Boolean = true): List<String> {
        return TxnContext.doInTxn {
            val typeInfo = getRecordsTypeInfo(typeRef) ?: error("Type is null. Migration can't be executed")
            val columns = ecosTypeService.getColumnsForTypes(listOf(typeInfo)).map { it.column }
            dataService.resetColumnsCache()
            val migrations = ArrayList(dataService.runMigrations(columns, mock, diff))
            migrations
        }
    }

    fun updatePermissions(records: List<String>) {
        daoCtx.permsDao.updatePermissions(records)
    }

    fun getRecordsDaoCtx(): DbRecordsDaoCtx {
        return daoCtx
    }

    fun getTableRef(): DbTableRef {
        return daoCtx.tableRef
    }

    fun getTableMeta(): DbTableMetaDto {
        return dataService.getTableMeta()
    }

    private fun getRecordsTypeInfo(typeRef: EntityRef): TypeInfo? {
        val type = getRecordsTypeRef(typeRef)
        if (EntityRef.isEmpty(type)) {
            log.warn { "Type is not defined for Records DAO" }
            return null
        }
        return ecosTypeService.getTypeInfo(type.getLocalId())
    }

    private fun getRecordsTypeRef(typeRef: EntityRef): EntityRef {
        return if (EntityRef.isEmpty(typeRef)) {
            config.typeRef
        } else {
            typeRef
        }
    }

    override fun getRecordsAtts(recordIds: List<String>): List<AttValue> {
        return daoCtx.attsDao.getRecordsAtts(recordIds)
    }

    override fun queryRecords(recsQuery: RecordsQuery): RecsQueryRes<DbRecord> {
        return TxnContext.doInTxn(readOnly = true) {
            daoCtx.queryDao.queryRecords(recsQuery)
        }
    }

    override fun delete(recordIds: List<String>): List<DelStatus> {

        if (!config.deletable) {
            error("Records DAO is not deletable. Records can't be deleted: '$recordIds'")
        }
        return TxnContext.doInTxn {
            if (!AuthContext.isRunAsSystem()) {
                recordIds.forEach {
                    val recordPerms = getRecordPerms(it)
                    if (!recordPerms.hasWritePerms()) {
                        error("Permissions Denied. You can't delete record '${daoCtx.getGlobalRef(it)}'")
                    }
                }
            }
            daoCtx.deleteDao.delete(recordIds, daoCtx.getRecsCurrentlyInDeletion())
        }
    }

    override fun mutate(record: LocalRecordAtts): String {
        return daoCtx.mutateDao.mutate(record)
    }

    fun getRecordPerms(record: Any): DbRecordPermsContext {
        return daoCtx.permsDao.getRecordPerms(record)
    }

    fun getRecordPerms(record: Any, user: String, authorities: Set<String>): DbRecordPermsContext {
        return daoCtx.permsDao.getRecordPerms(record, user, authorities)
    }

    fun setDefaultContentStorage(storage: EcosContentStorageConfig?) {
        daoCtx.contentDao.setDefaultContentStorage(storage)
    }

    override fun getId(): String {
        return config.id
    }

    @Synchronized
    fun addListener(listener: DbRecordsListener) {
        this.listeners.add(listener)
        if (daoCtxInitialized.get() && listener is DbRecordsDaoCtxAware) {
            listener.setRecordsDaoCtx(daoCtx)
        }
    }

    fun removeListener(listener: DbRecordsListener) {
        this.listeners.remove(listener)
    }

    @Synchronized
    override fun setRecordsServiceFactory(serviceFactory: RecordsServiceFactory) {
        super.setRecordsServiceFactory(serviceFactory)

        val dataSourceCtx = dataService.getTableContext().getSchemaCtx().dataSourceCtx
        val remoteActionsClient: DbRecordsRemoteActionsClient? = dataSourceCtx.remoteActionsClient

        ecosTypeService = DbEcosModelService(modelServices)
        val appName = serviceFactory.webappProps.appName

        daoCtx = DbRecordsDaoCtx(
            appName,
            getId(),
            dataService.getTableRef(),
            dataService.getTableContext(),
            config,
            dataService,
            contentService,
            recordRefService,
            ecosTypeService,
            serviceFactory.recordsService,
            serviceFactory.attSchemaWriter,
            serviceFactory.getEcosWebAppApi()?.getContentApi(),
            listeners,
            this,
            serviceFactory.attValuesConverter,
            serviceFactory.getEcosWebAppApi()?.getWebClientApi(),
            modelServices.delegationService,
            assocsService,
            globalRefCalculator ?: DefaultDbGlobalRefCalculator(),
            dataSourceCtx.converter,
            remoteActionsClient,
            computedAttsComponent,
            serviceFactory,
            permsComponent
        )
        daoCtxInitialized.set(true)
        listeners.forEach {
            if (it is DbRecordsDaoCtxAware) {
                it.setRecordsDaoCtx(daoCtx)
            }
        }
        onInitialized()
    }
}
