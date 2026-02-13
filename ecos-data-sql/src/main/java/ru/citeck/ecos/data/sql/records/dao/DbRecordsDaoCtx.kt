package ru.citeck.ecos.data.sql.records.dao

import ru.citeck.ecos.context.lib.auth.AuthContext
import ru.citeck.ecos.context.lib.auth.AuthUser
import ru.citeck.ecos.data.sql.content.DbContentService
import ru.citeck.ecos.data.sql.context.DbTableContext
import ru.citeck.ecos.data.sql.dto.DbColumnDef
import ru.citeck.ecos.data.sql.dto.DbTableRef
import ru.citeck.ecos.data.sql.ecostype.DbEcosModelService
import ru.citeck.ecos.data.sql.records.DbRecordsDao
import ru.citeck.ecos.data.sql.records.DbRecordsDaoConfig
import ru.citeck.ecos.data.sql.records.assocs.DbAssocsService
import ru.citeck.ecos.data.sql.records.computed.DbComputedAttsComponent
import ru.citeck.ecos.data.sql.records.dao.atts.DbRecord
import ru.citeck.ecos.data.sql.records.dao.atts.DbRecordsAttsDao
import ru.citeck.ecos.data.sql.records.dao.content.DbRecContentHandler
import ru.citeck.ecos.data.sql.records.dao.content.DbRecordsContentDao
import ru.citeck.ecos.data.sql.records.dao.delete.DbRecordsDeleteDao
import ru.citeck.ecos.data.sql.records.dao.events.DbRecEventsHandler
import ru.citeck.ecos.data.sql.records.dao.mutate.DbRecordsMutateDao
import ru.citeck.ecos.data.sql.records.dao.mutate.RecMutAssocHandler
import ru.citeck.ecos.data.sql.records.dao.mutate.RecMutConverter
import ru.citeck.ecos.data.sql.records.dao.mutate.operation.RecMutAttOperationsHandler
import ru.citeck.ecos.data.sql.records.dao.perms.DbRecordsPermsDao
import ru.citeck.ecos.data.sql.records.dao.query.DbRecordsQueryDao
import ru.citeck.ecos.data.sql.records.listener.DbRecordsListener
import ru.citeck.ecos.data.sql.records.perms.DbPermsComponent
import ru.citeck.ecos.data.sql.records.refs.DbGlobalRefCalculator
import ru.citeck.ecos.data.sql.records.refs.DbRecordRefService
import ru.citeck.ecos.data.sql.records.utils.DbAttValueUtils
import ru.citeck.ecos.data.sql.remote.DbRecordsRemoteActionsClient
import ru.citeck.ecos.data.sql.repo.entity.DbEntity
import ru.citeck.ecos.data.sql.service.DbDataService
import ru.citeck.ecos.data.sql.type.DbTypesConverter
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.delegation.service.DelegationService
import ru.citeck.ecos.model.lib.workspace.WorkspaceService
import ru.citeck.ecos.records3.RecordsService
import ru.citeck.ecos.records3.RecordsServiceFactory
import ru.citeck.ecos.records3.record.atts.schema.write.AttSchemaWriter
import ru.citeck.ecos.records3.record.atts.value.AttValuesConverter
import ru.citeck.ecos.txn.lib.TxnContext
import ru.citeck.ecos.txn.lib.transaction.Transaction
import ru.citeck.ecos.webapp.api.content.EcosContentApi
import ru.citeck.ecos.webapp.api.entity.EntityRef
import ru.citeck.ecos.webapp.api.web.client.EcosWebClientApi
import java.util.concurrent.atomic.AtomicBoolean

class DbRecordsDaoCtx(
    val appName: String,
    val sourceId: String,
    val tableRef: DbTableRef,
    val tableCtx: DbTableContext,
    val config: DbRecordsDaoConfig,
    val dataService: DbDataService<DbEntity>,
    val contentService: DbContentService?,
    val recordRefService: DbRecordRefService,
    val ecosTypeService: DbEcosModelService,
    val recordsService: RecordsService,
    val schemaWriter: AttSchemaWriter,
    val contentApi: EcosContentApi?,
    val listeners: List<DbRecordsListener>,
    val recordsDao: DbRecordsDao,
    val attValuesConverter: AttValuesConverter,
    val webApiClient: EcosWebClientApi?,
    val delegationService: DelegationService,
    val assocsService: DbAssocsService,
    val globalRefCalculator: DbGlobalRefCalculator,
    val typesConverter: DbTypesConverter,
    val remoteActionsClient: DbRecordsRemoteActionsClient?,
    val computedAttsComponent: DbComputedAttsComponent?,
    val recordsServiceFactory: RecordsServiceFactory,
    val permsComponent: DbPermsComponent,
    val workspaceService: WorkspaceService
) {

    companion object {
        // This set will contain the global references, and it's safe to use it across multiple DAOs.
        private val recsCurrentlyInDeletion = IdentityKey()
    }

    val recContentHandler: DbRecContentHandler by lazySingleton { DbRecContentHandler(this) }
    val mutConverter = RecMutConverter()
    val mutAssocHandler: RecMutAssocHandler by lazySingleton { RecMutAssocHandler(this) }
    val mutAttOperationHandler: RecMutAttOperationsHandler by lazySingleton { RecMutAttOperationsHandler() }
    val recEventsHandler: DbRecEventsHandler by lazySingleton { DbRecEventsHandler(this) }
    val deleteDao: DbRecordsDeleteDao by lazySingleton { DbRecordsDeleteDao(this) }
    val queryDao: DbRecordsQueryDao by lazySingleton { DbRecordsQueryDao(this) }
    val attsDao: DbRecordsAttsDao by lazySingleton { DbRecordsAttsDao(this) }
    val mutateDao: DbRecordsMutateDao by lazySingleton { DbRecordsMutateDao() }
    val contentDao: DbRecordsContentDao by lazySingleton { DbRecordsContentDao() }
    val permsDao: DbRecordsPermsDao by lazySingleton { DbRecordsPermsDao(this) }

    val authoritiesApi = dataService.getTableContext().getAuthoritiesApi()
    val trashcanService = tableCtx.getSchemaCtx().trashcanService

    private val recsUpdatedInThisTxnKey = IdentityKey()

    fun getLocalRef(extId: String): EntityRef {
        return EntityRef.create(appName, sourceId, extId)
    }

    fun getGlobalRef(extId: String): EntityRef {
        return globalRefCalculator.getGlobalRef(appName, sourceId, extId)
    }

    fun getDbColumnByName(name: String): DbColumnDef? {
        return dataService.getTableContext().getColumnByName(name)
    }

    fun getEntityMeta(entity: DbEntity): DbEntityMeta {

        val aspectsIds = DbAttValueUtils.collectLongValues(entity.attributes[DbRecord.ATT_ASPECTS])
        val refsIds = ArrayList(aspectsIds)
        if (entity.type != -1L) {
            refsIds.add(entity.type)
        }
        val refsById = recordRefService.getEntityRefsByIdsMap(refsIds)
        val aspectsRefs = aspectsIds.map {
            refsById[it] ?: error("Aspect ref doesn't found for id $it")
        }.toMutableSet()

        val typeId = refsById[entity.type]?.getLocalId()
            ?: entity.legacyType.ifBlank { config.typeRef.getLocalId() }
        val typeInfo = ecosTypeService.getTypeInfoNotNull(typeId)
        typeInfo.aspects.forEach {
            aspectsRefs.add(it.ref)
        }
        val allAttributes = mutableMapOf<String, AttributeDef>()
        val systemAtts = mutableMapOf<String, AttributeDef>()
        val nonSystemAtts = mutableMapOf<String, AttributeDef>()

        val aspectsInfo = ecosTypeService.getAspectsInfo(aspectsRefs)

        aspectsInfo.forEach { aspectInfo ->
            aspectInfo.attributes.forEach {
                allAttributes[it.id] = it
                nonSystemAtts[it.id] = it
            }
            aspectInfo.systemAttributes.forEach {
                allAttributes[it.id] = it
                systemAtts[it.id] = it
            }
        }
        typeInfo.model.attributes.forEach {
            allAttributes[it.id] = it
            nonSystemAtts[it.id] = it
        }
        typeInfo.model.systemAttributes.forEach {
            allAttributes[it.id] = it
            systemAtts[it.id] = it
        }

        val localRef = getLocalRef(entity.extId)
        val globalRef = getGlobalRef(entity.extId)

        val isDraft = entity.attributes[DbRecord.COLUMN_IS_DRAFT.name] == true

        return DbEntityMeta(
            localRef,
            globalRef,
            isDraft,
            typeInfo,
            aspectsInfo,
            systemAtts,
            nonSystemAtts,
            allAttributes
        )
    }

    fun getOrCreateUserRefId(userName: String): Long {
        return recordRefService.getOrCreateIdByEntityRef(getUserRef(userName))
    }

    fun getUserRef(userName: String): EntityRef {
        val nonEmptyUserName = userName.ifBlank { AuthUser.ANONYMOUS }
        return authoritiesApi.getPersonRef(nonEmptyUserName)
    }

    fun getUpdatedInTxnIds(txn: Transaction? = TxnContext.getTxnOrNull()): MutableSet<String> {
        // If user has already modified a record in this transaction,
        // he can modify it again until commit without checking permissions.
        return txn?.getData(AuthContext.getCurrentRunAsUser() to recsUpdatedInThisTxnKey) { HashSet() } ?: HashSet()
    }

    /**
     * Get set of records currently being deleted.
     */
    fun getRecsCurrentlyInDeletion(txn: Transaction? = TxnContext.getTxnOrNull()): MutableSet<EntityRef> {
        return txn?.getData(recsCurrentlyInDeletion) { HashSet() } ?: HashSet()
    }

    private fun <T> lazySingleton(initializer: () -> T): Lazy<T> {
        val initializationInProgress = AtomicBoolean()
        var createdValue: T? = null
        return lazy {
            if (initializationInProgress.compareAndSet(false, true)) {
                val value = initializer()
                createdValue = value
                if (value is DbRecordsDaoCtxAware) {
                    value.setRecordsDaoCtx(this)
                }
                value
            } else {
                createdValue ?: error("Cyclic reference")
            }
        }
    }

    private class IdentityKey
}
