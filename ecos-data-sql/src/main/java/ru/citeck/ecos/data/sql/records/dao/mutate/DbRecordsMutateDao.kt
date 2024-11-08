package ru.citeck.ecos.data.sql.records.dao.mutate

import io.github.oshai.kotlinlogging.KotlinLogging
import ru.citeck.ecos.commons.data.DataValue
import ru.citeck.ecos.commons.data.ObjectData
import ru.citeck.ecos.commons.data.Version
import ru.citeck.ecos.context.lib.auth.AuthContext
import ru.citeck.ecos.context.lib.auth.AuthUser
import ru.citeck.ecos.data.sql.content.DbContentService
import ru.citeck.ecos.data.sql.dto.DbColumnDef
import ru.citeck.ecos.data.sql.ecostype.DbEcosModelService
import ru.citeck.ecos.data.sql.ecostype.EcosAttColumnDef
import ru.citeck.ecos.data.sql.perms.DbEntityPermsService
import ru.citeck.ecos.data.sql.records.DbRecordsControlAtts
import ru.citeck.ecos.data.sql.records.DbRecordsDaoConfig
import ru.citeck.ecos.data.sql.records.DbRecordsUtils
import ru.citeck.ecos.data.sql.records.assocs.DbAssocRefsDiff
import ru.citeck.ecos.data.sql.records.assocs.DbAssocsService
import ru.citeck.ecos.data.sql.records.computed.DbComputedAttsComponent
import ru.citeck.ecos.data.sql.records.dao.DbRecordsDaoCtx
import ru.citeck.ecos.data.sql.records.dao.DbRecordsDaoCtxAware
import ru.citeck.ecos.data.sql.records.dao.atts.DbAssocAttValuesContainer
import ru.citeck.ecos.data.sql.records.dao.atts.DbRecord
import ru.citeck.ecos.data.sql.records.dao.content.DbRecordsContentDao
import ru.citeck.ecos.data.sql.records.dao.delete.DbRecordsDeleteDao
import ru.citeck.ecos.data.sql.records.dao.mutate.operation.OperationType
import ru.citeck.ecos.data.sql.records.dao.perms.DbRecordsPermsDao
import ru.citeck.ecos.data.sql.records.perms.DbRecordAllowedAllPerms
import ru.citeck.ecos.data.sql.records.perms.DbRecordPermsContext
import ru.citeck.ecos.data.sql.records.refs.DbRecordRefService
import ru.citeck.ecos.data.sql.records.utils.DbAttValueUtils
import ru.citeck.ecos.data.sql.records.workspace.DbWorkspaceService
import ru.citeck.ecos.data.sql.repo.entity.DbEntity
import ru.citeck.ecos.data.sql.repo.find.DbFindPage
import ru.citeck.ecos.data.sql.service.DbDataService
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType
import ru.citeck.ecos.model.lib.status.constants.StatusConstants
import ru.citeck.ecos.model.lib.type.dto.QueryPermsPolicy
import ru.citeck.ecos.model.lib.type.dto.TypeInfo
import ru.citeck.ecos.model.lib.type.dto.WorkspaceScope
import ru.citeck.ecos.model.lib.utils.ModelUtils
import ru.citeck.ecos.model.lib.workspace.WorkspaceService
import ru.citeck.ecos.records2.RecordConstants
import ru.citeck.ecos.records3.record.atts.dto.LocalRecordAtts
import ru.citeck.ecos.records3.record.atts.schema.ScalarType
import ru.citeck.ecos.txn.lib.TxnContext
import ru.citeck.ecos.webapp.api.entity.EntityRef
import ru.citeck.ecos.webapp.api.entity.toEntityRef
import java.time.Instant
import java.util.*
import kotlin.collections.ArrayList
import kotlin.collections.HashSet
import kotlin.collections.LinkedHashMap
import kotlin.collections.LinkedHashSet
import kotlin.system.measureTimeMillis

class DbRecordsMutateDao : DbRecordsDaoCtxAware {

    companion object {

        private const val ATT_ID = "id"

        private const val ATT_STATE = "_state"
        private const val ATT_CUSTOM_NAME = "name"

        private const val ASPECT_VERSIONABLE_DATA = "${DbRecord.ASPECT_VERSIONABLE}-data"

        private val AUDIT_ATTS = setOf(
            RecordConstants.ATT_CREATED,
            RecordConstants.ATT_CREATOR,
            RecordConstants.ATT_MODIFIED,
            RecordConstants.ATT_MODIFIER,
        )

        private val log = KotlinLogging.logger {}
    }

    private lateinit var daoCtx: DbRecordsDaoCtx
    private lateinit var config: DbRecordsDaoConfig
    private lateinit var dataService: DbDataService<DbEntity>
    private lateinit var recordRefService: DbRecordRefService
    private lateinit var ecosTypeService: DbEcosModelService
    private lateinit var assocsService: DbAssocsService
    private lateinit var permsDao: DbRecordsPermsDao
    private lateinit var contentDao: DbRecordsContentDao
    private lateinit var workspaceService: WorkspaceService
    private lateinit var wsDbService: DbWorkspaceService

    private var contentService: DbContentService? = null
    private var computedAttsComponent: DbComputedAttsComponent? = null

    private lateinit var entityPermsService: DbEntityPermsService

    private val recsPrepareToCommitTxnKey = Any()

    override fun setRecordsDaoCtx(recordsDaoCtx: DbRecordsDaoCtx) {

        daoCtx = recordsDaoCtx
        config = daoCtx.config
        dataService = daoCtx.dataService
        recordRefService = daoCtx.recordRefService
        ecosTypeService = daoCtx.ecosTypeService
        assocsService = daoCtx.assocsService
        permsDao = daoCtx.permsDao
        contentDao = daoCtx.contentDao
        workspaceService = daoCtx.workspaceService
        wsDbService = dataService.getTableContext().getWorkspaceService()

        contentService = daoCtx.contentService
        computedAttsComponent = daoCtx.computedAttsComponent

        entityPermsService = dataService.getTableContext().getPermsService()
    }

    fun mutate(record: LocalRecordAtts): String {

        if (!config.updatable) {
            error("Records DAO is not mutable. Record can't be mutated: '${record.id}'")
        }
        val currentSchemaVer = dataService.getSchemaVersion()
        if (currentSchemaVer < DbDataService.NEW_TABLE_SCHEMA_VERSION) {
            error(
                "Records can't be mutated until current schema version will be fully migrated. " +
                    "Current schema version: $currentSchemaVer " +
                    "Expected schema version: ${DbDataService.NEW_TABLE_SCHEMA_VERSION}"
            )
        } else if (currentSchemaVer > DbDataService.NEW_TABLE_SCHEMA_VERSION) {
            error(
                "Records can't be mutated because the current schema version ($currentSchemaVer) is greater " +
                    "than the expected schema version (${DbDataService.NEW_TABLE_SCHEMA_VERSION}). " +
                    "This likely means that the ecos-data library was downgraded after executing migrations from a newer version. " +
                    "To resolve this issue:\n" +
                    "    |- Update the ecos-data library to the latest version that supports the current schema, or\n" +
                    "    |- Downgrade the database schema to match the expected schema version (${DbDataService.NEW_TABLE_SCHEMA_VERSION})."
            )
        }
        return TxnContext.doInTxn {
            val resultEntity = mutateInTxn(record)
            if (resultEntity == null) {
                record.id
            } else {
                val resultRecId = resultEntity.extId

                var queryPermsPolicy = QueryPermsPolicy.OWN
                val typeRef = recordRefService.getEntityRefById(resultEntity.type)
                queryPermsPolicy =
                    ecosTypeService.getTypeInfo(typeRef.getLocalId())?.queryPermsPolicy ?: queryPermsPolicy

                val txn = TxnContext.getTxn()
                if (queryPermsPolicy == QueryPermsPolicy.OWN) {
                    val prepareToCommitEntities = txn.getData(recsPrepareToCommitTxnKey) {
                        LinkedHashSet<String>()
                    }
                    if (prepareToCommitEntities.isEmpty()) {
                        TxnContext.doBeforeCommit(0f) {
                            val time = measureTimeMillis {
                                entityPermsService.setReadPerms(permsDao.getEntitiesPerms(prepareToCommitEntities))
                            }
                            log.trace {
                                "[${config.id}] Update permissions before " +
                                    "commit for <$prepareToCommitEntities> in $time ms"
                            }
                        }
                    }
                    prepareToCommitEntities.add(resultRecId)
                }
                daoCtx.getUpdatedInTxnIds(txn).add(resultRecId)

                resultRecId
            }
        }
    }

    private fun mutateInTxn(record: LocalRecordAtts): DbEntity? {

        val typeId = getTypeIdForRecord(record)
        val typeInfo = ecosTypeService.getTypeInfo(typeId)
            ?: error("Type is not found: '$typeId'. Record ID: '${record.id}'")

        val typeAttColumns = ecosTypeService.getColumnsForTypes(listOf(typeInfo))
        return mutateRecordInTxn(record, typeInfo, typeAttColumns)
    }

    private fun mutateRecordInTxn(
        record: LocalRecordAtts,
        typeInfo: TypeInfo,
        typeAttColumnsArg: List<EcosAttColumnDef>,
    ): DbEntity? {

        val isMutationFromChild = record.getAtt(RecMutAssocHandler.MUTATION_FROM_CHILD_FLAG).asBoolean()

        if (record.id.isNotBlank() &&
            isMutationFromChild &&
            daoCtx.getRecsCurrentlyInDeletion().contains(daoCtx.getGlobalRef(record.id))
        ) {
            return null
        }

        if (record.attributes.has(DbRecord.ATT_ASPECTS)) {
            error(
                "Aspects can't be changed by ${DbRecord.ATT_ASPECTS} attribute. " +
                    "Please use att_add_${DbRecord.ATT_ASPECTS} and att_rem_${DbRecord.ATT_ASPECTS} to change aspects"
            )
        }

        val isAssocForceDeletion = record.getAtt(DbRecordsDeleteDao.ASSOC_FORCE_DELETION_FLAG)
            .asBoolean(true)

        val typeAspects = typeInfo.aspects.map { it.ref }.toSet()

        val knownColumnIds = HashSet<String>()
        val typeAttColumns = ArrayList(typeAttColumnsArg)
        val typeAttColumnsByAtt = LinkedHashMap<String, EcosAttColumnDef>()
        typeAttColumns.forEach { typeAttColumnsByAtt[it.attribute.id] = it }

        val typeColumns = typeAttColumns.map { it.column }.toMutableList()
        val typeColumnNames = typeColumns.map { it.name }.toMutableSet()

        fun addTypeAttColumn(column: EcosAttColumnDef) {
            if (knownColumnIds.add(column.attribute.id)) {
                typeAttColumns.add(column)
                typeAttColumnsByAtt[column.attribute.id] = column
                typeColumns.add(column.column)
                typeColumnNames.add(column.column.name)
            }
        }

        val runAsAuth = AuthContext.getCurrentRunAsAuth()
        val isRunAsSystem = AuthContext.isSystemAuth(runAsAuth)
        val isRunAsAdmin = AuthContext.isAdminAuth(runAsAuth)
        val isRunAsSystemOrAdmin = isRunAsSystem || isRunAsAdmin

        val disableAudit = record.getAtt(DbRecordsControlAtts.DISABLE_AUDIT).asBoolean()
        if (disableAudit && !isRunAsSystem) {
            error("${DbRecordsControlAtts.DISABLE_AUDIT} attribute can't be used outside of system context")
        }
        val disableEvents = record.getAtt(DbRecordsControlAtts.DISABLE_EVENTS).asBoolean()
        if (disableEvents && !isRunAsSystem) {
            error("${DbRecordsControlAtts.DISABLE_EVENTS} attribute can't be used outside of system context")
        }

        val currentRunAsUser = runAsAuth.getUser()
        val currentRunAsAuthorities = DbRecordsUtils.getCurrentAuthorities(runAsAuth)

        val extId = record.id.ifEmpty { record.attributes[ATT_ID].asText() }
        val currentAspectRefs = LinkedHashSet(typeAspects)
        val aspectRefsInDb = LinkedHashSet<EntityRef>()

        val entityToMutate: DbEntity = if (extId.isEmpty()) {
            DbEntity()
        } else {
            var entity = daoCtx.attsDao.findDbEntityByExtId(extId)
            if (entity == null) {
                if (record.id.isNotEmpty()) {
                    error("Record with id: '$extId' doesn't found")
                } else {
                    entity = DbEntity()
                }
            } else {
                val aspects = entity.attributes[DbRecord.ATT_ASPECTS]
                if (aspects != null && aspects is Collection<*>) {
                    val aspectIds = aspects.mapNotNull { it as? Long }
                    aspectRefsInDb.addAll(daoCtx.recordRefService.getEntityRefsByIds(aspectIds))
                    currentAspectRefs.addAll(aspectRefsInDb)
                    val aspectsColumns = ecosTypeService.getColumnsForAspects(aspectRefsInDb)
                    for (column in aspectsColumns) {
                        addTypeAttColumn(column)
                    }
                }
                if (record.attributes[DbRecordsControlAtts.UPDATE_PERMISSIONS].asBoolean()) {
                    if (!isRunAsSystemOrAdmin) {
                        error("Permissions update allowed only for admin. Record: $record sourceId: '${config.id}'")
                    }
                    permsDao.updatePermissions(listOf(record.id))
                    return entity
                }
            }
            entity
        }

        val entityBeforeMutation = entityToMutate.copy()

        var customExtId = record.attributes[ATT_ID].asText()
        if (customExtId.isBlank()) {
            customExtId = record.attributes[ScalarType.LOCAL_ID.mirrorAtt].asText()
        }
        if (customExtId.isNotBlank() && entityToMutate.extId != customExtId) {

            if (entityToMutate.id == DbEntity.NEW_REC_ID) {
                entityToMutate.extId = customExtId
            } else {
                dataService.doWithPermsPolicy(QueryPermsPolicy.PUBLIC) {
                    if (dataService.isExistsByExtId(customExtId)) {
                        log.error {
                            "Record with ID $customExtId already exists. You should mutate it directly. " +
                                "Record: ${config.id}@$customExtId"
                        }
                        error("Read permission denied for ${daoCtx.getGlobalRef(customExtId)}")
                    }
                }
                return daoCtx.recContentHandler.withContentDbDataAware {
                    val attsToCopy = DbRecord(daoCtx, entityToMutate, null).getAttsForCopy()
                    val newRec = LocalRecordAtts("", record.attributes.deepCopy())
                    attsToCopy.forEach { (k, v) ->
                        if (!newRec.hasAtt(k)) {
                            newRec.setAtt(k, v)
                        }
                    }
                    mutateRecordInTxn(
                        newRec,
                        typeInfo,
                        typeAttColumns
                    )
                }
            }
        }

        val isNewEntity = entityToMutate.id == DbEntity.NEW_REC_ID
        var workspaceRef = EntityRef.EMPTY
        if (isNewEntity && typeInfo.workspaceScope == WorkspaceScope.PRIVATE) {
            val mutWorkspace = record.getAtt(RecordConstants.ATT_WORKSPACE).asText()
            if (mutWorkspace.isEmpty()) {
                error(
                    "You should provide ${RecordConstants.ATT_WORKSPACE} attribute to create new record " +
                        "with private workspace scope. Type: '${typeInfo.id}'"
                )
            }
            workspaceRef = mutWorkspace.toEntityRef()
            if (!isRunAsSystem) {
                val workspaces = workspaceService.getUserWorkspaces(currentRunAsUser)
                if (!workspaces.contains(workspaceRef.getLocalId())) {
                    error("You can't create records in workspace $workspaceRef")
                }
            }
        }
        record.attributes.remove(RecordConstants.ATT_WORKSPACE)

        val currentUser = AuthContext.getCurrentUser()
        val currentUserRefId = daoCtx.getOrCreateUserRefId(currentUser)

        val nowInstant = Instant.now()
        if (disableAudit) {
            fun getUserId(user: String): Long {
                val userName = if (user.contains('@')) {
                    EntityRef.valueOf(user).getLocalId()
                } else {
                    user
                }
                return daoCtx.getOrCreateUserRefId(userName)
            }
            if (record.hasAtt(RecordConstants.ATT_MODIFIER)) {
                entityToMutate.modifier = getUserId(record.getAtt(RecordConstants.ATT_MODIFIER).asText())
            }
            if (record.hasAtt(RecordConstants.ATT_MODIFIED)) {
                record.getAtt(RecordConstants.ATT_MODIFIED).getAsInstant()?.let { entityToMutate.modified = it }
            }
            if (record.hasAtt(RecordConstants.ATT_CREATOR)) {
                entityToMutate.creator = getUserId(record.getAtt(RecordConstants.ATT_CREATOR).asText())
            }
            if (record.hasAtt(RecordConstants.ATT_CREATED)) {
                record.getAtt(RecordConstants.ATT_CREATED).getAsInstant()?.let { entityToMutate.created = it }
            }
            if (record.hasAtt(DbRecord.ATT_STATUS_MODIFIED)) {
                record.getAtt(DbRecord.ATT_STATUS_MODIFIED).getAsInstant()?.let {
                    entityToMutate.attributes[DbRecord.ATT_STATUS_MODIFIED] = it
                }
            }
            if (entityToMutate.creator == -1L) {
                entityToMutate.creator = getUserId(AuthUser.ANONYMOUS)
            }
            if (entityToMutate.modifier == -1L) {
                entityToMutate.modifier = getUserId(AuthUser.ANONYMOUS)
            }
        }

        var recordPerms = DbRecordPermsContext(DbRecordAllowedAllPerms)
        if (!isNewEntity && !isRunAsSystem) {
            if (!daoCtx.getUpdatedInTxnIds().contains(entityToMutate.extId)) {
                recordPerms = permsDao.getRecordPerms(entityToMutate.extId, currentRunAsUser, currentRunAsAuthorities)
                if (!recordPerms.hasWritePerms()) {
                    if (isMutationFromChild && recordPerms.getAdditionalPerms().contains("create-children")) {
                        val deniedAtts = HashSet<String>()
                        record.attributes.forEach { k, _ ->
                            if (k != RecMutAssocHandler.MUTATION_FROM_CHILD_FLAG) {
                                if (!k.startsWith(OperationType.ATT_ADD.prefix)) {
                                    deniedAtts.add(k)
                                } else {
                                    val assocName = k.substring(OperationType.ATT_ADD.prefix.length)
                                    val attDef = typeInfo.model.attributes.find { it.id == assocName }
                                    if (attDef == null || attDef.type != AttributeType.ASSOC || !attDef.config["child"].asBoolean()) {
                                        deniedAtts.add(k)
                                    }
                                }
                            }
                        }
                        if (deniedAtts.isNotEmpty()) {
                            error(
                                "Permissions Denied. " +
                                    "You can't change attributes $deniedAtts " +
                                    "for record '${daoCtx.getGlobalRef(record.id)}'"
                            )
                        }
                    } else {
                        error("Permissions Denied. You can't change record '${daoCtx.getGlobalRef(record.id)}'")
                    }
                }
            }
        }

        if (isNewEntity) {
            if (!config.insertable) {
                error("Records DAO doesn't support new records creation. Record ID: '${record.id}'")
            }
        } else {
            if (!config.updatable) {
                error("Records DAO doesn't support records updating. Record ID: '${record.id}'")
            }
        }

        if (entityToMutate.extId.isEmpty()) {
            entityToMutate.extId = UUID.randomUUID().toString()
        }
        if (isNewEntity) {
            val globalRef = daoCtx.getGlobalRef(entityToMutate.extId)
            entityToMutate.refId = recordRefService.getOrCreateIdByEntityRefs(listOf(globalRef))[0]
            if (workspaceRef.isNotEmpty()) {
                entityToMutate.workspace = wsDbService.getOrCreateId(workspaceRef.getLocalId())
            }
        }

        val recAttributes = record.attributes.deepCopy()

        if (isNewEntity && typeInfo.defaultStatus.isNotBlank() &&
            recAttributes[StatusConstants.ATT_STATUS].isEmpty()
        ) {
            recAttributes[StatusConstants.ATT_STATUS] = typeInfo.defaultStatus
        }

        if (recAttributes[StatusConstants.ATT_STATUS].isNotEmpty() && !disableAudit &&
            entityToMutate.status != recAttributes[StatusConstants.ATT_STATUS].asText()
        ) {
            recAttributes[DbRecord.ATT_STATUS_MODIFIED] = nowInstant
        }

        val mainContentAtt = DbRecord.getDefaultContentAtt(typeInfo)

        var contentVersionWasChanged = false
        var isVersionable = false
        for (aspectRef in currentAspectRefs) {
            if (aspectRef.getLocalId() == DbRecord.ASPECT_VERSIONABLE) {
                val aspectConfig = typeInfo.aspects.find {
                    it.ref.getLocalId() == DbRecord.ASPECT_VERSIONABLE
                }?.config
                if (aspectConfig?.get("disabled", false) != true) {
                    isVersionable = true
                }
            }
        }

        if (isVersionable && !isNewEntity && recAttributes.has(DbRecord.ATT_CONTENT_VERSION)) {
            val newContentVersionStr = recAttributes[DbRecord.ATT_CONTENT_VERSION].asText()
            val currentVersionStr = (entityToMutate.attributes[DbRecord.ATT_CONTENT_VERSION] as? String) ?: "1.0"
            val currentVersion = Version.valueOf(currentVersionStr)
            if (newContentVersionStr.startsWith("+")) {
                val newContentVersion = Version.valueOf(newContentVersionStr.substring(1))
                val versionPartsCount = newContentVersion.toString(0).count { it == '.' } + 1
                val resultVersion = currentVersion.truncateTo(versionPartsCount) + newContentVersion
                recAttributes[DbRecord.ATT_CONTENT_VERSION] = resultVersion
            } else {
                val newContentVersion = Version.valueOf(newContentVersionStr)
                if (newContentVersion <= currentVersion) {
                    error(
                        "Version downgrading is not supported. " +
                            "Record: ${daoCtx.getGlobalRef(entityToMutate.extId)} " +
                            "Before: '$currentVersion' After: '$newContentVersion'"
                    )
                }
            }
            contentVersionWasChanged = true
        }

        if (recAttributes.has(DbRecord.ATT_NAME) || recAttributes.has(ScalarType.DISP.mirrorAtt)) {
            val newName = if (record.attributes.has(DbRecord.ATT_NAME)) {
                record.attributes[DbRecord.ATT_NAME]
            } else {
                record.attributes[ScalarType.DISP.mirrorAtt]
            }
            recAttributes[ATT_CUSTOM_NAME] = newName
            recAttributes.remove(DbRecord.ATT_NAME)
            recAttributes.remove(ScalarType.DISP.mirrorAtt)
        }

        var contentAttToExtractName = ""
        if (recAttributes.has(RecordConstants.ATT_CONTENT)) {

            if (mainContentAtt.contains(".")) {
                error("Inner content uploading is not supported. Content attribute: '$mainContentAtt'")
            }
            val contentValue = recAttributes[RecordConstants.ATT_CONTENT]
            recAttributes[mainContentAtt] = contentValue
            recAttributes.remove(RecordConstants.ATT_CONTENT)

            val hasCustomNameAtt = typeInfo.model.attributes.find { it.id == ATT_CUSTOM_NAME } != null
            if (hasCustomNameAtt && recAttributes[ATT_CUSTOM_NAME].isEmpty()) {
                contentAttToExtractName = mainContentAtt
            }
        }

        val changedByOperationsAtts = mutableSetOf<String>()
        val operations = daoCtx.mutAttOperationHandler.extractAttValueOperations(recAttributes)
            .filter { !recAttributes.has(it.getAttName()) }

        val allAssocsValues = LinkedHashMap<String, DbAssocAttValuesContainer>()
        if (operations.isNotEmpty()) {
            val currentAtts: Map<String, Any?> = if (isNewEntity) {
                emptyMap()
            } else {
                DbRecord(daoCtx, entityToMutate).getAttsForOperations()
            }
            operations.forEach {
                val currentValue = currentAtts[it.getAttName()]
                val newValue = it.invoke(currentValue)
                if (newValue != currentValue) {
                    changedByOperationsAtts.add(it.getAttName())
                    recAttributes[it.getAttName()] = newValue
                } else if (newValue is DbAssocAttValuesContainer) {
                    changedByOperationsAtts.add(it.getAttName())
                    allAssocsValues[it.getAttName()] = newValue
                }
            }
        }

        val newAspects = if (recAttributes.has(DbRecord.ATT_ASPECTS)) {
            recAttributes[DbRecord.ATT_ASPECTS].asList(EntityRef::class.java)
        } else {
            currentAspectRefs
        }.toMutableSet()
        newAspects.addAll(
            ecosTypeService.getAspectsForAtts(
                recAttributes.fieldNamesList().filter { it.contains(":") }.toSet()
            )
        )
        if (isVersionable && recAttributes.has(mainContentAtt)) {
            newAspects.add(ModelUtils.getAspectRef(ASPECT_VERSIONABLE_DATA))
        }
        val addedAspects = newAspects.filter { !currentAspectRefs.contains(it) }
        val aspectsColumns = ecosTypeService.getColumnsForAspects(addedAspects)
        for (column in aspectsColumns) {
            addTypeAttColumn(column)
        }

        // type aspects should not be saved in DB
        newAspects.removeAll(typeAspects)
        if (aspectRefsInDb.isEmpty() && newAspects.isEmpty()) {
            recAttributes.remove(DbRecord.ATT_ASPECTS)
        } else {
            recAttributes[DbRecord.ATT_ASPECTS] = newAspects
        }

        if (!isRunAsSystem) {
            val deniedAtts = typeAttColumns.filter {
                it.systemAtt && recAttributes.has(it.attribute.id)
            }.map {
                it.attribute.id
            }
            if (deniedAtts.isNotEmpty()) {
                error("Permission denied. You should be in system context to change system attributes: $deniedAtts")
            }
        }

        daoCtx.mutAssocHandler.preProcessContentAtts(
            recAttributes,
            entityToMutate,
            typeAttColumns,
            contentDao.getContentStorage(typeInfo),
            currentUserRefId
        )

        recAttributes.forEach { att, newValue ->
            val attDef: EcosAttColumnDef = typeAttColumnsByAtt[att] ?: return@forEach
            if (DbRecordsUtils.isAssocLikeAttribute(attDef.attribute)) {
                if (!allAssocsValues.containsKey(att)) {
                    val valuesBefore = if (isNewEntity) {
                        emptyList()
                    } else {
                        assocsService.getTargetAssocs(entityToMutate.refId, att, DbFindPage(0, 100))
                            .entities.map { it.targetId }
                    }
                    if (valuesBefore.size == 100) {
                        error(
                            "You can't edit large associations by providing full values list. " +
                                "Please, use att_add_... and att_rem_... to work with it. " +
                                "Assoc: $att Record: ${daoCtx.getGlobalRef(entityToMutate.extId)}"
                        )
                    }
                    val refsBefore = recordRefService.getEntityRefsByIds(valuesBefore).map {
                        it.toString()
                    }.toSet()

                    val assocValuesContainer = DbAssocAttValuesContainer(
                        daoCtx,
                        refsBefore,
                        DbRecordsUtils.isChildAssocAttribute(attDef.attribute),
                        attDef.attribute.multiple
                    )
                    allAssocsValues[att] = assocValuesContainer

                    val newValuesStrings = DbAttValueUtils.anyToSetOfStrings(newValue)
                    val added = newValuesStrings.filterTo(LinkedHashSet()) {
                        !refsBefore.contains(it)
                    }
                    assocValuesContainer.addAll(added)

                    val removed = refsBefore.filterTo(LinkedHashSet()) {
                        !newValuesStrings.contains(it)
                    }
                    assocValuesContainer.removeAll(removed)
                }
            } else if (attDef.attribute.type == AttributeType.OPTIONS && recAttributes.has(att)) {
                val value = recAttributes[att]
                if (value.isTextual() && value.asText().isEmpty()) {
                    recAttributes[att] = DataValue.NULL
                }
            }
        }

        if (isVersionable && recAttributes.has(mainContentAtt)) {
            val contentAfter = recAttributes[mainContentAtt].asLong(-1)
            val contentBefore = entityToMutate.attributes[mainContentAtt] as? Long ?: -1
            if (contentAfter != contentBefore) {
                val contentWasChanged = if (contentBefore == -1L || contentAfter == -1L) {
                    true
                } else {
                    val before = daoCtx.contentService?.getContent(contentBefore)
                    val after = daoCtx.contentService?.getContent(contentAfter)
                    before?.getDataKey() != after?.getDataKey() || before?.getStorageRef() != after?.getStorageRef()
                }
                if (contentWasChanged) {
                    if (recAttributes[DbRecord.ATT_CONTENT_VERSION].asText().isBlank()) {
                        if (contentBefore == -1L) {
                            recAttributes[DbRecord.ATT_CONTENT_VERSION] = "1.0"
                        } else {
                            val currentVersionStr =
                                entityToMutate.attributes[DbRecord.ATT_CONTENT_VERSION] as? String ?: "1.0"
                            val currentMajorVersion = Version.valueOf(currentVersionStr).truncateTo(1)
                            val newVersion = currentMajorVersion + Version.valueOf("1.0")
                            recAttributes[DbRecord.ATT_CONTENT_VERSION] = newVersion.toString()
                        }
                    }
                    contentVersionWasChanged = true
                }
            }
        }
        if (isVersionable && contentVersionWasChanged && !recAttributes.has(DbRecord.ATT_CONTENT_VERSION_COMMENT)) {
            recAttributes[DbRecord.ATT_CONTENT_VERSION_COMMENT] = ""
        }

        if (contentAttToExtractName.isNotBlank() && recordPerms.hasAttWritePerms(ATT_CUSTOM_NAME)) {
            val attribute = recAttributes[contentAttToExtractName]
            if (attribute.isNumber()) {
                contentService?.getContent(attribute.asLong())?.getName()?.let {
                    recAttributes[ATT_CUSTOM_NAME] = it
                }
            }
        }

        daoCtx.mutAssocHandler.replaceRefsById(recAttributes, typeAttColumns)

        val recordEntityBeforeMutation = entityToMutate.copy()

        val fullColumns = ArrayList(typeColumns)
        val perms = if (isNewEntity || isRunAsSystem) {
            null
        } else {
            permsDao.getRecordPerms(entityToMutate.extId)
        }
        val changedAssocs = ArrayList<DbAssocRefsDiff>()
        setMutationAtts(
            entityToMutate,
            recAttributes,
            typeColumns,
            changedAssocs,
            isAssocForceDeletion,
            currentUserRefId,
            perms,
            allAssocsValues
        )
        val optionalAtts = DbRecord.OPTIONAL_COLUMNS.filter { !typeColumnNames.contains(it.name) }
        if (optionalAtts.isNotEmpty()) {
            fullColumns.addAll(
                setMutationAtts(
                    entityToMutate,
                    recAttributes,
                    optionalAtts,
                    changedAssocs,
                    isAssocForceDeletion,
                    currentUserRefId
                )
            )
        }

        if (recAttributes.has(ATT_STATE)) {
            val state = recAttributes[ATT_STATE].asText()
            entityToMutate.attributes[DbRecord.COLUMN_IS_DRAFT.name] = state == "draft"
            fullColumns.add(DbRecord.COLUMN_IS_DRAFT)
        }

        entityToMutate.type = recordRefService.getOrCreateIdByEntityRef(ModelUtils.getTypeRef(typeInfo.id))

        if (recAttributes.has(StatusConstants.ATT_STATUS)) {
            val newStatus = recAttributes[StatusConstants.ATT_STATUS].asText()
            if (newStatus.isNotBlank()) {
                if (typeInfo.model.statuses.any { it.id == newStatus }) {
                    entityToMutate.status = newStatus
                } else {
                    error(
                        "Unknown status: '$newStatus'. " +
                            "Available statuses: ${typeInfo.model.statuses.joinToString { it.id }}"
                    )
                }
            }
        }

        if (!disableAudit) {
            val auditIgnoredAtts = typeInfo.model.systemAttributes.mapTo(HashSet()) { it.id }
            if (!entityToMutate.equals(entityBeforeMutation, auditIgnoredAtts)) {
                if (isNewEntity) {
                    entityToMutate.created = nowInstant
                    entityToMutate.creator = currentUserRefId
                }
                entityToMutate.modified = nowInstant
                entityToMutate.modifier = currentUserRefId
            }
        }

        val computedAttsComponent = computedAttsComponent
        if (computedAttsComponent != null) {
            dataService.doWithPermsPolicy(QueryPermsPolicy.PUBLIC) {
                AuthContext.runAsSystem {
                    val fullColumnNames = fullColumns.mapTo(HashSet()) { it.name }
                    computeAttsToStore(
                        computedAttsComponent,
                        entityToMutate,
                        isNewEntity,
                        typeInfo.id,
                        fullColumns,
                        currentUserRefId,
                        typeAttColumns
                    ).forEach {
                        if (!fullColumnNames.contains(it.name)) {
                            fullColumns.add(it)
                        }
                    }
                }
            }
        }

        daoCtx.mutAssocHandler.validateChildAssocs(
            record.attributes,
            changedByOperationsAtts,
            entityToMutate.extId,
            typeAttColumns
        )

        if (changedAssocs.isEmpty()) {
            val equalsIgnoredAtts = if (disableAudit) emptySet() else AUDIT_ATTS
            if (entityToMutate.equals(entityBeforeMutation, equalsIgnoredAtts)) {
                return entityBeforeMutation
            }
        }

        val recAfterSave = dataService.save(entityToMutate, fullColumns)

        daoCtx.mutAssocHandler.processChildrenAfterMutation(
            recordEntityBeforeMutation,
            recAfterSave,
            record.attributes,
            typeAttColumns,
            allAssocsValues,
            disableEvents
        )
        daoCtx.mutAssocHandler.processParentAfterMutation(
            recordEntityBeforeMutation,
            recAfterSave,
            record.attributes,
            disableEvents
        )

        val meta = daoCtx.getEntityMeta(recAfterSave)
        daoCtx.remoteActionsClient?.updateRemoteAssocs(daoCtx.tableCtx, meta.globalRef, currentUser, changedAssocs)

        if (!disableEvents) {
            daoCtx.recEventsHandler.emitEventsAfterMutation(
                recordEntityBeforeMutation,
                recAfterSave,
                meta,
                isNewEntity,
                changedAssocs
            )
        }

        return recAfterSave
    }

    private fun computeAttsToStore(
        component: DbComputedAttsComponent,
        entity: DbEntity,
        isNewRecord: Boolean,
        recTypeId: String,
        columns: List<DbColumnDef>,
        currentUserRefId: Long,
        typeAttColumns: List<EcosAttColumnDef>
    ): List<DbColumnDef> {

        val typeRef = ModelUtils.getTypeRef(recTypeId)
        val atts = component.computeAttsToStore(DbRecord(daoCtx, entity), isNewRecord, typeRef)

        daoCtx.mutAssocHandler.replaceRefsById(atts, typeAttColumns)

        val fullColumns = ArrayList(columns)
        DbRecord.COMPUTABLE_OPTIONAL_COLUMNS.forEach {
            if (atts.has(it.name)) {
                fullColumns.add(it)
            }
        }
        val mutatedColumns = setMutationAtts(
            entity,
            atts,
            fullColumns,
            ArrayList(),
            true,
            currentUserRefId
        )
        entity.name = component.computeDisplayName(DbRecord(daoCtx, entity), typeRef)

        return mutatedColumns
    }

    private fun getTypeIdForRecord(record: LocalRecordAtts): String {

        val typeRefStr = record.attributes[RecordConstants.ATT_TYPE].asText().ifBlank {
            // legacy type attribute
            record.attributes["_etype"].asText()
        }

        val typeRefFromAtts = EntityRef.valueOf(typeRefStr).getLocalId()
        if (typeRefFromAtts.isNotBlank()) {
            return typeRefFromAtts
        }

        val extId = record.id.ifBlank { record.attributes[ATT_ID].asText() }
        if (extId.isNotBlank()) {
            val entity = dataService.doWithPermsPolicy(QueryPermsPolicy.PUBLIC) {
                dataService.findByExtId(extId)
            }
            val typeId: String = if (entity != null) {
                if (entity.type > -1L) {
                    recordRefService.getEntityRefById(entity.type).getLocalId()
                } else if (entity.legacyType.isNotBlank()) {
                    entity.legacyType
                } else {
                    ""
                }
            } else {
                ""
            }
            if (typeId.isNotBlank()) {
                return typeId
            }
        }

        if (EntityRef.isNotEmpty(config.typeRef)) {
            return config.typeRef.getLocalId()
        }

        error(
            "${RecordConstants.ATT_TYPE} attribute is mandatory for mutation. " +
                "SourceId: '${config.id}' Record: ${record.id}"
        )
    }

    private fun setMutationAtts(
        recToMutate: DbEntity,
        atts: ObjectData,
        columns: List<DbColumnDef>,
        changedAssocs: MutableList<DbAssocRefsDiff>,
        isAssocForceDeletion: Boolean,
        currentUserRefId: Long,
        perms: DbRecordPermsContext? = null,
        multiAssocValues: Map<String, DbAssocAttValuesContainer> = emptyMap()
    ): List<DbColumnDef> {

        if (atts.isEmpty() && multiAssocValues.isEmpty()) {
            return emptyList()
        }
        val currentUser = AuthContext.getCurrentUser()

        val notEmptyColumns = ArrayList<DbColumnDef>()
        for (dbColumnDef in columns) {
            if (!atts.has(dbColumnDef.name) && !multiAssocValues.containsKey(dbColumnDef.name)) {
                continue
            }
            if (perms?.hasAttWritePerms(dbColumnDef.name) == false) {
                log.warn {
                    "User $currentUser can't change attribute ${dbColumnDef.name} " +
                        "for record ${config.id}@${recToMutate.extId}"
                }
            } else {
                notEmptyColumns.add(dbColumnDef)

                val multiAssocValue = multiAssocValues[dbColumnDef.name]
                if (multiAssocValue != null) {

                    val removedTargetIds = assocsService.removeAssocs(
                        recToMutate.refId,
                        dbColumnDef.name,
                        multiAssocValue.getRemovedTargetIds(),
                        isAssocForceDeletion
                    )
                    val addedTargetIds = assocsService.createAssocs(
                        recToMutate.refId,
                        dbColumnDef.name,
                        multiAssocValue.child,
                        multiAssocValue.getAddedTargetsIds(),
                        currentUserRefId
                    )

                    if (removedTargetIds.isNotEmpty() || addedTargetIds.isNotEmpty()) {
                        val maxAssocs = if (dbColumnDef.multiple) {
                            10
                        } else {
                            1
                        }
                        val targetIds = assocsService.getTargetAssocs(
                            recToMutate.refId,
                            dbColumnDef.name,
                            DbFindPage(0, maxAssocs)
                        ).entities.map { it.targetId }

                        recToMutate.attributes[dbColumnDef.name] = daoCtx.mutConverter.convert(
                            DataValue.create(targetIds),
                            dbColumnDef.multiple,
                            dbColumnDef.type
                        )
                        val changedAssocIds = HashSet<Long>(addedTargetIds.size + removedTargetIds.size)
                        changedAssocIds.addAll(addedTargetIds)
                        changedAssocIds.addAll(removedTargetIds)
                        val refsByIds = daoCtx.recordRefService.getEntityRefsByIdsMap(changedAssocIds)

                        changedAssocs.add(
                            DbAssocRefsDiff(
                                dbColumnDef.name,
                                addedTargetIds.mapNotNull { refsByIds[it] },
                                removedTargetIds.mapNotNull { refsByIds[it] },
                                multiAssocValue.child
                            )
                        )
                    }
                } else if (dbColumnDef.name == RecordConstants.ATT_PARENT_ATT) {
                    val textAtt = atts[dbColumnDef.name].asText()
                    val attributeId = daoCtx.assocsService.getIdForAtt(textAtt, true)
                    recToMutate.attributes[dbColumnDef.name] = attributeId
                } else {
                    recToMutate.attributes[dbColumnDef.name] = daoCtx.mutConverter.convert(
                        atts[dbColumnDef.name],
                        dbColumnDef.multiple,
                        dbColumnDef.type
                    )
                }
            }
        }
        return notEmptyColumns
    }
}
