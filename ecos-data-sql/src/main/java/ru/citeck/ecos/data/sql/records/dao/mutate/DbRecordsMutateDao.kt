package ru.citeck.ecos.data.sql.records.dao.mutate

import io.github.oshai.kotlinlogging.KotlinLogging
import ru.citeck.ecos.commons.data.DataValue
import ru.citeck.ecos.commons.data.ObjectData
import ru.citeck.ecos.commons.data.Version
import ru.citeck.ecos.commons.exception.I18nRuntimeException
import ru.citeck.ecos.commons.json.Json
import ru.citeck.ecos.commons.utils.DataUriUtil
import ru.citeck.ecos.context.lib.auth.AuthContext
import ru.citeck.ecos.context.lib.auth.AuthUser
import ru.citeck.ecos.context.lib.auth.data.AuthData
import ru.citeck.ecos.data.sql.columnmeta.DbExpectedAttTypes
import ru.citeck.ecos.data.sql.content.DbContentService
import ru.citeck.ecos.data.sql.dto.DbColumnDef
import ru.citeck.ecos.data.sql.ecostype.DbEcosModelService
import ru.citeck.ecos.data.sql.ecostype.EcosAttColumnDef
import ru.citeck.ecos.data.sql.perms.DbEntityPermsService
import ru.citeck.ecos.data.sql.props.DbEcosDataProps
import ru.citeck.ecos.data.sql.records.DbRecordsControlAtts
import ru.citeck.ecos.data.sql.records.DbRecordsDaoConfig
import ru.citeck.ecos.data.sql.records.DbRecordsUtils
import ru.citeck.ecos.data.sql.records.assocs.DbAssocEntity
import ru.citeck.ecos.data.sql.records.assocs.DbAssocRefsDiff
import ru.citeck.ecos.data.sql.records.assocs.DbAssocsService
import ru.citeck.ecos.data.sql.records.computed.DbComputedAttsComponent
import ru.citeck.ecos.data.sql.records.dao.DbRecordsDaoCtx
import ru.citeck.ecos.data.sql.records.dao.DbRecordsDaoCtxAware
import ru.citeck.ecos.data.sql.records.dao.atts.DbAssocAttValuesContainer
import ru.citeck.ecos.data.sql.records.dao.atts.DbRecord
import ru.citeck.ecos.data.sql.records.dao.atts.DbRecord.Companion.ATT_CUSTOM_ID
import ru.citeck.ecos.data.sql.records.dao.content.DbRecordsContentDao
import ru.citeck.ecos.data.sql.records.dao.delete.DbRecordsDeleteDao
import ru.citeck.ecos.data.sql.records.dao.mutate.operation.OperationType
import ru.citeck.ecos.data.sql.records.dao.perms.DbRecordsPermsDao
import ru.citeck.ecos.data.sql.records.listener.DbRecordRefChangedEvent
import ru.citeck.ecos.data.sql.records.listener.DbRecordTypeChangedEvent
import ru.citeck.ecos.data.sql.records.perms.DbRecordAllowedAllPerms
import ru.citeck.ecos.data.sql.records.perms.DbRecordPermsContext
import ru.citeck.ecos.data.sql.records.refs.DbRecordRefService
import ru.citeck.ecos.data.sql.records.utils.DbAttValueUtils
import ru.citeck.ecos.data.sql.records.workspace.DbWorkspaceDesc
import ru.citeck.ecos.data.sql.records.workspace.DbWorkspaceService
import ru.citeck.ecos.data.sql.repo.entity.DbEntity
import ru.citeck.ecos.data.sql.repo.find.DbFindPage
import ru.citeck.ecos.data.sql.service.DbDataService
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType
import ru.citeck.ecos.model.lib.status.constants.StatusConstants
import ru.citeck.ecos.model.lib.type.dto.QueryPermsPolicy
import ru.citeck.ecos.model.lib.type.dto.TypeInfo
import ru.citeck.ecos.model.lib.type.dto.WorkspaceScope
import ru.citeck.ecos.model.lib.utils.ModelUtils
import ru.citeck.ecos.model.lib.workspace.IdInWs
import ru.citeck.ecos.model.lib.workspace.WorkspaceService
import ru.citeck.ecos.records2.RecordConstants
import ru.citeck.ecos.records2.predicate.model.Predicates
import ru.citeck.ecos.records3.record.atts.dto.LocalRecordAtts
import ru.citeck.ecos.records3.record.atts.dto.RecordAtts
import ru.citeck.ecos.records3.record.atts.schema.ScalarType
import ru.citeck.ecos.records3.record.atts.schema.annotation.AttName
import ru.citeck.ecos.txn.lib.TxnContext
import ru.citeck.ecos.webapp.api.constants.AppName
import ru.citeck.ecos.webapp.api.entity.EntityRef
import ru.citeck.ecos.webapp.api.entity.toEntityRef
import java.time.Instant
import java.util.*
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

        private val TYPE_UPDATE_CONFLICTING_ATTS = listOf(
            DbRecordsControlAtts.UPDATE_ID,
            DbRecordsControlAtts.UPDATE_WORKSPACE,
            DbRecordsControlAtts.UPDATE_PERMISSIONS,
            DbRecordsControlAtts.UPDATE_CALCULATED_ATTS
        )

        private val MUT_ATTS_MAPPING = mapOf(
            // allow to use _localId as key for custom id for backward compatibility
            ScalarType.LOCAL_ID.mirrorAtt to ATT_ID
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
    private lateinit var dataProps: DbEcosDataProps

    private lateinit var allowedRecordIdPattern: String
    private lateinit var allowedRecordIdRegex: Regex
    private var recordIdMaxLength = 100

    private val recsPrepareToCommitTxnKey = Any()

    override fun setRecordsDaoCtx(recordsDaoCtx: DbRecordsDaoCtx) {

        daoCtx = recordsDaoCtx
        config = daoCtx.config

        allowedRecordIdPattern = config.allowedRecordIdPattern
        allowedRecordIdRegex = allowedRecordIdPattern.toRegex()
        recordIdMaxLength = config.recordIdMaxLength

        dataService = daoCtx.dataService
        recordRefService = daoCtx.recordRefService
        ecosTypeService = daoCtx.ecosTypeService
        assocsService = daoCtx.assocsService
        permsDao = daoCtx.permsDao
        contentDao = daoCtx.contentDao
        workspaceService = daoCtx.workspaceService
        wsDbService = dataService.getTableContext().getWorkspaceService()

        dataProps = daoCtx.tableCtx.getSchemaCtx().dataSourceCtx.props

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
            val resultEntity = mutateRecordInTxn(record)
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

    private fun throwRecordNotFound(extId: String) {
        error("Record ${daoCtx.getGlobalRef(extId)} was not found")
    }

    private fun throwAccessDenied(extId: String) {
        error("Access denied to record ${daoCtx.getGlobalRef(extId)}")
    }

    private fun isCurrentUserHasReadPermsForEntity(entity: DbEntity): Boolean {
        if (AuthContext.isRunAsSystem()) {
            return true
        }
        return DbRecord(daoCtx, entity).isCurrentUserHasReadPerms()
    }

    private fun mutateRecordInTxn(
        record: LocalRecordAtts,
        typeInfo: TypeInfo? = null,
        typeAttColumnsArg: List<EcosAttColumnDef>? = null,
    ): DbEntity? {

        val isMutationFromChild = record.getAtt(RecMutAssocHandler.MUTATION_FROM_CHILD_FLAG).asBoolean()

        if (record.id.isNotBlank() &&
            isMutationFromChild &&
            daoCtx.getRecsCurrentlyInDeletion().contains(daoCtx.getGlobalRef(record.id))
        ) {
            return null
        }

        val (record, isFullAtts) = expandFullAttsIfExists(record)

        if (record.attributes.has(DbRecord.ATT_ASPECTS)) {
            error(
                "Aspects can't be changed by ${DbRecord.ATT_ASPECTS} attribute. " +
                    "Please use att_add_${DbRecord.ATT_ASPECTS} and att_rem_${DbRecord.ATT_ASPECTS} to change aspects"
            )
        }

        MUT_ATTS_MAPPING.forEach { (key, value) ->
            if (record.hasAtt(key) && !record.hasAtt(value)) {
                record.setAtt(value, record.getAtt(key))
                record.attributes.remove(key)
            }
        }

        val isAssocForceDeletion = record.getAtt(DbRecordsDeleteDao.ASSOC_FORCE_DELETION_FLAG)
            .asBoolean(true)

        var typeInfo = typeInfo ?: ecosTypeService.getTypeInfoNotNull(getTypeIdForRecord(record))

        val runAsAuth = AuthContext.getCurrentRunAsAuth()
        val isRunAsSystem = AuthContext.isSystemAuth(runAsAuth)
        val isRunAsAdmin = AuthContext.isAdminAuth(runAsAuth)
        val isRunAsSystemOrAdmin = isRunAsSystem || isRunAsAdmin

        val isMutationFromParent = record.getAtt(RecMutAssocHandler.MUTATION_FROM_PARENT_FLAG).asBoolean()

        val mutComputeContext = MutationComputeContext(
            daoCtx = daoCtx,
            counterAttsToUpdate = extractCountersToUpdate(record.attributes)
        )

        // The workspace this mutation aims its record at, and whether this user may put a record
        // there - settled before anything with a side effect happens.
        //
        // Resolving a localIdTemplate that names a COUNTER attribute takes the next number from the
        // model application, in a transaction of its own, and the rollback of a refused mutation
        // does not give it back - so a creation denied for want of rights used to leave a hole in
        // the numbering (COREDEV-517). Every record is asked, not just the numbered ones: what is
        // being avoided is a side effect before a refusal, and the id resolution is only the one
        // that bites today.
        //
        // A mutation that names no record id is *asking* to create, which is not the same as
        // creating - an id built from the attributes can address a record that already exists. So
        // only what the request itself can answer is answered here: a workspace it names, through
        // `_parent`, `_workspace` or the type's default. Naming none is not an error yet, and the
        // question goes on to the branch below, which by then knows whether there is a record to
        // update.
        val requestedWorkspace = if (record.id.isEmpty() && typeInfo.workspaceScope == WorkspaceScope.PRIVATE) {
            resolveRequestedWorkspace(
                record = record,
                typeInfo = typeInfo,
                isMutationFromParent = isMutationFromParent,
                runAsAuth = runAsAuth,
                isRunAsSystem = isRunAsSystem
            )
        } else {
            null
        }

        val extIdFromAtts = getExtIdFromAtts(
            typeInfo = typeInfo,
            record = record,
            mutComputeCtx = mutComputeContext
        )

        val entityToMutate: DbEntity = if (record.id.isNotEmpty()) {
            val entity = daoCtx.attsDao.findDbEntityByExtId(record.id, checkPerms = false)
            if (entity == null) {
                throwRecordNotFound(record.id)
            } else if (!isCurrentUserHasReadPermsForEntity(entity)) {
                throwAccessDenied(record.id)
            }
            entity!!
        } else if (extIdFromAtts.isNotEmpty()) {
            val entity = daoCtx.attsDao.findDbEntityByExtId(extIdFromAtts, checkPerms = false)
            if (entity != null) {
                if (!isCurrentUserHasReadPermsForEntity(entity)) {
                    throwAccessDenied(extIdFromAtts)
                }
                if (!isRunAsSystem && !isFullAtts) {
                    error("Record '${daoCtx.getGlobalRef(extIdFromAtts)}' already exists. The id must be unique.")
                }
                entity
            } else {
                DbEntity()
            }
        } else {
            DbEntity()
        }

        val isNewEntity = entityToMutate.id == DbEntity.NEW_REC_ID

        if (!isNewEntity) {
            val typeIdFromDb = if (entityToMutate.type > -1L) {
                recordRefService.getEntityRefById(entityToMutate.type).getLocalId()
            } else {
                entityToMutate.legacyType.ifBlank { "" }
            }
            if (typeIdFromDb.isNotEmpty() && typeIdFromDb != typeInfo.id) {
                typeInfo = ecosTypeService.getTypeInfoNotNull(typeIdFromDb)
                if (record.id.isBlank()) {
                    val extIdForTypeFromDb = getExtIdFromAtts(
                        typeInfo = typeInfo,
                        record = record,
                        mutComputeCtx = mutComputeContext
                    )
                    if (extIdForTypeFromDb != extIdFromAtts) {
                        error(
                            "Identifier from atts '$extIdFromAtts' doesn't match " +
                                "identifier from database '$extIdForTypeFromDb'"
                        )
                    }
                }
            }
        }

        if (isFullAtts) {
            setNotPresentAttsAsNull(record.attributes, typeInfo.model.attributes)
        }

        val disableAudit = record.getAtt(DbRecordsControlAtts.DISABLE_AUDIT).asBoolean()
        if (disableAudit && !isRunAsSystem) {
            error("${DbRecordsControlAtts.DISABLE_AUDIT} attribute can't be used outside of system context")
        }
        val disableEvents = record.getAtt(DbRecordsControlAtts.DISABLE_EVENTS).asBoolean()
        if (disableEvents && !isRunAsSystem) {
            error("${DbRecordsControlAtts.DISABLE_EVENTS} attribute can't be used outside of system context")
        }

        val currentUser = AuthContext.getCurrentUser()
        val currentUserRefId = daoCtx.getOrCreateUserRefId(currentUser)

        val mutCtx = MutationContext(
            record = record,
            typeInfo = typeInfo,
            entityToMutate = entityToMutate,
            currentUser = currentUser,
            currentUserRefId = currentUserRefId,
            typeAttColumns = ArrayList(
                typeAttColumnsArg ?: run {
                    ecosTypeService.getColumnsForTypes(listOf(typeInfo))
                }
            ),
            frozenColumnsProvider = {
                val tableAttTypes = ecosTypeService.getTableAttTypes(typeInfo)
                DbExpectedAttTypes.ConflictsInfo(tableAttTypes.conflicts, tableAttTypes.groupingMayBeOverBroad)
            },
            isRunAsSystemOrAdmin = isRunAsSystemOrAdmin,
            disableEvents = disableEvents,
            isNewEntity = isNewEntity,
            extIdFromAtts = extIdFromAtts,
            computeContext = mutComputeContext
        )

        val currentRunAsUser = runAsAuth.getUser()
        val currentRunAsAuthorities = DbRecordsUtils.getCurrentAuthorities(runAsAuth)

        val currentAspectRefs = LinkedHashSet(mutCtx.typeAspects)
        val aspectRefsInDb = LinkedHashSet<EntityRef>()

        if (!isNewEntity) {
            val aspects = entityToMutate.attributes[DbRecord.ATT_ASPECTS]
            if (aspects != null && aspects is Collection<*>) {
                val aspectIds = aspects.mapNotNull { it as? Long }
                aspectRefsInDb.addAll(daoCtx.recordRefService.getEntityRefsByIds(aspectIds))
                currentAspectRefs.addAll(aspectRefsInDb)
                val aspectsColumns = ecosTypeService.getColumnsForAspects(aspectRefsInDb)
                for (column in aspectsColumns) {
                    mutCtx.addTypeAttColumn(column)
                }
            }
            handleControlAtts(mutCtx)?.let { return it }
        }

        val entityBeforeMutation = entityToMutate.copy()

        handleRecordCustomId(mutCtx)?.let { return it }

        // Handle workspace

        var workspaceRef = EntityRef.EMPTY
        var isRunAsSystemOrWsSystem = isRunAsSystem
        if (typeInfo.workspaceScope == WorkspaceScope.PRIVATE) {
            if (isNewEntity) {
                // The question was asked above: a new entity means the mutation named no record id,
                // and `typeInfo` is only ever replaced in the branch that requires an existing one,
                // so the scope was already PRIVATE when the asking was decided. Null is therefore
                // the answer "the request named no workspace", not "nobody asked" - and for a record
                // that really is being created, that is the error it always was.
                val requested = requestedWorkspace ?: error(
                    "You should provide ${RecordConstants.ATT_WORKSPACE} attribute to create new " +
                        "record with private workspace scope. Type: '${typeInfo.id}'"
                )
                workspaceRef = requested.ref
                isRunAsSystemOrWsSystem = requested.isRunAsSystemOrWsSystem
            } else if (!isRunAsSystemOrWsSystem) {
                val workspaceId = entityBeforeMutation.workspace ?: -1L
                if (workspaceId >= 0L) {
                    isRunAsSystemOrWsSystem = workspaceService.isRunAsSystemOrWsSystem(
                        wsDbService.getWorkspaceExtIdById(workspaceId)
                    )
                }
            }
        }
        record.attributes.remove(RecordConstants.ATT_WORKSPACE)

        // Handle audit props

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

        // Permissions Check

        var recordPerms = DbRecordPermsContext(DbRecordAllowedAllPerms)
        if (!isNewEntity && !isRunAsSystemOrWsSystem && !daoCtx.getUpdatedInTxnIds().contains(entityToMutate.extId)) {
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
                        throw I18nRuntimeException(
                            "ecos-data.permission-denied.attributes",
                            mapOf(
                                "attributes" to deniedAtts,
                                "recordRef" to daoCtx.getGlobalRef(record.id)
                            )
                        )
                    }
                } else {
                    throw I18nRuntimeException(
                        "ecos-data.permission-denied.record",
                        mapOf("recordRef" to daoCtx.getGlobalRef(record.id))
                    )
                }
            }
        }

        // Settings Check

        if (isNewEntity) {
            if (!config.insertable) {
                error(
                    "Records DAO doesn't support new records creation. " +
                        "Record ID: '${daoCtx.getGlobalRef(record.id)}'"
                )
            }
        } else {
            if (!config.updatable) {
                error(
                    "Records DAO doesn't support records updating. " +
                        "Record: '${daoCtx.getGlobalRef(record.id)}'"
                )
            }
        }

        // Record type updating

        handleTypeUpdate(
            mutCtx = mutCtx,
            aspectRefsInDb = aspectRefsInDb,
            nowInstant = nowInstant,
            disableAudit = disableAudit
        )?.let { return it }

        if (entityToMutate.extId.isEmpty()) {
            entityToMutate.extId = UUID.randomUUID().toString()
        }

        val globalRef = daoCtx.getGlobalRef(entityToMutate.extId)
        if (isNewEntity) {
            entityToMutate.refId = recordRefService.getOrCreateIdByEntityRefs(listOf(globalRef))[0]
            if (workspaceRef.isNotEmpty() && workspaceRef.getLocalId() != DbRecord.WS_DEFAULT) {
                entityToMutate.workspace = wsDbService.getOrCreateId(workspaceRef.getLocalId())
            }
        }

        val recAttributes = record.attributes.deepCopy()

        if (isNewEntity &&
            typeInfo.defaultStatus.isNotBlank() &&
            recAttributes[StatusConstants.ATT_STATUS].isEmpty()
        ) {
            recAttributes[StatusConstants.ATT_STATUS] = typeInfo.defaultStatus
        }

        if (recAttributes[StatusConstants.ATT_STATUS].isNotEmpty() &&
            !disableAudit &&
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
                            "Record: $globalRef " +
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
                error(
                    "Inner content uploading is not supported. " +
                        "Content attribute: '$mainContentAtt'. Record: $globalRef"
                )
            }
            val contentValue = recAttributes[RecordConstants.ATT_CONTENT]
            recAttributes[mainContentAtt] = contentValue
            recAttributes.remove(RecordConstants.ATT_CONTENT)

            val hasCustomNameAtt = typeInfo.model.attributes.any { it.id == ATT_CUSTOM_NAME }
            if (hasCustomNameAtt && recAttributes[ATT_CUSTOM_NAME].isEmpty()) {
                contentAttToExtractName = mainContentAtt
            }
        }

        // Before the operations are extracted, because this one takes its own out of the mutation:
        // a child telling its parent it is gone, for an attribute whose links a column migration
        // has parked. The parent has no association to remove anything from and the snapshot is
        // what has to stop naming the child.
        val parkedChildAtts = daoCtx.mutAssocHandler.forgetParkedChildLinks(
            recAttributes,
            entityToMutate,
            mutCtx.typeAttColumns
        )

        val changedByOperationsAtts = mutableSetOf<String>()
        val operations = daoCtx.mutAttOperationHandler.extractAttValueOperations(recAttributes)
            .filter { !recAttributes.has(it.getAttName()) }

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
                    mutCtx.allAssocsValues[it.getAttName()] = newValue
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
            mutCtx.addTypeAttColumn(column)
        }

        // type aspects should not be saved in DB
        newAspects.removeAll(mutCtx.typeAspects)
        if (aspectRefsInDb.isEmpty() && newAspects.isEmpty()) {
            recAttributes.remove(DbRecord.ATT_ASPECTS)
        } else {
            recAttributes[DbRecord.ATT_ASPECTS] = newAspects
        }
        if (!isRunAsSystemOrWsSystem) {
            val deniedAtts = mutCtx.typeAttColumns.filter {
                it.systemAtt && recAttributes.has(it.attribute.id)
            }.map {
                it.attribute.id
            }
            if (deniedAtts.isNotEmpty()) {
                error(
                    "Permission denied. You should be in system context " +
                        "to change system attributes: $deniedAtts. Record: $globalRef"
                )
            }
            if (recAttributes.has(DbRecord.ATT_VISIBLE_IN_WORKSPACES)) {
                error(
                    "Permission denied. You should be in system context " +
                        "to change attribute: ${DbRecord.ATT_VISIBLE_IN_WORKSPACES}. Record: $globalRef"
                )
            }
        }

        daoCtx.mutAssocHandler.preProcessContentAtts(
            recAttributes,
            entityToMutate,
            mutCtx.typeAttColumns,
            contentDao.getContentStorage(typeInfo),
            currentUserRefId
        )

        recAttributes.forEach { att, newValue ->
            val attDef: EcosAttColumnDef = mutCtx.typeAttColumnsByAtt[att] ?: return@forEach
            if (DbRecordsUtils.isStoredInAssocsTable(attDef.attribute.type)) {
                registerAssocValuesContainerIfRequired(att, newValue, mutCtx)
            } else if (attDef.attribute.type == AttributeType.OPTIONS && recAttributes.has(att)) {
                val value = recAttributes[att]
                if (value.isTextual() && value.asText().isEmpty()) {
                    recAttributes[att] = DataValue.NULL
                } else if (value.isArray() && value.any { !it.isTextual() || it.asText().isEmpty() }) {
                    val valueWithoutEmptyValues = DataValue.createArr()
                    for (element in value) {
                        if (element.isTextual() && element.asText().isNotEmpty()) {
                            valueWithoutEmptyValues.add(element)
                        }
                    }
                    recAttributes[att] = valueWithoutEmptyValues
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

        daoCtx.mutAssocHandler.replaceRefsById(recAttributes, mutCtx.typeAttColumns)

        val recordEntityBeforeMutation = entityToMutate.copy()

        val fullColumns = ArrayList(mutCtx.typeColumns)
        val perms = if (isNewEntity || isRunAsSystemOrWsSystem) {
            null
        } else {
            permsDao.getRecordPerms(entityToMutate.extId)
        }
        val changedAssocs = ArrayList<DbAssocRefsDiff>()
        setMutationAtts(
            entityToMutate,
            recAttributes,
            mutCtx.typeColumns,
            changedAssocs,
            isAssocForceDeletion,
            currentUserRefId,
            isMutationFromChild,
            perms,
            mutCtx.allAssocsValues
        )
        val optionalAtts = DbRecord.OPTIONAL_COLUMNS.filter { !mutCtx.typeColumnNames.contains(it.name) }
        if (optionalAtts.isNotEmpty()) {
            fullColumns.addAll(
                setMutationAtts(
                    entityToMutate,
                    recAttributes,
                    optionalAtts,
                    changedAssocs,
                    isAssocForceDeletion,
                    currentUserRefId,
                    isMutationFromChild
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
                checkStatusSupportedByType(newStatus, typeInfo, globalRef)
                entityToMutate.status = newStatus
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

        dataService.doWithPermsPolicy(QueryPermsPolicy.PUBLIC) {
            AuthContext.runAsSystem {
                val fullColumnNames = fullColumns.mapTo(HashSet()) { it.name }
                computeAttsToStore(
                    computedAttsComponent,
                    fullColumns,
                    changedAssocs,
                    mutCtx
                ).forEach {
                    if (!fullColumnNames.contains(it.name)) {
                        fullColumns.add(it)
                    }
                }
            }
        }

        daoCtx.mutAssocHandler.validateChildAssocs(
            record.attributes,
            changedByOperationsAtts,
            entityToMutate.extId,
            mutCtx.typeAttColumns,
            parkedChildAtts
        )

        // If while mutation nothing changed, then we return
        if (changedAssocs.isEmpty()) {
            val equalsIgnoredAtts = if (disableAudit) emptySet() else AUDIT_ATTS
            if (entityToMutate.equals(entityBeforeMutation, equalsIgnoredAtts)) {
                return mutCtx.runPostMutationActions(entityBeforeMutation)
            }
        }

        val recAfterSave = dataService.save(entityToMutate, fullColumns, mutCtx.getExpectedAttTypes())
        val metaAfterSave = daoCtx.getEntityMeta(recAfterSave)

        processAssocsAfterMutation(
            recordEntityBeforeMutation,
            recAfterSave,
            record,
            changedAssocs,
            mutCtx.typeAttColumns,
            mutCtx.allAssocsValues,
            disableEvents,
            currentUser,
            metaAfterSave.globalRef
        )

        if (!disableEvents) {
            daoCtx.recEventsHandler.emitEventsAfterMutation(
                recordEntityBeforeMutation,
                recAfterSave,
                metaAfterSave,
                isNewEntity,
                changedAssocs
            )
        }

        return mutCtx.runPostMutationActions(recAfterSave)
    }

    private fun registerAssocValuesContainerIfRequired(att: String, newValue: DataValue, mutCtx: MutationContext) {

        val entityToMutate = mutCtx.entityToMutate
        val allAssocsValues = mutCtx.allAssocsValues

        val attDef: EcosAttColumnDef = mutCtx.typeAttColumnsByAtt[att] ?: return
        if (allAssocsValues.containsKey(att) || !DbRecordsUtils.isStoredInAssocsTable(attDef.attribute.type)) {
            return
        }
        val maxAssocsToEditByFullValuesList = dataProps.assocs.maxCountToEditByFullValuesList
        val valuesBefore = if (mutCtx.isNewEntity) {
            emptyList()
        } else {
            assocsService.getTargetAssocs(
                sourceId = mutCtx.entityToMutate.refId,
                attribute = att,
                page = DbFindPage(0, maxAssocsToEditByFullValuesList + 1)
            ).entities.map { it.targetId }
        }
        if (valuesBefore.size > maxAssocsToEditByFullValuesList) {
            error(
                "You can't edit large associations by providing full values list. " +
                    "Please, use att_add_... and att_rem_... to work with it. " +
                    "Max values count to edit by full values list: $maxAssocsToEditByFullValuesList. " +
                    "Assoc: $att Record: ${daoCtx.getGlobalRef(mutCtx.entityToMutate.extId)}"
            )
        }
        val refsBefore = recordRefService.getEntityRefsByIds(valuesBefore).map {
            it.toString()
        }.toSet()

        val assocValuesContainer = DbAssocAttValuesContainer(
            entityToMutate,
            daoCtx,
            refsBefore,
            DbRecordsUtils.isChildAssocAttribute(attDef.attribute),
            attDef.attribute
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

    private fun processAssocsAfterMutation(
        recordEntityBeforeMutation: DbEntity,
        recAfterSave: DbEntity,
        record: LocalRecordAtts,
        changedAssocs: List<DbAssocRefsDiff>,
        typeAttColumns: List<EcosAttColumnDef>,
        allAssocsValues: Map<String, DbAssocAttValuesContainer>,
        disableEvents: Boolean,
        currentUser: String,
        globalRef: EntityRef
    ) {
        daoCtx.mutAssocHandler.processChildrenAfterMutation(
            recBeforeSave = recordEntityBeforeMutation,
            recAfterSave = recAfterSave,
            attributes = record.attributes,
            columns = typeAttColumns,
            assocsValues = allAssocsValues,
            disableEvents = disableEvents
        )
        daoCtx.mutAssocHandler.processParentAfterMutation(
            recBeforeSave = recordEntityBeforeMutation,
            recAfterSave = recAfterSave,
            attributes = record.attributes,
            disableEvents = disableEvents
        )
        daoCtx.remoteActionsClient?.updateRemoteAssocs(
            currentCtx = daoCtx.tableCtx,
            sourceRef = globalRef,
            creator = currentUser,
            assocsDiff = changedAssocs
        )
    }

    private fun computeAttsToStore(
        component: DbComputedAttsComponent?,
        columns: List<DbColumnDef>,
        changedAssocs: MutableList<DbAssocRefsDiff>,
        mutCtx: MutationContext
    ): List<DbColumnDef> {

        val entity = mutCtx.entityToMutate
        val isNewRecord = mutCtx.isNewEntity
        val currentUserRefId = mutCtx.currentUserRefId
        val typeInfo = mutCtx.typeInfo
        val typeAttColumns = mutCtx.typeAttColumns

        val mutatedColumns = HashMap<String, DbColumnDef>()

        // stage is calculated by the status, so a blank status resets it too
        val entityStatus = entity.status
        var stageId = ""
        if (entityStatus.isNotBlank()) {
            for ((idx, stage) in typeInfo.model.stages.withIndex()) {
                if (stage.statuses.contains(entityStatus)) {
                    stageId = stage.id.ifBlank { idx.toString() }
                    break
                }
            }
        }
        var stageChanged = false
        if (stageId.isNotEmpty()) {
            if (entity.attributes[DbRecord.ATT_STAGE] != stageId) {
                entity.attributes[DbRecord.ATT_STAGE] = stageId
                stageChanged = true
            }
        } else {
            if (entity.attributes[DbRecord.ATT_STAGE] != null) {
                entity.attributes[DbRecord.ATT_STAGE] = null
                stageChanged = true
            }
        }
        if (stageChanged) {
            mutatedColumns[DbRecord.COLUMN_STAGE.name] = DbRecord.COLUMN_STAGE
        }

        if (component == null) {
            return mutatedColumns.values.toList()
        }

        val dbRecord = DbRecord(daoCtx, entity)

        val typeRef = ModelUtils.getTypeRef(typeInfo.id)
        val atts = component.computeAttsToStore(
            dbRecord,
            isNewRecord,
            typeRef,
            mutCtx.computeContext.preCalculatedComputedAtts
        )

        atts.forEach { att, newValue ->
            registerAssocValuesContainerIfRequired(att, newValue, mutCtx)
        }

        daoCtx.mutAssocHandler.replaceRefsById(atts, typeAttColumns)
        mutCtx.computeContext.updateCountersIfRequired(dbRecord, atts, typeInfo)
        val fullColumns = ArrayList(columns)

        DbRecord.COMPUTABLE_OPTIONAL_COLUMNS.forEach {
            if (atts.has(it.name)) {
                fullColumns.add(it)
            }
        }

        val calculatedMutatedColumns = setMutationAtts(
            entity,
            atts,
            fullColumns,
            changedAssocs,
            true,
            currentUserRefId,
            false,
            multiAssocValues = mutCtx.allAssocsValues
        )
        for (column in calculatedMutatedColumns) {
            mutatedColumns[column.name] = column
        }

        entity.name = component.computeDisplayName(DbRecord(daoCtx, entity), typeRef)

        return mutatedColumns.values.toList()
    }

    private fun expandFullAttsIfExists(record: LocalRecordAtts): Pair<LocalRecordAtts, Boolean> {
        val fullAtts = readFullAttsAttribute(record.getAtt(DbRecordsControlAtts.FULL_ATTS))
        return if (fullAtts.isObject()) {
            val newAtts = fullAtts.copy().asObjectData()
            record.attributes.forEach { k, v ->
                if (k.startsWith("_") && !newAtts.has(k)) {
                    newAtts[k] = v
                }
            }
            LocalRecordAtts(record.id, newAtts) to true
        } else {
            record.attributes.remove(DbRecordsControlAtts.FULL_ATTS)
            if (fullAtts.isBoolean() && fullAtts.asBoolean()) {
                record to true
            } else {
                record to false
            }
        }
    }

    private fun readFullAttsAttribute(value: DataValue): DataValue {
        return if (value.isNull()) {
            value
        } else if (value.isArray()) {
            if (value.size() > 0) {
                readFullAttsAttribute(value[0])
            } else {
                DataValue.NULL
            }
        } else if (value.isObject()) {
            val url = value["url"]
            if (url.isTextual() && url.asText().startsWith(DataUriUtil.DATA_PREFIX)) {
                Json.mapper.read(url.asText(), ObjectData::class.java)?.getData() ?: DataValue.NULL
            } else {
                value
            }
        } else if (value.isBoolean()) {
            value
        } else {
            DataValue.NULL
        }
    }

    private fun getExtIdFromAtts(
        typeInfo: TypeInfo,
        attributes: ObjectData,
        mutComputeCtx: MutationComputeContext
    ): String {
        return getExtIdFromAtts(typeInfo, LocalRecordAtts("", attributes), mutComputeCtx)
    }

    private fun getExtIdFromAtts(
        typeInfo: TypeInfo,
        record: LocalRecordAtts,
        mutComputeCtx: MutationComputeContext
    ): String {

        val attributes = record.attributes
        attributes.remove(ATT_CUSTOM_ID)

        val localIdTemplate = typeInfo.localIdTemplate
        if (localIdTemplate.isEmpty()) {
            return attributes[ATT_ID].asText()
        }
        if (record.id.isNotBlank()) {
            return ""
        }

        val record = MutLocalRecForLocalId(attributes, typeInfo, mutComputeCtx)
        return daoCtx.recordsServiceFactory.recordsTemplateService.resolve(localIdTemplate, record)
    }

    /**
     * @return record after processing or null if processing was not performed
     */
    private fun handleRecordCustomId(mutCtx: MutationContext): DbEntity? {

        val recAtts = mutCtx.record.attributes
        val entityToMutate = mutCtx.entityToMutate

        val customExtId = mutCtx.extIdFromAtts

        if (customExtId.isBlank() || entityToMutate.extId == customExtId) {
            return null
        }

        if (mutCtx.isNewEntity) {
            if (!allowedRecordIdRegex.matches(customExtId)) {
                error("Invalid id: '$customExtId'. Valid pattern: '$allowedRecordIdPattern'")
            }
            if (customExtId.length > recordIdMaxLength) {
                error("Invalid id: '$customExtId'. Max length $recordIdMaxLength")
            }
            val idAtt = mutCtx.record.getAtt("id").asText()
            if (idAtt.isNotBlank() && idAtt != customExtId) {
                mutCtx.record.setAtt(ATT_CUSTOM_ID, idAtt)
            }
            entityToMutate.extId = customExtId
            return null
        } else {
            val localIdTemplate = mutCtx.typeInfo.localIdTemplate
            if (localIdTemplate.isNotEmpty()) {
                // copying by mutation with other id doesn't supported for records with templated ext id
                return null
            }
            dataService.doWithPermsPolicy(QueryPermsPolicy.PUBLIC) {
                if (dataService.isExistsByExtId(customExtId)) {
                    log.error {
                        "Record with ID $customExtId already exists. You should mutate it directly. " +
                            "Record: ${daoCtx.getGlobalRef(customExtId)}"
                    }
                    error("Read permission denied for ${daoCtx.getGlobalRef(customExtId)}")
                }
            }
            return daoCtx.recContentHandler.withContentDbDataAware {
                val attsToCopy = DbRecord(daoCtx, entityToMutate, null).getAttsForCopy()
                val newRec = LocalRecordAtts("", recAtts.deepCopy())
                attsToCopy.forEach { (k, v) ->
                    if (!newRec.hasAtt(k)) {
                        newRec.setAtt(k, v)
                    }
                }
                mutateRecordInTxn(
                    newRec,
                    mutCtx.typeInfo,
                    mutCtx.typeAttColumns
                )
            }
        }
    }

    /**
     * @return record after processing or null if processing was not performed
     */
    private fun handleControlAtts(mutCtx: MutationContext): DbEntity? {

        val record = mutCtx.record
        val isRunAsSystemOrAdmin = mutCtx.isRunAsSystemOrAdmin
        val entityToMutate = mutCtx.entityToMutate

        if (record.attributes.has(DbRecordsControlAtts.UPDATE_TYPE)) {
            // type updating is processed after permissions checking, so other control
            // operations would silently win over it if they are used in the same mutation
            val conflictingAtt = TYPE_UPDATE_CONFLICTING_ATTS.find { record.attributes.has(it) }
            if (conflictingAtt != null) {
                error(
                    "${DbRecordsControlAtts.UPDATE_TYPE} can't be used with '$conflictingAtt' " +
                        "in the same mutation. Record: ${daoCtx.getGlobalRef(record.id)}"
                )
            }
        }

        if (record.attributes[DbRecordsControlAtts.UPDATE_PERMISSIONS].asBoolean()) {
            if (!isRunAsSystemOrAdmin) {
                error("Permissions update allowed only for admin. Record: $record sourceId: '${config.id}'")
            }
            permsDao.updatePermissions(listOf(record.id))
            return entityToMutate
        }
        if (record.attributes.has(DbRecordsControlAtts.UPDATE_WORKSPACE)) {
            if (!isRunAsSystemOrAdmin) {
                error("Workspace update allowed only for admin. Record: $record sourceId: '${config.id}'")
            }
            val updateToWorkspace = record.attributes[DbRecordsControlAtts.UPDATE_WORKSPACE].asText()
            assocsService.forEachAssoc(
                predicate = Predicates.and(
                    Predicates.eq(DbAssocEntity.SOURCE_ID, entityToMutate.refId),
                    Predicates.eq(DbAssocEntity.CHILD, true)
                ),
                batchSize = 100
            ) { children ->
                val childrenMutAtts = recordRefService.getEntityRefsByIds(children.map { it.targetId }).map {
                    val atts = RecordAtts(it)
                    atts.setAtt(DbRecordsControlAtts.UPDATE_WORKSPACE, updateToWorkspace)
                    atts
                }
                daoCtx.recordsService.mutate(childrenMutAtts)
                false
            }
            val newWs = if (updateToWorkspace.isBlank()) {
                null
            } else {
                wsDbService.getOrCreateId(updateToWorkspace)
            }
            return if (entityToMutate.workspace != newWs) {
                entityToMutate.workspace = newWs
                dataService.save(entityToMutate)
            } else {
                entityToMutate
            }
        }
        if (record.attributes.has(DbRecordsControlAtts.UPDATE_ID)) {
            if (!config.allowRecordIdUpdate) {
                error("Record ID updating disabled for '${daoCtx.appName}/${config.id}'")
            }
            if (!isRunAsSystemOrAdmin) {
                error("Id update allowed only for admin. Record: $record sourceId: '${daoCtx.appName}/${config.id}'")
            }
            val updateIdValue = record.attributes[DbRecordsControlAtts.UPDATE_ID]
            record.attributes.remove(DbRecordsControlAtts.UPDATE_ID)
            var newId = ""
            if (updateIdValue.isTextual()) {
                newId = updateIdValue.asText()
            }
            mutCtx.postMutationActions.add { entity ->
                updateIdForEntity(mutCtx, entity, newId)
            }
            return null
        }
        updateCalculatedAttsIfRequired(mutCtx)?.let { return it }
        return null
    }

    private fun updateIdForEntity(mutCtx: MutationContext, entity: DbEntity, newId: String): DbEntity {
        val attsToCopy = AuthContext.runAsSystem {
            DbRecord(daoCtx, entity, null).getAttsForCopy()
        }
        val attsToEvalExtId = ObjectData.create(attsToCopy)
        if (entity.attributes.containsKey(RecordConstants.ATT_DOC_NUM)) {
            attsToEvalExtId[RecordConstants.ATT_DOC_NUM] = entity.attributes[RecordConstants.ATT_DOC_NUM]
        }
        if (newId.isNotBlank()) {
            attsToEvalExtId[ATT_ID] = newId
        } else if (entity.attributes.containsKey(ATT_CUSTOM_ID)) {
            attsToEvalExtId[ATT_ID] = entity.attributes[ATT_CUSTOM_ID]
        }
        val newExtId = getExtIdFromAtts(
            typeInfo = mutCtx.typeInfo,
            attributes = attsToEvalExtId,
            mutComputeCtx = mutCtx.computeContext
        )
        var recordWasChanged = false
        var extIdWasChanged = false
        if (newExtId.isNotBlank() && entity.extId != newExtId) {
            if (daoCtx.attsDao.findDbEntityByExtId(newExtId, checkPerms = false) != null) {
                error("Record '${daoCtx.getGlobalRef(newExtId)}' already exists. The id must be unique.")
            }
            entity.extId = newExtId
            recordWasChanged = true
            extIdWasChanged = true
        }
        if (newId.isNotBlank() && entity.extId != newId) {
            entity.attributes[ATT_CUSTOM_ID] = newId
            recordWasChanged = true
        }
        val resultEntity = if (recordWasChanged) {
            dataService.save(entity)
        } else {
            entity
        }
        if (!extIdWasChanged) {
            return resultEntity
        }
        val beforeAfterRef = recordRefService.moveRef(
            fromRefId = entity.refId,
            toRef = EntityRef.create("", entity.extId),
            migratedBy = mutCtx.currentUserRefId
        )
        if (beforeAfterRef != null) {
            daoCtx.tableCtx.getSchemaCtx().forEachNeighbourSchema { _, context ->
                context.recordRefService.migrateRefIfExists(
                    fromRef = beforeAfterRef.first,
                    toRef = beforeAfterRef.second,
                    migratedBy = mutCtx.currentUserRefId
                )
            }
            val appsWithSrcAssocs = assocsService.findSourceExternalApps(entity.refId)
            if (appsWithSrcAssocs.isNotEmpty()) {
                log.debug {
                    "Found external apps with links to " +
                        "migrated ref '${beforeAfterRef.first}': ${appsWithSrcAssocs.joinToString()}"
                }
                for (appName in appsWithSrcAssocs) {
                    try {
                        daoCtx.remoteActionsClient?.migrateRemoteRef(
                            targetApp = appName,
                            fromRef = beforeAfterRef.first,
                            toRef = beforeAfterRef.second,
                            migratedBy = mutCtx.currentUser,
                        )
                    } catch (e: Throwable) {
                        val ex = RuntimeException(
                            "Remote ref migration failed. " +
                                "TargetApp: '$appName' " +
                                "fromRef: '${beforeAfterRef.first}' " +
                                "toRef: '${beforeAfterRef.second}' " +
                                "migratedBy: '${mutCtx.currentUser}'"
                        )
                        ex.addSuppressed(e)
                        throw ex
                    }
                }
            }
            val entityInfo = daoCtx.getEntityMeta(entity)
            daoCtx.listeners.forEach {
                it.onRecordRefChangedEvent(
                    DbRecordRefChangedEvent(
                        before = beforeAfterRef.first,
                        after = beforeAfterRef.second,
                        isDraft = entity.attributes[DbRecord.COLUMN_IS_DRAFT.name] == true,
                        record = DbRecord(daoCtx, entity, null),
                        typeDef = entityInfo.typeInfo,
                        aspects = entityInfo.aspectsInfo,
                        localRef = entityInfo.localRef,
                        globalRef = entityInfo.globalRef
                    )
                )
            }
        }
        return resultEntity
    }

    /**
     * @return record after save if atts was updated
     */
    private fun updateCalculatedAttsIfRequired(mutCtx: MutationContext): DbEntity? {

        if (!mutCtx.record.attributes.has(DbRecordsControlAtts.UPDATE_CALCULATED_ATTS)) {
            return null
        }

        if (!mutCtx.isRunAsSystemOrAdmin) {
            error(
                "Calculated fields updating allowed only for admin. " +
                    "Record: ${mutCtx.record} sourceId: '${config.id}'"
            )
        }
        return recomputeAttsAndSave(
            mutCtx = mutCtx,
            entityBeforeMutation = mutCtx.entityToMutate.copy(),
            extraColumns = emptyList(),
            changedAssocs = ArrayList()
        )
    }

    /**
     * Recompute calculated attributes, display name and stage for the type from [mutCtx]
     * and save the entity if anything was changed.
     *
     * @return entity after save or entity without changes if nothing was changed
     */
    private fun recomputeAttsAndSave(
        mutCtx: MutationContext,
        entityBeforeMutation: DbEntity,
        extraColumns: List<DbColumnDef>,
        changedAssocs: MutableList<DbAssocRefsDiff>
    ): DbEntity {

        val entityToMutate = mutCtx.entityToMutate

        val fullColumns = ArrayList(mutCtx.typeColumns)
        fullColumns.addAll(extraColumns)

        var recAfterSave: DbEntity = entityToMutate
        dataService.doWithPermsPolicy(QueryPermsPolicy.PUBLIC) {
            AuthContext.runAsSystem {

                val fullColumnNames = fullColumns.mapTo(HashSet()) { it.name }

                val mutatedColumns = computeAttsToStore(
                    computedAttsComponent,
                    fullColumns,
                    changedAssocs,
                    mutCtx
                )

                if (changedAssocs.isNotEmpty() ||
                    !entityToMutate.equals(entityBeforeMutation, AUDIT_ATTS)
                ) {

                    for (column in mutatedColumns) {
                        if (!fullColumnNames.contains(column.name)) {
                            fullColumns.add(column)
                        }
                    }
                    recAfterSave = dataService.save(entityToMutate, fullColumns, mutCtx.getExpectedAttTypes())
                    val metaAfterSave = daoCtx.getEntityMeta(recAfterSave)

                    processAssocsAfterMutation(
                        entityBeforeMutation,
                        recAfterSave,
                        mutCtx.record,
                        changedAssocs,
                        mutCtx.typeAttColumns,
                        emptyMap(),
                        mutCtx.disableEvents,
                        mutCtx.currentUser,
                        metaAfterSave.globalRef
                    )
                }
            }
        }
        return recAfterSave
    }

    /**
     * Change record type by [DbRecordsControlAtts.UPDATE_TYPE] control attribute.
     * Type may be changed only to the type with the same sourceId, so the record
     * stays in the current records DAO. Write permissions are checked before this
     * method is called and the current type is always taken from the database.
     *
     * Status may be changed by the same mutation. When the status attribute is passed
     * explicitly with a blank value, the status is reset - this is the way to move
     * a record with status to a type without statuses in its model.
     *
     * @return record after type changing or null if type updating was not requested
     */
    private fun handleTypeUpdate(
        mutCtx: MutationContext,
        aspectRefsInDb: Set<EntityRef>,
        nowInstant: Instant,
        disableAudit: Boolean
    ): DbEntity? {

        val record = mutCtx.record
        if (!record.attributes.has(DbRecordsControlAtts.UPDATE_TYPE)) {
            return null
        }
        val entityToMutate = mutCtx.entityToMutate

        val newTypeId = EntityRef.valueOf(
            record.attributes[DbRecordsControlAtts.UPDATE_TYPE].asText()
        ).getLocalId()

        if (newTypeId.isBlank()) {
            error(
                "${DbRecordsControlAtts.UPDATE_TYPE} attribute is empty. " +
                    "Record: ${daoCtx.getGlobalRef(record.id)}"
            )
        }
        if (mutCtx.isNewEntity) {
            error(
                "Type can't be changed for a new record. " +
                    "Type: '$newTypeId' sourceId: '${config.id}'"
            )
        }
        if (mutCtx.computeContext.counterAttsToUpdate.isNotEmpty()) {
            error(
                "${DbRecordsControlAtts.UPDATE_TYPE} can't be used with " +
                    "'${DbRecordsControlAtts.UPDATE_COUNTER_ATT}' in the same mutation. " +
                    "Record: ${daoCtx.getGlobalRef(record.id)}"
            )
        }

        val currentTypeInfo = mutCtx.typeInfo
        val isTypeChanged = newTypeId != currentTypeInfo.id

        // status may be changed by the same mutation. Blank value of the explicitly passed
        // status attribute means that the status should be reset
        val newStatus = if (record.attributes.has(StatusConstants.ATT_STATUS)) {
            record.attributes[StatusConstants.ATT_STATUS].asText()
        } else {
            entityToMutate.status
        }
        if (!isTypeChanged && newStatus == entityToMutate.status) {
            return entityToMutate
        }

        validateTypeIdForDao(newTypeId)
        val newTypeInfo = ecosTypeService.getTypeInfoNotNull(newTypeId)

        val globalRef = daoCtx.getGlobalRef(entityToMutate.extId)

        if (newTypeInfo.sourceId != currentTypeInfo.sourceId) {
            error(
                "Type can be changed only to the type with the same sourceId. " +
                    "Record: $globalRef " +
                    "Before: '${currentTypeInfo.id}' (sourceId: '${currentTypeInfo.sourceId}') " +
                    "After: '${newTypeInfo.id}' (sourceId: '${newTypeInfo.sourceId}')"
            )
        }
        if (newTypeInfo.workspaceScope != currentTypeInfo.workspaceScope) {
            error(
                "Type can't be changed to the type with another workspace scope. " +
                    "Record: $globalRef " +
                    "Before: '${currentTypeInfo.id}' (${currentTypeInfo.workspaceScope}) " +
                    "After: '${newTypeInfo.id}' (${newTypeInfo.workspaceScope})"
            )
        }

        if (newStatus.isNotBlank()) {
            checkStatusSupportedByType(newStatus, newTypeInfo, globalRef)
        }

        val entityBeforeMutation = entityToMutate.copy()

        entityToMutate.type = recordRefService.getOrCreateIdByEntityRef(ModelUtils.getTypeRef(newTypeId))

        val extraColumns = ArrayList<DbColumnDef>()
        if (newStatus != entityToMutate.status) {
            entityToMutate.status = newStatus
            if (!disableAudit) {
                entityToMutate.attributes[DbRecord.ATT_STATUS_MODIFIED] = nowInstant
            }
        }
        // status modified date may also be set explicitly before this method when audit is disabled
        if (entityToMutate.attributes.containsKey(DbRecord.ATT_STATUS_MODIFIED)) {
            extraColumns.add(DbRecord.COLUMN_STATUS_MODIFIED)
        }
        if (!disableAudit) {
            entityToMutate.modified = nowInstant
            entityToMutate.modifier = mutCtx.currentUserRefId
        }

        val newTypeMutCtx = createMutationContextForType(mutCtx, newTypeInfo, aspectRefsInDb)

        val changedAssocs = ArrayList<DbAssocRefsDiff>()
        val recAfterSave = recomputeAttsAndSave(
            mutCtx = newTypeMutCtx,
            entityBeforeMutation = entityBeforeMutation,
            extraColumns = extraColumns,
            changedAssocs = changedAssocs
        )

        if (!mutCtx.disableEvents) {
            val metaAfterSave = daoCtx.getEntityMeta(recAfterSave)
            if (isTypeChanged) {
                val typeChangedEvent = DbRecordTypeChangedEvent(
                    localRef = metaAfterSave.localRef,
                    globalRef = metaAfterSave.globalRef,
                    isDraft = metaAfterSave.isDraft,
                    record = DbRecord(daoCtx, recAfterSave),
                    typeDef = newTypeInfo,
                    aspects = metaAfterSave.aspectsInfo,
                    before = currentTypeInfo
                )
                daoCtx.listeners.forEach {
                    it.onTypeChanged(typeChangedEvent)
                }
            }
            daoCtx.recEventsHandler.emitEventsAfterMutation(
                entityBeforeMutation,
                recAfterSave,
                metaAfterSave,
                false,
                changedAssocs
            )
        }

        return recAfterSave
    }

    private fun createMutationContextForType(
        mutCtx: MutationContext,
        typeInfo: TypeInfo,
        aspectRefsInDb: Set<EntityRef>
    ): MutationContext {

        val newMutCtx = MutationContext(
            record = mutCtx.record,
            typeInfo = typeInfo,
            disableEvents = mutCtx.disableEvents,
            entityToMutate = mutCtx.entityToMutate,
            typeAttColumns = ArrayList(ecosTypeService.getColumnsForTypes(listOf(typeInfo))),
            frozenColumnsProvider = {
                val tableAttTypes = ecosTypeService.getTableAttTypes(typeInfo)
                DbExpectedAttTypes.ConflictsInfo(tableAttTypes.conflicts, tableAttTypes.groupingMayBeOverBroad)
            },
            currentUser = mutCtx.currentUser,
            currentUserRefId = mutCtx.currentUserRefId,
            isRunAsSystemOrAdmin = mutCtx.isRunAsSystemOrAdmin,
            isNewEntity = mutCtx.isNewEntity,
            extIdFromAtts = mutCtx.extIdFromAtts,
            computeContext = mutCtx.computeContext
        )
        if (aspectRefsInDb.isNotEmpty()) {
            for (column in ecosTypeService.getColumnsForAspects(aspectRefsInDb)) {
                newMutCtx.addTypeAttColumn(column)
            }
        }
        return newMutCtx
    }

    private fun setNotPresentAttsAsNull(data: ObjectData, attributes: List<AttributeDef>): ObjectData {
        for (att in attributes) {
            if (!data.has(att.id)) {
                data[att.id] = null
            }
        }
        return data
    }

    private fun checkStatusSupportedByType(status: String, typeInfo: TypeInfo, globalRef: EntityRef) {
        if (typeInfo.model.statuses.none { it.id == status }) {
            error(
                "Status '$status' is not supported by type '${typeInfo.id}'. " +
                    "Available statuses: ${typeInfo.model.statuses.joinToString { it.id }}. " +
                    "Record: $globalRef"
            )
        }
    }

    /**
     * Check that the type may be used for records of this DAO.
     */
    private fun validateTypeIdForDao(typeId: String) {
        if (EntityRef.isNotEmpty(config.typeRef) &&
            !ecosTypeService.isSubType(typeId, config.typeRef.getLocalId())
        ) {
            throw I18nRuntimeException(
                messageKey = "ecos-data.invalid-type.subtype",
                messageArgs = mapOf(
                    "typeId" to typeId,
                    "sourceId" to config.id,
                    "cfgType" to config.typeRef.getLocalId()
                )
            )
        }
        if (!config.typeRef.getLocalId().contains(IdInWs.WS_DELIM) &&
            typeId.contains(IdInWs.WS_DELIM)
        ) {
            throw I18nRuntimeException(
                messageKey = "ecos-data.invalid-type.ws-scoped",
                messageArgs = mapOf(
                    "typeId" to typeId,
                    "sourceId" to config.id
                )
            )
        }
    }

    private fun getTypeIdForRecord(record: LocalRecordAtts): String {

        val typeRefStr = record.attributes[RecordConstants.ATT_TYPE].asText().ifBlank {
            // legacy type attribute
            record.attributes["_etype"].asText()
        }

        val typeId = EntityRef.valueOf(typeRefStr).getLocalId()
        if (typeId.isNotBlank()) {
            validateTypeIdForDao(typeId)
            return typeId
        }

        if (EntityRef.isNotEmpty(config.typeRef)) {
            return config.typeRef.getLocalId()
        }

        error(
            "${RecordConstants.ATT_TYPE} attribute is mandatory for mutation. " +
                "Record: ${daoCtx.getGlobalRef(record.id)}"
        )
    }

    private fun setMutationAtts(
        recToMutate: DbEntity,
        atts: ObjectData,
        columns: List<DbColumnDef>,
        changedAssocs: MutableList<DbAssocRefsDiff>,
        isAssocForceDeletion: Boolean,
        currentUserRefId: Long,
        isMutationFromChild: Boolean,
        perms: DbRecordPermsContext? = null,
        multiAssocValues: MutableMap<String, DbAssocAttValuesContainer> = LinkedHashMap()
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
                val exception = I18nRuntimeException(
                    "ecos-data.permission-denied.user-cannot-change-attribute",
                    mapOf(
                        "user" to currentUser,
                        "attribute" to dbColumnDef.name,
                        "recordRef" to daoCtx.getGlobalRef(recToMutate.extId)
                    )
                )
                // Values of attributes stored in the assocs table are applied from the
                // container prepared before the permissions check and shared with the later
                // mutation stages (computed atts are evaluated in the system context, where
                // perms are null). Drop the denied value from it, otherwise these stages will
                // apply the change which was refused here.
                multiAssocValues.remove(dbColumnDef.name)

                if (isMutationFromChild) {
                    throw exception
                } else {
                    log.warn { exception.message }
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

    private fun extractCountersToUpdate(attributes: ObjectData): Set<String> {
        if (!attributes.has(DbRecordsControlAtts.UPDATE_COUNTER_ATT)) {
            return emptySet()
        }
        val updateCountersRaw = attributes[DbRecordsControlAtts.UPDATE_COUNTER_ATT]
        attributes.remove(DbRecordsControlAtts.UPDATE_COUNTER_ATT)
        return if (updateCountersRaw.isEmpty()) {
            emptySet()
        } else if (updateCountersRaw.isTextual()) {
            setOf(updateCountersRaw.asText())
        } else if (updateCountersRaw.isArray()) {
            val result = LinkedHashSet<String>()
            for (element in updateCountersRaw) {
                val elementText = element.asText()
                if (elementText.isNotBlank()) {
                    result.add(elementText)
                }
            }
            result
        } else {
            emptySet()
        }
    }

    /**
     * The workspace a mutation of a `PRIVATE`-scoped type aims its record at, and the check that
     * this user may put a record there - refused here, where a refusal still costs nothing.
     *
     * Null when the request names no workspace, directly or through the parent or the type's
     * default. Not an error at this point: this runs before the record's id is resolved - because
     * resolving it can take a number that cannot be given back - and the request may still turn out
     * to address a record that already exists, which has a workspace of its own and needs none
     * named. The caller raises the error once it knows the mutation is a creation.
     */
    private fun resolveRequestedWorkspace(
        record: LocalRecordAtts,
        typeInfo: TypeInfo,
        isMutationFromParent: Boolean,
        runAsAuth: AuthData,
        isRunAsSystem: Boolean
    ): RequestedWorkspace? {

        var parentWsId = ""
        val parentRef = record.getAtt(RecordConstants.ATT_PARENT).asText().toEntityRef()
        if (!isMutationFromParent && parentRef.isNotEmpty() && parentRef.getAppName() != AppName.ALFRESCO) {
            val parentAtts = if (DbWorkspaceDesc.isWorkspaceRef(parentRef)) {
                OnCreateParentAtts(parentRef.getLocalId())
            } else {
                AuthContext.runAsSystem {
                    daoCtx.recordsService.getAtts(parentRef, OnCreateParentAtts::class.java)
                }
            }
            /*
            // todo?
            // move to before commit action?
            if (parentAtts.notExists) {
                error(
                    "Parent record doesn't exists: '$parentRef'. " +
                        "Child record with type ${typeInfo.id} can't be created"
                )
            }*/
            parentWsId = parentAtts.workspace
        }
        var mutWorkspaceId = parentWsId.ifBlank {
            record.getAtt(RecordConstants.ATT_WORKSPACE).asText().toEntityRef().getLocalId()
        }
        if (mutWorkspaceId.isEmpty() && typeInfo.defaultWorkspace.isNotBlank()) {
            mutWorkspaceId = typeInfo.defaultWorkspace
        }
        if (mutWorkspaceId.isEmpty()) {
            return null
        }
        var workspaceRef = EntityRef.EMPTY
        if (mutWorkspaceId.isNotBlank()) {
            workspaceRef = DbWorkspaceDesc.getRef(mutWorkspaceId)
        }
        if (!isRunAsSystem && workspaceRef.isNotEmpty()) {
            val workspaces = workspaceService.getUserOrWsSystemUserWorkspaces(runAsAuth) ?: emptySet()
            if (!workspaces.contains(workspaceRef.getLocalId())) {
                error("You can't create records in workspace $workspaceRef")
            }
            return RequestedWorkspace(
                workspaceRef,
                workspaceService.isRunAsSystemOrWsSystem(workspaceRef.getLocalId())
            )
        }
        return RequestedWorkspace(workspaceRef, isRunAsSystem)
    }

    private class RequestedWorkspace(
        val ref: EntityRef,
        val isRunAsSystemOrWsSystem: Boolean
    )

    private class OnCreateParentAtts(
        @param:AttName(RecordConstants.ATT_WORKSPACE + ScalarType.LOCAL_ID_SCHEMA + "!")
        val workspace: String
    )
}
