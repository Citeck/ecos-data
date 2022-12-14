package ru.citeck.ecos.data.sql.records

import mu.KotlinLogging
import ru.citeck.ecos.commons.data.DataValue
import ru.citeck.ecos.commons.data.ObjectData
import ru.citeck.ecos.context.lib.auth.AuthContext
import ru.citeck.ecos.context.lib.auth.AuthRole
import ru.citeck.ecos.data.sql.content.EcosContentService
import ru.citeck.ecos.data.sql.dto.DbColumnDef
import ru.citeck.ecos.data.sql.ecostype.DbEcosTypeService
import ru.citeck.ecos.data.sql.ecostype.EcosAttColumnDef
import ru.citeck.ecos.data.sql.job.DbJobsProvider
import ru.citeck.ecos.data.sql.meta.dto.DbTableMetaDto
import ru.citeck.ecos.data.sql.records.computed.DbComputedAttsComponent
import ru.citeck.ecos.data.sql.records.dao.DbRecordsDaoCtx
import ru.citeck.ecos.data.sql.records.dao.atts.DbContentValue
import ru.citeck.ecos.data.sql.records.dao.atts.DbEmptyRecord
import ru.citeck.ecos.data.sql.records.dao.atts.DbRecord
import ru.citeck.ecos.data.sql.records.dao.content.RecordFileUploadData
import ru.citeck.ecos.data.sql.records.listener.*
import ru.citeck.ecos.data.sql.records.migration.AssocsDbMigration
import ru.citeck.ecos.data.sql.records.perms.DbPermsComponent
import ru.citeck.ecos.data.sql.records.perms.DbRecordPerms
import ru.citeck.ecos.data.sql.records.refs.DbRecordRefService
import ru.citeck.ecos.data.sql.repo.entity.DbEntity
import ru.citeck.ecos.data.sql.repo.find.DbFindPage
import ru.citeck.ecos.data.sql.repo.find.DbFindSort
import ru.citeck.ecos.data.sql.service.DbCommitEntityDto
import ru.citeck.ecos.data.sql.service.DbDataService
import ru.citeck.ecos.data.sql.service.DbDataServiceImpl
import ru.citeck.ecos.data.sql.service.DbMigrationsExecutor
import ru.citeck.ecos.data.sql.service.aggregation.AggregateFunc
import ru.citeck.ecos.data.sql.txn.ExtTxnContext
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType
import ru.citeck.ecos.model.lib.status.constants.StatusConstants
import ru.citeck.ecos.model.lib.type.dto.TypeInfo
import ru.citeck.ecos.model.lib.type.repo.TypesRepo
import ru.citeck.ecos.model.lib.type.service.utils.TypeUtils
import ru.citeck.ecos.records2.RecordConstants
import ru.citeck.ecos.records2.predicate.PredicateService
import ru.citeck.ecos.records2.predicate.PredicateUtils
import ru.citeck.ecos.records2.predicate.model.EmptyPredicate
import ru.citeck.ecos.records2.predicate.model.Predicate
import ru.citeck.ecos.records2.predicate.model.Predicates
import ru.citeck.ecos.records2.predicate.model.ValuePredicate
import ru.citeck.ecos.records2.source.dao.local.job.Job
import ru.citeck.ecos.records2.source.dao.local.job.JobsProvider
import ru.citeck.ecos.records2.source.dao.local.job.PeriodicJob
import ru.citeck.ecos.records3.RecordsServiceFactory
import ru.citeck.ecos.records3.record.atts.dto.LocalRecordAtts
import ru.citeck.ecos.records3.record.atts.schema.ScalarType
import ru.citeck.ecos.records3.record.atts.schema.resolver.AttContext
import ru.citeck.ecos.records3.record.atts.value.AttValue
import ru.citeck.ecos.records3.record.atts.value.impl.EmptyAttValue
import ru.citeck.ecos.records3.record.dao.AbstractRecordsDao
import ru.citeck.ecos.records3.record.dao.atts.RecordsAttsDao
import ru.citeck.ecos.records3.record.dao.delete.DelStatus
import ru.citeck.ecos.records3.record.dao.delete.RecordsDeleteDao
import ru.citeck.ecos.records3.record.dao.mutate.RecordMutateDao
import ru.citeck.ecos.records3.record.dao.query.RecordsQueryDao
import ru.citeck.ecos.records3.record.dao.query.RecsGroupQueryDao
import ru.citeck.ecos.records3.record.dao.query.dto.query.RecordsQuery
import ru.citeck.ecos.records3.record.dao.query.dto.res.RecsQueryRes
import ru.citeck.ecos.records3.record.dao.txn.TxnRecordsDao
import ru.citeck.ecos.records3.record.request.RequestContext
import ru.citeck.ecos.records3.utils.RecordRefUtils
import ru.citeck.ecos.webapp.api.content.EcosContentData
import ru.citeck.ecos.webapp.api.entity.EntityRef
import java.util.*
import java.util.concurrent.CopyOnWriteArrayList
import java.util.regex.Pattern
import kotlin.collections.ArrayList
import kotlin.math.min

class DbRecordsDao(
    private val config: DbRecordsDaoConfig,
    private val ecosTypesRepo: TypesRepo,
    private val dbDataService: DbDataService<DbEntity>,
    private val dbRecordRefService: DbRecordRefService,
    private val permsComponent: DbPermsComponent,
    private val computedAttsComponent: DbComputedAttsComponent?,
    private val contentService: EcosContentService?
) : AbstractRecordsDao(),
    RecordsAttsDao,
    RecordsQueryDao,
    RecordMutateDao,
    RecordsDeleteDao,
    TxnRecordsDao,
    JobsProvider,
    RecsGroupQueryDao {

    companion object {

        private const val ATT_STATE = "_state"
        private const val ATT_CUSTOM_NAME = "name"

        private val AGG_FUNC_PATTERN = Pattern.compile("^(\\w+)\\((\\w+|\\*)\\)$")

        private val log = KotlinLogging.logger {}

        private val GLOBAL_ATTS = listOf(
            AttributeDef.create()
                .withId(RecordConstants.ATT_PARENT)
                .withType(AttributeType.ASSOC)
                .build()
        ).associateBy { it.id }
    }

    private lateinit var ecosTypeService: DbEcosTypeService
    private lateinit var daoCtx: DbRecordsDaoCtx

    private val listeners: MutableList<DbRecordsListener> = CopyOnWriteArrayList()

    private val recordsJobs: List<Job> by lazy { evalJobs() }

    init {
        dbDataService.registerMigration(AssocsDbMigration(dbRecordRefService))
    }

    fun uploadFile(data: RecordFileUploadData): EntityRef {

        if (contentService == null) {
            error("RecordsDao doesn't support content attributes")
        }
        val typeId = data.ecosType.ifBlank { config.typeRef.id }
        if (typeId.isBlank()) {
            error("Type is blank. Uploading is impossible")
        }
        val typeDef = daoCtx.ecosTypeService.getTypeInfo(typeId)
            ?: error("type '${data.ecosType}' is not found")

        val contentAttribute = typeDef.contentConfig.path.ifBlank { "content" }
        if (contentAttribute.contains(".")) {
            error("You can't upload file with content as complex path")
        }
        if (typeDef.model.attributes.all { it.id != contentAttribute } &&
            typeDef.model.systemAttributes.all { it.id != contentAttribute }
        ) {

            error("Content attribute is not found: $contentAttribute")
        }
        val storageType = typeDef.contentConfig.storageType
        val contentId = daoCtx.recContentHandler.uploadContent(data, storageType) ?: error("File uploading failed")

        val recordToMutate = LocalRecordAtts()
        recordToMutate.setAtt(contentAttribute, contentId)
        recordToMutate.setAtt("name", data.name)
        recordToMutate.setAtt(RecordConstants.ATT_TYPE, TypeUtils.getTypeRef(data.ecosType))
        data.attributes.forEach { key, value ->
            recordToMutate.setAtt(key, value)
        }
        return daoCtx.recContentHandler.withContentDbDataAware {
            EntityRef.create(daoCtx.appName, daoCtx.sourceId, mutate(recordToMutate))
        }
    }

    @JvmOverloads
    fun getContent(recordId: String, attribute: String = "", index: Int = 0): EcosContentData? {
        if (contentService == null) {
            error("RecordsDao doesn't support content attributes")
        }
        val entity = dbDataService.findByExtId(recordId) ?: error("Entity doesn't found with id '$recordId'")
        val notBlankAttribute = attribute.ifBlank { RecordConstants.ATT_CONTENT }
        val dotIdx = notBlankAttribute.indexOf('.')

        if (dotIdx > 0) {
            val contentApi = daoCtx.contentApi ?: error("ContentAPI is null")
            val pathBeforeDot = notBlankAttribute.substring(0, dotIdx)
            val pathAfterDot = notBlankAttribute.substring(dotIdx + 1)
            var linkedRefId = entity.attributes[pathBeforeDot]
            if (linkedRefId is Collection<*>) {
                linkedRefId = linkedRefId.firstOrNull()
            }
            return if (linkedRefId !is Long) {
                null
            } else {
                val entityRef = daoCtx.recordRefService.getRecordRefById(linkedRefId)
                contentApi.getContent(entityRef, pathAfterDot, index)
            }
        } else {
            val atts = getRecordsAtts(listOf(recordId)).first()
            atts.init()
            val contentValue = atts.getAtt(notBlankAttribute)
            return if (contentValue is DbContentValue) {
                contentValue.contentData
            } else {
                null
            }
        }
    }

    fun runMigrationByType(
        type: String,
        typeRef: EntityRef,
        mock: Boolean,
        config: ObjectData
    ) {

        val typeInfo = getRecordsTypeInfo(typeRef) ?: error("Type is null. Migration can't be executed")
        val newConfig = config.deepCopy()
        newConfig["typeInfo"] = typeInfo
        if (!newConfig.has("sourceId")) {
            newConfig["sourceId"] = getId()
        }
        if (!newConfig.has("appName")) {
            newConfig["appName"] = serviceFactory.webappProps.appName
        }

        dbDataService.runMigrationByType(type, mock, newConfig)
    }

    fun runMigrations(typeRef: EntityRef, mock: Boolean = true, diff: Boolean = true): List<String> {
        val typeInfo = getRecordsTypeInfo(typeRef) ?: error("Type is null. Migration can't be executed")
        val columns = ecosTypeService.getColumnsForTypes(listOf(typeInfo)).map { it.column }
        dbDataService.resetColumnsCache()
        val migrations = ArrayList(dbDataService.runMigrations(columns, mock, diff, false))
        if (contentService is DbMigrationsExecutor) {
            migrations.addAll(contentService.runMigrations(mock, diff))
        }
        migrations.addAll(dbRecordRefService.runMigrations(mock, diff))
        return migrations
    }

    fun updatePermissions(records: List<String>) {
        AuthContext.runAsSystem {
            dbDataService.commit(records.map { getEntityToCommit(it) })
        }
    }

    fun getTableMeta(): DbTableMetaDto {
        return dbDataService.getTableMeta()
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

    override fun commit(txnId: UUID, recordIds: List<String>) {
        log.debug { "${this.hashCode()} commit " + txnId + " records: " + recordIds }
        AuthContext.runAsSystem {
            ExtTxnContext.withExtTxn(txnId, false) {
                dbDataService.commit(recordIds.map { getEntityToCommit(it) })
            }
        }
    }

    override fun rollback(txnId: UUID, recordIds: List<String>) {
        log.debug { "${this.hashCode()} rollback " + txnId + " records: " + recordIds }
        ExtTxnContext.withExtTxn(txnId, false) {
            dbDataService.rollback(recordIds)
        }
    }

    override fun getRecordsAtts(recordIds: List<String>): List<AttValue> {

        val txnId = RequestContext.getCurrent()?.ctxData?.txnId

        return if (txnId != null) {
            ExtTxnContext.withExtTxn(txnId, true) {
                getRecordsAttsInTxn(recordIds)
            }
        } else {
            getRecordsAttsInTxn(recordIds)
        }
    }

    private fun getRecordsAttsInTxn(recordsId: List<String>): List<AttValue> {
        return recordsId.map { id ->
            if (id.isEmpty()) {
                DbEmptyRecord(daoCtx)
            } else {
                findDbEntityByExtId(id)?.let { DbRecord(daoCtx, it) } ?: EmptyAttValue.INSTANCE
            }
        }
    }

    private fun findDbEntityByExtId(extId: String): DbEntity? {

        if (AuthContext.isRunAsSystem()) {
            return dbDataService.findByExtId(extId)
        }
        var entity = dbDataService.findByExtId(extId)
        if (entity != null || !config.inheritParentPerms) {
            return entity
        }
        entity = AuthContext.runAsSystem {
            dbDataService.findByExtId(extId)
        } ?: return null

        val parentRefId = entity.attributes[RecordConstants.ATT_PARENT] as? Long
        if (parentRefId == null || parentRefId <= 0) {
            return null
        }
        val parentRef = dbRecordRefService.getRecordRefById(parentRefId)

        if (recordsService.getAtt(parentRef, RecordConstants.ATT_NOT_EXISTS + "?bool").asBoolean()) {
            return null
        }
        return entity
    }

    override fun queryRecords(recsQuery: RecordsQuery): Any? {

        if (recsQuery.language != PredicateService.LANGUAGE_PREDICATE) {
            return null
        }
        val originalPredicate = recsQuery.getQuery(Predicate::class.java)

        val queryTypePred = PredicateUtils.filterValuePredicates(originalPredicate) {
            it.getAttribute() == "_type" && it.getValue().asText().isNotBlank()
        }.orElse(null)

        val ecosTypeRef = if (queryTypePred is ValuePredicate) {
            EntityRef.valueOf(queryTypePred.getValue().asText())
        } else {
            config.typeRef
        }

        val attributesById = if (EntityRef.isNotEmpty(ecosTypeRef)) {
            val typeInfo = ecosTypeService.getTypeInfo(ecosTypeRef.getLocalId())
            typeInfo?.model?.getAllAttributes()?.associateBy { it.id } ?: emptyMap()
        } else {
            emptyMap()
        }

        val predicate = PredicateUtils.mapAttributePredicates(recsQuery.getQuery(Predicate::class.java)) {
            val attribute = it.getAttribute()
            if (it is ValuePredicate) {
                when (attribute) {
                    RecordConstants.ATT_TYPE -> {
                        val typeLocalId = EntityRef.valueOf(it.getValue().asText()).getLocalId()
                        ValuePredicate(DbEntity.TYPE, it.getType(), typeLocalId)
                    }
                    else -> {
                        var newPred = if (DbRecord.ATTS_MAPPING.containsKey(attribute)) {
                            ValuePredicate(DbRecord.ATTS_MAPPING[attribute], it.getType(), it.getValue())
                        } else {
                            it
                        }
                        val attDef = attributesById[newPred.getAttribute()]
                            ?: GLOBAL_ATTS[newPred.getAttribute()]

                        if (newPred.getType() == ValuePredicate.Type.EQ) {
                            if (newPred.getAttribute() == DbEntity.NAME || attDef?.type == AttributeType.MLTEXT) {
                                // MLText fields stored as json text like '{"en":"value"}'
                                // and for equals predicate we should use '"value"' instead of 'value' to search
                                // and replace "EQ" to "CONTAINS"
                                newPred = ValuePredicate(
                                    newPred.getAttribute(),
                                    ValuePredicate.Type.CONTAINS,
                                    DataValue.createStr(newPred.getValue().toString())
                                )
                            }
                        }
                        if (DbRecordsUtils.isAssocLikeAttribute(attDef) && newPred.getValue().isTextual()) {
                            val txtValue = newPred.getValue().asText()
                            if (txtValue.isNotBlank()) {
                                val refs = dbRecordRefService.getIdByRecordRefs(
                                    listOf(
                                        EntityRef.valueOf(txtValue)
                                    )
                                )
                                newPred = ValuePredicate(
                                    newPred.getAttribute(),
                                    newPred.getType(),
                                    refs.firstOrNull() ?: -1L
                                )
                            }
                        }
                        newPred
                    }
                }
            } else if (it is EmptyPredicate) {
                if (DbRecord.ATTS_MAPPING.containsKey(attribute)) {
                    EmptyPredicate(DbRecord.ATTS_MAPPING[attribute])
                } else {
                    it
                }
            } else {
                log.error { "Unknown predicate type: ${it::class}" }
                null
            }
        }

        val selectFunctions = mutableListOf<AggregateFunc>()
        AttContext.getCurrentNotNull().getSchemaAtt().inner.forEach {
            if (it.name.contains("(")) {
                val matcher = AGG_FUNC_PATTERN.matcher(it.name)
                if (matcher.matches()) {
                    selectFunctions.add(
                        AggregateFunc(
                            it.name,
                            matcher.group(1),
                            matcher.group(2)
                        )
                    )
                }
            }
        }

        val page = recsQuery.page
        val findRes = dbDataService.find(
            predicate ?: Predicates.alwaysTrue(),
            recsQuery.sortBy.map {
                DbFindSort(DbRecord.ATTS_MAPPING.getOrDefault(it.attribute, it.attribute), it.ascending)
            },
            DbFindPage(
                page.skipCount,
                if (page.maxItems == -1) {
                    config.queryMaxItems
                } else {
                    min(page.maxItems, config.queryMaxItems)
                }
            ),
            false,
            recsQuery.groupBy,
            selectFunctions
        )

        val queryRes = RecsQueryRes<Any>()
        queryRes.setTotalCount(findRes.totalCount)
        queryRes.setRecords(findRes.entities.map { DbRecord(daoCtx, it) })
        queryRes.setHasMore(findRes.totalCount > findRes.entities.size + page.skipCount)

        return queryRes
    }

    override fun delete(recordIds: List<String>): List<DelStatus> {

        if (!config.deletable) {
            error("Records DAO is not deletable. Records can't be deleted: '$recordIds'")
        }

        val txnId = RequestContext.getCurrent()?.ctxData?.txnId

        return if (txnId != null) {
            ExtTxnContext.withExtTxn(txnId, false) {
                deleteInTxn(recordIds)
            }
        } else {
            deleteInTxn(recordIds)
        }
    }

    private fun deleteInTxn(recordsId: List<String>): List<DelStatus> {

        for (recordId in recordsId) {
            dbDataService.findByExtId(recordId)?.let { entity ->
                dbDataService.delete(entity)
                val typeInfo = ecosTypeService.getTypeInfo(entity.type)
                if (typeInfo != null) {
                    val event = DbRecordDeletedEvent(DbRecord(daoCtx, entity), typeInfo)
                    listeners.forEach {
                        it.onDeleted(event)
                    }
                }
            }
        }

        return recordsId.map { DelStatus.OK }
    }

    private fun getTypeIdForRecord(record: LocalRecordAtts): String {

        val typeRefStr = record.attributes[RecordConstants.ATT_TYPE].asText()
        val typeRefFromAtts = EntityRef.valueOf(typeRefStr).getLocalId()
        if (typeRefFromAtts.isNotBlank()) {
            return typeRefFromAtts
        }

        val extId = record.id.ifBlank { record.attributes["id"].asText() }
        if (extId.isNotBlank()) {
            val typeFromRecord = AuthContext.runAsSystem {
                dbDataService.findByExtId(extId)?.type
            }
            if (!typeFromRecord.isNullOrBlank()) {
                return typeFromRecord
            }
        }

        if (EntityRef.isNotEmpty(config.typeRef)) {
            return config.typeRef.id
        }

        error(
            "${RecordConstants.ATT_TYPE} attribute is mandatory for mutation. " +
                "SourceId: '${getId()}' Record: ${record.id}"
        )
    }

    override fun mutate(record: LocalRecordAtts): String {
        if (!config.updatable) {
            error("Records DAO is not mutable. Record can't be mutated: '${record.id}'")
        }
        val txnId = RequestContext.getCurrent()?.ctxData?.txnId
        return if (txnId != null) {
            ExtTxnContext.withExtTxn(txnId, false) {
                mutateInTxn(record, true)
            }
        } else {
            mutateInTxn(record, false)
        }
    }

    private fun mutateInTxn(record: LocalRecordAtts, txnExists: Boolean): String {

        val typeId = getTypeIdForRecord(record)
        val typeInfo = ecosTypeService.getTypeInfo(typeId)
            ?: error("Type is not found: '$typeId'. Record ID: '${record.id}'")

        val typesAttColumns = ecosTypeService.getColumnsForTypes(listOf(typeInfo))
        val typesColumns = typesAttColumns.map { it.column }
        val typesColumnNames = typesColumns.map { it.name }.toSet()

        return mutateRecordInTxn(
            record,
            typeInfo,
            typesAttColumns,
            typesColumns,
            typesColumnNames,
            txnExists
        )
    }

    private fun mutateRecordInTxn(
        record: LocalRecordAtts,
        typeDef: TypeInfo,
        typesAttColumns: List<EcosAttColumnDef>,
        typesColumns: List<DbColumnDef>,
        typesColumnNames: Set<String>,
        txnExists: Boolean
    ): String {

        if (record.attributes["__updatePermissions"].asBoolean()) {
            if (!AuthContext.getCurrentAuthorities().contains(AuthRole.ADMIN)) {
                error("Permissions update allowed only for admin. Record: $record sourceId: '${getId()}'")
            }
            updatePermissions(listOf(record.id))
            return record.id
        }
        if (record.attributes["__runAssocsMigration"].asBoolean()) {
            if (!AuthContext.getCurrentAuthorities().contains(AuthRole.ADMIN)) {
                error("Assocs migration allowed only for admin")
            }
            ExtTxnContext.withoutModifiedMeta {
                ExtTxnContext.withoutExtTxn {
                    runMigrationByType(AssocsDbMigration.TYPE, EntityRef.EMPTY, false, ObjectData.create())
                }
            }
            return record.id
        }

        if (!AuthContext.isRunAsSystem()) {
            val deniedAtts = typesAttColumns.filter {
                it.systemAtt && record.attributes.has(it.attribute.id)
            }.map {
                it.attribute.id
            }
            if (deniedAtts.isNotEmpty()) {
                error("Permission denied. You should be in system context to change system attributes: $deniedAtts")
            }
        }

        val extId = record.id.ifEmpty { record.attributes["id"].asText() }

        val recToMutate: DbEntity = if (extId.isEmpty()) {
            DbEntity()
        } else {
            var entity = findDbEntityByExtId(extId)
            if (entity == null) {
                if (record.id.isNotEmpty()) {
                    error("Record with id: '$extId' doesn't found")
                } else {
                    entity = DbEntity()
                }
            }
            entity
        }

        if (recToMutate.id != DbEntity.NEW_REC_ID && !AuthContext.isRunAsSystem()) {
            val txnId = RequestContext.getCurrent()?.ctxData?.txnId
            if (!txnExists || recToMutate.attributes[DbDataServiceImpl.COLUMN_EXT_TXN_ID] != txnId) {
                val recordPerms = getRecordPerms(recToMutate.extId)
                if (!recordPerms.isCurrentUserHasWritePerms()) {
                    error("Permissions Denied. You can't change record '${record.id}'")
                }
            }
        }

        var customExtId = record.attributes["id"].asText()
        if (customExtId.isBlank()) {
            customExtId = record.attributes[ScalarType.LOCAL_ID.mirrorAtt].asText()
        }
        if (customExtId.isNotBlank()) {
            if (recToMutate.extId != customExtId) {
                AuthContext.runAsSystem {
                    if (dbDataService.findByExtId(customExtId) != null) {
                        error("Record with ID $customExtId already exists. You should mutate it directly")
                    } else {
                        // copy of existing record
                        recToMutate.id = DbEntity.NEW_REC_ID
                    }
                }
                recToMutate.extId = customExtId
            }
        }

        if (recToMutate.id == DbEntity.NEW_REC_ID) {
            if (!config.insertable) {
                error("Records DAO doesn't support new records creation. Record ID: '${record.id}'")
            }
        } else {
            if (!config.updatable) {
                error("Records DAO doesn't support records updating. Record ID: '${record.id}'")
            }
        }

        if (recToMutate.extId.isEmpty()) {
            recToMutate.extId = UUID.randomUUID().toString()
        }

        val recAttributes = record.attributes.deepCopy()
        if (record.attributes.has(DbRecord.ATT_NAME) || record.attributes.has(ScalarType.DISP.mirrorAtt)) {
            val newName = if (record.attributes.has(DbRecord.ATT_NAME)) {
                record.attributes[DbRecord.ATT_NAME]
            } else {
                record.attributes[ScalarType.DISP.mirrorAtt]
            }
            recAttributes[ATT_CUSTOM_NAME] = newName
            recAttributes.remove(DbRecord.ATT_NAME)
            recAttributes.remove(ScalarType.DISP.mirrorAtt)
        }

        daoCtx.mutAssocHandler.preProcessContentAtts(
            recAttributes,
            recToMutate,
            typesAttColumns,
            typeDef.contentConfig.storageType
        )
        daoCtx.mutAssocHandler.replaceRefsById(recAttributes, typesAttColumns)

        val operations = daoCtx.mutAttOperationHandler.extractAttValueOperations(recAttributes)
        operations.forEach {
            if (!recAttributes.has(it.getAttName())) {
                val value = it.invoke(recToMutate.attributes[it.getAttName()])
                recAttributes[it.getAttName()] = value
            }
        }

        val recordEntityBeforeMutation = recToMutate.copy()

        val fullColumns = ArrayList(typesColumns)
        val perms = if (recToMutate.id == DbEntity.NEW_REC_ID || AuthContext.isRunAsSystem()) {
            null
        } else {
            getRecordPerms(recToMutate.extId)
        }
        setMutationAtts(recToMutate, recAttributes, typesColumns, perms)
        val optionalAtts = DbRecord.OPTIONAL_COLUMNS.filter { !typesColumnNames.contains(it.name) }
        if (optionalAtts.isNotEmpty()) {
            fullColumns.addAll(setMutationAtts(recToMutate, recAttributes, optionalAtts))
        }

        if (recAttributes.has(ATT_STATE)) {
            val state = recAttributes[ATT_STATE].asText()
            recToMutate.attributes[DbRecord.COLUMN_IS_DRAFT.name] = state == "draft"
            fullColumns.add(DbRecord.COLUMN_IS_DRAFT)
        }

        recToMutate.type = typeDef.id

        if (recAttributes.has(StatusConstants.ATT_STATUS)) {
            val newStatus = recAttributes[StatusConstants.ATT_STATUS].asText()
            if (newStatus.isNotBlank()) {
                if (typeDef.model.statuses.any { it.id == newStatus }) {
                    recToMutate.status = newStatus
                } else {
                    error(
                        "Unknown status: '$newStatus'. " +
                            "Available statuses: ${typeDef.model.statuses.joinToString { it.id }}"
                    )
                }
            }
        }

        val isNewRecord = recToMutate.id == DbEntity.NEW_REC_ID
        if (isNewRecord) {
            val sourceIdMapping = RequestContext.getCurrentNotNull().ctxData.sourceIdMapping
            val currentAppName = serviceFactory.webappProps.appName
            val currentRef = EntityRef.create(currentAppName, getId(), recToMutate.extId)
            val newRecordRef = RecordRefUtils.mapAppIdAndSourceId(currentRef, currentAppName, sourceIdMapping)
            recToMutate.refId = dbRecordRefService.getOrCreateIdByRecordRefs(listOf(newRecordRef))[0]
        }
        var recAfterSave = dbDataService.save(recToMutate, fullColumns)

        recAfterSave = AuthContext.runAsSystem {
            val newEntity = if (computedAttsComponent != null) {
                computeAttsToStore(
                    computedAttsComponent,
                    recAfterSave,
                    isNewRecord,
                    typeDef.id,
                    fullColumns
                )
            } else {
                recAfterSave
            }
            if (!txnExists) {
                dbDataService.commit(listOf(getEntityToCommit(newEntity.extId)))
            }
            newEntity
        }

        daoCtx.mutAssocHandler.processChildrenAfterMutation(
            recordEntityBeforeMutation,
            recAfterSave,
            record.attributes,
            typesAttColumns
        )
        daoCtx.mutAssocHandler.processParentAfterMutation(
            recordEntityBeforeMutation,
            recAfterSave,
            record.attributes
        )

        daoCtx.recEventsHandler.emitEventsAfterMutation(recordEntityBeforeMutation, recAfterSave, isNewRecord)

        return recAfterSave.extId
    }

    private fun computeAttsToStore(
        component: DbComputedAttsComponent,
        entity: DbEntity,
        isNewRecord: Boolean,
        recTypeId: String,
        columns: List<DbColumnDef>
    ): DbEntity {

        val typeRef = TypeUtils.getTypeRef(recTypeId)
        val atts = component.computeAttsToStore(DbRecord(daoCtx, entity), isNewRecord, typeRef)

        val fullColumns = ArrayList(columns)
        DbRecord.COMPUTABLE_OPTIONAL_COLUMNS.forEach {
            if (atts.has(it.name)) {
                fullColumns.add(it)
            }
        }
        var changed = setMutationAtts(entity, atts, fullColumns).isNotEmpty()

        val dispName = component.computeDisplayName(DbRecord(daoCtx, entity), typeRef)
        if (entity.name != dispName) {
            entity.name = dispName
            changed = true
        }

        if (!changed) {
            return entity
        }

        return dbDataService.save(entity, columns)
    }

    private fun getEntityToCommit(recordId: String): DbCommitEntityDto {
        return DbCommitEntityDto(
            recordId,
            getRecordPerms(recordId).getAuthoritiesWithReadPermission(),
            // todo
            emptySet()
        )
    }

    fun getRecordPerms(recordId: String): DbRecordPerms {
        // Optimization to enable caching
        return RequestContext.doWithCtx(
            serviceFactory,
            {
                it.withReadOnly(true)
            },
            {
                AuthContext.runAsSystem {
                    permsComponent.getRecordPerms(EntityRef.create(getId(), recordId))
                }
            }
        )
    }

    private fun setMutationAtts(
        recToMutate: DbEntity,
        atts: ObjectData,
        columns: List<DbColumnDef>,
        perms: DbRecordPerms? = null
    ): List<DbColumnDef> {

        if (atts.size() == 0) {
            return emptyList()
        }
        val currentUser = AuthContext.getCurrentUser()

        val notEmptyColumns = ArrayList<DbColumnDef>()
        for (dbColumnDef in columns) {
            if (!atts.has(dbColumnDef.name)) {
                continue
            }
            if (perms?.isCurrentUserHasAttWritePerms(dbColumnDef.name) == false) {
                log.warn {
                    "User $currentUser can't change attribute ${dbColumnDef.name} " +
                        "for record ${getId()}@${recToMutate.extId}"
                }
            } else {
                notEmptyColumns.add(dbColumnDef)
                val value = atts[dbColumnDef.name]
                recToMutate.attributes[dbColumnDef.name] = daoCtx.mutConverter.convert(
                    value,
                    dbColumnDef.multiple,
                    dbColumnDef.type
                )
            }
        }
        return notEmptyColumns
    }

    override fun getId() = config.id

    fun addListener(listener: DbRecordsListener) {
        this.listeners.add(listener)
    }

    fun removeListener(listener: DbRecordsListener) {
        this.listeners.remove(listener)
    }

    override fun getJobs(): List<Job> {
        return recordsJobs
    }

    private fun evalJobs(): List<Job> {
        if (dbDataService !is DbJobsProvider) {
            return emptyList()
        }
        val jobs = dbDataService.getJobs()
        if (jobs.isEmpty()) {
            return emptyList()
        }
        return jobs.map {
            object : PeriodicJob {
                override fun getInitDelay(): Long {
                    return it.getInitDelay()
                }

                override fun execute(): Boolean {
                    return it.execute()
                }

                override fun getPeriod(): Long {
                    return it.getPeriod()
                }
            }
        }
    }

    override fun setRecordsServiceFactory(serviceFactory: RecordsServiceFactory) {
        super.setRecordsServiceFactory(serviceFactory)
        ecosTypeService = DbEcosTypeService(ecosTypesRepo)
        daoCtx = DbRecordsDaoCtx(
            serviceFactory.webappProps.appName,
            getId(),
            dbDataService.getTableRef(),
            config,
            contentService,
            dbRecordRefService,
            ecosTypeService,
            serviceFactory.recordsServiceV1,
            serviceFactory.getEcosWebAppApi()?.getContentApi(),
            serviceFactory.getEcosWebAppApi()?.getAuthoritiesApi(),
            listeners,
            this
        )
    }
}
