package ru.citeck.ecos.data.sql.records

import mu.KotlinLogging
import ru.citeck.ecos.commons.data.DataValue
import ru.citeck.ecos.commons.data.MLText
import ru.citeck.ecos.commons.data.ObjectData
import ru.citeck.ecos.commons.json.Json
import ru.citeck.ecos.commons.utils.DataUriUtil
import ru.citeck.ecos.context.lib.auth.AuthContext
import ru.citeck.ecos.context.lib.auth.AuthRole
import ru.citeck.ecos.data.sql.content.EcosContentMeta
import ru.citeck.ecos.data.sql.content.EcosContentService
import ru.citeck.ecos.data.sql.content.data.storage.local.EcosContentLocalStorage
import ru.citeck.ecos.data.sql.dto.DbColumnDef
import ru.citeck.ecos.data.sql.dto.DbColumnType
import ru.citeck.ecos.data.sql.ecostype.DbEcosTypeService
import ru.citeck.ecos.data.sql.job.DbJobsProvider
import ru.citeck.ecos.data.sql.meta.dto.DbTableMetaDto
import ru.citeck.ecos.data.sql.records.computed.DbComputedAttsComponent
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
import ru.citeck.ecos.data.sql.txn.ExtTxnContext
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType
import ru.citeck.ecos.model.lib.status.constants.StatusConstants
import ru.citeck.ecos.model.lib.status.dto.StatusDef
import ru.citeck.ecos.model.lib.type.dto.TypeInfo
import ru.citeck.ecos.model.lib.type.repo.TypesRepo
import ru.citeck.ecos.model.lib.type.service.utils.TypeUtils
import ru.citeck.ecos.records2.RecordConstants
import ru.citeck.ecos.records2.RecordRef
import ru.citeck.ecos.records2.predicate.PredicateService
import ru.citeck.ecos.records2.predicate.PredicateUtils
import ru.citeck.ecos.records2.predicate.model.EmptyPredicate
import ru.citeck.ecos.records2.predicate.model.Predicate
import ru.citeck.ecos.records2.predicate.model.ValuePredicate
import ru.citeck.ecos.records2.source.dao.local.job.Job
import ru.citeck.ecos.records2.source.dao.local.job.JobsProvider
import ru.citeck.ecos.records2.source.dao.local.job.PeriodicJob
import ru.citeck.ecos.records3.RecordsServiceFactory
import ru.citeck.ecos.records3.record.atts.dto.LocalRecordAtts
import ru.citeck.ecos.records3.record.atts.dto.RecordAtts
import ru.citeck.ecos.records3.record.atts.schema.ScalarType
import ru.citeck.ecos.records3.record.atts.value.AttEdge
import ru.citeck.ecos.records3.record.atts.value.AttValue
import ru.citeck.ecos.records3.record.atts.value.impl.EmptyAttValue
import ru.citeck.ecos.records3.record.dao.AbstractRecordsDao
import ru.citeck.ecos.records3.record.dao.atts.RecordsAttsDao
import ru.citeck.ecos.records3.record.dao.delete.DelStatus
import ru.citeck.ecos.records3.record.dao.delete.RecordsDeleteDao
import ru.citeck.ecos.records3.record.dao.mutate.RecordsMutateDao
import ru.citeck.ecos.records3.record.dao.query.RecordsQueryDao
import ru.citeck.ecos.records3.record.dao.query.dto.query.RecordsQuery
import ru.citeck.ecos.records3.record.dao.query.dto.res.RecsQueryRes
import ru.citeck.ecos.records3.record.dao.txn.TxnRecordsDao
import ru.citeck.ecos.records3.record.request.RequestContext
import ru.citeck.ecos.records3.utils.RecordRefUtils
import java.io.InputStream
import java.net.URLDecoder
import java.net.URLEncoder
import java.util.*
import java.util.concurrent.CopyOnWriteArrayList
import java.util.function.Function
import kotlin.collections.ArrayList
import kotlin.collections.HashMap
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
    RecordsMutateDao,
    RecordsDeleteDao,
    TxnRecordsDao,
    JobsProvider {

    companion object {

        private const val ATT_STATE = "_state"

        private const val ATT_NAME = "_name"
        private const val ATT_CUSTOM_NAME = "name"

        private const val CONTENT_DATA = "content-data"

        private val ATTS_MAPPING = mapOf(
            "id" to DbEntity.EXT_ID,
            ScalarType.DISP.mirrorAtt to DbEntity.NAME,
            ATT_NAME to DbEntity.NAME,
            RecordConstants.ATT_CREATED to DbEntity.CREATED,
            RecordConstants.ATT_CREATOR to DbEntity.CREATOR,
            RecordConstants.ATT_MODIFIED to DbEntity.MODIFIED,
            RecordConstants.ATT_MODIFIER to DbEntity.MODIFIER,
            ScalarType.LOCAL_ID.mirrorAtt to DbEntity.EXT_ID,
            StatusConstants.ATT_STATUS to DbEntity.STATUS,
            RecordConstants.ATT_CONTENT to "content"
        )

        private val OPTIONAL_COLUMNS = listOf(
            DbColumnDef.create {
                withName("_docNum")
                withType(DbColumnType.INT)
            },
            DbColumnDef.create {
                withName("_proc")
                withMultiple(true)
                withType(DbColumnType.JSON)
            },
            DbColumnDef.create {
                withName("_cipher")
                withType(DbColumnType.JSON)
            },
            DbColumnDef.create {
                withName("_parent")
                withType(DbColumnType.LONG)
            }
        )

        private const val ATT_ADD_PREFIX = "att_add_"
        private const val ATT_REMOVE_PREFIX = "att_rem_"

        private val OPERATION_PREFIXES = listOf(
            ATT_ADD_PREFIX,
            ATT_REMOVE_PREFIX
        )

        private val COLUMN_IS_DRAFT = DbColumnDef.create {
            withName("_isDraft")
            withType(DbColumnType.BOOLEAN)
        }

        private val log = KotlinLogging.logger {}
    }

    private lateinit var ecosTypeService: DbEcosTypeService

    private val listeners: MutableList<DbRecordsListener> = CopyOnWriteArrayList()

    private val recordsJobs: List<Job> by lazy { evalJobs() }

    init {
        dbDataService.registerMigration(AssocsDbMigration(dbRecordRefService))
    }

    fun <T> readContent(recordId: String, attribute: String, action: (EcosContentMeta, InputStream) -> T): T {
        if (contentService == null) {
            error("RecordsDao doesn't support content attributes")
        }
        val entity = dbDataService.findByExtId(recordId) ?: error("Entity doesn't found with id '$recordId'")

        val attValue = entity.attributes[attribute]
        if (attValue !is Long) {
            error("Attribute doesn't have content id. Record: '$recordId' Attribute: $attValue")
        }
        return contentService.readContent(attValue, action)
    }

    fun runMigrationByType(
        type: String,
        typeRef: RecordRef,
        mock: Boolean,
        config: ObjectData
    ) {

        val typeInfo = getRecordsTypeInfo(typeRef) ?: error("Type is null. Migration can't be executed")
        val newConfig = config.deepCopy()
        newConfig.set("typeInfo", typeInfo)
        if (!newConfig.has("sourceId")) {
            newConfig.set("sourceId", getId())
        }
        if (!newConfig.has("appName")) {
            newConfig.set("appName", serviceFactory.properties.appName)
        }

        dbDataService.runMigrationByType(type, mock, newConfig)
    }

    fun runMigrations(typeRef: RecordRef, mock: Boolean = true, diff: Boolean = true): List<String> {
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

    private fun getRecordsTypeInfo(typeRef: RecordRef): TypeInfo? {
        val type = getRecordsTypeRef(typeRef)
        if (RecordRef.isEmpty(type)) {
            log.warn { "Type is not defined for Records DAO" }
            return null
        }
        return ecosTypeService.getTypeInfo(type.id)
    }

    private fun getRecordsTypeRef(typeRef: RecordRef): RecordRef {
        return if (RecordRef.isEmpty(typeRef)) {
            config.typeRef
        } else {
            typeRef
        }
    }

    override fun commit(txnId: UUID, recordsId: List<String>) {
        log.debug { "${this.hashCode()} commit " + txnId + " records: " + recordsId }
        AuthContext.runAsSystem {
            ExtTxnContext.withExtTxn(txnId, false) {
                dbDataService.commit(recordsId.map { getEntityToCommit(it) })
            }
        }
    }

    override fun rollback(txnId: UUID, recordsId: List<String>) {
        log.debug { "${this.hashCode()} rollback " + txnId + " records: " + recordsId }
        ExtTxnContext.withExtTxn(txnId, false) {
            dbDataService.rollback(recordsId)
        }
    }

    override fun getRecordsAtts(recordsId: List<String>): List<*> {

        val txnId = RequestContext.getCurrentNotNull().ctxData.txnId

        return if (txnId != null) {
            ExtTxnContext.withExtTxn(txnId, true) {
                getRecordsAttsInTxn(recordsId)
            }
        } else {
            getRecordsAttsInTxn(recordsId)
        }
    }

    private fun getRecordsAttsInTxn(recordsId: List<String>): List<*> {
        return recordsId.map { id ->
            if (id.isEmpty()) {
                EmptyRecord()
            } else {
                findDbEntityByExtId(id)?.let { Record(it) } ?: EmptyAttValue.INSTANCE
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

        val queryTypePred = PredicateUtils.filterValuePredicates(
            originalPredicate,
            Function {
                it.getAttribute() == "_type" && it.getValue().asText().isNotBlank()
            }
        ).orElse(null)

        val ecosTypeRef = if (queryTypePred is ValuePredicate) {
            RecordRef.valueOf(queryTypePred.getValue().asText())
        } else {
            config.typeRef
        }

        val attributesById = if (RecordRef.isNotEmpty(ecosTypeRef)) {
            val typeInfo = ecosTypeService.getTypeInfo(ecosTypeRef.id)
            typeInfo?.model?.attributes?.associateBy { it.id } ?: emptyMap()
        } else {
            emptyMap()
        }

        val predicate = PredicateUtils.mapAttributePredicates(recsQuery.getQuery(Predicate::class.java)) {
            val attribute = it.getAttribute()
            if (it is ValuePredicate) {
                when (attribute) {
                    "_type" -> {
                        val typeLocalId = RecordRef.valueOf(it.getValue().asText()).id
                        ValuePredicate(DbEntity.TYPE, it.getType(), typeLocalId)
                    }
                    else -> {
                        var newPred = if (ATTS_MAPPING.containsKey(attribute)) {
                            ValuePredicate(ATTS_MAPPING[attribute], it.getType(), it.getValue())
                        } else {
                            it
                        }
                        val attDef = attributesById[newPred.getAttribute()]
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
                                        RecordRef.valueOf(txtValue)
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
                if (ATTS_MAPPING.containsKey(attribute)) {
                    EmptyPredicate(ATTS_MAPPING[attribute])
                } else {
                    it
                }
            } else {
                log.error { "Unknown predicate type: ${it::class}" }
                null
            }
        }

        val page = recsQuery.page
        val findRes = dbDataService.find(
            predicate,
            recsQuery.sortBy.map {
                DbFindSort(ATTS_MAPPING.getOrDefault(it.attribute, it.attribute), it.ascending)
            },
            DbFindPage(
                page.skipCount,
                if (page.maxItems == -1) {
                    config.queryMaxItems
                } else {
                    min(page.maxItems, config.queryMaxItems)
                }
            )
        )

        val queryRes = RecsQueryRes<Any>()
        queryRes.setTotalCount(findRes.totalCount)
        queryRes.setRecords(findRes.entities.map { Record(it) })
        queryRes.setHasMore(findRes.totalCount > findRes.entities.size + page.skipCount)

        return queryRes
    }

    override fun delete(recordsId: List<String>): List<DelStatus> {

        if (!config.deletable) {
            error("Records DAO is not deletable. Records can't be deleted: '$recordsId'")
        }

        val txnId = RequestContext.getCurrentNotNull().ctxData.txnId

        return if (txnId != null) {
            ExtTxnContext.withExtTxn(txnId, false) {
                deleteInTxn(recordsId)
            }
        } else {
            deleteInTxn(recordsId)
        }
    }

    private fun deleteInTxn(recordsId: List<String>): List<DelStatus> {

        for (recordId in recordsId) {
            dbDataService.findByExtId(recordId)?.let { entity ->
                dbDataService.delete(entity)
                val typeInfo = ecosTypeService.getTypeInfo(entity.type)
                if (typeInfo != null) {
                    val event = DbRecordDeletedEvent(Record(entity), typeInfo)
                    listeners.forEach {
                        it.onDeleted(event)
                    }
                }
            }
        }

        return recordsId.map { DelStatus.OK }
    }

    private fun getTypeIdForRecord(record: LocalRecordAtts): String {

        val typeRefStr = record.attributes.get(RecordConstants.ATT_TYPE).asText()
        val typeRefFromAtts = RecordRef.valueOf(typeRefStr).id
        if (typeRefFromAtts.isNotBlank()) {
            return typeRefFromAtts
        }

        val extId = record.id.ifBlank { record.attributes.get("id").asText() }
        if (extId.isNotBlank()) {
            val typeFromRecord = AuthContext.runAsSystem {
                dbDataService.findByExtId(extId)?.type
            }
            if (!typeFromRecord.isNullOrBlank()) {
                return typeFromRecord
            }
        }

        if (RecordRef.isNotEmpty(config.typeRef)) {
            return config.typeRef.id
        }

        error(
            "${RecordConstants.ATT_TYPE} attribute is mandatory for mutation. " +
                "SourceId: '${getId()}' Record: ${record.id}"
        )
    }

    override fun mutate(records: List<LocalRecordAtts>): List<String> {
        if (!config.updatable) {
            error("Records DAO is not mutable. Records can't be mutated: '${records.map { it.id }}'")
        }
        val txnId = RequestContext.getCurrentNotNull().ctxData.txnId
        return if (txnId != null) {
            ExtTxnContext.withExtTxn(txnId, false) {
                mutateInTxn(records, true)
            }
        } else {
            mutateInTxn(records, false)
        }
    }

    private fun mutateInTxn(records: List<LocalRecordAtts>, txnExists: Boolean): List<String> {

        val typesId = records.map { getTypeIdForRecord(it) }
        val typesInfo = typesId.mapIndexed { idx, typeId ->
            ecosTypeService.getTypeInfo(typeId)
                ?: error("Type is not found: '$typeId'. Record ID: '${records[idx]}'")
        }

        val typesAttColumns = ecosTypeService.getColumnsForTypes(typesInfo)
        val typesColumns = typesAttColumns.map { it.column }
        val typesColumnNames = typesColumns.map { it.name }.toSet()

        return records.mapIndexed { recordIdx, record ->

            if (record.attributes.get("__updatePermissions").asBoolean()) {
                if (!AuthContext.getCurrentAuthorities().contains(AuthRole.ADMIN)) {
                    error("Permissions update allowed only for admin. Record: $record sourceId: '${getId()}'")
                }
                updatePermissions(listOf(record.id))
                return@mapIndexed record.id
            }
            if (record.attributes.get("__runAssocsMigration").asBoolean()) {
                if (!AuthContext.getCurrentAuthorities().contains(AuthRole.ADMIN)) {
                    error("Assocs migration allowed only for admin")
                }
                ExtTxnContext.withoutModifiedMeta {
                    ExtTxnContext.withoutExtTxn {
                        runMigrationByType(AssocsDbMigration.TYPE, RecordRef.EMPTY, false, ObjectData.create())
                    }
                }
                return@mapIndexed record.id
            }

            val extId = record.id.ifEmpty { record.attributes.get("id").asText() }

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
                val txnId = RequestContext.getCurrentNotNull().ctxData.txnId
                if (!txnExists || recToMutate.attributes[DbDataServiceImpl.COLUMN_EXT_TXN_ID] != txnId) {
                    val recordPerms = getRecordPerms(recToMutate.extId)
                    if (!recordPerms.isCurrentUserHasWritePerms()) {
                        error("Permissions Denied. You can't change record '${record.id}'")
                    }
                }
            }

            val recordTypeId = typesId[recordIdx]

            var customExtId = record.attributes.get("id").asText()
            if (customExtId.isBlank()) {
                customExtId = record.attributes.get(ScalarType.LOCAL_ID.mirrorAtt).asText()
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
            if (record.attributes.has(ATT_NAME) || record.attributes.has(ScalarType.DISP.mirrorAtt)) {
                val newName = if (record.attributes.has(ATT_NAME)) {
                    record.attributes.get(ATT_NAME)
                } else {
                    record.attributes.get(ScalarType.DISP.mirrorAtt)
                }
                recAttributes.set(ATT_CUSTOM_NAME, newName)
                recAttributes.remove(ATT_NAME)
                recAttributes.remove(ScalarType.DISP.mirrorAtt)
            }

            typesAttColumns.forEach {
                if (it.attribute.type == AttributeType.CONTENT) {
                    val contentData = recAttributes.get(it.attribute.id)
                    recAttributes.set(
                        it.column.name,
                        uploadContent(
                            recToMutate,
                            it.attribute.id,
                            contentData,
                            it.column.multiple
                        )
                    )
                } else if (DbRecordsUtils.isAssocLikeAttribute(it.attribute)) {
                    val assocValue = recAttributes.get(it.attribute.id)
                    val convertedValue = preProcessAssocBeforeMutate(
                        recToMutate.extId,
                        it.attribute.id,
                        assocValue
                    )
                    if (convertedValue !== assocValue) {
                        recAttributes.set(it.attribute.id, convertedValue)
                    }
                }
            }

            val assocAttsDef = typesAttColumns.filter { DbRecordsUtils.isAssocLikeAttribute(it.attribute) }

            if (assocAttsDef.isNotEmpty()) {
                val assocRefs = mutableSetOf<RecordRef>()
                assocAttsDef.forEach {
                    if (recAttributes.has(it.attribute.id)) {
                        extractRecordRefs(recAttributes.get(it.attribute.id), assocRefs)
                    } else {
                        OPERATION_PREFIXES.forEach { prefix ->
                            extractRecordRefs(recAttributes.get(prefix + it.attribute.id), assocRefs)
                        }
                    }
                }
                val idByRef = mutableMapOf<RecordRef, Long>()
                if (assocRefs.isNotEmpty()) {
                    val refsList = assocRefs.toList()
                    val refsId = dbRecordRefService.getOrCreateIdByRecordRefs(refsList)
                    for ((idx, ref) in refsList.withIndex()) {
                        idByRef[ref] = refsId[idx]
                    }
                }
                assocAttsDef.forEach {
                    if (recAttributes.has(it.attribute.id)) {
                        recAttributes.set(
                            it.attribute.id,
                            replaceRecordRefsToId(recAttributes.get(it.attribute.id), idByRef)
                        )
                    } else {
                        OPERATION_PREFIXES.forEach { prefix ->
                            val attWithPrefix = prefix + it.attribute.id
                            if (recAttributes.has(attWithPrefix)) {
                                recAttributes.set(
                                    attWithPrefix,
                                    replaceRecordRefsToId(recAttributes.get(attWithPrefix), idByRef)
                                )
                            }
                        }
                    }
                }
            }

            val operations = extractAttValueOperations(recAttributes)
            operations.forEach {
                if (!recAttributes.has(it.getAttName())) {
                    val value = it.invoke(recToMutate.attributes[it.getAttName()])
                    recAttributes.set(it.getAttName(), value)
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

            val optionalAtts = OPTIONAL_COLUMNS.filter { !typesColumnNames.contains(it.name) }
            if (optionalAtts.isNotEmpty()) {
                fullColumns.addAll(setMutationAtts(recToMutate, recAttributes, optionalAtts))
            }

            if (recAttributes.has(ATT_STATE)) {
                val state = recAttributes.get(ATT_STATE).asText()
                recToMutate.attributes[COLUMN_IS_DRAFT.name] = state == "draft"
                fullColumns.add(COLUMN_IS_DRAFT)
            }

            recToMutate.type = recordTypeId
            val typeDef = typesInfo[recordIdx]

            if (recAttributes.has(StatusConstants.ATT_STATUS)) {
                val newStatus = recAttributes.get(StatusConstants.ATT_STATUS).asText()
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
                val currentRef = RecordRef.create(
                    serviceFactory.properties.appName,
                    getId(),
                    recToMutate.extId
                )
                recToMutate.refId = dbRecordRefService.getOrCreateIdByRecordRefs(listOf(currentRef))[0]
            }
            var recAfterSave = dbDataService.save(recToMutate, fullColumns)

            recAfterSave = AuthContext.runAsSystem {
                val newEntity = if (computedAttsComponent != null) {
                    computeAttsToStore(
                        computedAttsComponent,
                        recAfterSave,
                        isNewRecord,
                        recordTypeId,
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

            if (recAfterSave.refId != DbEntity.NEW_REC_ID) {

                val childAssociations = typesAttColumns.filter {
                    DbRecordsUtils.isChildAssocAttribute(it.attribute)
                }

                val addedChildren = mutableSetOf<Long>()
                val removedChildren = mutableSetOf<Long>()

                for (att in childAssociations) {
                    val before = anyToSetOfLong(recordEntityBeforeMutation.attributes[att.attribute.id])
                    val after = anyToSetOfLong(recAfterSave.attributes[att.attribute.id])
                    addedChildren.addAll(after.subtract(before))
                    removedChildren.addAll(before.subtract(after))
                }

                val changedChildren = mutableSetOf<Long>()
                changedChildren.addAll(addedChildren)
                changedChildren.addAll(removedChildren)

                if (changedChildren.isNotEmpty()) {

                    log.debug {
                        val recRef = RecordRef.create(getId(), recAfterSave.extId)
                        "Children of $recRef was changed. Added: $addedChildren Removed: $removedChildren"
                    }

                    val sourceIdMapping = RequestContext.getCurrentNotNull().ctxData.sourceIdMapping
                    val currentAppName = serviceFactory.properties.appName

                    val currentRef = RecordRef.create(currentAppName, getId(), recAfterSave.extId)
                    val newRecordRef = RecordRefUtils.mapAppIdAndSourceId(currentRef, currentAppName, sourceIdMapping)

                    val parentRefId = if (newRecordRef == currentRef) {
                        recAfterSave.refId
                    } else {
                        dbRecordRefService.getOrCreateIdByRecordRef(newRecordRef)
                    }

                    val childRefsById = dbRecordRefService.getRecordRefsByIdsMap(changedChildren)
                    val addOrRemoveParentRef = { children: Set<Long>, add: Boolean ->
                        for (removedId in children) {
                            val childRef = childRefsById[removedId]
                                ?: error("Child ref doesn't found by id. Refs: $childRefsById id: $removedId")

                            if (RecordRef.isNotEmpty(childRef)) {
                                val childAtts = RecordAtts(childRef)
                                if (add) {
                                    childAtts.setAtt(RecordConstants.ATT_PARENT, parentRefId)
                                } else {
                                    childAtts.setAtt(RecordConstants.ATT_PARENT, null)
                                }
                                recordsService.mutate(childAtts)
                            }
                        }
                    }
                    addOrRemoveParentRef.invoke(removedChildren, false)
                    addOrRemoveParentRef.invoke(addedChildren, true)
                }
            }

            emitEventAfterMutation(recordEntityBeforeMutation, recAfterSave, isNewRecord)

            recAfterSave.extId
        }
    }

    private fun anyToSetOfLong(value: Any?): Set<Long> {
        value ?: return emptySet()
        if (value is Collection<*>) {
            val res = hashSetOf<Long>()
            for (item in value) {
                if (item is Long) {
                    res.add(item)
                }
            }
            return res
        }
        if (value is Long) {
            return setOf(value)
        }
        return emptySet()
    }

    private fun getRefForContentData(value: DataValue): RecordRef {

        val url = value.get("url").asText()
        if (url.isBlank()) {
            return RecordRef.EMPTY
        }
        if (url.startsWith("/share/page/card-details")) {
            val nodeRef = value.get("data").get("nodeRef").asText()
            if (nodeRef.isNotBlank()) {
                return RecordRef.create("alfresco", "", nodeRef)
            }
        } else if (url.startsWith("/gateway")) {
            return getRecordRefFromContentUrl(url)
        }
        return RecordRef.EMPTY
    }

    private fun preProcessAssocBeforeMutate(
        recordId: String,
        attId: String,
        value: DataValue
    ): DataValue {
        if (value.isNull()) {
            return value
        }
        if (value.isArray()) {
            if (value.size() == 0) {
                return value
            }
            val result = DataValue.createArr()
            value.forEach { result.add(preProcessAssocBeforeMutate(recordId, attId, it)) }
            return result
        }
        if (value.isObject() && value.has("fileType")) {

            val existingRef = getRefForContentData(value)
            if (RecordRef.isNotEmpty(existingRef)) {
                return DataValue.createStr(existingRef.toString())
            }

            val type = value.get("fileType")
            if (type.isNull() || type.asText().isBlank()) {
                return value
            }
            val typeId = type.asText()

            val typeInfo = ecosTypeService.getTypeInfo(typeId) ?: error("Type doesn't found for id '$typeId'")

            val childAttributes = ObjectData.create()
            childAttributes.set("_type", TypeUtils.getTypeRef(typeId))
            childAttributes.set("_content", listOf(value))

            val appName = serviceFactory.properties.appName
            val sourceIdMapping = RequestContext.getCurrentNotNull().ctxData.sourceIdMapping
            val sourceId = sourceIdMapping.getOrDefault(getId(), getId())

            childAttributes.set("_parent", RecordRef.create(appName, sourceId, recordId))

            val name = value.get("originalName")
            if (name.isNotNull()) {
                // todo: should be _name
                childAttributes.set("_disp", name)
            }

            val childRef = recordsService.create(typeInfo.sourceId, childAttributes)
            return DataValue.createStr(childRef.toString())
        }
        return value
    }

    private fun extractRecordRefs(value: DataValue, target: MutableSet<RecordRef>) {
        if (value.isNull()) {
            return
        }
        if (value.isArray()) {
            for (element in value) {
                extractRecordRefs(element, target)
            }
        } else if (value.isTextual()) {
            val ref = RecordRef.valueOf(value.asText())
            if (RecordRef.isNotEmpty(ref)) {
                target.add(ref)
            }
        }
    }

    private fun replaceRecordRefsToId(value: DataValue, mapping: Map<RecordRef, Long>): DataValue {
        if (value.isArray()) {
            val result = DataValue.createArr()
            for (element in value) {
                val elemRes = replaceRecordRefsToId(element, mapping)
                if (elemRes.isNotNull()) {
                    result.add(elemRes)
                }
            }
            return result
        } else if (value.isTextual()) {
            val ref = RecordRef.valueOf(value.asText())
            return DataValue.create(mapping[ref])
        }
        return DataValue.NULL
    }

    private fun uploadContent(
        record: DbEntity,
        attribute: String,
        contentData: DataValue,
        multiple: Boolean
    ): Any? {

        val contentService = contentService ?: return null

        if (contentData.isArray()) {
            if (contentData.size() == 0) {
                return null
            }
            return if (multiple) {
                val newArray = DataValue.createArr()
                contentData.forEach {
                    val contentId = uploadContent(record, attribute, it, false)
                    if (contentId != null) {
                        newArray.add(contentId)
                    }
                }
                newArray
            } else {
                uploadContent(record, attribute, contentData.get(0), false)
            }
        }
        if (!contentData.isObject()) {
            return null
        }
        val urlData = contentData.get("url")
        if (!urlData.isTextual()) {
            return null
        }
        val recordContentUrl = createContentUrl(record.extId, attribute)

        if (urlData.asText() == recordContentUrl) {
            return record.attributes[attribute]
        }

        val data = DataUriUtil.parseData(urlData.asText())
        val dataBytes = data.data
        if (dataBytes == null || dataBytes.isEmpty()) {
            return null
        }

        val contentMeta = EcosContentMeta.create {
            withMimeType(data.mimeType)
            withName(contentData.get("originalName").asText())
        }

        return contentService.writeContent(EcosContentLocalStorage.TYPE, contentMeta, dataBytes).id
    }

    private fun emitEventAfterMutation(before: DbEntity, after: DbEntity, isNewRecord: Boolean) {

        if (listeners.isEmpty()) {
            return
        }

        val typeInfo = ecosTypeService.getTypeInfo(after.type) ?: error("Entity with unknown type: " + after.type)

        if (isNewRecord) {
            val event = DbRecordCreatedEvent(Record(after), typeInfo)
            listeners.forEach {
                it.onCreated(event)
            }
            return
        }

        val recBefore = Record(before)
        val recAfter = Record(after)
        val attsBefore = mutableMapOf<String, Any?>()
        val attsAfter = mutableMapOf<String, Any?>()

        val attsDef = typeInfo.model.attributes
        attsDef.forEach {
            attsBefore[it.id] = recBefore.getAtt(it.id)
            attsAfter[it.id] = recAfter.getAtt(it.id)
        }

        if (attsBefore != attsAfter) {
            val recChangedEvent = DbRecordChangedEvent(recAfter, typeInfo, attsBefore, attsAfter)
            listeners.forEach {
                it.onChanged(recChangedEvent)
            }
        }

        val statusBefore = recordsService.getAtt(recBefore, StatusConstants.ATT_STATUS_STR).asText()
        val statusAfter = recordsService.getAtt(recAfter, StatusConstants.ATT_STATUS_STR).asText()

        if (statusBefore != statusAfter) {

            val statusBeforeDef = getStatusDef(statusBefore, typeInfo)
            val statusAfterDef = getStatusDef(statusAfter, typeInfo)

            val statusChangedEvent = DbRecordStatusChangedEvent(recAfter, typeInfo, statusBeforeDef, statusAfterDef)
            listeners.forEach {
                it.onStatusChanged(statusChangedEvent)
            }
        }

        if (!isNewRecord) {
            val isDraftBefore = before.attributes[COLUMN_IS_DRAFT.name] as? Boolean
            val isDraftAfter = after.attributes[COLUMN_IS_DRAFT.name] as? Boolean
            if (isDraftBefore != null && isDraftAfter != null && isDraftBefore != isDraftAfter) {
                val event = DbRecordDraftStatusChangedEvent(recAfter, typeInfo, isDraftBefore, isDraftAfter)
                listeners.forEach {
                    it.onDraftStatusChanged(event)
                }
            }
        }
    }

    private fun getStatusDef(id: String, typeInfo: TypeInfo): StatusDef {
        if (id.isBlank()) {
            return StatusDef.create {}
        }
        return typeInfo.model.statuses.firstOrNull { it.id == id } ?: StatusDef.create {
            withId(id)
        }
    }

    private fun computeAttsToStore(
        component: DbComputedAttsComponent,
        entity: DbEntity,
        isNewRecord: Boolean,
        recTypeId: String,
        columns: List<DbColumnDef>
    ): DbEntity {

        val typeRef = TypeUtils.getTypeRef(recTypeId)
        val atts = component.computeAttsToStore(Record(entity), isNewRecord, typeRef)

        var changed = setMutationAtts(entity, atts, columns).isNotEmpty()

        val dispName = component.computeDisplayName(Record(entity), typeRef)
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

    private fun getRecordPerms(recordId: String): DbRecordPerms {
        // Optimization to enable caching
        return RequestContext.doWithCtx(
            serviceFactory,
            {
                it.withReadOnly(true)
            },
            {
                AuthContext.runAsSystem {
                    permsComponent.getRecordPerms(RecordRef.create(getId(), recordId))
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
                val value = atts.get(dbColumnDef.name)

                recToMutate.attributes[dbColumnDef.name] = convert(
                    value,
                    dbColumnDef.multiple,
                    dbColumnDef.type
                )
            }
        }
        return notEmptyColumns
    }

    private fun convert(
        rawValue: DataValue,
        multiple: Boolean,
        columnType: DbColumnType
    ): Any? {

        return if (columnType == DbColumnType.JSON) {
            val converted = convertToClass(rawValue, multiple, DataValue::class.java)
            Json.mapper.toString(converted)
        } else {
            convertToClass(rawValue, multiple, columnType.type.java)
        }
    }

    private fun convertToClass(rawValue: DataValue, multiple: Boolean, javaType: Class<*>): Any? {

        val value = if (multiple) {
            if (!rawValue.isArray()) {
                val arr = DataValue.createArr()
                if (!rawValue.isNull()) {
                    arr.add(rawValue)
                }
                arr
            } else {
                rawValue
            }
        } else {
            if (rawValue.isArray()) {
                if (rawValue.size() == 0) {
                    DataValue.NULL
                } else {
                    rawValue.get(0)
                }
            } else {
                rawValue
            }
        }

        if (multiple) {
            val result = ArrayList<Any?>(value.size())
            for (element in value) {
                result.add(convertToClass(element, false, javaType))
            }
            return result
        }

        return if (value.isObject() && javaType == String::class.java) {
            Json.mapper.toString(value)
        } else {
            Json.mapper.convert(value, javaType)
        }
    }

    override fun getId() = config.id

    fun addListener(listener: DbRecordsListener) {
        this.listeners.add(listener)
    }

    fun removeListener(listener: DbRecordsListener) {
        this.listeners.remove(listener)
    }

    private fun createContentUrl(recordId: String, attribute: String): String {

        val appName = serviceFactory.properties.appName

        val recordsDaoIdEnc = URLEncoder.encode(getId(), Charsets.UTF_8.name())
        val recordIdEnc = URLEncoder.encode(recordId, Charsets.UTF_8.name())
        val attributeEnc = URLEncoder.encode(attribute, Charsets.UTF_8.name())

        return "/gateway/$appName/api/record-content/$recordsDaoIdEnc/$recordIdEnc/$attributeEnc"
    }

    private fun getRecordRefFromContentUrl(url: String): RecordRef {
        if (url.isBlank()) {
            return RecordRef.EMPTY
        }
        val parts = url.split("/")
        if (parts.size != 7) {
            error("Unexpected URL parts size: ${parts.size}. Url: " + url)
        }
        val appName = parts[2]
        val recordsDaoId = URLDecoder.decode(parts[5], Charsets.UTF_8.name())
        val recId = URLDecoder.decode(parts[6], Charsets.UTF_8.name())

        if (recId.isNotBlank()) {
            return RecordRef.create(appName, recordsDaoId, recId)
        }
        return RecordRef.EMPTY
    }

    private fun extractAttValueOperations(attributes: ObjectData): List<AttValueOperation> {
        val operations = ArrayList<AttValueOperation>()
        val operationsNames = hashSetOf<String>()
        attributes.forEach { name, value ->
            if (name.startsWith(ATT_ADD_PREFIX)) {
                operations.add(
                    AttValueAddOrRemoveOperation(
                        name.replaceFirst(ATT_ADD_PREFIX, ""),
                        add = true,
                        exclusive = true,
                        value = value
                    )
                )
                operationsNames.add(name)
            } else if (name.startsWith(ATT_REMOVE_PREFIX)) {
                operations.add(
                    AttValueAddOrRemoveOperation(
                        name.replaceFirst(ATT_REMOVE_PREFIX, ""),
                        add = false,
                        exclusive = true,
                        value = value
                    )
                )
                operationsNames.add(name)
            }
        }
        if (operationsNames.isNotEmpty()) {
            operationsNames.forEach { attributes.remove(it) }
        }
        return operations
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

    private interface AttValueOperation {
        fun invoke(value: Any?): Any?
        fun getAttName(): String
    }

    private class AttValueAddOrRemoveOperation(
        private val att: String,
        private val add: Boolean,
        private val exclusive: Boolean,
        value: DataValue,
    ) : AttValueOperation {

        private val operationValues: List<Any?> = if (value.isNull()) {
            emptyList()
        } else if (value.isArray()) {
            val opValues = ArrayList<Any?>()
            value.forEach { opValues.add(it.asJavaObj()) }
            opValues
        } else {
            listOf(value.asJavaObj())
        }

        override fun invoke(value: Any?): Any? {
            if (operationValues.isEmpty()) {
                return value
            }
            if (value == null) {
                return if (add) {
                    this.operationValues
                } else {
                    null
                }
            }
            if (value is Collection<*>) {
                val newValue = ArrayList(value)
                if (add) {
                    if (exclusive) {
                        newValue.addAll(operationValues.filter { !newValue.contains(it) })
                    } else {
                        newValue.addAll(operationValues)
                    }
                } else {
                    newValue.removeAll(operationValues.map { it })
                }
                return newValue
            }
            if (!add) {
                if ((value is String || value is Long) && operationValues.any { it == value }) {
                    return null
                }
            } else {
                val newValue = ArrayList<Any?>()
                newValue.add(value)
                if (exclusive) {
                    newValue.addAll(operationValues.filter { it != value })
                } else {
                    newValue.addAll(operationValues)
                }
                return newValue
            }
            return value
        }

        override fun getAttName() = att
    }

    inner class EmptyRecord : AttValue {
        override fun getEdge(name: String?): AttEdge? {
            if (name == StatusConstants.ATT_STATUS) {
                return StatusEdge(config.typeRef)
            }
            return super.getEdge(name)
        }
        override fun getDisplayName(): Any {
            if (RecordRef.isEmpty(config.typeRef)) {
                return ""
            }
            val typeInfo = ecosTypeService.getTypeInfo(config.typeRef.id)
            return typeInfo?.name ?: ""
        }
    }

    inner class Record(
        val entity: DbEntity
    ) : AttValue {

        private val additionalAtts: Map<String, Any?>
        private val assocMapping: Map<Long, RecordRef>

        private val permsValue by lazy { PermissionsValue(entity.extId) }
        private val attTypes: Map<String, AttributeType>

        init {
            val recData = LinkedHashMap(entity.attributes)

            attTypes = HashMap()
            ecosTypeService.getTypeInfo(entity.type)?.model?.attributes?.forEach {
                attTypes[it.id] = it.type
            }
            OPTIONAL_COLUMNS.forEach {
                if (!attTypes.containsKey(it.name)) {
                    when (it.type) {
                        DbColumnType.JSON -> {
                            attTypes[it.name] = AttributeType.JSON
                        }
                        DbColumnType.TEXT -> {
                            attTypes[it.name] = AttributeType.TEXT
                        }
                        DbColumnType.INT -> {
                            attTypes[it.name] = AttributeType.NUMBER
                        }
                        DbColumnType.LONG -> {
                            if (it.name == RecordConstants.ATT_PARENT) {
                                attTypes[it.name] = AttributeType.ASSOC
                            }
                        }
                        else -> {
                        }
                    }
                }
            }

            // assoc mapping
            val assocIdValues = mutableSetOf<Long>()
            attTypes.filter { DbRecordsUtils.isAssocLikeAttribute(it.value) }.keys.forEach { attId ->
                val value = recData[attId]
                if (value is Iterable<*>) {
                    for (elem in value) {
                        if (elem is Long) {
                            assocIdValues.add(elem)
                        }
                    }
                } else if (value is Long) {
                    assocIdValues.add(value)
                }
            }
            assocMapping = if (assocIdValues.isNotEmpty()) {
                val assocIdValuesList = assocIdValues.toList()
                val assocRefValues = dbRecordRefService.getRecordRefsByIds(assocIdValuesList)
                assocIdValuesList.mapIndexed { idx, id -> id to assocRefValues[idx] }.toMap()
            } else {
                emptyMap()
            }

            attTypes.forEach { (attId, attType) ->
                val value = recData[attId]
                if (DbRecordsUtils.isAssocLikeAttribute(attType)) {
                    if (value != null) {
                        recData[attId] = toRecordRef(value)
                    }
                } else {
                    when (attType) {
                        AttributeType.JSON -> {
                            if (value is String) {
                                recData[attId] = Json.mapper.read(value)
                            }
                        }
                        AttributeType.MLTEXT -> {
                            if (value is String) {
                                recData[attId] = Json.mapper.read(value, MLText::class.java)
                            }
                        }
                        AttributeType.CONTENT -> {
                            recData[attId] = convertContentAtt(value, attId)
                        }
                        else -> {
                        }
                    }
                }
            }
            this.additionalAtts = recData
        }

        private fun convertContentAtt(value: Any?, attId: String): Any? {
            if (value is List<*>) {
                return value.mapNotNull { convertContentAtt(it, attId) }
            }
            if (value !is Long || value < 0) {
                return null
            }
            return if (contentService != null) {
                ContentValue(contentService, entity.extId, entity.name, value, attId)
            } else {
                null
            }
        }

        private fun toRecordRef(value: Any): Any {
            return when (value) {
                is Iterable<*> -> {
                    val result = ArrayList<Any>()
                    value.forEach {
                        if (it != null) {
                            result.add(toRecordRef(it))
                        }
                    }
                    result
                }
                is Long -> assocMapping[value] ?: error("Assoc doesn't found for id $value")
                else -> error("Unexpected assoc value type: ${value::class}")
            }
        }

        override fun getId(): Any {
            return RecordRef.create(this@DbRecordsDao.getId(), entity.extId)
        }

        override fun asText(): String {
            return entity.name.getClosest(RequestContext.getLocale()).ifBlank { "No name" }
        }

        override fun getDisplayName(): Any {
            return entity.name
        }

        override fun asJson(): Any {
            return additionalAtts
        }

        override fun has(name: String): Boolean {
            return additionalAtts.contains(ATTS_MAPPING.getOrDefault(name, name))
        }

        override fun getAs(type: String): Any? {
            if (type == CONTENT_DATA) {
                val content = getAtt(RecordConstants.ATT_CONTENT) as? ContentValue ?: return null
                return content.getAs(CONTENT_DATA)
            }
            return super.getAs(type)
        }

        override fun getAtt(name: String): Any? {
            return when (name) {
                "id" -> entity.extId
                ATT_NAME -> displayName
                RecordConstants.ATT_MODIFIED, "cm:modified" -> entity.modified
                RecordConstants.ATT_CREATED, "cm:created" -> entity.created
                RecordConstants.ATT_MODIFIER -> getAsPersonRef(entity.modifier)
                RecordConstants.ATT_CREATOR -> getAsPersonRef(entity.creator)
                StatusConstants.ATT_STATUS -> {
                    val statusId = entity.status
                    val typeInfo = ecosTypeService.getTypeInfo(entity.type) ?: return statusId
                    val statusDef = typeInfo.model.statuses.firstOrNull { it.id == statusId } ?: return statusId
                    return StatusValue(statusDef)
                }
                "permissions" -> permsValue
                else -> {
                    val perms = permsValue.recordPerms
                    if (perms != null && !perms.isCurrentUserHasAttReadPerms(name)) {
                        null
                    } else {
                        additionalAtts[ATTS_MAPPING.getOrDefault(name, name)]
                    }
                }
            }
        }

        override fun getEdge(name: String): AttEdge {
            if (name == StatusConstants.ATT_STATUS) {
                return StatusEdge(type)
            }
            return DbRecordAttEdge(this, name, permsValue.recordPerms)
        }

        private fun getAsPersonRef(name: String): RecordRef {
            if (name.isBlank()) {
                return RecordRef.EMPTY
            }
            return RecordRef.create("alfresco", "people", name)
        }

        override fun getType(): RecordRef {
            return TypeUtils.getTypeRef(entity.type)
        }
    }

    override fun setRecordsServiceFactory(serviceFactory: RecordsServiceFactory) {
        super.setRecordsServiceFactory(serviceFactory)
        ecosTypeService = DbEcosTypeService(ecosTypesRepo)
    }

    inner class StatusEdge(val type: RecordRef) : AttEdge {
        override fun isMultiple() = false
        override fun getOptions(): List<Any> {
            if (type.id.isBlank()) {
                return emptyList()
            }
            val typeInfo = ecosTypeService.getTypeInfo(type.id) ?: return emptyList()
            return typeInfo.model.statuses.map { StatusValue(it) }
        }
    }

    private class DbRecordAttEdge(
        private val rec: Record,
        private val name: String,
        private val perms: DbRecordPerms?
    ) : AttEdge {

        override fun getName(): String {
            return name
        }

        override fun getValue(): Any? {
            return rec.getAtt(name)
        }

        override fun isProtected(): Boolean {
            return perms != null && !perms.isCurrentUserHasAttWritePerms(name)
        }

        override fun isUnreadable(): Boolean {
            return perms != null && !perms.isCurrentUserHasAttReadPerms(name)
        }
    }

    inner class ContentValue(
        private val contentService: EcosContentService,
        private val recId: String,
        private val name: MLText,
        private val contentId: Long,
        private val attribute: String
    ) : AttValue {

        private val meta: EcosContentMeta by lazy {
            contentService.getMeta(contentId) ?: error("Content doesn't found by id '$id'")
        }

        override fun getAtt(name: String): Any? {
            return when (name) {
                "name" -> meta.name
                "sha256" -> meta.sha256
                "size" -> meta.size
                "mimeType" -> meta.mimeType
                "encoding" -> meta.encoding
                "created" -> meta.created
                else -> null
            }
        }

        override fun getAs(type: String): Any? {
            if (type == CONTENT_DATA) {
                val name = if (MLText.isEmpty(name)) {
                    meta.name
                } else {
                    MLText.getClosestValue(name, RequestContext.getLocale())
                }
                return ContentData(
                    createContentUrl(recId, attribute),
                    name,
                    meta.size
                )
            }
            return null
        }
    }

    data class ContentData(
        val url: String,
        val name: String,
        val size: Long
    )

    class ChildRef(
        val ref: RecordRef,
        val assoc: AttributeDef
    )

    data class StatusValue(private val def: StatusDef) : AttValue {
        override fun getDisplayName(): Any {
            return def.name
        }

        override fun asText(): String {
            return def.id
        }
    }

    private inner class PermissionsValue(private val recordId: String) : AttValue {
        val recordPerms: DbRecordPerms? by lazy {
            if (!AuthContext.isRunAsSystem()) {
                getRecordPerms(recordId)
            } else {
                null
            }
        }
        override fun has(name: String): Boolean {
            val perms = recordPerms ?: return true
            if (name.equals("Write", true)) {
                return perms.isCurrentUserHasWritePerms()
            }
            if (name.equals("Read", true)) {
                val authoritiesWithReadPermissions = perms.getAuthoritiesWithReadPermission().toSet()
                if (AuthContext.getCurrentRunAsUserWithAuthorities().any { authoritiesWithReadPermissions.contains(it) }) {
                    return true
                }
                return false
            }

            return super.has(name)
        }
    }
}
