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
import ru.citeck.ecos.data.sql.meta.dto.DbTableMetaDto
import ru.citeck.ecos.data.sql.records.computed.DbComputedAttsComponent
import ru.citeck.ecos.data.sql.records.listener.*
import ru.citeck.ecos.data.sql.records.perms.DbPermsComponent
import ru.citeck.ecos.data.sql.records.perms.DbRecordPerms
import ru.citeck.ecos.data.sql.repo.entity.DbEntity
import ru.citeck.ecos.data.sql.repo.find.DbFindPage
import ru.citeck.ecos.data.sql.repo.find.DbFindSort
import ru.citeck.ecos.data.sql.service.DbCommitEntityDto
import ru.citeck.ecos.data.sql.service.DbDataService
import ru.citeck.ecos.data.sql.service.DbMigrationsExecutor
import ru.citeck.ecos.data.sql.txn.ExtTxnContext
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
import ru.citeck.ecos.records3.RecordsServiceFactory
import ru.citeck.ecos.records3.record.atts.dto.LocalRecordAtts
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
import java.io.InputStream
import java.net.URLDecoder
import java.net.URLEncoder
import java.util.*
import java.util.concurrent.CopyOnWriteArrayList
import kotlin.math.min

class DbRecordsDao(
    private val id: String,
    private val config: DbRecordsDaoConfig,
    private val ecosTypesRepo: TypesRepo,
    private val dbDataService: DbDataService<DbEntity>,
    private val permsComponent: DbPermsComponent,
    private val computedAttsComponent: DbComputedAttsComponent?,
    private val contentService: EcosContentService?
) : AbstractRecordsDao(),
    RecordsAttsDao,
    RecordsQueryDao,
    RecordsMutateDao,
    RecordsDeleteDao,
    TxnRecordsDao {

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
            }
        )

        private val COLUMN_IS_DRAFT = DbColumnDef.create {
            withName("_isDraft")
            withType(DbColumnType.BOOLEAN)
        }

        private val log = KotlinLogging.logger {}
    }

    private lateinit var ecosTypeService: DbEcosTypeService

    private val listeners: MutableList<DbRecordsListener> = CopyOnWriteArrayList()

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

    fun runMigrations(typeRef: RecordRef, mock: Boolean = true, diff: Boolean = true): List<String> {
        val typeInfo = getRecordsTypeInfo(typeRef) ?: error("Type is null. Migration can't be executed")
        val columns = ecosTypeService.getColumnsForTypes(listOf(typeInfo)).map { it.column }
        dbDataService.resetColumnsCache()
        var migrations = dbDataService.runMigrations(columns, mock, diff, false)
        if (contentService is DbMigrationsExecutor) {
            val newMigrations = ArrayList(migrations)
            newMigrations.addAll(contentService.runMigrations(mock, diff))
            migrations = newMigrations
        }
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
        ExtTxnContext.withExtTxn(txnId, false) {
            dbDataService.commit(recordsId.map { getEntityToCommit(it) })
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
                dbDataService.findByExtId(id)?.let {
                    Record(it)
                } ?: EmptyAttValue.INSTANCE
            }
        }
    }

    override fun queryRecords(recsQuery: RecordsQuery): Any? {

        if (recsQuery.language != PredicateService.LANGUAGE_PREDICATE) {
            return null
        }
        val attributesById = if (RecordRef.isNotEmpty(config.typeRef)) {
            val typeInfo = ecosTypeService.getTypeInfo(config.typeRef.id)
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
                        if (newPred.getType() == ValuePredicate.Type.EQ) {
                            if (newPred.getAttribute() == DbEntity.NAME ||
                                attributesById[newPred.getAttribute()]?.type == AttributeType.MLTEXT
                            ) {

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

        val typeFromRecord = dbDataService.findByExtId(record.id)?.type
        if (!typeFromRecord.isNullOrBlank()) {
            return typeFromRecord
        }

        if (RecordRef.isNotEmpty(config.typeRef)) {
            return config.typeRef.id
        }

        error("${RecordConstants.ATT_TYPE} attribute is mandatory for mutation. Record: ${record.id}")
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

            if (record.id.isNotBlank()) {
                val recordPerms = getRecordPerms(record.id)
                if (!recordPerms.isCurrentUserHasWritePerms()) {
                    error("Permissions Denied. You can't change record '${record.id}'")
                }
            }

            val recordTypeId = typesId[recordIdx]

            val recToMutate: DbEntity = if (record.id.isEmpty()) {
                DbEntity()
            } else {
                val existingEntity = dbDataService.findByExtId(record.id)
                if (existingEntity != null) {
                    existingEntity
                } else {
                    val newEntity = DbEntity()
                    newEntity.extId = record.id
                    newEntity
                }
            }

            var customExtId = record.attributes.get("id").asText()
            if (customExtId.isBlank()) {
                customExtId = record.attributes.get(ScalarType.LOCAL_ID.mirrorAtt).asText()
            }
            if (customExtId.isNotBlank()) {
                if (recToMutate.extId != customExtId) {
                    recToMutate.extId = customExtId
                    if (dbDataService.findByExtId(customExtId) != null) {
                        error("Record with ID $customExtId already exists. You should mutate it directly")
                    } else {
                        // copy of existing record
                        recToMutate.id = DbEntity.NEW_REC_ID
                    }
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
                } else if (it.attribute.type == AttributeType.ASSOC) {
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

            val recordEntityBeforeMutation = recToMutate.copy()

            val fullColumns = ArrayList(typesColumns)
            setMutationAtts(recToMutate, recAttributes, typesColumns)
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
            emitEventAfterMutation(recordEntityBeforeMutation, recAfterSave, isNewRecord)

            recAfterSave.extId
        }
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
                permsComponent.getRecordPerms(RecordRef.create(getId(), recordId))
            }
        )
    }

    private fun setMutationAtts(
        recToMutate: DbEntity,
        atts: ObjectData,
        columns: List<DbColumnDef>
    ): List<DbColumnDef> {

        if (atts.size() == 0) {
            return emptyList()
        }

        val notEmptyColumns = ArrayList<DbColumnDef>()
        for (dbColumnDef in columns) {
            if (!atts.has(dbColumnDef.name)) {
                continue
            }
            notEmptyColumns.add(dbColumnDef)
            val value = atts.get(dbColumnDef.name)
            recToMutate.attributes[dbColumnDef.name] = convert(
                value,
                dbColumnDef.multiple,
                dbColumnDef.type
            )
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

    override fun getId() = id

    fun addListener(listener: DbRecordsListener) {
        this.listeners.add(listener)
    }

    fun removeListener(listener: DbRecordsListener) {
        this.listeners.remove(listener)
    }

    private fun createContentUrl(recordId: String, attribute: String): String {

        val appName = serviceFactory.properties.appName

        val recordsDaoIdEnc = URLEncoder.encode(id, Charsets.UTF_8.name())
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

    inner class EmptyRecord : AttValue {

        override fun getEdge(name: String?): AttEdge? {
            if (name == StatusConstants.ATT_STATUS) {
                return StatusEdge(config.typeRef)
            }
            return super.getEdge(name)
        }
    }

    inner class Record(
        val entity: DbEntity
    ) : AttValue {

        private val additionalAtts: Map<String, Any?>

        init {
            val recData = LinkedHashMap(entity.attributes)

            val attTypes = HashMap<String, AttributeType>()
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
                        else -> {}
                    }
                }
            }
            attTypes.forEach { (attId, attType) ->
                val value = recData[attId]
                when (attType) {
                    AttributeType.ASSOC,
                    AttributeType.AUTHORITY,
                    AttributeType.PERSON,
                    AttributeType.AUTHORITY_GROUP -> {
                        if (value != null) {
                            recData[attId] = toRecordRef(value)
                        }
                    }
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
                is String -> RecordRef.valueOf(value)
                else -> value
            }
        }

        override fun getId(): Any {
            return RecordRef.create(this@DbRecordsDao.id, entity.extId)
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
                else -> additionalAtts[ATTS_MAPPING.getOrDefault(name, name)]
            }
        }

        override fun getEdge(name: String): AttEdge? {
            if (name == StatusConstants.ATT_STATUS) {
                return StatusEdge(type)
            }
            return super.getEdge(name)
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

    data class StatusValue(private val def: StatusDef) : AttValue {
        override fun getDisplayName(): Any {
            return def.name
        }

        override fun asText(): String {
            return def.id
        }
    }
}
