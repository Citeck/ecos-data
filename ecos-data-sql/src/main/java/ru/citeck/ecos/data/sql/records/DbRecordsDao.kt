package ru.citeck.ecos.data.sql.records

import mu.KotlinLogging
import ru.citeck.ecos.commons.data.DataValue
import ru.citeck.ecos.commons.data.MLText
import ru.citeck.ecos.commons.data.ObjectData
import ru.citeck.ecos.commons.json.Json
import ru.citeck.ecos.context.lib.auth.AuthContext
import ru.citeck.ecos.data.sql.dto.DbColumnDef
import ru.citeck.ecos.data.sql.dto.DbColumnType
import ru.citeck.ecos.data.sql.ecostype.DbEcosTypeInfo
import ru.citeck.ecos.data.sql.ecostype.DbEcosTypeRepo
import ru.citeck.ecos.data.sql.ecostype.DbEcosTypeService
import ru.citeck.ecos.data.sql.meta.dto.DbTableMetaDto
import ru.citeck.ecos.data.sql.records.perms.DbPermsComponent
import ru.citeck.ecos.data.sql.repo.entity.DbEntity
import ru.citeck.ecos.data.sql.repo.find.DbFindPage
import ru.citeck.ecos.data.sql.repo.find.DbFindSort
import ru.citeck.ecos.data.sql.service.DbDataService
import ru.citeck.ecos.data.sql.txn.ExtTxnContext
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType
import ru.citeck.ecos.model.lib.status.constants.StatusConstants
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
import java.util.*
import kotlin.math.min

class DbRecordsDao(
    private val id: String,
    private val config: DbRecordsDaoConfig,
    private val ecosTypeRepo: DbEcosTypeRepo,
    private val dbDataService: DbDataService<DbEntity>,
    private val permsComponent: DbPermsComponent?
) : AbstractRecordsDao(),
    RecordsAttsDao,
    RecordsQueryDao,
    RecordsMutateDao,
    RecordsDeleteDao,
    TxnRecordsDao {

    companion object {

        private const val ATT_STATE = "_state"

        private val ATTS_MAPPING = mapOf(
            "_created" to DbEntity.CREATED,
            "_creator" to DbEntity.CREATOR,
            "_modified" to DbEntity.MODIFIED,
            "_modifier" to DbEntity.MODIFIER,
            "_localId" to DbEntity.EXT_ID,
            "_status" to DbEntity.STATUS
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

    fun runMigrations(typeRef: RecordRef, mock: Boolean = true, diff: Boolean = true): List<String> {
        val typeInfo = getRecordsTypeInfo(typeRef) ?: error("Type is null. Migration can't be executed")
        val columns = ecosTypeService.getColumnsForTypes(listOf(typeInfo))
        dbDataService.resetColumnsCache()
        return dbDataService.runMigrations(columns, mock, diff, false)
    }

    fun updatePermissions(records: List<String>) {
        AuthContext.runAsSystem {
            dbDataService.commit(records.associateWith { getAuthoritiesWithReadPerms(it) })
        }
    }

    fun getTableMeta(): DbTableMetaDto {
        return dbDataService.getTableMeta()
    }

    private fun getRecordsTypeInfo(typeRef: RecordRef): DbEcosTypeInfo? {
        val type = getRecordsTypeRef(typeRef)
        if (RecordRef.isEmpty(type)) {
            log.warn { "Type is not defined for Records DAO" }
            return null
        }
        return ecosTypeRepo.getTypeInfo(type.id)
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
            dbDataService.commit(recordsId.associateWith { getAuthoritiesWithReadPerms(it) })
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
            dbDataService.findById(id)?.let {
                Record(it)
            } ?: EmptyAttValue.INSTANCE
        }
    }

    override fun queryRecords(recsQuery: RecordsQuery): Any? {

        if (recsQuery.language != PredicateService.LANGUAGE_PREDICATE) {
            return null
        }

        val predicate = PredicateUtils.mapAttributePredicates(recsQuery.getQuery(Predicate::class.java)) {
            val attribute = it.getAttribute()
            if (it is ValuePredicate) {
                when (attribute) {
                    "_type" -> {
                        val typeLocalId = RecordRef.valueOf(it.getValue().asText()).id
                        ValuePredicate(DbEntity.TYPE, it.getType(), typeLocalId)
                    }
                    else ->
                        if (ATTS_MAPPING.containsKey(attribute)) {
                            ValuePredicate(ATTS_MAPPING[attribute], it.getType(), it.getValue())
                        } else {
                            it
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
                recordsId.map {
                    dbDataService.delete(it)
                    DelStatus.OK
                }
            }
        } else {
            recordsId.map {
                dbDataService.delete(it)
                DelStatus.OK
            }
        }
    }

    private fun getTypeIdForRecord(record: LocalRecordAtts): String {

        val typeRefStr = record.attributes.get(RecordConstants.ATT_TYPE).asText()
        val typeRefFromAtts = RecordRef.valueOf(typeRefStr).id
        if (typeRefFromAtts.isNotBlank()) {
            return typeRefFromAtts
        }

        val typeFromRecord = dbDataService.findById(record.id)?.type
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
            ecosTypeRepo.getTypeInfo(typeId) ?: error("Type is not found: '$typeId'. Record ID: '${records[idx]}'")
        }

        val typesColumns = ecosTypeService.getColumnsForTypes(typesInfo)
        val typesColumnNames = typesColumns.map { it.name }.toSet()

        return records.mapIndexed { recordIdx, record ->

            val recordTypeId = typesId[recordIdx]

            val recToMutate: DbEntity = if (record.id.isEmpty()) {
                DbEntity()
            } else {
                val existingEntity = dbDataService.findById(record.id)
                if (existingEntity != null) {
                    existingEntity
                } else {
                    val newEntity = DbEntity()
                    newEntity.extId = record.id
                    newEntity
                }
            }
            val localId = record.attributes.get(ScalarType.LOCAL_ID.mirrorAtt).asText()
            if (localId.isNotBlank()) {
                if (recToMutate.extId != localId) {
                    recToMutate.extId = localId
                    if (dbDataService.findById(localId) != null) {
                        error("Record with ID $localId already exists. You should mutate it directly")
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

            val fullColumns = ArrayList(typesColumns)
            setMutationAtts(recToMutate, record.attributes, typesColumns)
            val optionalAtts = OPTIONAL_COLUMNS.filter { !typesColumnNames.contains(it.name) }
            if (optionalAtts.isNotEmpty()) {
                fullColumns.addAll(setMutationAtts(recToMutate, record.attributes, optionalAtts))
            }

            if (record.attributes.has(ATT_STATE)) {
                val state = record.attributes.get(ATT_STATE).asText()
                recToMutate.attributes[COLUMN_IS_DRAFT.name] = state == "draft"
                fullColumns.add(COLUMN_IS_DRAFT)
            }

            recToMutate.type = recordTypeId
            val typeDef = typesInfo[recordIdx]

            if (record.attributes.has(StatusConstants.ATT_STATUS)) {
                val newStatus = record.attributes.get(StatusConstants.ATT_STATUS).asText()
                if (newStatus.isNotBlank()) {
                    if (typeDef.statuses.any { it.id == newStatus }) {
                        recToMutate.status = newStatus
                    } else {
                        error(
                            "Unknown status: '$newStatus'. " +
                                "Available statuses: ${typeDef.statuses.joinToString { it.id }}"
                        )
                    }
                }
            }

            val recAfterSave = dbDataService.save(recToMutate, fullColumns)

            AuthContext.runAsSystem {
                val resId = if (ecosTypeService.fillComputedAtts(getId(), recAfterSave)) {
                    dbDataService.save(recAfterSave, fullColumns)
                } else {
                    recAfterSave
                }.extId

                if (!txnExists) {
                    dbDataService.commit(mapOf(resId to getAuthoritiesWithReadPerms(resId)))
                }

                resId
            }
        }
    }

    private fun getAuthoritiesWithReadPerms(recordId: String): Set<String> {
        if (permsComponent == null) {
            val user = AuthContext.getCurrentUser()
            if (user.isNotBlank()) {
                return setOf(user)
            }
        } else {
            // Optimization to enable caching
            return RequestContext.doWithCtx(
                serviceFactory,
                {
                    it.withReadOnly(true)
                },
                {
                    permsComponent.getAuthoritiesWithReadPermission(RecordRef.create(getId(), recordId))
                }
            )
        }
        return emptySet()
    }

    private fun setMutationAtts(recToMutate: DbEntity, atts: ObjectData, types: List<DbColumnDef>): List<DbColumnDef> {
        val notEmptyColumns = ArrayList<DbColumnDef>()
        for (dbColumnDef in types) {
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

        return Json.mapper.convert(value, javaType)
    }

    override fun getId() = id

    inner class Record(
        val entity: DbEntity
    ) : AttValue {

        private val additionalAtts: Map<String, Any?>

        init {
            val recData = LinkedHashMap(entity.attributes)

            val attTypes = HashMap<String, AttributeType>()
            ecosTypeRepo.getTypeInfo(entity.type)?.attributes?.forEach {
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
                        if (value is String) {
                            recData[attId] = RecordRef.valueOf(value)
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
                    else -> {}
                }
            }
            this.additionalAtts = recData
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
            return additionalAtts.contains(name)
        }

        override fun getAtt(name: String): Any? {
            return when (name) {
                RecordConstants.ATT_MODIFIED, "cm:modified" -> entity.modified
                RecordConstants.ATT_CREATED, "cm:created" -> entity.created
                RecordConstants.ATT_MODIFIER -> getAsPersonRef(entity.modifier)
                RecordConstants.ATT_CREATOR -> getAsPersonRef(entity.creator)
                StatusConstants.ATT_STATUS -> entity.status
                else -> additionalAtts[ATTS_MAPPING.getOrDefault(name, name)]
            }
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
        ecosTypeService = DbEcosTypeService(ecosTypeRepo, serviceFactory)
    }
}
