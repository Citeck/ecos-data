package ru.citeck.ecos.data.sql.records

import mu.KotlinLogging
import ru.citeck.ecos.commons.data.DataValue
import ru.citeck.ecos.commons.data.MLText
import ru.citeck.ecos.commons.json.Json
import ru.citeck.ecos.data.sql.ecostype.DbEcosTypeInfo
import ru.citeck.ecos.data.sql.ecostype.DbEcosTypeRepo
import ru.citeck.ecos.data.sql.ecostype.DbEcosTypeService
import ru.citeck.ecos.data.sql.repo.entity.DbEntity
import ru.citeck.ecos.data.sql.repo.find.DbFindPage
import ru.citeck.ecos.data.sql.repo.find.DbFindSort
import ru.citeck.ecos.data.sql.service.DbDataService
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType
import ru.citeck.ecos.model.lib.status.constants.StatusConstants
import ru.citeck.ecos.model.lib.type.service.utils.TypeUtils
import ru.citeck.ecos.records2.RecordConstants
import ru.citeck.ecos.records2.RecordRef
import ru.citeck.ecos.records2.predicate.PredicateService
import ru.citeck.ecos.records2.predicate.PredicateUtils
import ru.citeck.ecos.records2.predicate.model.Predicate
import ru.citeck.ecos.records2.predicate.model.ValuePredicate
import ru.citeck.ecos.records3.RecordsServiceFactory
import ru.citeck.ecos.records3.record.atts.dto.LocalRecordAtts
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
import ru.citeck.ecos.records3.record.request.RequestContext
import kotlin.collections.LinkedHashMap
import kotlin.math.min

class DbRecordsDao(
    private val id: String,
    private val config: DbRecordsDaoConfig,
    private val ecosTypeRepo: DbEcosTypeRepo,
    private val dbDataService: DbDataService<DbEntity>
) : AbstractRecordsDao(), RecordsAttsDao, RecordsQueryDao, RecordsMutateDao, RecordsDeleteDao {

    companion object {
        private val ATTS_MAPPING = mapOf(
            "_created" to DbEntity.CREATED,
            "_modified" to DbEntity.MODIFIED
        )

        private val log = KotlinLogging.logger {}
    }

    private lateinit var ecosTypeService: DbEcosTypeService

    fun runMigrations(typeRef: RecordRef, mock: Boolean = true, diff: Boolean = true): List<String> {
        val typeInfo = getRecordsTypeInfo(typeRef) ?: error("Type is null. Migration can't be executed")
        val columns = ecosTypeService.getColumnsForTypes(listOf(typeInfo))
        val resp = dbDataService.runMigrations(columns, mock, diff)
        dbDataService.resetColumnsCache()
        return resp
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

    override fun getRecordsAtts(recordsId: List<String>): List<*> {
        return recordsId.map { id ->
            dbDataService.findById(id)?.let {
                Record(this, it)
            } ?: EmptyAttValue.INSTANCE
        }
    }

    override fun queryRecords(recsQuery: RecordsQuery): Any? {

        if (recsQuery.language != PredicateService.LANGUAGE_PREDICATE) {
            return null
        }

        val predicate = PredicateUtils.mapValuePredicates(recsQuery.getQuery(Predicate::class.java)) {
            when (it.getAttribute()) {
                "_type" -> {
                    val typeLocalId = RecordRef.valueOf(it.getValue().asText()).id
                    ValuePredicate(DbEntity.TYPE, it.getType(), typeLocalId)
                }
                else ->
                    if (ATTS_MAPPING.containsKey(it.getAttribute())) {
                        ValuePredicate(ATTS_MAPPING[it.getAttribute()], it.getType(), it.getValue())
                    } else {
                        it
                    }
            }
        }

        val page = recsQuery.page
        val findRes = dbDataService.find(
            predicate,
            recsQuery.sortBy.map {
                DbFindSort(ATTS_MAPPING.getOrDefault(it.attribute, it.attribute), it.isAscending)
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
        queryRes.setRecords(findRes.entities.map { Record(this, it) })

        return queryRes
    }

    override fun delete(recordsId: List<String>): List<DelStatus> {

        if (!config.deletable) {
            error("Records DAO is not deletable. Records can't be deleted: '$recordsId'")
        }

        return recordsId.map {
            dbDataService.delete(it)
            DelStatus.OK
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

        error("${RecordConstants.ATT_TYPE} attribute is mandatory for mutation. Record: ${record.id}")
    }

    override fun mutate(records: List<LocalRecordAtts>): List<String> {

        if (!config.updatable) {
            error("Records DAO is not mutable. Records can't be mutated: '${records.map { it.id }}'")
        }
        val typesId = records.map { getTypeIdForRecord(it) }
        val typesInfo = typesId.mapIndexed { idx, typeId ->
            ecosTypeRepo.getTypeInfo(typeId) ?: error("Type is not found: '$typeId'. Record ID: '${records[idx]}'")
        }

        val columns = ecosTypeService.getColumnsForTypes(typesInfo)

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
            if (recToMutate.id == DbEntity.NEW_REC_ID) {
                if (!config.insertable) {
                    error("Records DAO doesn't support new records creation. Record ID: '${record.id}'")
                }
            } else {
                if (!config.updatable) {
                    error("Records DAO doesn't support records updating. Record ID: '${record.id}'")
                }
            }

            val atts = record.attributes

            for (dbColumnDef in columns) {
                if (!atts.has(dbColumnDef.name)) {
                    continue
                }
                val value = atts.get(dbColumnDef.name)
                recToMutate.attributes[dbColumnDef.name] = convert(
                    value,
                    dbColumnDef.multiple,
                    dbColumnDef.type.type.java
                )
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

            val recAfterSave = dbDataService.save(recToMutate, columns)

            if (ecosTypeService.fillComputedAtts(getId(), recAfterSave)) {
                dbDataService.save(recAfterSave, columns)
            } else {
                recAfterSave
            }.extId
        }
    }

    private fun convert(rawValue: DataValue, multiple: Boolean, javaType: Class<*>): Any? {

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
                result.add(convert(element, false, javaType))
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

    class Record(
        private val owner: DbRecordsDao,
        val entity: DbEntity
    ) : AttValue {

        private val additionalAtts: Map<String, Any?>

        init {
            val recData = LinkedHashMap(entity.attributes)

            val typeInfo = owner.ecosTypeRepo.getTypeInfo(entity.type)
            typeInfo?.attributes?.forEach {
                val value = recData[it.id]
                when (it.type) {
                    AttributeType.ASSOC,
                    AttributeType.AUTHORITY,
                    AttributeType.PERSON,
                    AttributeType.AUTHORITY_GROUP -> {
                        if (value is String) {
                            recData[it.id] = RecordRef.valueOf(value)
                        }
                    }
                    AttributeType.JSON -> {
                        if (value is String) {
                            recData[it.id] = Json.mapper.read(value)
                        }
                    }
                    AttributeType.MLTEXT -> {
                        if (value is String) {
                            recData[it.id] = Json.mapper.read(value, MLText::class.java)
                        }
                    }
                    else -> {}
                }
            }
            this.additionalAtts = recData
        }

        override fun getId(): Any {
            return RecordRef.create(owner.id, entity.extId)
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

        override fun getAtt(name: String?): Any? {
            if (name == RecordConstants.ATT_DOC_NUM) {
                return entity.docNum
            } else if (name == RecordConstants.ATT_MODIFIED || name == "cm:modified") {
                return entity.modified
            } else if (name == RecordConstants.ATT_CREATED || name == "cm:created") {
                return entity.created
            } else if (name == RecordConstants.ATT_MODIFIER) {
                return getAsPersonRef(entity.modifier)
            } else if (name == RecordConstants.ATT_CREATOR) {
                return getAsPersonRef(entity.creator)
            } else if (name == StatusConstants.ATT_STATUS) {
                return entity.status
            }
            return additionalAtts[name]
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
