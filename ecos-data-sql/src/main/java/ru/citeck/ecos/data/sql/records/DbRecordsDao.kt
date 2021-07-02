package ru.citeck.ecos.data.sql.records

import mu.KotlinLogging
import ru.citeck.ecos.commons.data.MLText
import ru.citeck.ecos.commons.json.Json
import ru.citeck.ecos.data.sql.SqlDataService
import ru.citeck.ecos.data.sql.SqlDataServiceConfig
import ru.citeck.ecos.data.sql.datasource.DbDataSource
import ru.citeck.ecos.data.sql.dto.DbColumnType
import ru.citeck.ecos.data.sql.ecostype.DbEcosTypeRepo
import ru.citeck.ecos.data.sql.ecostype.DbEcosTypeService
import ru.citeck.ecos.data.sql.repo.DbContextManager
import ru.citeck.ecos.data.sql.repo.entity.DbEntity
import ru.citeck.ecos.data.sql.repo.find.DbFindPage
import ru.citeck.ecos.data.sql.repo.find.DbFindSort
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

class DbRecordsDao(
    private val id: String,
    private val config: DbRecordsDaoConfig,
    private val ecosTypeRepo: DbEcosTypeRepo,
    dataSource: DbDataSource,
    contextManager: DbContextManager
) : AbstractRecordsDao(), RecordsAttsDao, RecordsQueryDao, RecordsMutateDao, RecordsDeleteDao {

    companion object {
        private val ATTS_MAPPING = mapOf(
            "_created" to DbEntity.CREATED,
            "_modified" to DbEntity.MODIFIED
        )
    }

    private lateinit var ecosTypeService: DbEcosTypeService

    private val sqlDataService = SqlDataService(
        SqlDataServiceConfig(config.authEnabled),
        config.tableRef,
        dataSource,
        DbEntity::class,
        contextManager,
        true
    )

     override fun getRecordsAtts(recordsId: List<String>): List<*> {
        return recordsId.map { id ->
            sqlDataService.findById(id)?.let {
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
                else -> if (ATTS_MAPPING.containsKey(it.getAttribute())) {
                    ValuePredicate(ATTS_MAPPING[it.getAttribute()], it.getType(), it.getValue())
                } else {
                    it
                }
            }
        }

        val page = recsQuery.page
        val findRes = sqlDataService.find(
            predicate,
            recsQuery.sortBy.map {
                DbFindSort(ATTS_MAPPING.getOrDefault(it.attribute, it.attribute), it.isAscending)
            },
            DbFindPage(page.skipCount, page.maxItems)
        )

        val queryRes = RecsQueryRes<Any>()
        queryRes.setTotalCount(findRes.totalCount)
        queryRes.setRecords(findRes.entities.map { Record(this, it) })

        return queryRes
    }

    override fun delete(recordsId: List<String>): List<DelStatus> {

        if (!config.mutable) {
            error("Records DAO is not mutable. Records can't be deleted: '$recordsId'")
        }

        return recordsId.map {
            sqlDataService.delete(it)
            DelStatus.OK
        }
    }

    private fun getTypeIdForRecord(record: LocalRecordAtts): String {

        val typeRefStr = record.attributes.get(RecordConstants.ATT_TYPE).asText()
        val typeRefFromAtts = RecordRef.valueOf(typeRefStr).id
        if (typeRefFromAtts.isNotBlank()) {
            return typeRefFromAtts
        }

        val typeFromRecord = sqlDataService.findById(record.id)?.type
        if (!typeFromRecord.isNullOrBlank()) {
            return typeFromRecord
        }

        error("${RecordConstants.ATT_TYPE} attribute is mandatory for mutation. Record: ${record.id}")
    }

    override fun mutate(records: List<LocalRecordAtts>): List<String> {

        if (!config.mutable) {
            error("Records DAO is not mutable. Records can't be mutated: '${records.map { it.id }}'")
        }
        val typesId = records.map { getTypeIdForRecord(it) }

        val columns = ecosTypeService.getColumnsForTypes(typesId).filter {
            !it.name.startsWith("__")
        }

        return records.mapIndexed { recordIdx, record ->

            val recordTypeId = typesId[recordIdx]

            val recToMutate: DbEntity = if (record.id.isEmpty()) {
                DbEntity()
            } else {
                val existingEntity = sqlDataService.findById(record.id)
                if (existingEntity != null) {
                    existingEntity
                } else {
                    val newEntity = DbEntity()
                    newEntity.extId = record.id
                    newEntity
                }
            }

            columns.forEach {
                if (record.attributes.has(it.name)) {
                    val value = record.attributes.get(it.name)
                    recToMutate.attributes[it.name] = if (value.isObject()
                            && it.type == DbColumnType.TEXT || it.type == DbColumnType.JSON) {
                        Json.mapper.toString(value)
                    } else {
                        value.getAs(it.type.type.java)
                    }
                }
            }
            recToMutate.type = recordTypeId

            if (record.attributes.has(StatusConstants.ATT_STATUS)) {
                val newStatus = record.attributes.get(StatusConstants.ATT_STATUS).asText()
                // todo: add status validation
                if (newStatus.isNotBlank()) {
                    recToMutate.status = newStatus
                }
            }

            val recAfterSave = sqlDataService.save(recToMutate, columns)

            if (ecosTypeService.fillComputedAtts(getId(), recAfterSave)) {
                sqlDataService.save(recAfterSave, columns)
            } else {
                recAfterSave
            }.extId
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
            typeInfo.attributes.forEach {
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
                    else -> {
                    } // do nothing
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
