package ru.citeck.ecos.data.sql.records

import mu.KotlinLogging
import ru.citeck.ecos.commons.data.DataValue
import ru.citeck.ecos.commons.data.ObjectData
import ru.citeck.ecos.commons.data.Version
import ru.citeck.ecos.context.lib.auth.AuthContext
import ru.citeck.ecos.data.sql.content.DbContentService
import ru.citeck.ecos.data.sql.dto.DbColumnDef
import ru.citeck.ecos.data.sql.dto.DbTableRef
import ru.citeck.ecos.data.sql.ecostype.DbEcosModelService
import ru.citeck.ecos.data.sql.ecostype.EcosAttColumnDef
import ru.citeck.ecos.data.sql.meta.table.dto.DbTableMetaDto
import ru.citeck.ecos.data.sql.perms.DbEntityPermsDto
import ru.citeck.ecos.data.sql.perms.DbEntityPermsService
import ru.citeck.ecos.data.sql.records.assocs.DbAssocRefsDiff
import ru.citeck.ecos.data.sql.records.assocs.DbAssocsService
import ru.citeck.ecos.data.sql.records.computed.DbComputedAttsComponent
import ru.citeck.ecos.data.sql.records.dao.DbRecordsDaoCtx
import ru.citeck.ecos.data.sql.records.dao.DbRecordsDaoCtxAware
import ru.citeck.ecos.data.sql.records.dao.atts.DbAssocAttValuesContainer
import ru.citeck.ecos.data.sql.records.dao.atts.DbEmptyRecord
import ru.citeck.ecos.data.sql.records.dao.atts.DbRecord
import ru.citeck.ecos.data.sql.records.dao.atts.content.HasEcosContentDbData
import ru.citeck.ecos.data.sql.records.listener.*
import ru.citeck.ecos.data.sql.records.perms.DbPermsComponent
import ru.citeck.ecos.data.sql.records.perms.DbRecordAllowedAllPerms
import ru.citeck.ecos.data.sql.records.perms.DbRecordPermsContext
import ru.citeck.ecos.data.sql.records.refs.DbGlobalRefCalculator
import ru.citeck.ecos.data.sql.records.refs.DbRecordRefService
import ru.citeck.ecos.data.sql.records.refs.DefaultDbGlobalRefCalculator
import ru.citeck.ecos.data.sql.records.utils.DbAttValueUtils
import ru.citeck.ecos.data.sql.records.utils.DbDateUtils
import ru.citeck.ecos.data.sql.repo.entity.DbEntity
import ru.citeck.ecos.data.sql.repo.find.DbFindPage
import ru.citeck.ecos.data.sql.repo.find.DbFindSort
import ru.citeck.ecos.data.sql.service.DbDataService
import ru.citeck.ecos.data.sql.service.aggregation.AggregateFunc
import ru.citeck.ecos.data.sql.service.assocs.AssocJoin
import ru.citeck.ecos.model.lib.ModelServiceFactory
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType
import ru.citeck.ecos.model.lib.status.constants.StatusConstants
import ru.citeck.ecos.model.lib.type.dto.QueryPermsPolicy
import ru.citeck.ecos.model.lib.type.dto.TypeInfo
import ru.citeck.ecos.model.lib.utils.ModelUtils
import ru.citeck.ecos.records2.RecordConstants
import ru.citeck.ecos.records2.predicate.PredicateService
import ru.citeck.ecos.records2.predicate.PredicateUtils
import ru.citeck.ecos.records2.predicate.model.*
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
import ru.citeck.ecos.records3.record.request.RequestContext
import ru.citeck.ecos.txn.lib.TxnContext
import ru.citeck.ecos.txn.lib.transaction.Transaction
import ru.citeck.ecos.webapp.api.content.EcosContentData
import ru.citeck.ecos.webapp.api.content.EcosContentWriter
import ru.citeck.ecos.webapp.api.entity.EntityRef
import ru.citeck.ecos.webapp.api.entity.toEntityRef
import java.util.*
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicBoolean
import java.util.regex.Pattern
import kotlin.collections.ArrayList
import kotlin.collections.HashSet
import kotlin.collections.LinkedHashMap
import kotlin.collections.LinkedHashSet
import kotlin.math.min

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

        private const val ATT_ID = "id"
        private const val ATT_STATE = "_state"
        private const val ATT_CUSTOM_NAME = "name"

        private const val ASPECT_VERSIONABLE_DATA = "${DbRecord.ASPECT_VERSIONABLE}-data"

        private val AGG_FUNC_PATTERN = Pattern.compile("^(\\w+)\\((\\w+|\\*)\\)$")

        private val log = KotlinLogging.logger {}
    }

    private lateinit var ecosTypeService: DbEcosModelService
    private lateinit var daoCtx: DbRecordsDaoCtx
    private val daoCtxInitialized = AtomicBoolean(false)

    private val recordRefService: DbRecordRefService = dataService.getTableContext().getRecordRefsService()
    private val assocsService: DbAssocsService = dataService.getTableContext().getAssocsService()
    private val contentService: DbContentService = dataService.getTableContext().getContentService()
    private val entityPermsService: DbEntityPermsService = dataService.getTableContext().getPermsService()

    private val listeners: MutableList<DbRecordsListener> = CopyOnWriteArrayList()
    private val recsUpdatedInThisTxnKey = IdentityKey()

    private val recsPrepareToCommitTxnKey = IdentityKey()

    fun uploadFile(
        ecosType: String? = null,
        name: String? = null,
        mimeType: String? = null,
        encoding: String? = null,
        attributes: ObjectData? = null,
        writer: (EcosContentWriter) -> Unit
    ): EntityRef {

        val typeId = (ecosType ?: "").ifBlank { config.typeRef.getLocalId() }
        if (typeId.isBlank()) {
            error("Type is blank. Uploading is impossible")
        }
        val typeDef = daoCtx.ecosTypeService.getTypeInfoNotNull(typeId)

        val contentAttribute = typeDef.contentConfig.path.ifBlank { "content" }
        if (contentAttribute.contains(".")) {
            error("You can't upload file with content as complex path: '$contentAttribute'")
        }
        if (typeDef.model.attributes.all { it.id != contentAttribute } &&
            typeDef.model.systemAttributes.all { it.id != contentAttribute }
        ) {
            error("Content attribute is not found: $contentAttribute")
        }
        val storageType = typeDef.contentConfig.storageType
        return TxnContext.doInTxn {
            val contentId = daoCtx.recContentHandler.uploadContent(
                name,
                mimeType,
                encoding,
                storageType,
                writer
            ) ?: error("File uploading failed")

            val recordToMutate = LocalRecordAtts()
            recordToMutate.setAtt(contentAttribute, contentId)
            recordToMutate.setAtt("name", name)
            recordToMutate.setAtt(RecordConstants.ATT_TYPE, ModelUtils.getTypeRef(typeId))
            attributes?.forEach { key, value ->
                recordToMutate.setAtt(key, value)
            }
            val result = daoCtx.recContentHandler.withContentDbDataAware {
                EntityRef.create(daoCtx.appName, daoCtx.sourceId, mutate(recordToMutate))
            }
            // this content record already cloned while mutation and should be deleted
            daoCtx.contentService?.removeContent(contentId)
            result
        }
    }

    @JvmOverloads
    fun getContent(recordId: String, attribute: String = "", index: Int = 0): EcosContentData? {

        val entity = findDbEntityByExtId(recordId) ?: error("Entity doesn't found with id '$recordId'")
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
                val entityRef = daoCtx.recordRefService.getEntityRefById(linkedRefId)
                contentApi.getContent(entityRef, pathAfterDot, index)
            }
        } else {
            val atts = getRecordsAtts(listOf(recordId)).first()
            atts.init()
            val contentValue = atts.getAtt(notBlankAttribute)
            return if (contentValue is HasEcosContentDbData) {
                contentValue.getContentDbData()
            } else {
                null
            }
        }
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
        TxnContext.doInTxn {
            val perms = AuthContext.runAsSystem { getEntitiesPerms(records) }
            entityPermsService.setReadPerms(perms)
        }
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
        return recordIds.map { id ->
            if (id.isEmpty()) {
                DbEmptyRecord(daoCtx)
            } else {
                findDbEntityByExtId(id)?.let { DbRecord(daoCtx, it) } ?: EmptyAttValue.INSTANCE
            }
        }
    }

    private fun findDbEntityByExtId(extId: String): DbEntity? {

        val entity = dataService.doWithPermsPolicy(QueryPermsPolicy.PUBLIC) {
            dataService.findByExtId(extId)
        } ?: return null

        if (getUpdatedInTxnIds().contains(extId) || AuthContext.isRunAsSystem()) {
            return entity
        }
        if (getRecordPerms(extId).hasReadPerms()) {
            return entity
        }
        return null
    }

    override fun queryRecords(recsQuery: RecordsQuery): RecsQueryRes<DbRecord> {

        val language = recsQuery.language
        if (language.isNotEmpty() && language != PredicateService.LANGUAGE_PREDICATE) {
            return RecsQueryRes()
        }
        val originalPredicate = if (recsQuery.query.isNull()) {
            Predicates.alwaysTrue()
        } else {
            recsQuery.getQuery(Predicate::class.java)
        }

        val ecosTypeRef = if (recsQuery.ecosType.isNotEmpty()) {
            ModelUtils.getTypeRef(recsQuery.ecosType)
        } else {
            val queryTypePred = PredicateUtils.filterValuePredicates(originalPredicate) {
                it.getAttribute() == RecordConstants.ATT_TYPE && it.getValue().asText().isNotBlank()
            }.orElse(null)
            if (queryTypePred is ValuePredicate) {
                EntityRef.valueOf(queryTypePred.getValue().asText())
            } else {
                config.typeRef
            }
        }

        val attributesById: Map<String, AttributeDef>
        val typeAspects: Set<EntityRef>
        var queryPermsPolicy = QueryPermsPolicy.OWN

        if (ecosTypeRef.isNotEmpty()) {
            val typeInfo = ecosTypeService.getTypeInfo(ecosTypeRef.getLocalId())
            attributesById = typeInfo?.model?.getAllAttributes()?.associateBy { it.id } ?: emptyMap()
            typeAspects = typeInfo?.aspects?.map { it.ref }?.toSet() ?: emptySet()
            queryPermsPolicy = typeInfo?.queryPermsPolicy ?: queryPermsPolicy
        } else {
            attributesById = emptyMap()
            typeAspects = emptySet()
        }
        if (AuthContext.isRunAsSystem()) {
            queryPermsPolicy = QueryPermsPolicy.PUBLIC
        }

        fun replaceRefsToIds(value: DataValue): DataValue {
            if (value.isArray()) {
                return DataValue.create(
                    recordRefService.getIdByEntityRefs(
                        value.mapNotNull {
                            val txt = if (it.isTextual()) {
                                it.asText()
                            } else {
                                ""
                            }
                            if (txt.isNotEmpty()) {
                                txt.toEntityRef()
                            } else {
                                null
                            }
                        }
                    )
                )
            }
            if (value.isTextual()) {
                val txt = value.asText()
                if (txt.isEmpty()) {
                    return value
                }
                val refIds = recordRefService.getIdByEntityRefs(
                    listOf(txt.toEntityRef())
                )
                return DataValue.create(refIds.firstOrNull() ?: -1)
            }
            return value
        }

        var typePredicateExists = false
        val assocJoins = mutableMapOf<String, AssocJoin>()
        var assocJoinsCounter = 0
        val assocsTableExists = assocsService.isAssocsTableExists()

        var predicate =
            PredicateUtils.mapAttributePredicates(recsQuery.getQuery(Predicate::class.java)) { currentPred ->
                val attribute = currentPred.getAttribute()
                if (currentPred is ValuePredicate) {
                    when (attribute) {
                        RecordConstants.ATT_TYPE -> {
                            typePredicateExists = true
                            val value = currentPred.getValue()
                            when (currentPred.getType()) {
                                ValuePredicate.Type.EQ,
                                ValuePredicate.Type.CONTAINS,
                                ValuePredicate.Type.IN -> {
                                    val expandedValue = mutableSetOf<String>()
                                    if (value.isArray()) {
                                        value.forEach { typeRef ->
                                            val typeId = typeRef.asText().toEntityRef().getLocalId()
                                            expandedValue.add(typeId)
                                            ecosTypeService.getAllChildrenIds(typeId, expandedValue)
                                        }
                                    } else {
                                        val typeId = EntityRef.valueOf(value.asText()).getLocalId()
                                        expandedValue.add(typeId)
                                        ecosTypeService.getAllChildrenIds(typeId, expandedValue)
                                    }
                                    if (currentPred.getType() != ValuePredicate.Type.IN) {
                                        if (expandedValue.size > 1) {
                                            ValuePredicate(DbEntity.TYPE, ValuePredicate.Type.IN, expandedValue)
                                        } else if (expandedValue.size == 1) {
                                            ValuePredicate(DbEntity.TYPE, currentPred.getType(), expandedValue.first())
                                        } else {
                                            Predicates.alwaysTrue()
                                        }
                                    } else {
                                        ValuePredicate(DbEntity.TYPE, currentPred.getType(), expandedValue)
                                    }
                                }

                                else -> {
                                    Predicates.alwaysFalse()
                                }
                            }
                        }

                        DbRecord.ATT_ASPECTS -> {
                            val aspectsPredicate: Predicate = when (currentPred.getType()) {
                                ValuePredicate.Type.EQ,
                                ValuePredicate.Type.CONTAINS -> {
                                    val value = currentPred.getValue()
                                    if (value.isTextual()) {
                                        if (typeAspects.contains(value.asText().toEntityRef())) {
                                            Predicates.alwaysTrue()
                                        } else {
                                            currentPred
                                        }
                                    } else {
                                        currentPred
                                    }
                                }

                                ValuePredicate.Type.IN -> {
                                    val aspects = currentPred.getValue().toList(EntityRef::class.java)
                                    if (aspects.any { typeAspects.contains(it) }) {
                                        Predicates.alwaysTrue()
                                    } else {
                                        currentPred
                                    }
                                }

                                else -> currentPred
                            }
                            if (aspectsPredicate is ValuePredicate) {
                                ValuePredicate(
                                    aspectsPredicate.getAttribute(),
                                    aspectsPredicate.getType(),
                                    replaceRefsToIds(aspectsPredicate.getValue())
                                )
                            } else {
                                aspectsPredicate
                            }
                        }

                        else -> {

                            val attDef = attributesById[attribute]
                                ?: DbRecord.GLOBAL_ATTS[attribute]

                            var newPred: Predicate = if (DbRecord.ATTS_MAPPING.containsKey(attribute)) {
                                ValuePredicate(
                                    DbRecord.ATTS_MAPPING[attribute],
                                    currentPred.getType(),
                                    currentPred.getValue()
                                )
                            } else {
                                currentPred
                            }

                            if (newPred is ValuePredicate && newPred.getType() == ValuePredicate.Type.EQ) {
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
                            if (newPred is ValuePredicate && DbRecordsUtils.isAssocLikeAttribute(attDef)) {
                                val newAttribute = if (assocsTableExists && attDef?.multiple == true) {
                                    val assocAtt = newPred.getAttribute()
                                    val joinAtt = "$assocAtt-${assocJoinsCounter++}"
                                    assocJoins[joinAtt] = AssocJoin(
                                        daoCtx.assocsService.getIdForAtt(assocAtt),
                                        assocAtt
                                    )
                                    joinAtt
                                } else {
                                    newPred.getAttribute()
                                }
                                newPred = ValuePredicate(
                                    newAttribute,
                                    newPred.getType(),
                                    replaceRefsToIds(newPred.getValue())
                                )
                            } else if (newPred is ValuePredicate &&
                                (attDef?.type == AttributeType.DATE || attDef?.type == AttributeType.DATETIME)
                            ) {

                                val value = newPred.getValue()

                                if (value.isTextual()) {

                                    val textVal = DbDateUtils.normalizeDateTimePredicateValue(
                                        value.asText(),
                                        attDef.type == AttributeType.DATETIME
                                    )
                                    val rangeDelimIdx = textVal.indexOf('/')

                                    newPred = if (rangeDelimIdx > 0 && textVal.length > rangeDelimIdx + 1) {

                                        val rangeFrom = textVal.substring(0, rangeDelimIdx)
                                        val rangeTo = textVal.substring(rangeDelimIdx + 1)

                                        AndPredicate.of(
                                            ValuePredicate.ge(newPred.getAttribute(), rangeFrom),
                                            ValuePredicate.lt(newPred.getAttribute(), rangeTo)
                                        )
                                    } else {
                                        ValuePredicate(newPred.getAttribute(), newPred.getType(), textVal)
                                    }
                                }
                            }
                            newPred
                        }
                    }
                } else if (currentPred is EmptyPredicate) {
                    if (DbRecord.ATTS_MAPPING.containsKey(attribute)) {
                        EmptyPredicate(DbRecord.ATTS_MAPPING[attribute])
                    } else {
                        currentPred
                    }
                } else {
                    log.error { "Unknown predicate type: ${currentPred::class}" }
                    null
                }
            } ?: Predicates.alwaysTrue()

        if (!typePredicateExists && config.typeRef.getLocalId() != ecosTypeRef.getLocalId()) {
            val typeIds = mutableListOf<String>()
            typeIds.add(ecosTypeRef.getLocalId())
            ecosTypeService.getAllChildrenIds(ecosTypeRef.getLocalId(), typeIds)
            val typePred: Predicate = if (typeIds.size == 1) {
                ValuePredicate(DbEntity.TYPE, ValuePredicate.Type.EQ, typeIds[0])
            } else {
                ValuePredicate(DbEntity.TYPE, ValuePredicate.Type.IN, typeIds)
            }
            predicate = Predicates.and(typePred, predicate)
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

        val findRes = dataService.doWithPermsPolicy(queryPermsPolicy) {
            dataService.find(
                predicate,
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
                selectFunctions,
                assocJoins,
                true
            )
        }

        val queryRes = RecsQueryRes<DbRecord>()
        queryRes.setTotalCount(findRes.totalCount)
        queryRes.setRecords(findRes.entities.map { DbRecord(daoCtx, it) })
        queryRes.setHasMore(findRes.totalCount > findRes.entities.size + page.skipCount)

        return queryRes
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
            daoCtx.deleteDao.delete(recordIds)
        }
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
            val typeFromRecord = dataService.doWithPermsPolicy(QueryPermsPolicy.PUBLIC) {
                dataService.findByExtId(extId)?.type
            }
            if (!typeFromRecord.isNullOrBlank()) {
                return typeFromRecord
            }
        }

        if (EntityRef.isNotEmpty(config.typeRef)) {
            return config.typeRef.getLocalId()
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
        return TxnContext.doInTxn {
            val resultEntity = mutateInTxn(record)
            val resultRecId = resultEntity.extId

            var queryPermsPolicy = QueryPermsPolicy.OWN
            queryPermsPolicy = ecosTypeService.getTypeInfo(resultEntity.type)?.queryPermsPolicy ?: queryPermsPolicy

            val txn = TxnContext.getTxn()
            if (queryPermsPolicy == QueryPermsPolicy.OWN) {
                val prepareToCommitEntities = txn.getData(recsPrepareToCommitTxnKey) {
                    LinkedHashSet<String>()
                }
                if (prepareToCommitEntities.isEmpty()) {
                    TxnContext.doBeforeCommit(0f) {
                        entityPermsService.setReadPerms(getEntitiesPerms(prepareToCommitEntities))
                    }
                }
                prepareToCommitEntities.add(resultRecId)
            }
            getUpdatedInTxnIds(txn).add(resultRecId)

            resultRecId
        }
    }

    private fun mutateInTxn(record: LocalRecordAtts): DbEntity {

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
    ): DbEntity {

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

        val currentUser = runAsAuth.getUser()
        val currentAuthorities = DbRecordsUtils.getCurrentAuthorities(runAsAuth)

        val extId = record.id.ifEmpty { record.attributes[ATT_ID].asText() }
        val currentAspectRefs = LinkedHashSet(typeAspects)
        val aspectRefsInDb = LinkedHashSet<EntityRef>()

        val entityToMutate: DbEntity = if (extId.isEmpty()) {
            DbEntity()
        } else {
            var entity = findDbEntityByExtId(extId)
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
                if (record.attributes["__updatePermissions"].asBoolean()) {
                    if (!isRunAsSystemOrAdmin) {
                        error("Permissions update allowed only for admin. Record: $record sourceId: '${getId()}'")
                    }
                    updatePermissions(listOf(record.id))
                    return entity
                }
            }
            entity
        }
        var customExtId = record.attributes[ATT_ID].asText()
        if (customExtId.isBlank()) {
            customExtId = record.attributes[ScalarType.LOCAL_ID.mirrorAtt].asText()
        }
        if (customExtId.isNotBlank() && entityToMutate.extId != customExtId) {

            if (entityToMutate.id == DbEntity.NEW_REC_ID) {
                entityToMutate.extId = customExtId
            } else {
                dataService.doWithPermsPolicy(QueryPermsPolicy.PUBLIC) {
                    if (dataService.findByExtId(customExtId) != null) {
                        log.error {
                            "Record with ID $customExtId already exists. You should mutate it directly. " +
                                "Record: ${getId()}@$customExtId"
                        }
                        error("Read permission denied for ${daoCtx.getGlobalRef(customExtId)}")
                    }
                }
                return daoCtx.recContentHandler.withContentDbDataAware {
                    val attsToCopy = DbRecord(daoCtx, entityToMutate).getAttsForCopy()
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

        var recordPerms = DbRecordPermsContext(DbRecordAllowedAllPerms)
        if (!isNewEntity && !isRunAsSystem) {
            if (!getUpdatedInTxnIds().contains(entityToMutate.extId)) {
                recordPerms = getRecordPerms(entityToMutate.extId, currentUser, currentAuthorities)
                if (!recordPerms.hasWritePerms()) {
                    error("Permissions Denied. You can't change record '${daoCtx.getGlobalRef(record.id)}'")
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
        }

        val recAttributes = record.attributes.deepCopy()

        if (isNewEntity && typeInfo.defaultStatus.isNotBlank() &&
            recAttributes[StatusConstants.ATT_STATUS].isEmpty()
        ) {
            recAttributes[StatusConstants.ATT_STATUS] = typeInfo.defaultStatus
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
            typeInfo.contentConfig.storageType
        )

        recAttributes.forEach { att, newValue ->
            val attDef = typeAttColumnsByAtt[att]
            if (attDef != null && DbRecordsUtils.isAssocLikeAttribute(attDef.attribute)) {
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
            }
        }

        if (isVersionable && recAttributes.has(mainContentAtt)) {
            val contentAfter = recAttributes[mainContentAtt].asLong(-1)
            val contentBefore = entityToMutate.attributes[mainContentAtt] as? Long ?: -1
            if (contentAfter != contentBefore) {
                val contentWasChanged = if (contentBefore == -1L || contentAfter == -1L) {
                    true
                } else {
                    val uriBefore = daoCtx.contentService?.getContent(contentBefore)?.getUri()
                    val uriAfter = daoCtx.contentService?.getContent(contentAfter)?.getUri()
                    uriBefore != uriAfter
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
                contentService.getContent(attribute.asLong())?.getName()?.let {
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
            getRecordPerms(entityToMutate.extId)
        }
        val changedAssocs = ArrayList<DbAssocRefsDiff>()
        setMutationAtts(
            entityToMutate,
            recAttributes,
            typeColumns,
            changedAssocs,
            isAssocForceDeletion,
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
                    isAssocForceDeletion
                )
            )
        }

        if (recAttributes.has(ATT_STATE)) {
            val state = recAttributes[ATT_STATE].asText()
            entityToMutate.attributes[DbRecord.COLUMN_IS_DRAFT.name] = state == "draft"
            fullColumns.add(DbRecord.COLUMN_IS_DRAFT)
        }

        entityToMutate.type = typeInfo.id

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

        var recAfterSave = dataService.save(entityToMutate, fullColumns)

        recAfterSave = dataService.doWithPermsPolicy(QueryPermsPolicy.PUBLIC) {
            val newEntity = if (computedAttsComponent != null) {
                computeAttsToStore(
                    computedAttsComponent,
                    recAfterSave,
                    isNewEntity,
                    typeInfo.id,
                    fullColumns
                )
            } else {
                recAfterSave
            }
            newEntity
        }

        daoCtx.mutAssocHandler.processChildrenAfterMutation(
            recordEntityBeforeMutation,
            recAfterSave,
            record.attributes,
            typeAttColumns,
            changedByOperationsAtts,
            allAssocsValues
        )
        daoCtx.mutAssocHandler.processParentAfterMutation(
            recordEntityBeforeMutation,
            recAfterSave,
            record.attributes
        )

        daoCtx.recEventsHandler.emitEventsAfterMutation(
            recordEntityBeforeMutation,
            recAfterSave,
            isNewEntity,
            changedAssocs
        )

        return recAfterSave
    }

    private fun computeAttsToStore(
        component: DbComputedAttsComponent,
        entity: DbEntity,
        isNewRecord: Boolean,
        recTypeId: String,
        columns: List<DbColumnDef>
    ): DbEntity {

        val typeRef = ModelUtils.getTypeRef(recTypeId)
        val atts = component.computeAttsToStore(DbRecord(daoCtx, entity), isNewRecord, typeRef)

        val fullColumns = ArrayList(columns)
        DbRecord.COMPUTABLE_OPTIONAL_COLUMNS.forEach {
            if (atts.has(it.name)) {
                fullColumns.add(it)
            }
        }
        var changed = setMutationAtts(
            entity,
            atts,
            fullColumns,
            ArrayList(),
            true
        ).isNotEmpty()

        val dispName = component.computeDisplayName(DbRecord(daoCtx, entity), typeRef)
        if (entity.name != dispName) {
            entity.name = dispName
            changed = true
        }

        if (!changed) {
            return entity
        }

        return dataService.save(entity, fullColumns)
    }

    private fun getEntitiesPerms(recordIds: Collection<String>): List<DbEntityPermsDto> {
        val recordIdsList: List<String> = if (recordIds is List<String>) {
            recordIds
        } else {
            ArrayList(recordIds)
        }
        val refIds = getEntityRefIds(recordIdsList)
        val result = arrayListOf<DbEntityPermsDto>()
        for ((idx, refId) in refIds.withIndex()) {
            if (refId != -1L) {
                result.add(
                    DbEntityPermsDto(
                        refId,
                        getRecordPerms(recordIdsList[idx]).getAuthoritiesWithReadPermission(),
                        // todo
                        emptySet()
                    )
                )
            }
        }
        return result
    }

    fun getRecordPerms(record: Any): DbRecordPermsContext {
        val auth = AuthContext.getCurrentRunAsAuth()
        val currentUser = auth.getUser()
        val currentAuthorities = DbRecordsUtils.getCurrentAuthorities(auth)
        return getRecordPerms(record, currentUser, currentAuthorities)
    }

    fun getRecordPerms(record: Any, user: String, authorities: Set<String>): DbRecordPermsContext {
        // Optimization to enable caching
        return RequestContext.doWithCtx(
            serviceFactory,
            {
                it.withReadOnly(true)
            },
            {
                TxnContext.doInTxn(true) {
                    AuthContext.runAsSystem {
                        val recordToGetPerms = if (record is String) {
                            findDbEntityByExtId(record)?.let { DbRecord(daoCtx, it) }
                        } else {
                            record
                        }
                        val perms = if (recordToGetPerms != null) {
                            permsComponent.getRecordPerms(user, authorities, recordToGetPerms)
                        } else {
                            DbRecordAllowedAllPerms
                        }
                        DbRecordPermsContext(perms)
                    }
                }
            }
        )
    }

    private fun getEntityRefIds(recordIds: List<String>): List<Long> {
        val entitiesByExtId = dataService.findAll(
            Predicates.inVals(DbEntity.EXT_ID, recordIds)
        ).associateBy { it.extId }
        return recordIds.map {
            entitiesByExtId[it]?.refId ?: -1
        }
    }

    private fun setMutationAtts(
        recToMutate: DbEntity,
        atts: ObjectData,
        columns: List<DbColumnDef>,
        changedAssocs: MutableList<DbAssocRefsDiff>,
        isAssocForceDeletion: Boolean,
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
                        "for record ${getId()}@${recToMutate.extId}"
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
                        multiAssocValue.getAddedTargetsIds()
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

    override fun getId() = config.id

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
        ecosTypeService = DbEcosModelService(modelServices)
        val appName = serviceFactory.webappProps.appName
        daoCtx = DbRecordsDaoCtx(
            appName,
            getId(),
            dataService.getTableRef(),
            config,
            dataService,
            contentService,
            recordRefService,
            ecosTypeService,
            serviceFactory.recordsServiceV1,
            serviceFactory.getEcosWebAppApi()?.getContentApi(),
            serviceFactory.getEcosWebAppApi()?.getAuthoritiesApi(),
            listeners,
            this,
            serviceFactory.attValuesConverter,
            serviceFactory.getEcosWebAppApi()?.getWebClientApi(),
            modelServices.delegationService,
            assocsService,
            globalRefCalculator ?: DefaultDbGlobalRefCalculator()
        )
        daoCtxInitialized.set(true)
        listeners.forEach {
            if (it is DbRecordsDaoCtxAware) {
                it.setRecordsDaoCtx(daoCtx)
            }
        }
        onInitialized()
    }

    private fun getUpdatedInTxnIds(txn: Transaction? = TxnContext.getTxnOrNull()): MutableSet<String> {
        // If user has already modified a record in this transaction,
        // he can modify it again until commit without checking permissions.
        return txn?.getData(AuthContext.getCurrentRunAsUser() to recsUpdatedInThisTxnKey) { HashSet() } ?: HashSet()
    }

    private class IdentityKey
}
