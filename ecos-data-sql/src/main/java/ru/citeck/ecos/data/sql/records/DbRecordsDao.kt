package ru.citeck.ecos.data.sql.records

import mu.KotlinLogging
import ru.citeck.ecos.commons.data.DataValue
import ru.citeck.ecos.commons.data.ObjectData
import ru.citeck.ecos.context.lib.auth.AuthContext
import ru.citeck.ecos.data.sql.content.DbContentService
import ru.citeck.ecos.data.sql.dto.DbColumnDef
import ru.citeck.ecos.data.sql.dto.DbTableRef
import ru.citeck.ecos.data.sql.ecostype.DbEcosModelService
import ru.citeck.ecos.data.sql.ecostype.EcosAttColumnDef
import ru.citeck.ecos.data.sql.meta.table.dto.DbTableMetaDto
import ru.citeck.ecos.data.sql.perms.DbEntityPermsDto
import ru.citeck.ecos.data.sql.perms.DbEntityPermsService
import ru.citeck.ecos.data.sql.records.computed.DbComputedAttsComponent
import ru.citeck.ecos.data.sql.records.dao.DbRecordsDaoCtx
import ru.citeck.ecos.data.sql.records.dao.DbRecordsDaoCtxAware
import ru.citeck.ecos.data.sql.records.dao.atts.DbEmptyRecord
import ru.citeck.ecos.data.sql.records.dao.atts.DbRecord
import ru.citeck.ecos.data.sql.records.dao.atts.content.HasEcosContentDbData
import ru.citeck.ecos.data.sql.records.listener.*
import ru.citeck.ecos.data.sql.records.perms.DbPermsComponent
import ru.citeck.ecos.data.sql.records.perms.DbRecordAllowedAllPerms
import ru.citeck.ecos.data.sql.records.perms.DbRecordPerms
import ru.citeck.ecos.data.sql.records.perms.DbRecordPermsSystemAdapter
import ru.citeck.ecos.data.sql.records.refs.DbRecordRefService
import ru.citeck.ecos.data.sql.repo.entity.DbEntity
import ru.citeck.ecos.data.sql.repo.find.DbFindPage
import ru.citeck.ecos.data.sql.repo.find.DbFindSort
import ru.citeck.ecos.data.sql.service.DbDataService
import ru.citeck.ecos.data.sql.service.aggregation.AggregateFunc
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
import ru.citeck.ecos.records2.predicate.model.EmptyPredicate
import ru.citeck.ecos.records2.predicate.model.Predicate
import ru.citeck.ecos.records2.predicate.model.Predicates
import ru.citeck.ecos.records2.predicate.model.ValuePredicate
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
import ru.citeck.ecos.records3.utils.RecordRefUtils
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
import kotlin.collections.LinkedHashSet
import kotlin.math.min

class DbRecordsDao(
    private val config: DbRecordsDaoConfig,
    private val modelServices: ModelServiceFactory,
    private val dataService: DbDataService<DbEntity>,
    private val permsComponent: DbPermsComponent,
    private val computedAttsComponent: DbComputedAttsComponent?
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

        private val AGG_FUNC_PATTERN = Pattern.compile("^(\\w+)\\((\\w+|\\*)\\)$")

        private val GLOBAL_ATTS = listOf(
            AttributeDef.create()
                .withId(RecordConstants.ATT_PARENT)
                .withType(AttributeType.ASSOC)
                .build()
        ).associateBy { it.id }

        private val log = KotlinLogging.logger {}
    }

    private lateinit var ecosTypeService: DbEcosModelService
    private lateinit var daoCtx: DbRecordsDaoCtx
    private val daoCtxInitialized = AtomicBoolean(false)

    private val recordRefService: DbRecordRefService = dataService.getTableContext().getRecordRefsService()
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
            daoCtx.recContentHandler.withContentDbDataAware {
                EntityRef.create(daoCtx.appName, daoCtx.sourceId, mutate(recordToMutate))
            }
        }
    }

    @JvmOverloads
    fun getContent(recordId: String, attribute: String = "", index: Int = 0): EcosContentData? {
        val entity = dataService.findByExtId(recordId) ?: error("Entity doesn't found with id '$recordId'")
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
        val perms = getRecordPerms(extId)
        val authoritiesWithReadPermissions = perms.getAuthoritiesWithReadPermission().toSet()
        val currentAuthorities = DbRecordsUtils.getCurrentAuthorities()
        if (currentAuthorities.any { authoritiesWithReadPermissions.contains(it) }) {
            return entity
        }
        return null
    }

    override fun queryRecords(recsQuery: RecordsQuery): RecsQueryRes<DbRecord> {

        val language = recsQuery.language
        if (language.isNotEmpty() && language != PredicateService.LANGUAGE_PREDICATE) {
            return RecsQueryRes()
        }
        val originalPredicate = recsQuery.getQuery(Predicate::class.java)

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
                            val txt = it.asText()
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

        var predicate = PredicateUtils.mapAttributePredicates(recsQuery.getQuery(Predicate::class.java)) { currentPred ->
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
                        var newPred = if (DbRecord.ATTS_MAPPING.containsKey(attribute)) {
                            ValuePredicate(DbRecord.ATTS_MAPPING[attribute], currentPred.getType(), currentPred.getValue())
                        } else {
                            currentPred
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
                            newPred = ValuePredicate(
                                newPred.getAttribute(),
                                newPred.getType(),
                                replaceRefsToIds(newPred.getValue())
                            )
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
                selectFunctions
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
                    if (!recordPerms.isCurrentUserHasWritePerms()) {
                        error("Permissions Denied. You can't delete record '$it'")
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

        val typeAspects = typeInfo.aspects.map { it.ref }.toSet()

        val knownColumnIds = HashSet<String>()
        val typeAttColumns = ArrayList(typeAttColumnsArg)
        val typeColumns = typeAttColumns.map { it.column }.toMutableList()
        val typeColumnNames = typeColumns.map { it.name }.toMutableSet()

        fun addTypeAttColumn(column: EcosAttColumnDef) {
            if (knownColumnIds.add(column.attribute.id)) {
                typeAttColumns.add(column)
                typeColumns.add(column.column)
                typeColumnNames.add(column.column.name)
            }
        }

        val runAsAuth = AuthContext.getCurrentRunAsAuth()
        val isRunAsSystem = AuthContext.isSystemAuth(runAsAuth)
        val isRunAsAdmin = AuthContext.isAdminAuth(runAsAuth)
        val isRunAsSystemOrAdmin = isRunAsSystem || isRunAsAdmin

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
                        error("Permission denied")
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

        var recordPerms: DbRecordPerms = DbRecordAllowedAllPerms
        if (!isNewEntity && !isRunAsSystem) {
            if (!getUpdatedInTxnIds().contains(entityToMutate.extId)) {
                recordPerms = getRecordPerms(entityToMutate.extId)
                if (!recordPerms.isCurrentUserHasWritePerms()) {
                    error("Permissions Denied. You can't change record '${record.id}'")
                }
            }
        }

        if (entityToMutate.id == DbEntity.NEW_REC_ID) {
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

        val recAttributes = record.attributes.deepCopy()

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

            val targetContentAtt = DbRecord.getDefaultContentAtt(typeInfo)
            if (targetContentAtt.contains(".")) {
                error("Inner content uploading is not supported. Content attribute: $targetContentAtt")
            }
            val contentValue = recAttributes[RecordConstants.ATT_CONTENT]
            recAttributes[targetContentAtt] = contentValue
            recAttributes.remove(RecordConstants.ATT_CONTENT)

            val hasCustomNameAtt = typeInfo.model.attributes.find { it.id == ATT_CUSTOM_NAME } != null
            if (hasCustomNameAtt && recAttributes[ATT_CUSTOM_NAME].isEmpty()) {
                contentAttToExtractName = targetContentAtt
            }
        }

        val operations = daoCtx.mutAttOperationHandler.extractAttValueOperations(recAttributes)
        operations.forEach {
            if (!recAttributes.has(it.getAttName())) {
                val currentAtts: Map<String, Any?> = if (entityToMutate.id == DbEntity.NEW_REC_ID) {
                    emptyMap()
                } else {
                    DbRecord(daoCtx, entityToMutate).getAttsForOperations()
                }
                val value = it.invoke(currentAtts[it.getAttName()])
                recAttributes[it.getAttName()] = value
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

        if (contentAttToExtractName.isNotBlank() && recordPerms.isCurrentUserHasAttWritePerms(ATT_CUSTOM_NAME)) {
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
        val perms = if (entityToMutate.id == DbEntity.NEW_REC_ID || isRunAsSystem) {
            null
        } else {
            getRecordPerms(entityToMutate.extId)
        }
        setMutationAtts(entityToMutate, recAttributes, typeColumns, perms)
        val optionalAtts = DbRecord.OPTIONAL_COLUMNS.filter { !typeColumnNames.contains(it.name) }
        if (optionalAtts.isNotEmpty()) {
            fullColumns.addAll(setMutationAtts(entityToMutate, recAttributes, optionalAtts))
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

        val isNewRecord = entityToMutate.id == DbEntity.NEW_REC_ID
        if (isNewRecord) {
            val sourceIdMapping = RequestContext.getCurrentNotNull().ctxData.sourceIdMapping
            val currentAppName = serviceFactory.webappProps.appName
            val currentRef = EntityRef.create(currentAppName, getId(), entityToMutate.extId)
            val newRecordRef = RecordRefUtils.mapAppIdAndSourceId(currentRef, currentAppName, sourceIdMapping)
            entityToMutate.refId = recordRefService.getOrCreateIdByEntityRefs(listOf(newRecordRef))[0]
        }
        var recAfterSave = dataService.save(entityToMutate, fullColumns)

        recAfterSave = dataService.doWithPermsPolicy(QueryPermsPolicy.PUBLIC) {
            val newEntity = if (computedAttsComponent != null) {
                computeAttsToStore(
                    computedAttsComponent,
                    recAfterSave,
                    isNewRecord,
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
            typeAttColumns
        )
        daoCtx.mutAssocHandler.processParentAfterMutation(
            recordEntityBeforeMutation,
            recAfterSave,
            record.attributes
        )

        daoCtx.recEventsHandler.emitEventsAfterMutation(recordEntityBeforeMutation, recAfterSave, isNewRecord)

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
        var changed = setMutationAtts(entity, atts, fullColumns).isNotEmpty()

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

    fun getRecordPerms(recordId: String): DbRecordPerms {
        // Optimization to enable caching
        return RequestContext.doWithCtx(
            serviceFactory,
            {
                it.withReadOnly(true)
            },
            {
                TxnContext.doInTxn(true) {
                    AuthContext.runAsSystem {
                        DbRecordPermsSystemAdapter(
                            permsComponent.getEntityPerms(EntityRef.create(getId(), recordId))
                        )
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
        daoCtx = DbRecordsDaoCtx(
            serviceFactory.webappProps.appName,
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
            serviceFactory.getEcosWebAppApi()?.getWebClientApi()
        )
        daoCtxInitialized.set(true)
        listeners.forEach {
            if (it is DbRecordsDaoCtxAware) {
                it.setRecordsDaoCtx(daoCtx)
            }
        }
    }

    private fun getUpdatedInTxnIds(txn: Transaction? = TxnContext.getTxnOrNull()): MutableSet<String> {
        // If user has already modified a record in this transaction,
        // he can modify it again until commit without checking permissions.
        return txn?.getData(AuthContext.getCurrentRunAsUser() to recsUpdatedInThisTxnKey) { HashSet() } ?: HashSet()
    }

    private class IdentityKey
}
