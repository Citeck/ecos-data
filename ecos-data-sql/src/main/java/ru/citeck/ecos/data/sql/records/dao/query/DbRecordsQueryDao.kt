package ru.citeck.ecos.data.sql.records.dao.query

import io.github.oshai.kotlinlogging.KotlinLogging
import ru.citeck.ecos.commons.data.DataValue
import ru.citeck.ecos.context.lib.auth.AuthContext
import ru.citeck.ecos.data.sql.records.DbRecordsUtils
import ru.citeck.ecos.data.sql.records.dao.DbRecordsDaoCtx
import ru.citeck.ecos.data.sql.records.dao.atts.DbRecord
import ru.citeck.ecos.data.sql.records.utils.DbDateUtils
import ru.citeck.ecos.data.sql.repo.entity.DbEntity
import ru.citeck.ecos.data.sql.repo.find.DbFindPage
import ru.citeck.ecos.data.sql.repo.find.DbFindQuery
import ru.citeck.ecos.data.sql.repo.find.DbFindSort
import ru.citeck.ecos.data.sql.service.assocs.AssocJoinWithPredicate
import ru.citeck.ecos.data.sql.service.assocs.AssocTableJoin
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType
import ru.citeck.ecos.model.lib.type.dto.QueryPermsPolicy
import ru.citeck.ecos.model.lib.type.dto.WorkspaceScope
import ru.citeck.ecos.model.lib.utils.ModelUtils
import ru.citeck.ecos.records2.RecordConstants
import ru.citeck.ecos.records2.predicate.PredicateService
import ru.citeck.ecos.records2.predicate.PredicateUtils
import ru.citeck.ecos.records2.predicate.model.*
import ru.citeck.ecos.records3.record.atts.schema.resolver.AttContext
import ru.citeck.ecos.records3.record.atts.schema.utils.AttStrUtils
import ru.citeck.ecos.records3.record.dao.query.dto.query.RecordsQuery
import ru.citeck.ecos.records3.record.dao.query.dto.res.RecsQueryRes
import ru.citeck.ecos.webapp.api.entity.EntityRef
import ru.citeck.ecos.webapp.api.entity.toEntityRef
import kotlin.collections.ArrayList
import kotlin.math.max
import kotlin.math.min

class DbRecordsQueryDao(private val daoCtx: DbRecordsDaoCtx) {

    companion object {
        private const val ATT_DBID = "dbid"
        private const val COMPLEX_TYPE_CONDITION_PREFIX = "_type."
        private const val TYPE_ATT_ID = "id"
        private const val TYPE_ATT_NAME = "name"
        private const val TYPE_SRC_ID = "emodel/type"
        private const val TYPE_REF_ID_PREFIX = "$TYPE_SRC_ID@"

        private val PROPS_WITH_REFS = setOf(
            RecordConstants.ATT_CREATOR,
            RecordConstants.ATT_MODIFIER
        )

        private val log = KotlinLogging.logger {}
    }

    private val config = daoCtx.config
    private val ecosTypeService = daoCtx.ecosTypeService
    private val recordRefService = daoCtx.recordRefService
    private val assocsService = daoCtx.assocsService
    private val dataService = daoCtx.dataService

    private val dbWsService = dataService.getTableContext().getWorkspaceService()
    private val workspaceService = daoCtx.workspaceService

    fun queryRecords(recsQuery: RecordsQuery): RecsQueryRes<DbRecord> {

        if (!dataService.isTableExists()) {
            return RecsQueryRes()
        }
        return try {
            queryRecordsImpl(recsQuery)
        } catch (e: DbQueryPreparingException) {
            log.debug(e) { "Query can't be executed: $recsQuery" }
            RecsQueryRes()
        }
    }

    private fun queryRecordsImpl(recsQuery: RecordsQuery): RecsQueryRes<DbRecord> {

        val language = recsQuery.language
        if (language.isNotEmpty() && language != PredicateService.LANGUAGE_PREDICATE) {
            return RecsQueryRes()
        }
        val isRunAsSystem = AuthContext.isRunAsSystem()

        var groupBy = recsQuery.groupBy
        // add support of legacy grouping with '&' as delimiter for attributes
        // groupBy(field0&field1) should work as groupBy(field0, field1)
        // see ru.citeck.ecos.records3.record.dao.impl.group.RecordsGroupDao
        if (groupBy.isNotEmpty() && AttStrUtils.indexOf(groupBy[0], '&') > 0) {
            val newGroupBy = ArrayList<String>()
            newGroupBy.addAll(AttStrUtils.split(groupBy[0], '&'))
            for (i in 1 until groupBy.size) {
                newGroupBy.add(groupBy[i])
            }
            groupBy = newGroupBy
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
        val queryCtx = DbFindQueryContext(
            daoCtx,
            ecosTypeRef.getLocalId(),
            groupBy.isNotEmpty(),
            null
        )
        queryCtx.registerAssocsTypesHints(originalPredicate)

        AttContext.getCurrent()?.getSchemaAtt()?.inner?.forEach {
            queryCtx.registerSelectAtt(it.name, false)
        }

        val predicateData = processPredicate(queryCtx, queryCtx.getMainTypeInfo(), originalPredicate)

        var predicate = predicateData.predicate

        if (!predicateData.typePredicateExists && config.typeRef.getLocalId() != ecosTypeRef.getLocalId()) {
            val typeIds = mutableListOf<String>()
            typeIds.add(ecosTypeRef.getLocalId())
            ecosTypeService.getAllChildrenIds(ecosTypeRef.getLocalId(), typeIds)
            val typeRefIds = recordRefService.getIdByEntityRefs(typeIds.map { ModelUtils.getTypeRef(it) })
            val typePred: Predicate = if (typeRefIds.size == 1) {
                ValuePredicate(DbEntity.TYPE, ValuePredicate.Type.EQ, typeRefIds[0])
            } else {
                ValuePredicate(DbEntity.TYPE, ValuePredicate.Type.IN, typeRefIds)
            }
            predicate = Predicates.and(typePred, predicate)
        }

        val typeInfo = ecosTypeService.getTypeInfo(ecosTypeRef.getLocalId())
        if (typeInfo?.workspaceScope == WorkspaceScope.PRIVATE) {
            val workspacesForQuery: Collection<String>
            if (isRunAsSystem) {
                workspacesForQuery = recsQuery.workspaces
            } else {
                val runAs = AuthContext.getCurrentRunAsAuth()
                val userWs = workspaceService.getUserWorkspaces(runAs.getUser())
                if (recsQuery.workspaces.isEmpty()) {
                    val wsToSearch = HashSet(userWs)
                    wsToSearch.add("")
                    workspacesForQuery = wsToSearch
                } else {
                    workspacesForQuery = recsQuery.workspaces.filter {
                        it.isEmpty() || userWs.contains(it)
                    }
                    if (workspacesForQuery.isEmpty()) {
                        return RecsQueryRes()
                    }
                }
            }
            if (workspacesForQuery.isNotEmpty()) {
                val existingWsIds = dbWsService.getIdsForExistingWsInAnyOrder(workspacesForQuery)
                val wsCondition = if (workspacesForQuery.contains("")) {
                    Predicates.or(
                        Predicates.empty(DbEntity.WORKSPACE),
                        Predicates.inVals(DbEntity.WORKSPACE, existingWsIds)
                    )
                } else if (existingWsIds.isEmpty()) {
                    return RecsQueryRes()
                } else {
                    Predicates.inVals(DbEntity.WORKSPACE, existingWsIds)
                }
                val andPred = if (predicate is AndPredicate) {
                    predicate
                } else {
                    val andPred = AndPredicate()
                    andPred.addPredicate(predicate)
                    andPred
                }
                andPred.addPredicate(wsCondition)
                predicate = andPred
            }
        }

        val page = recsQuery.page

        val groupByForQuery = groupBy.map { queryCtx.registerSelectAtt(it, true) }
        val expressionsToQuery = queryCtx.expressionsCtx.getExpressions()

        val runAsAuth = AuthContext.getCurrentRunAsAuth()
        val userAuthorities = DbRecordsUtils.getCurrentAuthorities(runAsAuth).toMutableSet()
        val delegationsList: MutableList<Pair<Set<Long>, Set<String>>> = ArrayList()
        if (!isRunAsSystem && predicateData.queryPermsPolicy != QueryPermsPolicy.PUBLIC) {
            val delegations = daoCtx.delegationService.getActiveAuthDelegations(
                runAsAuth.getUser(),
                listOf(ecosTypeRef.getLocalId())
            )
            val delegationsForTypeIds: MutableList<Pair<Set<String>, Set<String>>> = ArrayList()
            val fullTypesIds = HashSet<String>()
            for (delegation in delegations) {
                if (delegation.delegatedTypes.isEmpty() ||
                    delegation.delegatedTypes.contains(ecosTypeRef.getLocalId())
                ) {
                    userAuthorities.addAll(delegation.delegatedAuthorities)
                } else {
                    fullTypesIds.addAll(delegation.delegatedTypes)
                    delegationsForTypeIds.add(delegation.delegatedTypes to delegation.delegatedAuthorities)
                }
            }
            val typeRefIdByTypeId: Map<EntityRef, Long> = daoCtx.recordRefService.getIdByEntityRefsMap(
                fullTypesIds.mapTo(ArrayList()) { ModelUtils.getTypeRef(it) }
            )
            for ((typesIds, authorities) in delegationsForTypeIds) {
                val typeLongIds: Set<Long> = typesIds.mapNotNullTo(HashSet()) {
                    typeRefIdByTypeId[ModelUtils.getTypeRef(it)]
                }
                if (typeLongIds.isEmpty()) {
                    continue
                }
                delegationsList.add(typeLongIds to authorities)
            }
        }

        val dbQuery = DbFindQuery.create {
            withPredicate(predicate)
            withSortBy(
                recsQuery.sortBy.mapNotNull { sortBy ->
                    val sortAtt = sortBy.attribute
                    if (groupBy.isNotEmpty() && !groupBy.contains(sortAtt) && !sortAtt.contains("(")) {
                        null
                    } else {
                        if (sortAtt == ATT_DBID) {
                            DbFindSort(DbEntity.ID, sortBy.ascending)
                        } else {
                            val newAtt = queryCtx.registerSelectAtt(sortBy.attribute, false)
                            if (newAtt.isNotEmpty()) {
                                DbFindSort(newAtt, sortBy.ascending)
                            } else {
                                null
                            }
                        }
                    }
                }
            )
            withGroupBy(groupByForQuery)
            withAssocSelectJoins(queryCtx.assocSelectJoins)
            withAssocTableJoins(predicateData.assocTableJoins)
            withAssocJoinWithPredicates(predicateData.assocJoinWithPredicates)
            withExpressions(expressionsToQuery)
            withUserAuthorities(userAuthorities)
            withDelegatedAuthorities(delegationsList)
        }
        val resultMaxItems = if (page.maxItems == -1) {
            config.queryMaxItems
        } else if (config.queryMaxItems == -1) {
            page.maxItems
        } else {
            min(page.maxItems, config.queryMaxItems)
        }

        return dataService.doWithPermsPolicy(predicateData.queryPermsPolicy) {
            executeFind(
                dbQuery,
                queryCtx,
                predicateData,
                groupBy,
                page.skipCount,
                resultMaxItems
            )
        }
    }

    private fun executeFind(
        dbQuery: DbFindQuery,
        queryCtx: DbFindQueryContext,
        predicateData: ProcessedPredicateData,
        groupBy: List<String>,
        skipCount: Int,
        maxItems: Int
    ): RecsQueryRes<DbRecord> {

        if (maxItems == 0) {
            val findRes = dataService.find(dbQuery, DbFindPage.ZERO, true)
            val queryRes = RecsQueryRes<DbRecord>()
            queryRes.setTotalCount(findRes.totalCount)
            queryRes.setRecords(emptyList())
            queryRes.setHasMore(findRes.totalCount > 0)
            return queryRes
        }

        var totalSkipCount = skipCount
        val records = ArrayList<DbRecord>()
        var hasMoreItems: Boolean
        var totalCount: Long

        val queryMaxItems = if (config.enableTotalCount) {
            maxItems
        } else {
            // Increment by 1 to detect if the table contains more records than the defined maximum limit
            maxItems + 1
        }
        val findRes = dataService.find(
            dbQuery,
            DbFindPage(totalSkipCount, queryMaxItems),
            config.enableTotalCount
        )
        hasMoreItems = if (findRes.entities.size < queryMaxItems) {
            false
        } else if (config.enableTotalCount) {
            findRes.totalCount > (findRes.entities.size + totalSkipCount)
        } else {
            findRes.entities.size == queryMaxItems
        }

        var entities = if (config.enableTotalCount || findRes.entities.size <= maxItems) {
            findRes.entities
        } else {
            // If we found more elements than maxItems, then we should drop last.
            findRes.entities.dropLast(1)
        }
        totalCount = findRes.totalCount
        var mappedEntities = queryCtx.mapResultEntitiesAtts(entities)
        if (predicateData.queryPermsPolicy == QueryPermsPolicy.PUBLIC ||
            AuthContext.isRunAsSystem() ||
            groupBy.isNotEmpty()
        ) {
            val isGroupEntities = groupBy.isNotEmpty()
            mappedEntities.mapTo(records) { DbRecord(daoCtx, it, queryCtx, isGroupEntities) }
        } else {
            var filteredCount = 0
            mappedEntities.forEach {
                val record = DbRecord(daoCtx, it, queryCtx)
                if (!record.isCurrentUserHasReadPerms()) {
                    filteredCount++
                    totalCount--
                } else {
                    records.add(record)
                }
            }
            if (filteredCount > 0) {
                var iterations = 3
                while (--iterations >= 0 && filteredCount > 0) {

                    val newMaxItems = max(filteredCount, 10)
                    totalSkipCount += entities.size

                    entities = dataService.find(
                        dbQuery,
                        DbFindPage(totalSkipCount, newMaxItems),
                        false
                    ).entities
                    if (entities.isEmpty()) {
                        break
                    }
                    mappedEntities = queryCtx.expressionsCtx.mapEntitiesAtts(entities)

                    for ((entityIdx, entity) in mappedEntities.withIndex()) {
                        val record = DbRecord(daoCtx, entity, queryCtx)
                        if (!record.isCurrentUserHasReadPerms()) {
                            totalCount--
                        } else {
                            records.add(record)
                            if (--filteredCount <= 0) {
                                if (!config.enableTotalCount) {
                                    if (entityIdx < mappedEntities.lastIndex) {
                                        hasMoreItems = true
                                    } else {
                                        totalSkipCount += entities.size
                                        hasMoreItems = dataService.find(
                                            dbQuery,
                                            DbFindPage(totalSkipCount, 1),
                                            false
                                        ).entities.isNotEmpty()
                                    }
                                }
                                break
                            }
                        }
                    }
                }
                if (!config.enableTotalCount && filteredCount > 0) {
                    hasMoreItems = false
                }
            }
            Unit
        }

        totalCount = if (config.enableTotalCount) {
            max(totalCount, records.size.toLong())
        } else if (hasMoreItems) {
            totalSkipCount + records.size + 1L
        } else {
            totalSkipCount + records.size.toLong()
        }

        val queryRes = RecsQueryRes<DbRecord>()
        queryRes.setTotalCount(totalCount)
        queryRes.setRecords(records)
        queryRes.setHasMore(hasMoreItems)

        return queryRes
    }

    private fun processPredicate(
        queryCtx: DbFindQueryContext,
        typeData: DbQueryTypeInfoData,
        predicate: Predicate
    ): ProcessedPredicateData {

        var typePredicateExists = false
        val assocTableJoins = ArrayList<AssocTableJoin>()
        val assocJoinWithPredicates = ArrayList<AssocJoinWithPredicate>()
        val assocsTableExists = assocsService.isAssocsTableExists()

        val typeAspects = typeData.getTypeAspects()
        val queryPermsPolicy = if (AuthContext.isRunAsSystem()) {
            QueryPermsPolicy.PUBLIC
        } else {
            typeData.getQueryPermsPolicy()
        }

        val predicateWithConvertedAssocs = replaceComplexTypePredicate(
            replaceAssocRefsToIdsAndMergeOrPredicates(predicate, typeData),
            queryCtx
        )

        val processedPredicate = DbRecordsQueryUtils.mapAttributePredicates(
            predicateWithConvertedAssocs,
            false
        ) { currentPred ->
            val attribute = currentPred.getAttribute()
            if (currentPred is ValuePredicate) {
                when (attribute) {
                    RecordConstants.ATT_TYPE -> {
                        typePredicateExists = true
                        processTypePredicate(currentPred, queryCtx)
                    }

                    RecordConstants.ATT_PARENT_ATT -> {
                        var value = currentPred.getValue()
                        value = if (value.isArray()) {
                            DataValue.of(value.map { daoCtx.assocsService.getIdForAtt(it.asText()) })
                        } else {
                            DataValue.of(daoCtx.assocsService.getIdForAtt(value.asText()))
                        }
                        ValuePredicate(
                            currentPred.getAttribute(),
                            currentPred.getType(),
                            value
                        )
                    }

                    DbRecord.ATT_ASPECTS -> {
                        processAspectsPredicate(currentPred, typeAspects)
                    }

                    else -> {

                        var attDef: AttributeDef? = typeData.getAttribute(attribute)

                        if (attDef == null) {
                            if (attribute.contains('.')) {
                                val assocJoinPred = processAssocJoinWithPredicates(
                                    attribute,
                                    typeData,
                                    queryCtx,
                                    currentPred,
                                    assocJoinWithPredicates
                                )
                                if (assocJoinPred != null) {
                                    return@mapAttributePredicates assocJoinPred
                                }
                            }
                        }
                        if (attDef == null) {
                            attDef = DbRecord.GLOBAL_ATTS[attribute]
                        }

                        var newPred: Predicate = if (DbRecord.ATTS_MAPPING.containsKey(attribute)) {
                            ValuePredicate(
                                DbRecord.ATTS_MAPPING[attribute],
                                currentPred.getType(),
                                currentPred.getValue()
                            )
                        } else {
                            currentPred
                        }

                        if (newPred is ValuePredicate && (newPred.getAttribute() == DbEntity.NAME || attDef?.type == AttributeType.MLTEXT)) {
                            // MLText fields stored as json text like '{"en":"value"}'
                            // and we should pre-process predicate in some cases
                            if (newPred.getType() == ValuePredicate.Type.EQ) {
                                // For equals predicate we should use '"value"'
                                // instead of 'value' to search and replace "EQ" to "CONTAINS"
                                newPred = ValuePredicate(
                                    newPred.getAttribute(),
                                    ValuePredicate.Type.CONTAINS,
                                    DataValue.createStr(newPred.getValue().toString())
                                )
                            } else if (newPred.getType() == ValuePredicate.Type.LIKE) {
                                newPred = ValuePredicate(
                                    newPred.getAttribute(),
                                    ValuePredicate.Type.LIKE,
                                    // Warning: getValue().toString() return escaped value in quotes
                                    // getValue().asText() return unescaped value without quotes
                                    // and should not be used here
                                    DataValue.createStr("%" + newPred.getValue() + "%")
                                )
                            }
                        }
                        if (newPred is ValuePredicate && DbRecordsUtils.isAssocLikeAttribute(attDef)) {
                            val assocAtt = newPred.getAttribute()
                            newPred = if (newPred.getValue().isNull()) {
                                EmptyPredicate(assocAtt)
                            } else {
                                val newAttribute = if (assocAtt != RecordConstants.ATT_PARENT && assocsTableExists) {
                                    val joinAtt = "$assocAtt-${queryCtx.assocJoinsCounter.incrementAndGet()}"
                                    assocTableJoins.add(
                                        AssocTableJoin(
                                            assocsService.getIdForAtt(assocAtt),
                                            joinAtt,
                                            assocAtt,
                                            true
                                        )
                                    )
                                    joinAtt
                                } else {
                                    newPred.getAttribute()
                                }
                                ValuePredicate(
                                    newAttribute,
                                    newPred.getType(),
                                    newPred.getValue()
                                )
                            }
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
                        } else if (newPred is ValuePredicate &&
                            attDef == null &&
                            newPred.getAttribute().contains("(")
                        ) {
                            newPred = ValuePredicate(
                                queryCtx.registerConditionAtt(newPred.getAttribute()),
                                newPred.getType(),
                                newPred.getValue()
                            )
                        }
                        newPred
                    }
                }
            } else if (currentPred is EmptyPredicate) {
                if (DbRecord.ATTS_MAPPING.containsKey(attribute)) {
                    EmptyPredicate(DbRecord.ATTS_MAPPING[attribute])
                } else {
                    if (attribute.contains('.')) {
                        processAssocJoinWithPredicates(
                            attribute,
                            typeData,
                            queryCtx,
                            currentPred,
                            assocJoinWithPredicates
                        ) ?: currentPred
                    } else {
                        currentPred
                    }
                }
            } else {
                log.error { "Unknown predicate type: ${currentPred::class}" }
                Predicates.alwaysFalse()
            }
        }

        return ProcessedPredicateData(
            processedPredicate,
            assocTableJoins,
            assocJoinWithPredicates,
            typePredicateExists,
            queryPermsPolicy
        )
    }

    private fun processAssocJoinWithPredicates(
        attribute: String,
        typeData: DbQueryTypeInfoData,
        queryCtx: DbFindQueryContext,
        predicate: AttributePredicate,
        assocJoinWithPredicates: MutableList<AssocJoinWithPredicate>
    ): ValuePredicate? {

        val srcAttName = attribute.substringBefore('.')
        val srcAttDef = typeData.getAttribute(srcAttName)
        val targetRecordsCtx = queryCtx.getAssocRecordsCtxToJoin(srcAttDef?.id)
        if (srcAttDef == null || targetRecordsCtx == null) {
            return null
        }
        val assocTypeId = queryCtx.getAssocTargetTypeId(srcAttDef.id)
        val innerPredicate: AttributePredicate = predicate.copy()
        innerPredicate.setAtt(attribute.substringAfter('.'))
        val innerQueryCtx = DbFindQueryContext(
            targetRecordsCtx,
            assocTypeId,
            false,
            queryCtx
        )
        innerQueryCtx.registerAssocsTypesHints(innerPredicate)
        val innerPredData = processPredicate(
            innerQueryCtx,
            innerQueryCtx.getMainTypeInfo(),
            innerPredicate
        )
        val assocJoinAttName = "$attribute-${queryCtx.assocJoinsCounter.incrementAndGet()}"
        val srcAttId = if (srcAttDef.multiple) {
            daoCtx.assocsService.getIdForAtt(srcAttDef.id)
        } else {
            -1L
        }
        assocJoinWithPredicates.add(
            AssocJoinWithPredicate(
                assocJoinAttName,
                srcAttName,
                srcAttId,
                srcAttDef.multiple,
                targetRecordsCtx.dataService.getTableContext(),
                innerPredData.predicate,
                innerPredData.assocTableJoins,
                innerPredData.assocJoinWithPredicates
            )
        )
        return ValuePredicate(
            assocJoinAttName,
            ValuePredicate.Type.EQ,
            ""
        )
    }

    private fun replaceComplexTypePredicate(predicate: Predicate, queryCtx: DbFindQueryContext): Predicate {

        val typePredicates = AndPredicate()
        val predicateWithoutComplexTypeConditions = PredicateUtils.mapValuePredicates(predicate, { pred ->
            val attribute = pred.getAttribute()
            if (attribute.startsWith(COMPLEX_TYPE_CONDITION_PREFIX)) {
                typePredicates.addPredicate(
                    ValuePredicate(
                        attribute.substring(COMPLEX_TYPE_CONDITION_PREFIX.length),
                        pred.getType(),
                        pred.getValue()
                    )
                )
                null
            } else {
                pred
            }
        }, onlyAnd = true, optimize = false)

        if (typePredicates.getPredicates().isEmpty()) {
            return predicate
        }

        val typeRefsInTable = queryCtx.getTypeRefsInTable()
        if (typeRefsInTable.isEmpty()) {
            return Predicates.alwaysFalse()
        }
        val typesRefs = queryTypes(typePredicates, queryCtx)
        if (typesRefs.isEmpty()) {
            return Predicates.alwaysFalse()
        }
        return Predicates.and(
            predicateWithoutComplexTypeConditions,
            Predicates.inVals(RecordConstants.ATT_TYPE, typesRefs.map { it.toString() })
        )
    }

    private fun replaceAssocRefsToIdsAndMergeOrPredicates(
        predicate: Predicate,
        typeData: DbQueryTypeInfoData
    ): Predicate {

        return DbRecordsQueryUtils.mapAttributePredicates(predicate, true) {
            val attId = it.getAttribute()
            val attDef = typeData.getAttribute(attId)
            var newPred = it
            if (attId != DbRecord.ATT_ASPECTS &&
                newPred is ValuePredicate &&
                (DbRecordsUtils.isAssocLikeAttribute(attDef) || PROPS_WITH_REFS.contains(attId))
            ) {
                newPred = ValuePredicate(
                    newPred.getAttribute(),
                    newPred.getType(),
                    replaceRefsToIds(newPred.getValue())
                )
            }
            newPred
        }
    }

    private fun replaceRefsToIds(value: DataValue): DataValue {
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

    private fun resolveTypeByNonId(
        value: DataValue,
        predType: ValuePredicate.Type,
        queryCtx: DbFindQueryContext
    ): Pair<DataValue, ValuePredicate.Type>? {

        if (!value.isTextual()) {
            return value to predType
        }

        val typeStr = value.asText()

        if (typeStr.startsWith(TYPE_REF_ID_PREFIX) ||
            ecosTypeService.getTypeInfo(typeStr) != null ||
            daoCtx.recordsService.getRecordsDao(TYPE_SRC_ID) == null
        ) {
            return value to predType
        }
        val typeRefsInTable = queryCtx.getTypeRefsInTable().map { it.getLocalId() }
        if (typeRefsInTable.isEmpty()) {
            return null
        }
        val typesRefs = queryTypes(ValuePredicate(TYPE_ATT_NAME, predType, typeStr), queryCtx)
        if (typesRefs.isEmpty()) {
            return null
        }
        return DataValue.create(typesRefs) to ValuePredicate.Type.EQ
    }

    private fun queryTypes(predicate: Predicate, queryCtx: DbFindQueryContext): List<EntityRef> {

        val typeRefsInTable = queryCtx.getTypeRefsInTable().map { it.getLocalId() }
        if (typeRefsInTable.isEmpty()) {
            return emptyList()
        }

        return daoCtx.recordsService.query(
            RecordsQuery.create()
                .withSourceId(TYPE_SRC_ID)
                .withQuery(
                    Predicates.and(
                        Predicates.inVals(TYPE_ATT_ID, typeRefsInTable),
                        predicate
                    )
                )
                .build()
        ).getRecords()
    }

    private fun processTypePredicate(predicate: ValuePredicate, queryCtx: DbFindQueryContext): Predicate {
        var value = predicate.getValue()
        var predType: ValuePredicate.Type = predicate.getType()
        when (predicate.getType()) {
            ValuePredicate.Type.EQ,
            ValuePredicate.Type.CONTAINS,
            ValuePredicate.Type.LIKE -> {
                val valueWithType = resolveTypeByNonId(value, predType, queryCtx)
                    ?: return Predicates.alwaysFalse()
                value = valueWithType.first
                predType = valueWithType.second
            }

            else -> {}
        }
        return when (predType) {
            ValuePredicate.Type.EQ,
            ValuePredicate.Type.CONTAINS,
            ValuePredicate.Type.IN -> {
                val expandedValue = mutableSetOf<String>()
                if (value.isArray()) {
                    value.forEach { typeRef ->
                        val predTypeId = typeRef.asText().toEntityRef().getLocalId()
                        expandedValue.add(predTypeId)
                        ecosTypeService.getAllChildrenIds(predTypeId, expandedValue)
                    }
                } else {
                    val predTypeId = EntityRef.valueOf(value.asText()).getLocalId()
                    expandedValue.add(predTypeId)
                    ecosTypeService.getAllChildrenIds(predTypeId, expandedValue)
                }
                val expandedValueIds = recordRefService.getIdByEntityRefs(
                    expandedValue.map {
                        ModelUtils.getTypeRef(it)
                    }
                )
                if (predType != ValuePredicate.Type.IN) {
                    if (expandedValueIds.size > 1) {
                        ValuePredicate(DbEntity.TYPE, ValuePredicate.Type.IN, expandedValueIds)
                    } else if (expandedValueIds.size == 1) {
                        ValuePredicate(DbEntity.TYPE, predType, expandedValueIds.first())
                    } else {
                        Predicates.alwaysTrue()
                    }
                } else {
                    ValuePredicate(DbEntity.TYPE, predType, expandedValueIds)
                }
            }

            else -> {
                Predicates.alwaysFalse()
            }
        }
    }

    private fun processAspectsPredicate(predicate: ValuePredicate, typeAspects: Set<EntityRef>): Predicate {
        val aspectsPredicate: Predicate = when (predicate.getType()) {
            ValuePredicate.Type.EQ,
            ValuePredicate.Type.CONTAINS -> {
                val value = predicate.getValue()
                if (value.isTextual()) {
                    if (typeAspects.contains(value.asText().toEntityRef())) {
                        Predicates.alwaysTrue()
                    } else {
                        predicate
                    }
                } else {
                    predicate
                }
            }

            ValuePredicate.Type.IN -> {
                val aspects = predicate.getValue().toList(EntityRef::class.java)
                if (aspects.any { typeAspects.contains(it) }) {
                    Predicates.alwaysTrue()
                } else {
                    predicate
                }
            }

            else -> predicate
        }
        return if (aspectsPredicate is ValuePredicate) {
            ValuePredicate(
                aspectsPredicate.getAttribute(),
                aspectsPredicate.getType(),
                replaceRefsToIds(aspectsPredicate.getValue())
            )
        } else {
            aspectsPredicate
        }
    }

    private class ProcessedPredicateData(
        val predicate: Predicate,
        val assocTableJoins: List<AssocTableJoin>,
        val assocJoinWithPredicates: List<AssocJoinWithPredicate>,
        val typePredicateExists: Boolean,
        val queryPermsPolicy: QueryPermsPolicy,
    )
}
