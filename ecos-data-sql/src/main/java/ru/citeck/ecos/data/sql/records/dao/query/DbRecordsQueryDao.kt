package ru.citeck.ecos.data.sql.records.dao.query

import mu.KotlinLogging
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
import ru.citeck.ecos.data.sql.service.expression.token.*
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType
import ru.citeck.ecos.model.lib.type.dto.QueryPermsPolicy
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

class DbRecordsQueryDao(var daoCtx: DbRecordsDaoCtx) {

    companion object {
        private val log = KotlinLogging.logger {}
    }

    private val config = daoCtx.config
    private val ecosTypeService = daoCtx.ecosTypeService
    private val recordRefService = daoCtx.recordRefService
    private val assocsService = daoCtx.assocsService
    private val dataService = daoCtx.dataService

    fun queryRecords(recsQuery: RecordsQuery): RecsQueryRes<DbRecord> {

        val language = recsQuery.language
        if (language.isNotEmpty() && language != PredicateService.LANGUAGE_PREDICATE) {
            return RecsQueryRes()
        }

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

        val page = recsQuery.page

        val groupByForQuery = groupBy.map { queryCtx.registerSelectAtt(it, true) }
        val expressionsToQuery = if (groupByForQuery.isEmpty()) {
            queryCtx.expressionsCtx.getExpressions()
        } else {
            val expressions = queryCtx.expressionsCtx.getExpressions()
            val groupByExpressions = groupByForQuery.mapTo(HashSet()) {
                expressions[it] ?: ColumnToken(it)
            }
            expressions.filter {
                if (groupByForQuery.contains(it.key)) {
                    true
                } else {
                    isValidExpressionForQueryWithGrouping(groupByExpressions, it.value)
                }
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
                        val newAtt = queryCtx.registerSelectAtt(sortBy.attribute, false)
                        if (newAtt.isNotEmpty()) {
                            DbFindSort(newAtt, sortBy.ascending)
                        } else {
                            null
                        }
                    }
                }
            )
            withGroupBy(groupByForQuery)
            withAssocSelectJoins(queryCtx.assocSelectJoins)
            withAssocTableJoins(predicateData.assocTableJoins)
            withAssocJoinWithPredicates(predicateData.assocJoinWithPredicates)
            withExpressions(expressionsToQuery)
        }
        val resultMaxItems = if (page.maxItems == -1) {
            config.queryMaxItems
        } else {
            min(page.maxItems, config.queryMaxItems)
        }
        var skipCount = page.skipCount

        var totalCount: Long = 0
        val records = ArrayList<DbRecord>()

        dataService.doWithPermsPolicy(predicateData.queryPermsPolicy) {
            var findRes = dataService.find(
                dbQuery,
                DbFindPage(skipCount, resultMaxItems),
                true
            )
            totalCount = findRes.totalCount
            var mappedEntities = queryCtx.expressionsCtx.mapEntitiesAtts(findRes.entities)
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
                var iterations = 3
                while (--iterations >= 0 && filteredCount > 0) {

                    val allowedElements = findRes.totalCount - findRes.entities.size - skipCount
                    if (allowedElements <= 0) {
                        break
                    }

                    val newMaxItems = min(allowedElements, max(filteredCount, 10).toLong()).toInt()
                    skipCount += findRes.entities.size

                    findRes = dataService.find(dbQuery, DbFindPage(skipCount, newMaxItems), true)
                    mappedEntities = queryCtx.expressionsCtx.mapEntitiesAtts(findRes.entities)

                    for (entity in mappedEntities) {
                        val record = DbRecord(daoCtx, entity, queryCtx)
                        if (!record.isCurrentUserHasReadPerms()) {
                            totalCount--
                        } else {
                            records.add(record)
                            if (--filteredCount <= 0) {
                                break
                            }
                        }
                    }
                }
            }
        }

        val queryRes = RecsQueryRes<DbRecord>()
        queryRes.setTotalCount(max(totalCount, records.size.toLong()))
        queryRes.setRecords(records)
        queryRes.setHasMore(totalCount > records.size + skipCount)

        return queryRes
    }

    private fun isValidExpressionForQueryWithGrouping(
        groupBy: Set<ExpressionToken>,
        expression: ExpressionToken?
    ): Boolean {
        expression ?: return true
        if (groupBy.contains(expression)) {
            return true
        }
        return if (expression is ColumnToken) {
            groupBy.contains(expression)
        } else if (expression is FunctionToken) {
            if (expression.isAggregationFunc()) {
                true
            } else {
                expression.args.all {
                    isValidExpressionForQueryWithGrouping(groupBy, it)
                }
            }
        } else if (expression is GroupToken) {
            expression.tokens.all {
                isValidExpressionForQueryWithGrouping(groupBy, it)
            }
        } else if (expression is CaseToken) {
            expression.branches.all {
                isValidExpressionForQueryWithGrouping(groupBy, it.condition) &&
                    isValidExpressionForQueryWithGrouping(groupBy, it.thenResult)
            } && isValidExpressionForQueryWithGrouping(groupBy, expression.orElse)
        } else {
            true
        }
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

        val predicateWithConvertedAssocs = replaceAssocRefsToIds(predicate, typeData)

        val processedPredicate = DbRecordsQueryUtils.mapAttributePredicates(
            predicateWithConvertedAssocs,
            false
        ) { currentPred ->
            val attribute = currentPred.getAttribute()
            if (currentPred is ValuePredicate) {
                when (attribute) {
                    RecordConstants.ATT_TYPE -> {
                        typePredicateExists = true
                        processTypePredicate(currentPred)
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

    private fun replaceAssocRefsToIds(predicate: Predicate, typeData: DbQueryTypeInfoData): Predicate {

        return DbRecordsQueryUtils.mapAttributePredicates(predicate, true) {
            val attId = it.getAttribute()
            val attDef = typeData.getAttribute(attId)
            var newPred = it
            if (attId != DbRecord.ATT_ASPECTS &&
                newPred is ValuePredicate && DbRecordsUtils.isAssocLikeAttribute(attDef)
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

    private fun processTypePredicate(predicate: ValuePredicate): Predicate {
        val value = predicate.getValue()
        return when (predicate.getType()) {
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
                if (predicate.getType() != ValuePredicate.Type.IN) {
                    if (expandedValueIds.size > 1) {
                        ValuePredicate(DbEntity.TYPE, ValuePredicate.Type.IN, expandedValueIds)
                    } else if (expandedValueIds.size == 1) {
                        ValuePredicate(DbEntity.TYPE, predicate.getType(), expandedValueIds.first())
                    } else {
                        Predicates.alwaysTrue()
                    }
                } else {
                    ValuePredicate(DbEntity.TYPE, predicate.getType(), expandedValueIds)
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
