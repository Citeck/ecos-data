package ru.citeck.ecos.data.sql.records.dao.query

import ecos.com.fasterxml.jackson210.databind.node.JsonNodeType
import mu.KotlinLogging
import ru.citeck.ecos.commons.data.DataValue
import ru.citeck.ecos.context.lib.auth.AuthContext
import ru.citeck.ecos.data.sql.context.DbTableContext
import ru.citeck.ecos.data.sql.records.DbRecordsDao
import ru.citeck.ecos.data.sql.records.DbRecordsUtils
import ru.citeck.ecos.data.sql.records.dao.DbRecordsDaoCtx
import ru.citeck.ecos.data.sql.records.dao.atts.DbExpressionAttsContext
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
import ru.citeck.ecos.model.lib.type.dto.TypeInfo
import ru.citeck.ecos.model.lib.utils.ModelUtils
import ru.citeck.ecos.records2.RecordConstants
import ru.citeck.ecos.records2.predicate.PredicateService
import ru.citeck.ecos.records2.predicate.PredicateUtils
import ru.citeck.ecos.records2.predicate.model.*
import ru.citeck.ecos.records3.record.atts.schema.resolver.AttContext
import ru.citeck.ecos.records3.record.dao.query.dto.query.RecordsQuery
import ru.citeck.ecos.records3.record.dao.query.dto.res.RecsQueryRes
import ru.citeck.ecos.webapp.api.entity.EntityRef
import ru.citeck.ecos.webapp.api.entity.toEntityRef
import java.util.*
import kotlin.collections.ArrayList
import kotlin.collections.HashMap
import kotlin.collections.HashSet
import kotlin.collections.LinkedHashMap
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
        val typeInfoData = getTypeInfoData(ecosTypeRef.getLocalId())

        val assocsTypes = extractAssocsTypes(typeInfoData, originalPredicate)

        val expressionsCtx = DbExpressionAttsContext(recsQuery.groupBy.isNotEmpty())

        AttContext.getCurrent()?.getSchemaAtt()?.inner?.forEach {
            if (it.name.contains("(")) {
                expressionsCtx.register(it.name)
            }
        }

        val predicateData = processPredicate(typeInfoData, originalPredicate, assocsTypes) {
            expressionsCtx.register(it)
        }

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

        val assocTargetTableContextsByAttribute = HashMap<String, Optional<DbTableContext>>()
        val assocSelectJoins = HashMap<String, DbTableContext>()

        fun prepareAssocSelectJoin(att: String, strict: Boolean): String? {
            val dotIdx = att.indexOf('.')
            if (dotIdx == -1) {
                return DbRecord.ATTS_MAPPING.getOrDefault(att, att)
            }
            fun printError(msg: () -> String) {
                if (strict) {
                    error(msg())
                } else {
                    log.debug(msg)
                }
            }

            val srcAttName = att.substring(0, dotIdx)
            val targetAttName = att.substring(dotIdx + 1)
            if (targetAttName.contains('.')) {
                printError { "Assoc select can't be executed for multiple joins. Attribute: '$att'" }
                return null
            }
            var tableCtxOpt: Optional<DbTableContext>? = assocTargetTableContextsByAttribute[srcAttName]
            if (tableCtxOpt == null) {
                val attDef = typeInfoData.attributesById[srcAttName]
                    ?: DbRecord.GLOBAL_ATTS[srcAttName]
                    ?: error("Unknown attribute: '$srcAttName'")
                if (!DbRecordsUtils.isAssocLikeAttribute(attDef)) {
                    printError {
                        "Assoc select can't be executed for non-assoc like attributes. " +
                            "Attribute: '$srcAttName' Type: ${attDef.type}"
                    }
                    return null
                }
                tableCtxOpt = Optional.ofNullable(getAssocTableCtxToJoin(attDef.id, assocsTypes))
                assocTargetTableContextsByAttribute[srcAttName] = tableCtxOpt
            }
            if (!tableCtxOpt.isPresent) {
                printError { "Assoc select can't be executed for attribute: '$srcAttName'" }
                return null
            }
            val tableCtx: DbTableContext = tableCtxOpt.get()

            val mappedTargetColumnName = DbRecord.ATTS_MAPPING.getOrDefault(targetAttName, targetAttName)
            if (!tableCtx.hasColumn(mappedTargetColumnName)) {
                printError {
                    "Assoc select can't be executed for nonexistent inner " +
                        "attribute '$targetAttName'. Full attribute: '$att'"
                }
                return null
            }
            val mappedSrcAtt = DbRecord.ATTS_MAPPING.getOrDefault(srcAttName, srcAttName)
            assocSelectJoins[mappedSrcAtt] = tableCtx
            return "$mappedSrcAtt.$mappedTargetColumnName"
        }

        val findRes = dataService.doWithPermsPolicy(predicateData.queryPermsPolicy) {

            val dbQuery = DbFindQuery.create {
                withPredicate(predicate)
                withSortBy(
                    recsQuery.sortBy.mapNotNull { sortBy ->
                        val att = sortBy.attribute
                        if (att.contains('(')) {
                            DbFindSort(expressionsCtx.register(att), sortBy.ascending)
                        } else if (!att.contains(".")) {
                            DbFindSort(DbRecord.ATTS_MAPPING.getOrDefault(att, att), sortBy.ascending)
                        } else {
                            prepareAssocSelectJoin(att, false)?.let { DbFindSort(it, sortBy.ascending) }
                        }
                    }
                )
                withGroupBy(
                    recsQuery.groupBy.mapNotNull { groupBy ->
                        if (groupBy.contains('(')) {
                            expressionsCtx.register(groupBy)
                        } else if (!groupBy.contains('.')) {
                            DbRecord.ATTS_MAPPING.getOrDefault(groupBy, groupBy)
                        } else {
                            prepareAssocSelectJoin(groupBy, true)
                        }
                    }
                )
                withAssocSelectJoins(assocSelectJoins)
                withAssocTableJoins(predicateData.assocTableJoins)
                withAssocJoinWithPredicates(predicateData.assocJoinWithPredicates)
                withExpressions(expressionsCtx.getExpressions())
            }

            dataService.find(
                dbQuery,
                DbFindPage(
                    page.skipCount,
                    if (page.maxItems == -1) {
                        config.queryMaxItems
                    } else {
                        min(page.maxItems, config.queryMaxItems)
                    }
                ),
                true
            )
        }

        val entities = expressionsCtx.mapEntitiesAtts(findRes.entities)

        val queryRes = RecsQueryRes<DbRecord>()
        queryRes.setTotalCount(findRes.totalCount)
        queryRes.setRecords(entities.map { DbRecord(daoCtx, it, assocsTypes) })
        queryRes.setHasMore(findRes.totalCount > findRes.entities.size + page.skipCount)

        return queryRes
    }

    private fun extractAssocsTypes(typeData: TypeInfoData, predicate: Predicate): Map<String, EntityRef> {

        val typesByAssocId = LinkedHashMap<String, EntityRef>()
        typeData.attributesById.values.forEach {
            if (DbRecordsUtils.isAssocLikeAttribute(it)) {
                val typeRef = it.config["typeRef"].asText()
                if (typeRef.isNotBlank()) {
                    typesByAssocId[it.id] = typeRef.toEntityRef()
                }
            }
        }

        val assocIds = typeData.attributesById.values.filter {
            DbRecordsUtils.isAssocLikeAttribute(it)
        }.mapTo(HashSet()) { it.id }
        assocIds.add(RecordConstants.ATT_PARENT)

        fun tryToRegisterAssocTypeHint(att: String, value: DataValue) {
            if (!value.isTextual()) {
                return
            }
            val dotIdx = att.indexOf('.')
            if (dotIdx == -1) {
                return
            }
            val assocAtt = att.substring(0, dotIdx)
            if (!assocIds.contains(assocAtt)) {
                return
            }
            val innerAtt = att.substring(dotIdx + 1)
            if (innerAtt.startsWith(RecordConstants.ATT_TYPE) && (
                innerAtt.length == RecordConstants.ATT_TYPE.length ||
                    innerAtt[RecordConstants.ATT_TYPE.length] == '?'
                )
            ) {
                val strValue = value.asText()
                if (strValue.isBlank()) {
                    return
                }
                typesByAssocId[assocAtt] = if (strValue.contains('/')) {
                    EntityRef.valueOf(strValue)
                } else {
                    ModelUtils.getTypeRef(strValue)
                }
            }
        }
        mapAttributePredicates(predicate, tryToMergeOrPredicates = false, onlyAnd = true) {
            if (it is ValuePredicate) {
                val type = it.getType()
                if (type == ValuePredicate.Type.EQ || type == ValuePredicate.Type.CONTAINS) {
                    tryToRegisterAssocTypeHint(it.getAttribute(), it.getValue())
                }
            }
            it
        }
        return typesByAssocId
    }

    private fun getTypeInfoData(typeId: String): TypeInfoData {

        val attributesById: MutableMap<String, AttributeDef>
        val typeAspects: Set<EntityRef>
        var queryPermsPolicy = QueryPermsPolicy.OWN
        var typeInfo: TypeInfo? = null

        if (typeId.isNotEmpty()) {
            typeInfo = ecosTypeService.getTypeInfo(typeId)
            attributesById = LinkedHashMap(typeInfo?.model?.getAllAttributes()?.associateBy { it.id } ?: emptyMap())
            typeAspects = typeInfo?.aspects?.map { it.ref }?.toSet() ?: emptySet()
            queryPermsPolicy = typeInfo?.queryPermsPolicy ?: queryPermsPolicy
        } else {
            attributesById = LinkedHashMap()
            typeAspects = emptySet()
        }
        DbRecord.GLOBAL_ATTS.values.forEach {
            if (!attributesById.containsKey(it.id)) {
                attributesById[it.id] = it
            }
        }
        return TypeInfoData(typeInfo, typeAspects, attributesById, queryPermsPolicy)
    }

    private fun processPredicate(
        typeData: TypeInfoData,
        predicate: Predicate,
        assocsType: Map<String, EntityRef>,
        registerExpression: (String) -> String
    ): ProcessedPredicateData {

        var typePredicateExists = false
        val assocTableJoins = ArrayList<AssocTableJoin>()
        val assocJoinWithPredicates = ArrayList<AssocJoinWithPredicate>()
        var assocJoinsCounter = 0
        val assocsTableExists = assocsService.isAssocsTableExists()

        val attributesById = typeData.attributesById
        val typeAspects = typeData.typeAspects
        val queryPermsPolicy = if (AuthContext.isRunAsSystem()) {
            QueryPermsPolicy.PUBLIC
        } else {
            typeData.queryPermsPolicy
        }

        val predicateWithConvertedAssocs = replaceAssocRefsToIds(predicate, attributesById)

        val processedPredicate = mapAttributePredicates(
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

                        var attDef: AttributeDef? = attributesById[attribute]

                        if (attDef == null) {
                            if (attribute.contains('.')) {
                                val srcAttName = attribute.substringBefore('.')
                                val srcAttDef = attributesById[srcAttName]
                                val targetTableCtx = getAssocTableCtxToJoin(srcAttDef?.id, assocsType)
                                if (srcAttDef != null && targetTableCtx != null) {
                                    val assocTypeId = assocsType[srcAttDef.id]?.getLocalId() ?: ""
                                    val innerPredicate = ValuePredicate(
                                        attribute.substringAfter('.'),
                                        currentPred.getType(),
                                        currentPred.getValue()
                                    )
                                    val innerTypeInfoData = getTypeInfoData(assocTypeId)
                                    val innerPredData = processPredicate(
                                        innerTypeInfoData,
                                        innerPredicate,
                                        extractAssocsTypes(innerTypeInfoData, innerPredicate)
                                    ) { it }
                                    val assocJoinAttName = "$attribute-${assocJoinsCounter++}"
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
                                            targetTableCtx,
                                            innerPredData.predicate,
                                            innerPredData.assocTableJoins,
                                            innerPredData.assocJoinWithPredicates
                                        )
                                    )
                                    return@mapAttributePredicates ValuePredicate(
                                        assocJoinAttName,
                                        ValuePredicate.Type.EQ,
                                        ""
                                    )
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
                                    val joinAtt = "$assocAtt-${assocJoinsCounter++}"
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
                                registerExpression(newPred.getAttribute()),
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
                    currentPred
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

    private fun replaceAssocRefsToIds(predicate: Predicate, attsById: Map<String, AttributeDef>): Predicate {

        return mapAttributePredicates(predicate, true) {
            val attId = it.getAttribute()
            val attDef = attsById[attId] ?: DbRecord.GLOBAL_ATTS[attId]
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

    private fun mapAttributePredicates(
        predicate: Predicate,
        tryToMergeOrPredicates: Boolean,
        mapFunc: (AttributePredicate) -> Predicate
    ): Predicate {
        return mapAttributePredicates(predicate, tryToMergeOrPredicates, false, mapFunc)
    }

    private fun mapAttributePredicates(
        predicate: Predicate,
        tryToMergeOrPredicates: Boolean,
        onlyAnd: Boolean,
        mapFunc: (AttributePredicate) -> Predicate
    ): Predicate {
        return if (predicate is AttributePredicate) {
            mapFunc(predicate)
        } else if (predicate is ComposedPredicate) {
            val isAnd = predicate is AndPredicate
            if (onlyAnd && !isAnd) {
                return Predicates.alwaysFalse()
            }
            val mappedPredicates: MutableList<Predicate> = java.util.ArrayList()
            for (pred in predicate.getPredicates()) {
                val mappedPred = mapAttributePredicates(pred, tryToMergeOrPredicates, onlyAnd, mapFunc)
                if (PredicateUtils.isAlwaysTrue(mappedPred)) {
                    if (isAnd) {
                        continue
                    } else {
                        return mappedPred
                    }
                } else if (PredicateUtils.isAlwaysFalse(mappedPred)) {
                    if (isAnd) {
                        return mappedPred
                    } else {
                        continue
                    }
                }
                mappedPredicates.add(mappedPred)
            }
            if (mappedPredicates.isEmpty()) {
                if (isAnd) {
                    Predicates.alwaysTrue()
                } else {
                    Predicates.alwaysFalse()
                }
            } else if (mappedPredicates.size == 1) {
                mappedPredicates[0]
            } else {
                if (isAnd) {
                    Predicates.and(mappedPredicates)
                } else {
                    if (tryToMergeOrPredicates) {
                        joinOrPredicateElements(mappedPredicates)
                    } else {
                        Predicates.or(mappedPredicates)
                    }
                }
            }
        } else if (predicate is NotPredicate) {
            val mapped = mapAttributePredicates(predicate.getPredicate(), tryToMergeOrPredicates, false, mapFunc)
            if (mapped is NotPredicate) {
                mapped.getPredicate()
            } else {
                Predicates.not(mapped)
            }
        } else {
            predicate
        }
    }

    private fun joinOrPredicateElements(predicates: List<Predicate>): Predicate {

        if (predicates.isEmpty()) {
            return Predicates.alwaysFalse()
        } else if (predicates.size == 1) {
            return predicates[0]
        }

        fun getValueElementType(value: DataValue): JsonNodeType {
            return if (value.isArray()) {
                if (value.isEmpty()) {
                    JsonNodeType.NULL
                } else {
                    value[0].value.nodeType
                }
            } else {
                value.value.nodeType
            }
        }

        val firstPred = predicates.first()

        if (firstPred !is ValuePredicate) {
            return Predicates.or(predicates)
        }
        val predType = firstPred.getType()
        if (predType != ValuePredicate.Type.EQ &&
            predType != ValuePredicate.Type.CONTAINS &&
            predType != ValuePredicate.Type.IN
        ) {
            return Predicates.or(predicates)
        }

        val valueElemType = getValueElementType(firstPred.getValue())
        if (valueElemType != JsonNodeType.NUMBER) {
            return Predicates.or(predicates)
        }

        val attribute = firstPred.getAttribute()

        for (i in 1 until predicates.size) {
            val predToTest = predicates[i]
            if (predToTest !is ValuePredicate ||
                predToTest.getAttribute() != attribute ||
                getValueElementType(predToTest.getValue()) != valueElemType
            ) {

                return Predicates.or(predicates)
            }
        }

        fun extractValues(resultList: DataValue, value: DataValue) {
            if (value.isNumber() || value.isTextual()) {
                resultList.add(value)
            } else if (value.isArray()) {
                value.forEach { extractValues(resultList, it) }
            }
        }

        val resultList = DataValue.createArr()
        for (predicate in predicates) {
            if (predicate is ValuePredicate) {
                extractValues(resultList, predicate.getValue())
            }
        }
        if (resultList.isEmpty()) {
            return Predicates.alwaysFalse()
        }
        return ValuePredicate(attribute, ValuePredicate.Type.IN, resultList)
    }

    private fun getAssocTableCtxToJoin(assocAttId: String?, assocsType: Map<String, EntityRef>): DbTableContext? {

        val targetTypeId = assocsType[assocAttId ?: ""]?.getLocalId() ?: ""
        if (targetTypeId.isBlank()) {
            return null
        }

        val targetTypeInfo = ecosTypeService.getTypeInfo(targetTypeId)
        val sourceId = targetTypeInfo?.sourceId ?: ""
        if (sourceId.isBlank()) {
            return null
        }
        val targetDbDao = daoCtx.recordsService.getRecordsDao(sourceId, DbRecordsDao::class.java) ?: return null
        val targetTableCtx = targetDbDao.getRecordsDaoCtx().dataService.getTableContext()

        if (!targetTableCtx.isSameSchema(daoCtx.dataService.getTableContext())) {
            return null
        }
        return targetTableCtx
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

    private class TypeInfoData(
        val typeInfo: TypeInfo?,
        val typeAspects: Set<EntityRef>,
        val attributesById: Map<String, AttributeDef>,
        val queryPermsPolicy: QueryPermsPolicy
    )
}
