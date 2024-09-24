package ru.citeck.ecos.data.sql.records.dao.query

import io.github.oshai.kotlinlogging.KotlinLogging
import ru.citeck.ecos.commons.data.DataValue
import ru.citeck.ecos.data.sql.context.DbTableContext
import ru.citeck.ecos.data.sql.ecostype.DbEcosModelService
import ru.citeck.ecos.data.sql.records.DbRecordsDao
import ru.citeck.ecos.data.sql.records.DbRecordsUtils
import ru.citeck.ecos.data.sql.records.dao.DbRecordsDaoCtx
import ru.citeck.ecos.data.sql.records.dao.atts.DbExpressionAttsContext
import ru.citeck.ecos.data.sql.records.dao.atts.DbRecord
import ru.citeck.ecos.data.sql.repo.entity.DbEntity
import ru.citeck.ecos.data.sql.repo.find.DbFindPage
import ru.citeck.ecos.data.sql.repo.find.DbFindQuery
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.type.dto.QueryPermsPolicy
import ru.citeck.ecos.model.lib.type.dto.TypeInfo
import ru.citeck.ecos.records2.RecordConstants
import ru.citeck.ecos.records2.predicate.model.Predicate
import ru.citeck.ecos.records2.predicate.model.ValuePredicate
import ru.citeck.ecos.records3.record.dao.impl.proxy.RecordsDaoProxy
import ru.citeck.ecos.webapp.api.entity.EntityRef
import ru.citeck.ecos.webapp.api.entity.toEntityRef
import java.util.*
import java.util.concurrent.atomic.AtomicInteger
import kotlin.collections.HashMap
import kotlin.collections.HashSet
import kotlin.collections.LinkedHashMap
import kotlin.reflect.KClass

class DbFindQueryContext(
    private val ctx: DbRecordsDaoCtx,
    typeId: String,
    withGrouping: Boolean,
    private val parent: DbFindQueryContext?
) {

    companion object {
        private val log = KotlinLogging.logger {}
    }

    private val typeService = ctx.ecosTypeService
    private val currentAppSrcIdPrefix = ctx.appName + EntityRef.APP_NAME_DELIMITER

    private val typeInfoData = HashMap<String, DbQueryTypeInfoData>()
    private val mainTypeInfo = getTypeInfoData(typeId)

    val assocRecordsCtxByAttribute = HashMap<String, Optional<DbRecordsDaoCtx>>()
    val assocSelectJoins = HashMap<String, DbTableContext>()
    val assocSelectJoinsInvMapping = HashMap<String, String>()
    val assocSelectJoinsValueTypes = HashMap<String, KClass<*>>()
    val expressionsCtx = DbExpressionAttsContext(this, withGrouping)

    val assocJoinsCounter = AtomicInteger(0)

    private val assocsTypes: MutableMap<String, String> = HashMap()

    private var typeRefsInTable: List<EntityRef>? = null

    init {
        mainTypeInfo.getAttributesById().values.forEach {
            if (DbRecordsUtils.isAssocLikeAttribute(it)) {
                val typeRef = it.config["typeRef"].asText()
                if (typeRef.isNotBlank()) {
                    assocsTypes[it.id] = typeRef.toEntityRef().getLocalId()
                }
            }
        }
    }

    fun getTypeRefsInTable(): List<EntityRef> {
        var typeRefs = typeRefsInTable
        if (typeRefs == null) {
            val typesInTable = ctx.dataService.find(
                DbFindQuery.create()
                    .withGroupBy(listOf(DbEntity.TYPE))
                    .build(),
                DbFindPage.ALL,
                false
            ).entities.map { it.type }

            typeRefs = if (typesInTable.isEmpty()) {
                emptyList()
            } else {
                ctx.recordRefService.getEntityRefsByIds(typesInTable)
            }
            typeRefsInTable = typeRefs
        }
        return typeRefs
    }

    fun registerSelectAtt(attribute: String, strict: Boolean): String {
        if (attribute.contains('(')) {
            return expressionsCtx.register(attribute, strict) ?: ""
        }
        if (attribute.contains('.')) {
            return prepareAssocSelectJoin(attribute, strict) ?: ""
        }
        return DbRecord.ATTS_MAPPING.getOrDefault(attribute, attribute)
    }

    fun registerConditionAtt(attribute: String): String {
        if (attribute.contains('(')) {
            return expressionsCtx.register(attribute, true) ?: ""
        }
        return DbRecord.ATTS_MAPPING.getOrDefault(attribute, attribute)
    }

    fun mapResultEntitiesAtts(entities: List<DbEntity>): List<DbEntity> {
        var result = expressionsCtx.mapEntitiesAtts(entities)
        if (assocSelectJoinsValueTypes.isNotEmpty() || assocSelectJoinsInvMapping.isNotEmpty()) {
            result = result.map {
                val mappedEntity = it.copy()
                assocSelectJoinsValueTypes.forEach { (k, v) ->
                    mappedEntity.attributes[k] = ctx.typesConverter.convert(mappedEntity.attributes[k], v)
                }
                assocSelectJoinsInvMapping.forEach { (k, v) ->
                    mappedEntity.attributes[v] = mappedEntity.attributes.remove(k)
                }
                mappedEntity
            }
        }
        return result
    }

    fun registerAssocsTypesHints(predicate: Predicate) {

        val assocIds = mainTypeInfo.getAttributesById().values.mapTo(HashSet()) { it.id }
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
                assocsTypes[assocAtt] = if (strValue.contains('/')) {
                    EntityRef.valueOf(strValue).getLocalId()
                } else {
                    strValue
                }
            }
        }
        DbRecordsQueryUtils.mapAttributePredicates(predicate, tryToMergeOrPredicates = false, onlyAnd = true) {
            if (it is ValuePredicate) {
                val type = it.getType()
                if (type == ValuePredicate.Type.EQ || type == ValuePredicate.Type.CONTAINS) {
                    tryToRegisterAssocTypeHint(it.getAttribute(), it.getValue())
                }
            }
            it
        }
    }

    fun getAssocRecordsCtxToJoin(assocAttId: String?): DbRecordsDaoCtx? {
        if (assocAttId.isNullOrBlank()) {
            return null
        }
        return assocRecordsCtxByAttribute.computeIfAbsent(assocAttId) { key ->
            Optional.ofNullable(evalAssocRecordsCtxToJoin(key))
        }.orElse(null)
    }

    private fun evalAssocRecordsCtxToJoin(assocAttId: String): DbRecordsDaoCtx? {

        val targetTypeId = getAssocTargetTypeId(assocAttId)
        if (targetTypeId.isBlank()) {
            return null
        }
        val targetTypeInfo = ctx.ecosTypeService.getTypeInfo(targetTypeId)
        val sourceId = targetTypeInfo?.sourceId ?: ""
        if (sourceId.isBlank()) {
            return null
        }
        var targetDbDao = ctx.recordsService.getRecordsDao(sourceId) ?: return null
        var iterations = 3
        while (targetDbDao is RecordsDaoProxy && iterations-- > 0) {
            var targetId = targetDbDao.getTargetId()
            if (targetId.contains(EntityRef.APP_NAME_DELIMITER)) {
                if (!targetId.startsWith(currentAppSrcIdPrefix)) {
                    return null
                } else {
                    targetId = targetId.substring(0, currentAppSrcIdPrefix.length)
                }
            }
            targetDbDao = ctx.recordsService.getRecordsDao(targetId) ?: return null
        }
        if (targetDbDao !is DbRecordsDao) {
            return null
        }
        val recordsDaoCtx = targetDbDao.getRecordsDaoCtx()

        if (!recordsDaoCtx.dataService.getTableContext().isSameSchema(ctx.dataService.getTableContext())) {
            return null
        }
        return recordsDaoCtx
    }

    fun prepareAssocSelectJoin(att: String, strict: Boolean): String? {
        val dotIdx = att.indexOf('.')
        if (dotIdx == -1) {
            return DbRecord.ATTS_MAPPING.getOrDefault(att, att)
        }
        fun printError(msg: () -> String) {
            if (strict) {
                throw DbQueryPreparingException(msg())
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
        val recordsCtx = getAssocRecordsCtxToJoin(srcAttName)
        if (recordsCtx == null) {
            printError { "Assoc select can't be executed for attribute: '$srcAttName'" }
            return null
        }
        val tableCtx: DbTableContext = recordsCtx.dataService.getTableContext()

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

        val result = "$mappedSrcAtt.$mappedTargetColumnName"
        if (result != att) {
            assocSelectJoinsInvMapping[result] = att
        }
        val valueType = tableCtx.getEntityValueTypeForColumn(mappedTargetColumnName)
        if (valueType != Any::class) {
            assocSelectJoinsValueTypes[result] = valueType
        }

        return result
    }

    fun getTypeInfoData(typeId: String): DbQueryTypeInfoData {
        if (parent != null) {
            return parent.getTypeInfoData(typeId)
        }
        return typeInfoData.computeIfAbsent(typeId) {
            DbQueryTypeInfoDataImpl(typeId, typeService)
        }
    }

    fun getTableContext(): DbTableContext {
        return ctx.dataService.getTableContext()
    }

    fun getAssocsTypes(): Map<String, String> {
        return assocsTypes
    }

    fun getAssocTargetTypeId(assocId: String?): String {
        if (assocId.isNullOrBlank()) {
            return ""
        }
        return assocsTypes.computeIfAbsent(assocId) {
            val attDef = evalAttDefFromAspects(assocId)
            attDef?.config?.get("typeRef")?.asText()?.toEntityRef()?.getLocalId() ?: ""
        }
    }

    fun getMainTypeInfo(): DbQueryTypeInfoData {
        return mainTypeInfo
    }

    private fun evalAttDefFromAspects(attribute: String): AttributeDef? {
        if (!attribute.contains(':')) {
            return null
        }
        val aspect = ctx.ecosTypeService.getAspectsForAtts(setOf(attribute)).firstOrNull()
        if (aspect != null && EntityRef.isNotEmpty(aspect)) {
            val atts = ctx.ecosTypeService.getAttributesForAspects(listOf(aspect), true)
            return atts.find { it.id == attribute }
        }
        return null
    }

    private inner class DbQueryTypeInfoDataImpl(
        private val typeId: String,
        private val typeService: DbEcosModelService
    ) : DbQueryTypeInfoData {

        private val typeInfo: TypeInfo? = typeService.getTypeInfo(typeId)

        private val attributesById: MutableMap<String, Optional<AttributeDef>>
        private val queryPermsPolicy: QueryPermsPolicy
        private val typeAspects: Set<EntityRef>

        init {
            attributesById = if (typeInfo != null) {
                LinkedHashMap(typeInfo.model.getAllAttributes().associate { it.id to Optional.of(it) })
            } else {
                LinkedHashMap()
            }
            DbRecord.GLOBAL_ATTS.values.forEach {
                if (!attributesById.containsKey(it.id)) {
                    attributesById[it.id] = Optional.of(it)
                }
            }
            queryPermsPolicy = typeInfo?.queryPermsPolicy ?: QueryPermsPolicy.OWN
            typeAspects = typeInfo?.aspects?.map { it.ref }?.toSet() ?: emptySet()
        }

        override fun getAttributesById(): Map<String, AttributeDef> {
            return attributesById.entries.filter { it.value.isPresent }.associate { it.key to it.value.get() }
        }

        override fun getAttribute(attId: String): AttributeDef? {
            return attributesById.computeIfAbsent(attId) {
                var attDef = evalAttDefFromAspects(it)
                if (attDef == null) {
                    attDef = evalAttDefFromAspects(it)
                }
                if (attDef == null) {
                    attDef = typeService.getAttDefFromChildrenTypes(typeId, it)
                }
                Optional.ofNullable(attDef)
            }.orElse(null)
        }

        override fun getQueryPermsPolicy(): QueryPermsPolicy {
            return queryPermsPolicy
        }

        override fun getTypeAspects(): Set<EntityRef> {
            return typeAspects
        }

        override fun getTypeInfo(): TypeInfo? {
            return typeInfo
        }
    }
}
