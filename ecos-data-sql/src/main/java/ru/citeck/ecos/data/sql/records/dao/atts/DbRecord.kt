package ru.citeck.ecos.data.sql.records.dao.atts

import ecos.com.fasterxml.jackson210.databind.node.ArrayNode
import ecos.com.fasterxml.jackson210.databind.node.ObjectNode
import ru.citeck.ecos.commons.data.DataValue
import ru.citeck.ecos.commons.data.MLText
import ru.citeck.ecos.commons.data.ObjectData
import ru.citeck.ecos.commons.json.Json
import ru.citeck.ecos.context.lib.auth.AuthContext
import ru.citeck.ecos.context.lib.i18n.I18nContext
import ru.citeck.ecos.data.sql.dto.DbColumnDef
import ru.citeck.ecos.data.sql.dto.DbColumnIndexDef
import ru.citeck.ecos.data.sql.dto.DbColumnType
import ru.citeck.ecos.data.sql.records.DbRecordsUtils
import ru.citeck.ecos.data.sql.records.dao.DbRecordsDaoCtx
import ru.citeck.ecos.data.sql.records.dao.atts.content.DbContentValue
import ru.citeck.ecos.data.sql.records.dao.atts.content.DbContentValueWithCustomName
import ru.citeck.ecos.data.sql.repo.entity.DbEntity
import ru.citeck.ecos.data.sql.repo.find.DbFindPage
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType
import ru.citeck.ecos.model.lib.status.constants.StatusConstants
import ru.citeck.ecos.model.lib.type.dto.TypeInfo
import ru.citeck.ecos.model.lib.utils.ModelUtils
import ru.citeck.ecos.records2.RecordConstants
import ru.citeck.ecos.records3.record.atts.schema.ScalarType
import ru.citeck.ecos.records3.record.atts.value.AttEdge
import ru.citeck.ecos.records3.record.atts.value.AttValue
import ru.citeck.ecos.records3.record.atts.value.impl.EmptyAttValue
import ru.citeck.ecos.webapp.api.entity.EntityRef
import java.time.temporal.Temporal
import java.util.*
import kotlin.collections.ArrayList
import kotlin.collections.LinkedHashMap
import kotlin.collections.LinkedHashSet

class DbRecord(private val ctx: DbRecordsDaoCtx, val entity: DbEntity) : AttValue {

    companion object {
        const val ATT_NAME = "_name"
        const val ATT_ASPECTS = "_aspects"
        const val ATT_PERMISSIONS = "permissions"
        const val ATT_CONTENT_VERSION = "version:version"
        const val ATT_CONTENT_VERSION_COMMENT = "version:comment"
        const val ASSOC_SRC_ATT_PREFIX = "assoc_src_"

        const val ASPECT_VERSIONABLE = "versionable"

        val ATTS_MAPPING = mapOf(
            "id" to DbEntity.EXT_ID,
            ScalarType.DISP.mirrorAtt to DbEntity.NAME,
            ATT_NAME to DbEntity.NAME,
            RecordConstants.ATT_CREATED to DbEntity.CREATED,
            RecordConstants.ATT_CREATOR to DbEntity.CREATOR,
            RecordConstants.ATT_MODIFIED to DbEntity.MODIFIED,
            RecordConstants.ATT_MODIFIER to DbEntity.MODIFIER,
            ScalarType.LOCAL_ID.mirrorAtt to DbEntity.EXT_ID,
            StatusConstants.ATT_STATUS to DbEntity.STATUS
        )

        val DOC_NUM_COLUMN = DbColumnDef.create {
            withName("_docNum")
            withType(DbColumnType.INT)
        }
        val COMPUTABLE_OPTIONAL_COLUMNS = listOf(
            DOC_NUM_COLUMN
        )
        val OPTIONAL_COLUMNS = listOf(
            DOC_NUM_COLUMN,
            DbColumnDef.create {
                withName("_proc")
                withMultiple(true)
                withType(DbColumnType.JSON)
            },
            DbColumnDef.create {
                withName("_cipher")
                withType(DbColumnType.JSON)
            },
            DbColumnDef.create {
                withName(RecordConstants.ATT_PARENT)
                withType(DbColumnType.LONG)
                withIndex(DbColumnIndexDef(true))
            },
            DbColumnDef.create {
                withName(RecordConstants.ATT_PARENT_ATT)
                withType(DbColumnType.TEXT)
            },
            DbColumnDef.create {
                withName(ATT_ASPECTS)
                withType(DbColumnType.LONG)
                withMultiple(true)
                withIndex(DbColumnIndexDef(true))
            }
        )

        val COLUMN_IS_DRAFT = DbColumnDef.create {
            withName("_isDraft")
            withType(DbColumnType.BOOLEAN)
        }

        val GLOBAL_ATTS = listOf(
            AttributeDef.create()
                .withId(RecordConstants.ATT_PARENT)
                .withType(AttributeType.ASSOC)
                .build(),
            AttributeDef.create()
                .withId(DbRecord.ATT_ASPECTS)
                .withType(AttributeType.ASSOC)
                .withMultiple(true)
                .build()
        ).associateBy { it.id }

        fun getDefaultContentAtt(typeInfo: TypeInfo?): String {
            return (typeInfo?.contentConfig?.path ?: "").ifBlank { "content" }
        }
    }

    private val permsValue by lazy { DbRecPermsValue(ctx, this) }
    private val additionalAtts: Map<String, Any?>
    private val assocMapping: Map<Long, EntityRef>

    private val typeInfo: TypeInfo
    private val attTypes: Map<String, AttributeType>
    private val allAttDefs: Map<String, AttributeDef>
    private val nonSystemAttDefs: Map<String, AttributeDef>

    init {
        val recData = LinkedHashMap(entity.attributes)

        val meta = ctx.getEntityMeta(entity)
        typeInfo = meta.typeInfo

        attTypes = LinkedHashMap()
        allAttDefs = meta.allAttributes
        nonSystemAttDefs = meta.nonSystemAtts
        meta.allAttributes.forEach {
            attTypes[it.key] = it.value.type
        }

        allAttDefs.values.forEach {
            if (it.type == AttributeType.ASSOC && it.multiple) {
                val assocs = recData[it.id]
                if (assocs is Collection<*> && assocs.size == 10) {
                    recData[it.id] = ctx.assocsService.getTargetAssocs(
                        entity.refId,
                        it.id,
                        DbFindPage(0, 300)
                    ).entities.map { assoc -> assoc.targetId }
                }
            }
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
                    DbColumnType.LONG -> {
                        if (it.name == RecordConstants.ATT_PARENT) {
                            attTypes[it.name] = AttributeType.ASSOC
                        } else if (it.name == ATT_ASPECTS) {
                            attTypes[it.name] = AttributeType.ASSOC
                        }
                    }
                    else -> {
                    }
                }
            }
        }

        // assoc mapping
        val assocIdValues = mutableSetOf<Long>()
        attTypes.filter { DbRecordsUtils.isAssocLikeAttribute(it.value) }.keys.forEach { attId ->
            val value = recData[attId]
            if (value is Iterable<*>) {
                for (elem in value) {
                    if (elem is Long) {
                        assocIdValues.add(elem)
                    }
                }
            } else if (value is Long) {
                assocIdValues.add(value)
            }
        }
        assocMapping = if (assocIdValues.isNotEmpty()) {
            val assocIdValuesList = assocIdValues.toList()
            val assocRefValues = ctx.recordRefService.getEntityRefsByIds(assocIdValuesList)
            assocIdValuesList.mapIndexed { idx, id -> id to assocRefValues[idx] }.toMap()
        } else {
            emptyMap()
        }

        val defaultContentAtt = getDefaultContentAtt()

        attTypes.forEach { (attId, attType) ->
            val value = recData[attId]
            if (DbRecordsUtils.isAssocLikeAttribute(attType)) {
                if (attId == ATT_ASPECTS) {
                    val fullAspects = LinkedHashSet<String>()
                    typeInfo.aspects.forEach {
                        fullAspects.add(it.ref.getLocalId())
                    }
                    if (value != null) {
                        val refs = toEntityRef(value)
                        if (refs is Collection<*>) {
                            for (ref in refs) {
                                (ref as? EntityRef)?.let { fullAspects.add(it.getLocalId()) }
                            }
                        } else if (refs is EntityRef) {
                            fullAspects.add(refs.getLocalId())
                        }
                    }
                    recData[attId] = DbAspectsValue(fullAspects)
                } else if (value != null) {
                    recData[attId] = toEntityRef(value)
                }
            } else {
                when (attType) {
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
                        recData[attId] = convertContentAtt(value, attId, attId == defaultContentAtt)
                    }
                    else -> {
                    }
                }
            }
        }
        this.additionalAtts = recData
    }

    private fun convertContentAtt(value: Any?, attId: String, isDefaultContentAtt: Boolean): Any? {
        if (value is List<*>) {
            return value.mapNotNull { convertContentAtt(it, attId, isDefaultContentAtt) }
        }
        if (value !is Long || value < 0) {
            return null
        }
        return if (ctx.contentService != null) {
            DbContentValue(
                ctx,
                entity.extId,
                typeInfo,
                value,
                attId,
                isDefaultContentAtt
            )
        } else {
            null
        }
    }

    private fun toEntityRef(value: Any): Any {
        return when (value) {
            is Iterable<*> -> {
                val result = ArrayList<Any>()
                value.forEach {
                    if (it != null) {
                        result.add(toEntityRef(it))
                    }
                }
                result
            }
            is Long -> assocMapping[value] ?: error("Assoc doesn't found for id $value")
            else -> error("Unexpected assoc value type: ${value::class}")
        }
    }

    override fun getId(): EntityRef {
        return EntityRef.create(ctx.appName, ctx.sourceId, entity.extId)
    }

    override fun asText(): String {
        return id.toString()
    }

    override fun getDisplayName(): Any {
        var name: MLText = entity.name
        if (MLText.isEmpty(name)) {
            val typeName = typeInfo.name
            if (!MLText.isEmpty(typeName)) {
                name = typeName
            }
        }
        if (MLText.isEmpty(name)) {
            name = MLText(
                I18nContext.ENGLISH to "No name",
                I18nContext.RUSSIAN to "Нет имени"
            )
        }
        return name
    }

    @Suppress("UNCHECKED_CAST")
    private fun <T> filterUnknownJsonTypes(attribute: String, value: T): T {
        if (value == null) {
            return null as T
        }
        val nnValue: Any = value
        if (value is Map<*, *>) {
            if (value.isEmpty() || value.keys.first() !is String) {
                return value
            }
            nnValue as Map<String, Any?>
            val newMap = LinkedHashMap<String, Any?>()
            nnValue.forEach { (k, v) ->
                val filtered = filterUnknownJsonTypes(attribute, v)
                if (filtered != Unit) {
                    newMap[k] = filtered
                }
            }
            return newMap as T
        } else if (nnValue is List<Any?>) {
            val newList = ArrayList<Any?>()
            nnValue.forEach {
                val filtered = filterUnknownJsonTypes(attribute, it)
                if (filtered != Unit) {
                    newList.add(filtered)
                }
            }
            return newList as T
        } else if ((nnValue is ObjectNode || nnValue is ArrayNode) && attTypes[attribute] == AttributeType.JSON) {
            return nnValue as T
        } else if (
            nnValue::class.java.isPrimitive ||
            nnValue::class.java.isEnum ||
            nnValue is Boolean ||
            nnValue is Number ||
            nnValue is Char ||
            nnValue is String ||
            nnValue is EntityRef ||
            nnValue is Date ||
            nnValue is Temporal ||
            nnValue is ByteArray ||
            nnValue is MLText
        ) {
            return value
        }
        return Unit as T
    }

    override fun asJson(): Any {

        val jsonAtts = LinkedHashMap<String, Any?>()
        jsonAtts["id"] = entity.extId

        val nonSystemAttIds = typeInfo.model.attributes.map { it.id }.toSet()

        val validAdditionalAtts = mutableMapOf<String, Any?>()
        additionalAtts.forEach { (attName, value) ->
            if (nonSystemAttIds.contains(attName)) {
                validAdditionalAtts[attName] = filterUnknownJsonTypes(attName, value)
            }
        }
        for ((k, v) in validAdditionalAtts) {
            if (isCurrentUserHasAttReadPerms(k)) {
                jsonAtts[k] = v
            } else {
                jsonAtts[k] = null
            }
        }
        return jsonAtts
    }

    fun getAttsForOperations(): Map<String, Any?> {

        fun getValueForOperations(attribute: String, value: Any?, attDef: AttributeDef): Any? {
            if (attribute != ATT_ASPECTS && attDef.type == AttributeType.ASSOC) {
                return DbAssocAttValuesContainer(
                    ctx,
                    value,
                    DbRecordsUtils.isChildAssocAttribute(attDef),
                    attDef.multiple
                )
            } else {
                value ?: return null
                val resValue = when (value) {
                    is Collection<*> -> {
                        val result = ArrayList<Any?>()
                        value.forEach {
                            val newValue = getValueForOperations(attribute, it, attDef)
                            if (newValue != null) {
                                result.add(newValue)
                            }
                        }
                        value
                    }
                    is DbAspectsValue -> value.getAspectRefs()
                    else -> filterUnknownJsonTypes(attribute, value)
                }
                return DataValue.create(resValue).asJavaObj()
            }
        }

        val isRunAsSystem = AuthContext.isRunAsSystem()

        val attDefs = if (isRunAsSystem) {
            allAttDefs
        } else {
            nonSystemAttDefs
        }

        val attIds = LinkedHashSet<String>(attDefs.keys)
        attIds.add(ATT_ASPECTS)
        attIds.removeIf { !isCurrentUserHasAttReadPerms(it) }

        val resultAtts = LinkedHashMap<String, Any?>()

        for (attribute in attIds) {
            resultAtts[attribute] = getValueForOperations(
                attribute,
                additionalAtts[attribute],
                attDefs[attribute] ?: GLOBAL_ATTS[attribute] ?: error("Attribute def is not found for '$attribute'")
            )
        }

        return resultAtts
    }

    fun getAttsForCopy(): Map<String, Any?> {

        fun getValueForCopy(attribute: String, value: Any?): Any? {
            value ?: return null
            return if (value is Collection<*>) {
                val result = ArrayList<Any?>()
                value.forEach {
                    val newValue = getValueForCopy(attribute, it)
                    if (newValue != null) {
                        result.add(newValue)
                    }
                }
                value
            } else if (value is DbContentValue) {
                if (ctx.recContentHandler.isContentDbDataAware()) {
                    value.contentData.getDbId()
                } else {
                    null
                }
            } else {
                filterUnknownJsonTypes(attribute, value)
            }
        }

        val attIds = LinkedHashSet<String>(32)
        typeInfo.model.attributes.mapTo(attIds) { it.id }
        val nonReadableAtts = attIds.filter { !isCurrentUserHasAttReadPerms(it) }
        if (nonReadableAtts.isNotEmpty()) {
            error("You can't read attributes: $nonReadableAtts of record $id")
        }

        val resultAtts = ObjectData.create()

        for (attribute in attIds) {
            resultAtts[attribute] = getValueForCopy(attribute, additionalAtts[attribute])
        }

        return resultAtts.asMap(String::class.java, Any::class.java)
    }

    override fun has(name: String): Boolean {
        val fixedName = when (name) {
            RecordConstants.ATT_CONTENT -> getDefaultContentAtt()
            else -> ATTS_MAPPING.getOrDefault(name, name)
        }
        val value = additionalAtts[fixedName] ?: return false
        return when (value) {
            is String -> value != ""
            is Collection<*> -> !value.isEmpty()
            else -> true
        }
    }

    override fun getAs(type: String): Any? {
        if (type == DbContentValue.CONTENT_DATA) {
            return (getAtt(RecordConstants.ATT_CONTENT) as? AttValue)?.getAs(DbContentValue.CONTENT_DATA)
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
                val typeInfo = ctx.ecosTypeService.getTypeInfo(entity.type) ?: return statusId
                val statusDef = typeInfo.model.statuses.firstOrNull { it.id == statusId } ?: return statusId
                val attValue = ctx.attValuesConverter.toAttValue(statusDef) ?: EmptyAttValue.INSTANCE
                return DbStatusValue(statusDef, attValue)
            }
            ATT_PERMISSIONS -> permsValue
            RecordConstants.ATT_CONTENT -> getDefaultContent()
            RecordConstants.ATT_PARENT -> additionalAtts[RecordConstants.ATT_PARENT]
            "previewInfo" -> getDefaultContent()?.getContentValue()?.getAtt("previewInfo")
            else -> {
                if (name.startsWith(ASSOC_SRC_ATT_PREFIX)) {
                    val assocName = name.substring(ASSOC_SRC_ATT_PREFIX.length)
                    val sourceAssocsIds = ctx.assocsService.getSourceAssocs(
                        entity.refId,
                        assocName,
                        DbFindPage(0, 300)
                    ).entities.map { it.sourceId }
                    return ctx.recordRefService.getEntityRefsByIds(sourceAssocsIds)
                }
                if (isCurrentUserHasAttReadPerms(name)) {
                    if (name == ATT_CONTENT_VERSION &&
                        typeInfo.aspects.any { it.ref.getLocalId() == ASPECT_VERSIONABLE } &&
                        additionalAtts[ATT_CONTENT_VERSION] == null &&
                        getDefaultContent() != null
                    ) {
                        return "1.0"
                    }
                    additionalAtts[ATTS_MAPPING.getOrDefault(name, name)]
                } else {
                    null
                }
            }
        }
    }

    private fun isCurrentUserHasAttReadPerms(attribute: String): Boolean {
        if (AuthContext.isRunAsSystem()) {
            return true
        }
        return permsValue.getRecordPerms().isCurrentUserHasAttReadPerms(attribute)
    }

    fun getDefaultContentAtt(): String {
        return getDefaultContentAtt(typeInfo)
    }

    fun getDefaultContent(): DbContentValueWithCustomName? {
        val attributeWithContent = getDefaultContentAtt()
        if (attributeWithContent.contains(".")) {
            // todo
            return null
        }
        return if (isCurrentUserHasAttReadPerms(attributeWithContent)) {
            val contentValue = additionalAtts[attributeWithContent]
            if (contentValue is DbContentValue) {
                val entityName = MLText.getClosestValue(entity.name, I18nContext.getLocale())
                    .ifBlank { contentValue.contentData.getName() }
                    .ifBlank { "no-name" }
                DbContentValueWithCustomName(entityName, contentValue)
            } else {
                null
            }
        } else {
            null
        }
    }

    override fun getEdge(name: String): AttEdge {
        if (name == StatusConstants.ATT_STATUS) {
            return DbStatusEdge(ctx, type)
        }
        return DbRecordAttEdge(
            this,
            name,
            allAttDefs[name] ?: AttributeDef.create {
                withId(name)
                withName(MLText(name))
            },
            permsValue.getRecordPerms()
        )
    }

    private fun getAsPersonRef(name: String): Any {
        if (name.isBlank()) {
            return EntityRef.EMPTY
        }
        return ctx.authoritiesApi?.getAuthorityRef(name) ?: return name
    }

    override fun getType(): EntityRef {
        return ModelUtils.getTypeRef(entity.type)
    }
}
