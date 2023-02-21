package ru.citeck.ecos.data.sql.records.dao.atts

import ru.citeck.ecos.commons.data.MLText
import ru.citeck.ecos.commons.json.Json
import ru.citeck.ecos.context.lib.i18n.I18nContext
import ru.citeck.ecos.data.sql.dto.DbColumnDef
import ru.citeck.ecos.data.sql.dto.DbColumnType
import ru.citeck.ecos.data.sql.records.DbRecordsUtils
import ru.citeck.ecos.data.sql.records.dao.DbRecordsDaoCtx
import ru.citeck.ecos.data.sql.records.dao.atts.content.DbContentValue
import ru.citeck.ecos.data.sql.records.dao.atts.content.DbContentValueWithCustomName
import ru.citeck.ecos.data.sql.repo.entity.DbEntity
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType
import ru.citeck.ecos.model.lib.status.constants.StatusConstants
import ru.citeck.ecos.model.lib.type.dto.TypeInfo
import ru.citeck.ecos.model.lib.type.service.utils.TypeUtils
import ru.citeck.ecos.records2.RecordConstants
import ru.citeck.ecos.records2.RecordRef
import ru.citeck.ecos.records3.record.atts.schema.ScalarType
import ru.citeck.ecos.records3.record.atts.value.AttEdge
import ru.citeck.ecos.records3.record.atts.value.AttValue
import ru.citeck.ecos.records3.record.atts.value.impl.EmptyAttValue
import ru.citeck.ecos.webapp.api.entity.EntityRef
import java.time.temporal.Temporal
import java.util.*
import kotlin.collections.ArrayList
import kotlin.collections.HashMap
import kotlin.collections.LinkedHashMap

class DbRecord(private val ctx: DbRecordsDaoCtx, val entity: DbEntity) : AttValue {

    companion object {
        const val ATT_NAME = "_name"
        const val ATT_PERMISSIONS = "permissions"

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
                withName("_parent")
                withType(DbColumnType.LONG)
            },
            DbColumnDef.create {
                withName("_parentAtt")
                withType(DbColumnType.TEXT)
            }
        )

        val COLUMN_IS_DRAFT = DbColumnDef.create {
            withName("_isDraft")
            withType(DbColumnType.BOOLEAN)
        }

        fun getDefaultContentAtt(typeInfo: TypeInfo?): String {
            return (typeInfo?.contentConfig?.path ?: "").ifBlank { "content" }
        }
    }

    private val permsValue by lazy { DbRecPermsValue(ctx, entity.extId) }
    private val additionalAtts: Map<String, Any?>
    private val assocMapping: Map<Long, EntityRef>
    private val typeInfo: TypeInfo?

    init {
        val recData = LinkedHashMap(entity.attributes)

        val attTypes = HashMap<String, AttributeType>()
        typeInfo = ctx.ecosTypeService.getTypeInfo(entity.type)

        typeInfo?.model?.getAllAttributes()?.forEach {
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
                    DbColumnType.LONG -> {
                        if (it.name == RecordConstants.ATT_PARENT) {
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
            val assocRefValues = ctx.recordRefService.getRecordRefsByIds(assocIdValuesList)
            assocIdValuesList.mapIndexed { idx, id -> id to assocRefValues[idx] }.toMap()
        } else {
            emptyMap()
        }

        val defaultContentAtt = getDefaultContentAtt()

        attTypes.forEach { (attId, attType) ->
            val value = recData[attId]
            if (DbRecordsUtils.isAssocLikeAttribute(attType)) {
                if (value != null) {
                    recData[attId] = toRecordRef(value)
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
            DbContentValue(ctx, entity.extId, value, attId, isDefaultContentAtt)
        } else {
            null
        }
    }

    private fun toRecordRef(value: Any): Any {
        return when (value) {
            is Iterable<*> -> {
                val result = ArrayList<Any>()
                value.forEach {
                    if (it != null) {
                        result.add(toRecordRef(it))
                    }
                }
                result
            }
            is Long -> assocMapping[value] ?: error("Assoc doesn't found for id $value")
            else -> error("Unexpected assoc value type: ${value::class}")
        }
    }

    override fun getId(): Any {
        return RecordRef.create(ctx.appName, ctx.sourceId, entity.extId)
    }

    override fun asText(): String {
        return id.toString()
    }

    override fun getDisplayName(): Any {
        return entity.name
    }

    @Suppress("UNCHECKED_CAST")
    private fun <T> filterUnknownJsonTypes(value: T): T {
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
                val filtered = filterUnknownJsonTypes(v)
                if (filtered != Unit) {
                    newMap[k] = filtered
                }
            }
            return newMap as T
        } else if (nnValue is List<Any?>) {
            val newList = ArrayList<Any?>()
            nnValue.forEach {
                val filtered = filterUnknownJsonTypes(it)
                if (filtered != Unit) {
                    newList.add(filtered)
                }
            }
            return newList as T
        } else if (
            nnValue::class.java.isPrimitive ||
            nnValue::class.java.isEnum ||
            value is Boolean ||
            value is Number ||
            value is Char ||
            value is String ||
            value is EntityRef ||
            value is Date ||
            value is Temporal ||
            value is ByteArray
        ) {
            return value
        }
        return Unit as T
    }

    override fun asJson(): Any {
        val jsonAtts = LinkedHashMap<String, Any?>()
        jsonAtts["id"] = entity.extId

        if (typeInfo == null) {
            error("TypeInfo is null")
        }
        val nonSystemAttIds = typeInfo.model.attributes.map { it.id }.toSet()

        val validAdditionalAtts = filterUnknownJsonTypes(
            additionalAtts.entries.filter {
                nonSystemAttIds.contains(it.key)
            }.associate { it.key to it.value }
        )
        for ((k, v) in validAdditionalAtts) {
            if (isCurrentUserHasAttReadPerms(k)) {
                jsonAtts[k] = v
            } else {
                jsonAtts[k] = null
            }
        }
        return jsonAtts
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
            val content = getAtt(RecordConstants.ATT_CONTENT) as? DbContentValue ?: return null
            return content.getAs(DbContentValue.CONTENT_DATA)
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
            "previewInfo" -> getDefaultContent()?.getContentValue()?.getAtt("previewInfo")
            else -> {
                if (isCurrentUserHasAttReadPerms(name)) {
                    additionalAtts[ATTS_MAPPING.getOrDefault(name, name)]
                } else {
                    null
                }
            }
        }
    }

    private fun isCurrentUserHasAttReadPerms(attribute: String): Boolean {
        val perms = permsValue.getRecordPerms() ?: return true
        return perms.isCurrentUserHasAttReadPerms(attribute)
    }

    private fun getDefaultContentAtt(): String {
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
                return DbContentValueWithCustomName(entityName, contentValue)
            }
            return null
        } else {
            null
        }
    }

    override fun getEdge(name: String): AttEdge {
        if (name == StatusConstants.ATT_STATUS) {
            return DbStatusEdge(ctx, type)
        }
        return DbRecordAttEdge(this, name, permsValue.getRecordPerms())
    }

    private fun getAsPersonRef(name: String): Any {
        if (name.isBlank()) {
            return RecordRef.EMPTY
        }
        return ctx.authoritiesApi?.getAuthorityRef(name) ?: return name
    }

    override fun getType(): RecordRef {
        return TypeUtils.getTypeRef(entity.type)
    }
}
