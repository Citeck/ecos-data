package ru.citeck.ecos.data.sql.records.dao.atts

import ru.citeck.ecos.commons.data.MLText
import ru.citeck.ecos.commons.data.ObjectData
import ru.citeck.ecos.commons.json.Json
import ru.citeck.ecos.commons.json.YamlUtils
import ru.citeck.ecos.data.sql.dto.DbColumnDef
import ru.citeck.ecos.data.sql.dto.DbColumnType
import ru.citeck.ecos.data.sql.records.DbRecordsUtils
import ru.citeck.ecos.data.sql.records.dao.DbRecordsDaoCtx
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
import ru.citeck.ecos.records3.record.request.RequestContext
import kotlin.collections.LinkedHashMap

class DbRecord(private val ctx: DbRecordsDaoCtx, val entity: DbEntity) : AttValue {

    companion object {
        const val ATT_NAME = "_name"

        val ATTS_MAPPING = mapOf(
            "id" to DbEntity.EXT_ID,
            ScalarType.DISP.mirrorAtt to DbEntity.NAME,
            ATT_NAME to DbEntity.NAME,
            RecordConstants.ATT_CREATED to DbEntity.CREATED,
            RecordConstants.ATT_CREATOR to DbEntity.CREATOR,
            RecordConstants.ATT_MODIFIED to DbEntity.MODIFIED,
            RecordConstants.ATT_MODIFIER to DbEntity.MODIFIER,
            ScalarType.LOCAL_ID.mirrorAtt to DbEntity.EXT_ID,
            StatusConstants.ATT_STATUS to DbEntity.STATUS,
            RecordConstants.ATT_CONTENT to "content"
        )

        val OPTIONAL_COLUMNS = listOf(
            DbColumnDef.create {
                withName("_docNum")
                withType(DbColumnType.INT)
            },
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
            }
        )

        val COLUMN_IS_DRAFT = DbColumnDef.create {
            withName("_isDraft")
            withType(DbColumnType.BOOLEAN)
        }
    }

    private val additionalAtts: Map<String, Any?>
    private val assocMapping: Map<Long, RecordRef>
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
                        recData[attId] = convertContentAtt(value, attId)
                    }
                    else -> {
                    }
                }
            }
        }
        this.additionalAtts = recData
    }

    private fun convertContentAtt(value: Any?, attId: String): Any? {
        if (value is List<*>) {
            return value.mapNotNull { convertContentAtt(it, attId) }
        }
        if (value !is Long || value < 0) {
            return null
        }
        return if (ctx.contentService != null) {
            DbContentValue(ctx, entity.extId, entity.name, value, attId)
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
        return RecordRef.create(ctx.sourceId, entity.extId)
    }

    override fun asText(): String {
        return entity.name.getClosest(RequestContext.getLocale()).ifBlank { "No name" }
    }

    override fun getDisplayName(): Any {
        return entity.name
    }

    override fun asJson(): Any {
        val jsonAtts = LinkedHashMap<String, Any?>()
        jsonAtts["id"] = entity.extId
        if (typeInfo == null) {
            jsonAtts.putAll(additionalAtts)
            return jsonAtts
        }
        val nonSystemAttIds = typeInfo.model.attributes.map { it.id }.toSet()
        additionalAtts.keys.filter { nonSystemAttIds.contains(it) }.forEach {
            jsonAtts[it] = additionalAtts[it]
        }
        return jsonAtts
    }

    override fun has(name: String): Boolean {
        return additionalAtts.contains(ATTS_MAPPING.getOrDefault(name, name))
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
                return DbStatusValue(statusDef)
            }
            else -> additionalAtts[ATTS_MAPPING.getOrDefault(name, name)]
        }
    }

    override fun getEdge(name: String): AttEdge? {
        if (name == StatusConstants.ATT_STATUS) {
            return DbStatusEdge(ctx, type)
        }
        return super.getEdge(name)
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
