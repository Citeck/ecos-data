package ru.citeck.ecos.data.sql.records.dao.atts

import com.fasterxml.jackson.databind.node.ArrayNode
import com.fasterxml.jackson.databind.node.ObjectNode
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
import ru.citeck.ecos.data.sql.records.dao.atts.content.DbDefaultLocalContentValue
import ru.citeck.ecos.data.sql.records.dao.atts.content.DbDefaultRemoteContentValue
import ru.citeck.ecos.data.sql.records.dao.atts.stage.DbStageEdge
import ru.citeck.ecos.data.sql.records.dao.atts.stage.DbStageValue
import ru.citeck.ecos.data.sql.records.dao.atts.status.DbStatusEdge
import ru.citeck.ecos.data.sql.records.dao.atts.status.DbStatusValue
import ru.citeck.ecos.data.sql.records.dao.query.DbFindQueryContext
import ru.citeck.ecos.data.sql.records.utils.DbAttValueUtils
import ru.citeck.ecos.data.sql.records.workspace.DbWorkspaceDesc
import ru.citeck.ecos.data.sql.repo.entity.DbEntity
import ru.citeck.ecos.data.sql.repo.find.DbFindPage
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType
import ru.citeck.ecos.model.lib.status.constants.StatusConstants
import ru.citeck.ecos.model.lib.type.dto.TypeInfo
import ru.citeck.ecos.model.lib.type.dto.WorkspaceScope
import ru.citeck.ecos.model.lib.utils.ModelUtils
import ru.citeck.ecos.records2.RecordConstants
import ru.citeck.ecos.records3.record.atts.schema.ScalarType
import ru.citeck.ecos.records3.record.atts.schema.utils.AttStrUtils
import ru.citeck.ecos.records3.record.atts.value.AttEdge
import ru.citeck.ecos.records3.record.atts.value.AttValue
import ru.citeck.ecos.records3.record.atts.value.impl.EmptyAttValue
import ru.citeck.ecos.webapp.api.entity.EntityRef
import java.time.temporal.Temporal
import java.util.*

class DbRecord(
    internal val ctx: DbRecordsDaoCtx,
    val entity: DbEntity,
    queryCtx: DbFindQueryContext? = null,
    private val isGroupEntity: Boolean = false
) : AttValue {

    companion object {
        const val ATT_NAME = "_name"
        const val ATT_ASPECTS = "_aspects"
        const val ATT_PERMISSIONS = "permissions"
        const val ATT_PREVIEW_INFO = "previewInfo"
        const val ATT_CONTENT_VERSION = "version:version"
        const val ATT_CONTENT_VERSION_COMMENT = "version:comment"
        const val ATT_IS_DRAFT = "_isDraft"
        const val ATT_STAGE = "_stage"
        const val ATT_STATUS_MODIFIED = "_statusModified"
        const val ATT_PATH_BY_ASSOC = "_pathByAssoc"
        const val ATT_PATH_BY_PARENT = "_pathByParent"
        const val ATT_CUSTOM_ID = "_customId"

        const val ASSOC_SRC_ATT_PREFIX = "assoc_src_"

        const val ASPECT_VERSIONABLE = "versionable"

        const val WS_DEFAULT = "default"

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
            withName(RecordConstants.ATT_DOC_NUM)
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
                withName(ATT_CUSTOM_ID)
                withType(DbColumnType.TEXT)
            },
            DbColumnDef.create {
                withName(RecordConstants.ATT_PARENT)
                withType(DbColumnType.LONG)
                withIndex(DbColumnIndexDef(true))
            },
            DbColumnDef.create {
                withName(RecordConstants.ATT_PARENT_ATT)
                withType(DbColumnType.LONG)
            },
            DbColumnDef.create {
                withName(ATT_ASPECTS)
                withType(DbColumnType.LONG)
                withMultiple(true)
                withIndex(DbColumnIndexDef(true))
            },
            DbColumnDef.create {
                withName(ATT_STATUS_MODIFIED)
                withType(DbColumnType.DATETIME)
                withIndex(DbColumnIndexDef(true))
            }
        )

        val COLUMN_STAGE = DbColumnDef.create {
            withName(ATT_STAGE)
            withType(DbColumnType.TEXT)
            withIndex(DbColumnIndexDef(true))
        }

        val COLUMN_IS_DRAFT = DbColumnDef.create {
            withName(ATT_IS_DRAFT)
            withType(DbColumnType.BOOLEAN)
        }

        val GLOBAL_ATTS = listOf(
            AttributeDef.create()
                .withId(RecordConstants.ATT_PARENT)
                .withType(AttributeType.ASSOC)
                .build(),
            AttributeDef.create()
                .withId(ATT_ASPECTS)
                .withType(AttributeType.ASSOC)
                .withMultiple(true)
                .build(),
            AttributeDef.create()
                .withId(RecordConstants.ATT_CREATED)
                .withType(AttributeType.DATETIME)
                .build(),
            AttributeDef.create()
                .withId(RecordConstants.ATT_MODIFIED)
                .withType(AttributeType.DATETIME)
                .build(),
            AttributeDef.create()
                .withId(ATT_STATUS_MODIFIED)
                .withType(AttributeType.DATETIME)
                .build()
        ).associateBy { it.id }

        fun getDefaultContentAtt(typeInfo: TypeInfo?): String {
            return (typeInfo?.contentConfig?.path ?: "").ifBlank { "content" }
        }
    }

    private val globalRef: EntityRef by lazy { ctx.getGlobalRef(extId) }

    private val permsValue by lazy { DbRecPermsValue(ctx, this) }
    private val additionalAtts: Map<String, Any?>
    private val assocsInnerAdditionalAtts: Map<String, Any?>
    private val assocMapping: Map<Long, EntityRef>

    private val typeInfo: TypeInfo
    private val attTypes: Map<String, AttributeType>
    private val allAttDefs: Map<String, AttributeDef>
    private val nonSystemAttDefs: Map<String, AttributeDef>

    private val workspace: String? by lazy {
        val wsId = entity.workspace
        if (wsId != null) {
            ctx.dataService.getTableContext().getWorkspaceService().getWorkspaceExtIdById(wsId)
        } else {
            null
        }
    }

    private val defaultContentAtt: String

    private val extId = if (isGroupEntity) {
        "g-" + UUID.randomUUID()
    } else {
        entity.extId
    }

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

        if (recData.containsKey(RecordConstants.ATT_PARENT_ATT)) {
            val attId = recData[RecordConstants.ATT_PARENT_ATT] as? Long ?: -1L
            recData[RecordConstants.ATT_PARENT_ATT] = ctx.assocsService.getAttById(attId)
        }

        allAttDefs.values.forEach {
            if (DbRecordsUtils.isAssocLikeAttribute(it) && it.multiple) {
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
                        // do nothing
                    }
                }
            }
        }

        // assoc mapping
        val assocIdValues = mutableSetOf<Long>()
        if (entity.creator != -1L) {
            assocIdValues.add(entity.creator)
        }
        if (entity.modifier != -1L) {
            assocIdValues.add(entity.modifier)
        }

        attTypes.filter { DbRecordsUtils.isEntityRefAttribute(it.value) }.keys.forEach { attId ->
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

        val assocsTypes = queryCtx?.getAssocsTypes() ?: emptyMap()

        val innerAssocAttsByAtt = HashMap<String, Map<String, AttributeDef>>()
        fun getInnerAssocAtts(assocId: String) = innerAssocAttsByAtt.computeIfAbsent(assocId) { id ->
            val attsById = HashMap<String, AttributeDef>(GLOBAL_ATTS)
            val typeInfo = ctx.ecosTypeService.getTypeInfo(assocsTypes[id] ?: "")
            typeInfo?.model?.getAllAttributes()?.forEach {
                attsById[it.id] = it
            }
            attsById
        }

        if (assocsTypes.isNotEmpty()) {
            recData.forEach { recDataEntry ->
                val dotIdx = recDataEntry.key.indexOf('.')
                if (dotIdx != -1) {
                    val assocId = recDataEntry.key.substring(0, dotIdx)
                    val innerAssocAttsById = getInnerAssocAtts(assocId)
                    val attDef = innerAssocAttsById[recDataEntry.key.substring(dotIdx + 1)]
                    if (DbRecordsUtils.isAssocLikeAttribute(attDef)) {
                        DbAttValueUtils.forEachLongValue(recDataEntry.value) {
                            assocIdValues.add(it)
                        }
                    }
                }
            }
        }

        assocMapping = if (assocIdValues.isNotEmpty()) {
            val assocIdValuesList = assocIdValues.toList()
            val assocRefValues = ctx.recordRefService.getEntityRefsByIds(assocIdValuesList)
            assocIdValuesList.mapIndexed { idx, id -> id to assocRefValues[idx] }.toMap()
        } else {
            emptyMap()
        }

        defaultContentAtt = getDefaultContentAtt()

        attTypes.forEach { (attId, attType) ->
            if (attId == ATT_ASPECTS || recData.containsKey(attId)) {
                recData[attId] = convertValue(attId, attType, allAttDefs[attId], recData[attId])
            }
        }
        val assocInnerKeys = recData.keys.filter {
            AttStrUtils.indexOf(it, '.') != -1
        }
        if (assocInnerKeys.isEmpty() || assocsTypes.isEmpty()) {
            assocsInnerAdditionalAtts = emptyMap()
        } else {
            val complexAtts = LinkedHashMap<String, MutableMap<String, Any?>>()

            for (key in assocInnerKeys) {
                val data = recData[key]
                val dotIdx = AttStrUtils.indexOf(key, '.')
                val keyFirst = key.substring(0, dotIdx)
                if (recData[keyFirst] is EntityRef) {
                    continue
                }
                val keySecond = key.substring(dotIdx + 1)
                val innerAttDef = getInnerAssocAtts(keyFirst)[keySecond]
                val convertedData = if (innerAttDef != null) {
                    convertValue(keySecond, innerAttDef.type, innerAttDef, data)
                } else {
                    data
                }
                complexAtts.computeIfAbsent(keyFirst) { LinkedHashMap() }[keySecond] = convertedData
            }
            assocsInnerAdditionalAtts = complexAtts
        }
        this.additionalAtts = recData
    }

    private fun convertValue(attId: String, attType: AttributeType, attDef: AttributeDef?, value: Any?): Any? {
        if (DbRecordsUtils.isEntityRefAttribute(attType)) {
            return if (attId == ATT_ASPECTS) {
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
                DbAspectsValue(fullAspects)
            } else {
                toEntityRef(value)
            }
        } else {
            when (attType) {
                AttributeType.JSON -> {
                    if (value is String) {
                        return Json.mapper.read(value)
                    }
                }

                AttributeType.MLTEXT -> {
                    if (value is String) {
                        return Json.mapper.read(value, MLText::class.java)
                    }
                }

                AttributeType.CONTENT -> {
                    return convertContentAtt(value, attId, attId == defaultContentAtt)
                }

                AttributeType.OPTIONS -> {
                    return if (value is Collection<*>) {
                        val result = ArrayList<DbOptionsAttValue>()
                        for (element in value) {
                            val attValue = convertOptionsAtt(element as? String, attDef)
                            if (attValue != null) {
                                result.add(attValue)
                            }
                        }
                        result
                    } else {
                        convertOptionsAtt(value as? String, attDef)
                    }
                }
                else -> {
                    // do nothing
                }
            }
        }
        return value
    }

    private fun convertOptionsAtt(value: String?, attDef: AttributeDef?): DbOptionsAttValue? {
        value ?: return null
        attDef ?: return null
        return DbOptionsAttValue(this, attDef, value)
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
                extId,
                typeInfo,
                value,
                attId,
                isDefaultContentAtt
            )
        } else {
            null
        }
    }

    private fun toEntityRef(value: Any?): Any {
        value ?: return EntityRef.EMPTY
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
            is Long -> assocMapping[value] ?: error("Ref doesn't found for id $value")
            else -> error("Unexpected ref value type: ${value::class}")
        }
    }

    fun getWorkspaceId(): String? {
        val finalWorkspace = workspace
        return if (finalWorkspace.isNullOrBlank() && typeInfo.workspaceScope == WorkspaceScope.PRIVATE) {
            WS_DEFAULT
        } else {
            finalWorkspace
        }
    }

    fun getTypeInfo(): TypeInfo {
        return typeInfo
    }

    override fun getId(): EntityRef {
        return globalRef
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
        return when {
            value is Map<*, *> -> {
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
                newMap as T
            }
            nnValue is List<Any?> -> {
                val newList = ArrayList<Any?>()
                nnValue.forEach {
                    val filtered = filterUnknownJsonTypes(attribute, it)
                    if (filtered != Unit) {
                        newList.add(filtered)
                    }
                }
                newList as T
            }
            (nnValue is ObjectNode || nnValue is ArrayNode) && attTypes[attribute] == AttributeType.JSON -> {
                nnValue as T
            }
            nnValue is DbOptionsAttValue -> {
                nnValue.asText() as T
            }
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
            -> {
                value
            }
            else -> Unit as T
        }
    }

    private fun getIdAtt(): String {
        return (additionalAtts[ATT_CUSTOM_ID] as? String ?: "").ifBlank { extId }
    }

    override fun asJson(): Any {

        val jsonAtts = LinkedHashMap<String, Any?>()
        jsonAtts["id"] = getIdAtt()

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
            return if (attribute != ATT_ASPECTS && DbRecordsUtils.isAssocLikeAttribute(attDef)) {
                DbAssocAttValuesContainer(
                    entity,
                    ctx,
                    value,
                    DbRecordsUtils.isChildAssocAttribute(attDef),
                    attDef
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
                DataValue.create(resValue).asJavaObj()
            }
        }

        val isRunAsSystem = ctx.workspaceService.isRunAsSystemOrWsSystem(getWorkspaceId())

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
                result
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
        if (name.startsWith(ASSOC_SRC_ATT_PREFIX)) {
            return ctx.assocsService.getSourceAssocs(
                entity.refId,
                name.substringAfter(ASSOC_SRC_ATT_PREFIX),
                DbFindPage.FIRST
            ).entities.isNotEmpty()
        }
        if (name == RecordConstants.ATT_CONTENT) {
            val defaultContentAtt = getDefaultContentAtt()
            if (defaultContentAtt.contains('.')) {
                return getDefaultContent("") != null
            }
        }
        val fixedName = when (name) {
            RecordConstants.ATT_CONTENT -> getDefaultContentAtt()
            else -> ATTS_MAPPING.getOrDefault(name, name)
        }
        val value = additionalAtts[fixedName] ?: return false
        return when (value) {
            is String -> value != ""
            is Collection<*> -> value.isNotEmpty()
            is EntityRef -> value.isNotEmpty()
            else -> true
        }
    }

    override fun getAs(type: String): Any? {
        if (type == DbContentValue.CONTENT_DATA) {
            return getDefaultContent("_as.${DbContentValue.CONTENT_DATA}")
        }
        return super.getAs(type)
    }

    private fun metaAssocIdToRef(id: Long): EntityRef {
        return if (id < 0L) {
            EntityRef.EMPTY
        } else {
            assocMapping[id] ?: error("Ref doesn't found for id $id")
        }
    }

    override fun getAtt(name: String): Any? {
        if (assocsInnerAdditionalAtts.containsKey(name)) {
            return if (!isCurrentUserHasAttReadPerms(name)) {
                null
            } else {
                assocsInnerAdditionalAtts[name]
            }
        }
        return when (name) {
            "id" -> getIdAtt()
            ATT_NAME -> displayName
            RecordConstants.ATT_MODIFIED, "cm:modified" -> entity.modified
            RecordConstants.ATT_CREATED, "cm:created" -> entity.created
            RecordConstants.ATT_MODIFIER -> metaAssocIdToRef(entity.modifier)
            RecordConstants.ATT_CREATOR -> metaAssocIdToRef(entity.creator)
            RecordConstants.ATT_WORKSPACE -> {
                val workspaceId = getWorkspaceId()
                if (workspaceId.isNullOrBlank()) {
                    EntityRef.EMPTY
                } else {
                    DbWorkspaceDesc.getRef(workspaceId)
                }
            }
            StatusConstants.ATT_STATUS -> {
                val statusId = entity.status
                val statusDef = typeInfo.model.statuses.firstOrNull { it.id == statusId } ?: return statusId
                val attValue = ctx.attValuesConverter.toAttValue(statusDef) ?: EmptyAttValue.INSTANCE
                return DbStatusValue(statusDef, attValue)
            }
            ATT_STAGE -> {
                additionalAtts[ATT_STAGE]?.let { stageId ->
                    typeInfo.model.stages.firstOrNull { it.id == stageId }
                }?.let { stageDef ->
                    val attValue = ctx.attValuesConverter.toAttValue(stageDef) ?: EmptyAttValue.INSTANCE
                    DbStageValue(stageDef, attValue)
                }
            }
            ATT_PATH_BY_ASSOC -> DbPathByAssocValue.getValue(this)
            ATT_PATH_BY_PARENT -> DbPathByAssocValue.getValue(this)?.getAtt(RecordConstants.ATT_PARENT)
            ATT_PERMISSIONS -> permsValue
            RecordConstants.ATT_CONTENT -> getDefaultContent("")
            RecordConstants.ATT_PARENT -> additionalAtts[RecordConstants.ATT_PARENT]
            ATT_PREVIEW_INFO -> getDefaultContent(ATT_PREVIEW_INFO)
            ATT_IS_DRAFT -> additionalAtts[ATT_IS_DRAFT] ?: false
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
                        getDefaultContent("") != null
                    ) {
                        return "1.0"
                    }
                    return additionalAtts[ATTS_MAPPING.getOrDefault(name, name)]
                } else {
                    null
                }
            }
        }
    }

    fun isCurrentUserHasReadPerms(): Boolean {
        if (ctx.workspaceService.isRunAsSystemOrWsSystem(getWorkspaceId()) || ctx.getUpdatedInTxnIds().contains(extId)) {
            return true
        }
        val ws = workspace
        if (!ws.isNullOrEmpty() && ws != WS_DEFAULT) {
            val runAs = AuthContext.getCurrentRunAsAuth()
            if (!ctx.workspaceService.getUserWorkspaces(runAs.getUser()).contains(ws)) {
                return false
            }
        }
        return permsValue.getRecordPerms().hasReadPerms()
    }

    private fun isCurrentUserHasAttReadPerms(attribute: String): Boolean {
        if (ctx.workspaceService.isRunAsSystemOrWsSystem(getWorkspaceId())) {
            return true
        }
        return permsValue.getRecordPerms().hasAttReadPerms(attribute)
    }

    fun getDefaultContentAtt(): String {
        return getDefaultContentAtt(typeInfo)
    }

    /**
     * Get default content value
     *
     * @param innerPath path to inner value of content
     */
    fun getDefaultContent(innerPath: String): AttValue? {
        val attributeWithContent = getDefaultContentAtt()
        if (attributeWithContent.contains(".")) {
            val attContentRef = attributeWithContent.substringBefore(".")
            if (!isCurrentUserHasAttReadPerms(attContentRef)) {
                return null
            }
            val recordRef = additionalAtts[attContentRef]
            if (recordRef !is EntityRef || recordRef.isEmpty()) {
                return null
            }
            val contentAttPath = attributeWithContent.substringAfter(".")
            val size = ctx.recordsService.getAtt(recordRef, "$contentAttPath.size?num").asInt()
            if (size <= 0) {
                return null
            }
            val entityName = MLText.getClosestValue(entity.name, I18nContext.getLocale()).ifBlank { "no-name" }
            return DbDefaultRemoteContentValue(
                contentAttPath,
                innerPath,
                recordRef,
                ctx,
                extId,
                typeInfo.id,
                entityName
            )
        }
        return if (isCurrentUserHasAttReadPerms(attributeWithContent)) {
            val contentValue = additionalAtts[attributeWithContent]
            if (contentValue is DbContentValue) {
                val entityName = MLText.getClosestValue(entity.name, I18nContext.getLocale())
                    .ifBlank { contentValue.contentData.getName() }
                    .ifBlank { "no-name" }
                var value: AttValue? = DbDefaultLocalContentValue(entityName, contentValue)
                if (innerPath.isNotBlank()) {
                    val pathElements = innerPath.split(".")
                    var idx = 0
                    while (idx < pathElements.size) {
                        val newValue = if (pathElements[idx] == RecordConstants.ATT_AS) {
                            value?.getAs(pathElements[++idx])
                        } else {
                            value?.getAtt(pathElements[idx])
                        }
                        value = ctx.attValuesConverter.toAttValue(newValue)
                        idx++
                    }
                }
                value
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
        } else if (name == ATT_STAGE) {
            return DbStageEdge(ctx, type)
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

    override fun getType(): EntityRef {
        return if (isGroupEntity) {
            EntityRef.EMPTY
        } else {
            ModelUtils.getTypeRef(typeInfo.id)
        }
    }
}
