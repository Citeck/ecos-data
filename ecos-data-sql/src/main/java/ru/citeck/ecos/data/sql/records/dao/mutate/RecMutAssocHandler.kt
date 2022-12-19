package ru.citeck.ecos.data.sql.records.dao.mutate

import mu.KotlinLogging
import ru.citeck.ecos.commons.data.DataValue
import ru.citeck.ecos.commons.data.ObjectData
import ru.citeck.ecos.data.sql.ecostype.EcosAttColumnDef
import ru.citeck.ecos.data.sql.records.DbRecordsUtils
import ru.citeck.ecos.data.sql.records.dao.DbRecordsDaoCtx
import ru.citeck.ecos.data.sql.records.dao.mutate.operation.OperationType
import ru.citeck.ecos.data.sql.repo.entity.DbEntity
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType
import ru.citeck.ecos.model.lib.type.service.utils.TypeUtils
import ru.citeck.ecos.records2.RecordConstants
import ru.citeck.ecos.records3.record.atts.dto.RecordAtts
import ru.citeck.ecos.records3.record.atts.schema.ScalarType
import ru.citeck.ecos.records3.record.request.RequestContext
import ru.citeck.ecos.webapp.api.entity.EntityRef

class RecMutAssocHandler(private val ctx: DbRecordsDaoCtx) {

    companion object {
        private val log = KotlinLogging.logger {}

        private const val MUTATION_FROM_PARENT_FLAG = "__mutationFromParent"
        private const val MUTATION_FROM_CHILD_FLAG = "__mutationFromChild"
    }

    fun preProcessContentAtts(
        recAttributes: ObjectData,
        recToMutate: DbEntity,
        columns: List<EcosAttColumnDef>,
        contentStorageType: String
    ) {

        for (column in columns) {

            if (!recAttributes.has(column.attribute.id)) {
                continue
            }

            if (column.attribute.type == AttributeType.CONTENT) {
                val contentData = recAttributes[column.attribute.id]
                recAttributes[column.column.name] = ctx.recContentHandler.uploadContent(
                    recToMutate,
                    column.attribute.id,
                    contentData,
                    column.column.multiple,
                    contentStorageType
                )
            } else if (DbRecordsUtils.isAssocLikeAttribute(column.attribute)) {
                val assocValue = recAttributes[column.attribute.id]
                val convertedValue = preProcessContentAssocBeforeMutate(
                    recToMutate.extId,
                    column.attribute.id,
                    assocValue
                )
                if (convertedValue !== assocValue) {
                    recAttributes[column.attribute.id] = convertedValue
                }
            }
        }
    }

    private fun preProcessContentAssocBeforeMutate(
        recordId: String,
        attId: String,
        value: DataValue
    ): DataValue {
        if (value.isNull()) {
            return value
        }
        if (value.isArray()) {
            if (value.size() == 0) {
                return value
            }
            val result = DataValue.createArr()
            value.forEach { result.add(preProcessContentAssocBeforeMutate(recordId, attId, it)) }
            return result
        }
        if (value.isObject() && value.has("fileType")) {

            val existingRef = ctx.recContentHandler.getRefForContentData(value)
            if (EntityRef.isNotEmpty(existingRef)) {
                return DataValue.createStr(existingRef.toString())
            }

            val type = value["fileType"]
            if (type.isNull() || type.asText().isBlank()) {
                return value
            }
            val typeId = type.asText()

            val typeInfo = ctx.ecosTypeService.getTypeInfoNotNull(typeId)

            val childAttributes = ObjectData.create()
            childAttributes[RecordConstants.ATT_TYPE] = TypeUtils.getTypeRef(typeId)
            childAttributes[RecordConstants.ATT_CONTENT] = listOf(value)

            val sourceIdMapping = RequestContext.getCurrentNotNull().ctxData.sourceIdMapping
            val sourceId = sourceIdMapping.getOrDefault(ctx.sourceId, ctx.sourceId)

            childAttributes[RecordConstants.ATT_PARENT] = EntityRef.create(ctx.appName, sourceId, recordId)

            val name = value["originalName"]
            if (name.isNotNull()) {
                childAttributes[ScalarType.DISP.mirrorAtt] = name
            }

            val childRef = ctx.recordsService.create(typeInfo.sourceId, childAttributes)
            return DataValue.createStr(childRef.toString())
        }
        return value
    }

    fun replaceRefsById(recAttributes: ObjectData, columns: List<EcosAttColumnDef>) {

        val assocAtts = columns.filter {
            DbRecordsUtils.isAssocLikeAttribute(it.attribute)
        }.map { it.attribute.id }
            .toMutableList()

        if (recAttributes.has(RecordConstants.ATT_PARENT)) {
            assocAtts.add(RecordConstants.ATT_PARENT)
        }

        if (assocAtts.isNotEmpty()) {
            val assocRefs = mutableSetOf<EntityRef>()
            for (assocId in assocAtts) {
                if (recAttributes.has(assocId)) {
                    extractRecordRefs(recAttributes[assocId], assocRefs)
                } else {
                    OperationType.values().forEach { op ->
                        val attValue = recAttributes[op.prefix + assocId]
                        extractRecordRefs(attValue, assocRefs)
                    }
                }
            }
            val idByRef = mutableMapOf<EntityRef, Long>()
            if (assocRefs.isNotEmpty()) {
                val refsList = assocRefs.toList()
                val refsId = ctx.recordRefService.getOrCreateIdByRecordRefs(refsList)
                for ((idx, ref) in refsList.withIndex()) {
                    idByRef[ref] = refsId[idx]
                }
            }
            assocAtts.forEach { assocId ->
                if (recAttributes.has(assocId)) {
                    recAttributes[assocId] = replaceRecordRefsToId(recAttributes[assocId], idByRef)
                } else {
                    OperationType.values().forEach { op ->
                        val attWithPrefix = op.prefix + assocId
                        if (recAttributes.has(attWithPrefix)) {
                            val value = recAttributes[attWithPrefix]
                            recAttributes[attWithPrefix] = replaceRecordRefsToId(value, idByRef)
                        }
                    }
                }
            }
        }
    }

    private fun extractRecordRefs(value: DataValue, target: MutableSet<EntityRef>) {
        if (value.isNull()) {
            return
        }
        if (value.isArray()) {
            for (element in value) {
                extractRecordRefs(element, target)
            }
        } else if (value.isTextual()) {
            val ref = EntityRef.valueOf(value.asText())
            if (EntityRef.isNotEmpty(ref)) {
                target.add(ref)
            }
        }
    }

    private fun replaceRecordRefsToId(value: DataValue, mapping: Map<EntityRef, Long>): DataValue {
        if (value.isArray()) {
            val result = DataValue.createArr()
            for (element in value) {
                val elemRes = replaceRecordRefsToId(element, mapping)
                if (elemRes.isNotNull()) {
                    result.add(elemRes)
                }
            }
            return result
        } else if (value.isTextual()) {
            val ref = EntityRef.valueOf(value.asText())
            return DataValue.create(mapping[ref])
        }
        return DataValue.NULL
    }

    fun processParentAfterMutation(
        recBeforeSave: DbEntity,
        recAfterSave: DbEntity,
        attributes: ObjectData
    ) {
        if (!attributes.has(RecordConstants.ATT_PARENT) || attributes.get(MUTATION_FROM_PARENT_FLAG, false)) {
            return
        }
        val currentRef = ctx.recordRefService.getRecordRefById(recAfterSave.refId)
        if (EntityRef.isEmpty(currentRef)) {
            return
        }

        val parentAttBefore = recBeforeSave.attributes[RecordConstants.ATT_PARENT_ATT] as? String ?: ""
        val parentRefIdBefore = recBeforeSave.attributes[RecordConstants.ATT_PARENT] as? Long
        val parentAttAfter = recAfterSave.attributes[RecordConstants.ATT_PARENT_ATT] as? String ?: ""
        val parentRefIdAfter = recAfterSave.attributes[RecordConstants.ATT_PARENT] as? Long

        if (parentRefIdBefore == parentRefIdAfter && parentAttBefore == parentAttAfter) {
            return
        }
        val parentRefBefore = parentRefIdBefore?.let {
            ctx.recordRefService.getRecordRefById(it)
        } ?: EntityRef.EMPTY

        if (EntityRef.isNotEmpty(parentRefBefore)) {
            if (parentRefIdBefore == parentRefIdAfter) {
                // changed only parent attribute
                val atts = ObjectData.create()
                if (parentAttBefore.isNotEmpty()) {
                    atts[OperationType.ATT_REMOVE.prefix + parentAttBefore] = currentRef
                }
                if (parentAttAfter.isNotEmpty()) {
                    atts[OperationType.ATT_ADD.prefix + parentAttAfter] = currentRef
                }
                if (atts.isNotEmpty()) {
                    atts[MUTATION_FROM_CHILD_FLAG] = true
                    ctx.recordsService.mutate(RecordAtts(parentRefBefore, atts))
                }
            } else {
                // child was moved from one parent to another
                val atts = ObjectData.create()
                if (parentAttBefore.isNotEmpty()) {
                    atts[OperationType.ATT_REMOVE.prefix + parentAttBefore] = currentRef
                }
                if (atts.isNotEmpty()) {
                    atts[MUTATION_FROM_CHILD_FLAG] = true
                    ctx.recordsService.mutate(RecordAtts(parentRefBefore, atts))
                }
            }
        }

        val parentRefAfter = parentRefIdAfter?.let {
            ctx.recordRefService.getRecordRefById(it)
        } ?: EntityRef.EMPTY

        if (parentRefIdBefore == parentRefIdAfter || EntityRef.isEmpty(parentRefAfter) || parentAttAfter.isEmpty()) {
            return
        }

        val atts = ObjectData.create()
        atts[OperationType.ATT_ADD.prefix + parentAttAfter] = currentRef
        atts[MUTATION_FROM_CHILD_FLAG] = true
        ctx.recordsService.mutate(RecordAtts(parentRefAfter, atts))
    }

    fun processChildrenAfterMutation(
        recBeforeSave: DbEntity,
        recAfterSave: DbEntity,
        attributes: ObjectData,
        columns: List<EcosAttColumnDef>
    ) {

        if (recAfterSave.refId < 0 || attributes.get(MUTATION_FROM_CHILD_FLAG, false)) {
            return
        }

        val childAssociations = columns.filter {
            DbRecordsUtils.isChildAssocAttribute(it.attribute)
        }

        val childrenChanges = HashMap<String, AddedRemovedAssocs>()

        for (att in childAssociations) {
            val attributeId = att.attribute.id
            val before = anyToSetOfLong(recBeforeSave.attributes[attributeId])
            val after = anyToSetOfLong(recAfterSave.attributes[attributeId])

            val changes = AddedRemovedAssocs(
                after.subtract(before),
                before.subtract(after)
            )
            if (changes.isNotEmpty()) {
                childrenChanges[attributeId] = changes
            }
        }

        val changedChildren = mutableSetOf<Long>()
        childrenChanges.values.forEach {
            changedChildren.addAll(it.added)
            changedChildren.addAll(it.removed)
        }

        if (changedChildren.isEmpty()) {
            return
        }

        log.debug {
            val recRef = EntityRef.create(ctx.sourceId, recAfterSave.extId)
            "Children of $recRef was changed. " + childrenChanges.entries.joinToString {
                it.key + ": added: " + it.value.added + " removed: " + it.value.removed
            }
        }

        val parentRef = ctx.recordRefService.getRecordRefById(recAfterSave.refId)

        val childRefsById = ctx.recordRefService.getRecordRefsByIdsMap(changedChildren)
        val addOrRemoveParentRef = { attId: String, children: Set<Long>, add: Boolean ->
            for (childId in children) {
                val childRef = childRefsById[childId]
                    ?: error("Child ref doesn't found by id. Refs: $childRefsById id: $childId")

                if (EntityRef.isNotEmpty(childRef)) {
                    val childAtts = RecordAtts(childRef)
                    if (add) {
                        childAtts.setAtt(RecordConstants.ATT_PARENT, parentRef)
                        childAtts.setAtt(RecordConstants.ATT_PARENT_ATT, attId)
                    } else {
                        childAtts.setAtt(RecordConstants.ATT_PARENT, null)
                        childAtts.setAtt(RecordConstants.ATT_PARENT_ATT, null)
                    }
                    childAtts.setAtt(MUTATION_FROM_PARENT_FLAG, true)
                    ctx.recordsService.mutate(childAtts)
                }
            }
        }

        childrenChanges.forEach {
            addOrRemoveParentRef.invoke(it.key, it.value.removed, false)
            addOrRemoveParentRef.invoke(it.key, it.value.added, true)
        }
    }

    private fun anyToSetOfLong(value: Any?): Set<Long> {
        value ?: return emptySet()
        if (value is Collection<*>) {
            val res = hashSetOf<Long>()
            for (item in value) {
                if (item is Long) {
                    res.add(item)
                }
            }
            return res
        }
        if (value is Long) {
            return setOf(value)
        }
        return emptySet()
    }

    private data class AddedRemovedAssocs(
        val added: Set<Long>,
        val removed: Set<Long>
    ) {
        fun isNotEmpty(): Boolean {
            return added.isNotEmpty() || removed.isNotEmpty()
        }
    }
}
