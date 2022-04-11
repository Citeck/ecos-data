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
import ru.citeck.ecos.records2.RecordRef
import ru.citeck.ecos.records3.record.atts.dto.RecordAtts
import ru.citeck.ecos.records3.record.request.RequestContext

class RecMutAssocHandler(private val ctx: DbRecordsDaoCtx) {

    companion object {
        private val log = KotlinLogging.logger {}
    }

    fun preProcessContentAtts(recAttributes: ObjectData, recToMutate: DbEntity, columns: List<EcosAttColumnDef>) {

        columns.forEach {
            if (it.attribute.type == AttributeType.CONTENT) {
                val contentData = recAttributes.get(it.attribute.id)
                recAttributes.set(
                    it.column.name,
                    ctx.recContentHandler.uploadContent(
                        recToMutate,
                        it.attribute.id,
                        contentData,
                        it.column.multiple
                    )
                )
            } else if (DbRecordsUtils.isAssocLikeAttribute(it.attribute)) {
                val assocValue = recAttributes.get(it.attribute.id)
                val convertedValue = preProcessContentAssocBeforeMutate(
                    recToMutate.extId,
                    it.attribute.id,
                    assocValue
                )
                if (convertedValue !== assocValue) {
                    recAttributes.set(it.attribute.id, convertedValue)
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
            if (RecordRef.isNotEmpty(existingRef)) {
                return DataValue.createStr(existingRef.toString())
            }

            val type = value.get("fileType")
            if (type.isNull() || type.asText().isBlank()) {
                return value
            }
            val typeId = type.asText()

            val typeInfo = ctx.ecosTypeService.getTypeInfo(typeId) ?: error("Type doesn't found for id '$typeId'")

            val childAttributes = ObjectData.create()
            childAttributes.set("_type", TypeUtils.getTypeRef(typeId))
            childAttributes.set("_content", listOf(value))

            val sourceIdMapping = RequestContext.getCurrentNotNull().ctxData.sourceIdMapping
            val sourceId = sourceIdMapping.getOrDefault(ctx.sourceId, ctx.sourceId)

            childAttributes.set("_parent", RecordRef.create(ctx.appName, sourceId, recordId))

            val name = value.get("originalName")
            if (name.isNotNull()) {
                // todo: should be _name
                childAttributes.set("_disp", name)
            }

            val childRef = ctx.recordsService.create(typeInfo.sourceId, childAttributes)
            return DataValue.createStr(childRef.toString())
        }
        return value
    }

    fun replaceRefsById(recAttributes: ObjectData, columns: List<EcosAttColumnDef>) {

        val assocAttsDef = columns.filter { DbRecordsUtils.isAssocLikeAttribute(it.attribute) }

        if (assocAttsDef.isNotEmpty()) {
            val assocRefs = mutableSetOf<RecordRef>()
            assocAttsDef.forEach {
                if (recAttributes.has(it.attribute.id)) {
                    extractRecordRefs(recAttributes.get(it.attribute.id), assocRefs)
                } else {
                    OperationType.values().forEach { op ->
                        val attValue = recAttributes.get(op.prefix + it.attribute.id)
                        extractRecordRefs(attValue, assocRefs)
                    }
                }
            }
            val idByRef = mutableMapOf<RecordRef, Long>()
            if (assocRefs.isNotEmpty()) {
                val refsList = assocRefs.toList()
                val refsId = ctx.recordRefService.getOrCreateIdByRecordRefs(refsList)
                for ((idx, ref) in refsList.withIndex()) {
                    idByRef[ref] = refsId[idx]
                }
            }
            assocAttsDef.forEach {
                if (recAttributes.has(it.attribute.id)) {
                    recAttributes.set(
                        it.attribute.id,
                        replaceRecordRefsToId(recAttributes.get(it.attribute.id), idByRef)
                    )
                } else {
                    OperationType.values().forEach { op ->
                        val attWithPrefix = op.prefix + it.attribute.id
                        if (recAttributes.has(attWithPrefix)) {
                            val value = recAttributes.get(attWithPrefix)
                            recAttributes.set(
                                attWithPrefix,
                                replaceRecordRefsToId(value, idByRef)
                            )
                        }
                    }
                }
            }
        }
    }

    private fun extractRecordRefs(value: DataValue, target: MutableSet<RecordRef>) {
        if (value.isNull()) {
            return
        }
        if (value.isArray()) {
            for (element in value) {
                extractRecordRefs(element, target)
            }
        } else if (value.isTextual()) {
            val ref = RecordRef.valueOf(value.asText())
            if (RecordRef.isNotEmpty(ref)) {
                target.add(ref)
            }
        }
    }

    private fun replaceRecordRefsToId(value: DataValue, mapping: Map<RecordRef, Long>): DataValue {
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
            val ref = RecordRef.valueOf(value.asText())
            return DataValue.create(mapping[ref])
        }
        return DataValue.NULL
    }

    fun processChildrenAfterMutation(recBeforeSave: DbEntity, recAfterSave: DbEntity, columns: List<EcosAttColumnDef>) {

        if (recAfterSave.refId < 0) {
            return
        }

        val childAssociations = columns.filter {
            DbRecordsUtils.isChildAssocAttribute(it.attribute)
        }

        val addedChildren = mutableSetOf<Long>()
        val removedChildren = mutableSetOf<Long>()

        for (att in childAssociations) {
            val before = anyToSetOfLong(recBeforeSave.attributes[att.attribute.id])
            val after = anyToSetOfLong(recAfterSave.attributes[att.attribute.id])
            addedChildren.addAll(after.subtract(before))
            removedChildren.addAll(before.subtract(after))
        }

        val changedChildren = mutableSetOf<Long>()
        changedChildren.addAll(addedChildren)
        changedChildren.addAll(removedChildren)

        if (changedChildren.isEmpty()) {
            return
        }

        log.debug {
            val recRef = RecordRef.create(ctx.sourceId, recAfterSave.extId)
            "Children of $recRef was changed. Added: $addedChildren Removed: $removedChildren"
        }

        val parentRefId = recAfterSave.refId

        val childRefsById = ctx.recordRefService.getRecordRefsByIdsMap(changedChildren)
        val addOrRemoveParentRef = { children: Set<Long>, add: Boolean ->
            for (removedId in children) {
                val childRef = childRefsById[removedId]
                    ?: error("Child ref doesn't found by id. Refs: $childRefsById id: $removedId")

                if (RecordRef.isNotEmpty(childRef)) {
                    val childAtts = RecordAtts(childRef)
                    if (add) {
                        childAtts.setAtt(RecordConstants.ATT_PARENT, parentRefId)
                    } else {
                        childAtts.setAtt(RecordConstants.ATT_PARENT, null)
                    }
                    ctx.recordsService.mutate(childAtts)
                }
            }
        }
        addOrRemoveParentRef.invoke(removedChildren, false)
        addOrRemoveParentRef.invoke(addedChildren, true)
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
}
