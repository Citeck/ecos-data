package ru.citeck.ecos.data.sql.records.dao.mutate

import mu.KotlinLogging
import ru.citeck.ecos.commons.data.DataValue
import ru.citeck.ecos.commons.data.ObjectData
import ru.citeck.ecos.data.sql.ecostype.EcosAttColumnDef
import ru.citeck.ecos.data.sql.records.DbRecordsUtils
import ru.citeck.ecos.data.sql.records.dao.DbRecordsDaoCtx
import ru.citeck.ecos.data.sql.records.dao.atts.DbAssocAttValuesContainer
import ru.citeck.ecos.data.sql.records.dao.atts.DbRecord
import ru.citeck.ecos.data.sql.records.dao.mutate.operation.OperationType
import ru.citeck.ecos.data.sql.records.utils.DbAttValueUtils
import ru.citeck.ecos.data.sql.repo.entity.DbEntity
import ru.citeck.ecos.data.sql.repo.find.DbFindPage
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType
import ru.citeck.ecos.model.lib.utils.ModelUtils
import ru.citeck.ecos.records2.RecordConstants
import ru.citeck.ecos.records3.record.atts.dto.RecordAtts
import ru.citeck.ecos.records3.record.atts.schema.ScalarType
import ru.citeck.ecos.webapp.api.constants.AppName
import ru.citeck.ecos.webapp.api.entity.EntityRef

class RecMutAssocHandler(private val ctx: DbRecordsDaoCtx) {

    companion object {
        private val log = KotlinLogging.logger {}

        const val MUTATION_FROM_PARENT_FLAG = "__mutationFromParent"
        const val MUTATION_FROM_CHILD_FLAG = "__mutationFromChild"
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
        if (!value.isObject()) {
            return value
        }
        if (value.has("url")) {
            val entityFromUrl = ctx.recContentHandler.getRefFromContentUrl(value["url"].asText())
            if (entityFromUrl.isNotEmpty()) {
                return DataValue.createStr(entityFromUrl.toString())
            }
        }
        if (value.has("fileType")) {

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
            childAttributes[RecordConstants.ATT_TYPE] = ModelUtils.getTypeRef(typeId)
            childAttributes[RecordConstants.ATT_CONTENT] = listOf(value)
            childAttributes[RecordConstants.ATT_PARENT] = ctx.getGlobalRef(recordId)
            childAttributes[RecordConstants.ATT_PARENT_ATT] = attId
            childAttributes[MUTATION_FROM_PARENT_FLAG] = true

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
        if (recAttributes.has(DbRecord.ATT_ASPECTS)) {
            assocAtts.add(DbRecord.ATT_ASPECTS)
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
                val refsId = ctx.recordRefService.getOrCreateIdByEntityRefs(refsList)
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
        if (attributes.get(MUTATION_FROM_PARENT_FLAG, false)) {
            return
        }
        val hasParent = attributes[RecordConstants.ATT_PARENT].isNotEmpty()
        val hasParentAtt = attributes[RecordConstants.ATT_PARENT_ATT].isNotEmpty()
        if (!hasParent && !hasParentAtt) {
            // parent reference doesn't changed
            return
        } else if (hasParent && !hasParentAtt) {
            error(
                "You should supply parent attribute (_parentAtt) for parent ref (_parent). " +
                    "RecordRef: '${ctx.getGlobalRef(recAfterSave.extId)}' " +
                    "ParentRef: '${attributes[RecordConstants.ATT_PARENT].asText()}'"
            )
        } else if (!hasParent) {
            error(
                "You should supply parent ref (_parent) for parent attribute (_parentAtt). " +
                    "RecordRef: '${ctx.getGlobalRef(recAfterSave.extId)}' " +
                    "ParentAtt: '${attributes[RecordConstants.ATT_PARENT_ATT].asText()}'"
            )
        }
        val currentRef = ctx.recordRefService.getEntityRefById(recAfterSave.refId)
        if (EntityRef.isEmpty(currentRef)) {
            error("Current ref is empty. RecordRef: ${ctx.getGlobalRef(recAfterSave.extId)}")
        }

        val parentAttBefore = recBeforeSave.attributes[RecordConstants.ATT_PARENT_ATT] as? String ?: ""
        val parentRefIdBefore = recBeforeSave.attributes[RecordConstants.ATT_PARENT] as? Long
        val parentAttAfter = recAfterSave.attributes[RecordConstants.ATT_PARENT_ATT] as? String ?: ""
        val parentRefIdAfter = recAfterSave.attributes[RecordConstants.ATT_PARENT] as? Long

        if (parentRefIdBefore == parentRefIdAfter && parentAttBefore == parentAttAfter) {
            return
        }

        val parentRefAfter = parentRefIdAfter?.let {
            ctx.recordRefService.getEntityRefById(it)
        } ?: EntityRef.EMPTY

        var parentId: Long = parentRefIdAfter ?: -1
        while (parentId != -1L) {

            if (parentId == recAfterSave.refId) {
                error(
                    "Recursive parent link for record" +
                        " ${ctx.getGlobalRef(recAfterSave.extId)}" +
                        " parent: $parentRefAfter" +
                        " attribute: $parentAttAfter"
                )
            }

            val assoc = ctx.assocsService
                .getSourceAssocs(parentId, parentAttAfter, DbFindPage.FIRST)
                .entities.firstOrNull()

            parentId = if (assoc?.child == true) {
                assoc.sourceId
            } else {
                -1
            }
        }

        val parentRefBefore = parentRefIdBefore?.let {
            ctx.recordRefService.getEntityRefById(it)
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

        if (parentRefAfter.getAppName() == AppName.ALFRESCO) {
            return
        }

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
        columns: List<EcosAttColumnDef>,
        changedByOperationsAtts: Set<String>,
        assocsValues: Map<String, DbAssocAttValuesContainer>
    ) {

        if (recAfterSave.refId < 0) {
            return
        }

        if (attributes.get(MUTATION_FROM_CHILD_FLAG, false)) {
            if (changedByOperationsAtts.isEmpty()) {
                error(
                    "Mutation from child without operations... " +
                        "RecordRef: '${ctx.getGlobalRef(recAfterSave.extId)}'"
                )
            }
            val columnsById = columns.associateBy { it.attribute.id }
            changedByOperationsAtts.forEach {
                val column = columnsById[it]
                if (column == null ||
                    column.attribute.type != AttributeType.ASSOC ||
                    !column.attribute.config.get("child", false)
                ) {

                    error("'$it' is not a child association. RecordRef: '${ctx.getGlobalRef(recAfterSave.extId)}'")
                }
            }
            return
        }

        val childAssociations = columns.filter {
            DbRecordsUtils.isChildAssocAttribute(it.attribute)
        }

        val childrenChanges = HashMap<String, AddedRemovedAssocs>()

        for (att in childAssociations) {
            val attributeId = att.attribute.id

            val multiAssocAttValues = assocsValues[attributeId]

            val changes = if (multiAssocAttValues != null) {

                AddedRemovedAssocs(
                    multiAssocAttValues.getAddedTargetsIds().toSet(),
                    multiAssocAttValues.getRemovedTargetIds().toSet()
                )
            } else {

                val before = DbAttValueUtils.anyToSetOfLongs(recBeforeSave.attributes[attributeId])
                val after = DbAttValueUtils.anyToSetOfLongs(recAfterSave.attributes[attributeId])

                AddedRemovedAssocs(
                    after.subtract(before),
                    before.subtract(after)
                )
            }
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

        val parentRef = ctx.recordRefService.getEntityRefById(recAfterSave.refId)

        val childRefsById = ctx.recordRefService.getEntityRefsByIdsMap(changedChildren)
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
/*
    fun processMultiAssocValuesAfterMutation(values: Map<String, DbMultiAssocAttValuesContainer>) {

    }*/

    private data class AddedRemovedAssocs(
        val added: Set<Long>,
        val removed: Set<Long>
    ) {
        fun isNotEmpty(): Boolean {
            return added.isNotEmpty() || removed.isNotEmpty()
        }
    }
}
