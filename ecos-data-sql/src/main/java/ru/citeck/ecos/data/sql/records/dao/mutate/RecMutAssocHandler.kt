package ru.citeck.ecos.data.sql.records.dao.mutate

import io.github.oshai.kotlinlogging.KotlinLogging
import ru.citeck.ecos.commons.data.DataValue
import ru.citeck.ecos.commons.data.ObjectData
import ru.citeck.ecos.data.sql.content.storage.EcosContentStorageConfig
import ru.citeck.ecos.data.sql.ecostype.EcosAttColumnDef
import ru.citeck.ecos.data.sql.records.DbRecordsControlAtts
import ru.citeck.ecos.data.sql.records.DbRecordsUtils
import ru.citeck.ecos.data.sql.records.dao.DbRecordsDaoCtx
import ru.citeck.ecos.data.sql.records.dao.atts.DbAssocAttValuesContainer
import ru.citeck.ecos.data.sql.records.dao.atts.DbRecord
import ru.citeck.ecos.data.sql.records.dao.mutate.operation.OperationType
import ru.citeck.ecos.data.sql.records.utils.DbAttValueUtils
import ru.citeck.ecos.data.sql.repo.entity.DbEntity
import ru.citeck.ecos.data.sql.repo.find.DbFindPage
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType
import ru.citeck.ecos.model.lib.type.dto.WorkspaceScope
import ru.citeck.ecos.model.lib.utils.ModelUtils
import ru.citeck.ecos.records2.RecordConstants
import ru.citeck.ecos.records3.RecordsService
import ru.citeck.ecos.records3.record.atts.dto.RecordAtts
import ru.citeck.ecos.records3.record.atts.schema.ScalarType
import ru.citeck.ecos.webapp.api.constants.AppName
import ru.citeck.ecos.webapp.api.entity.EntityRef

class RecMutAssocHandler(private val ctx: DbRecordsDaoCtx) {

    companion object {
        private val log = KotlinLogging.logger {}

        const val MUTATION_FROM_PARENT_FLAG = "__mutationFromParent"
        const val MUTATION_FROM_CHILD_FLAG = "__mutationFromChild"

        /**
         * [RecMutAssocHandler.updateParentRefOfChildren] over a bare [RecordsService], for a caller
         * that has one and no [DbRecordsDaoCtx].
         *
         * **The whole of this function's dependency on the dao context is the records service**, and
         * saying so in the signature is what lets the background column migration share this code
         * without reaching for a whole `DbRecordsDaoCtx` of a table it does not own - see
         * [ru.citeck.ecos.data.sql.context.DbSchemaContext.getRecordsService]. A records service is
         * one per application, so "whichever dao registered last" is not a choice at all; every
         * other member of a dao context - `sourceId`, `tableCtx`, `ecosTypeService` - is per-dao,
         * and a caller that took one of those out of a table-keyed map would be making one.
         *
         * @param disableEvents suppress the child's change event: a background transfer
         *        is not a user edit, and a table with a million children would otherwise emit a
         *        million change events nobody asked for.
         * @param disableAudit leave the child's `_modified`/`_modifier` alone, for the same reason.
         *        **System context only** - `DbRecordsMutateDao` refuses it anywhere else - which is
         *        exactly where the batch engine runs (`DbBatchTaskEngine.runTaskAttempt`).
         */
        /**
         * Takes back the `_parent`/`_parentAtt` a parent wrote on a child, **without deleting the
         * child**.
         *
         * [updateParentRefOfChildren] with `add = false` is not this: it sends `_parent = null`
         * under [MUTATION_FROM_PARENT_FLAG], which `DbRecordsDao.mutate` reads as "the parent has
         * let this child go" and answers by deleting the record. That is right for a parent
         * dropping a child and wrong for a parent taking back a back-reference it should not have
         * written - a background transfer undoing a pass whose claim did not hold, say, where the
         * child is a record the user made and nobody asked to delete.
         *
         * Goes through the records service rather than the child's columns, so a child living in
         * another schema or another application is released by the application that owns it, which
         * is the only one holding the ids its `_parent` is written in.
         *
         * @param disableEvents as in [updateParentRefOfChildren].
         * @param disableAudit as in [updateParentRefOfChildren]. **System context only.**
         */
        @JvmStatic
        fun releaseChildren(
            recordsService: RecordsService,
            childRefs: Collection<EntityRef>,
            disableEvents: Boolean = false,
            disableAudit: Boolean = false
        ) {
            for (childRef in childRefs) {
                if (EntityRef.isEmpty(childRef)) {
                    continue
                }
                val childAtts = RecordAtts(childRef)
                childAtts.setAtt(RecordConstants.ATT_PARENT, null)
                childAtts.setAtt(RecordConstants.ATT_PARENT_ATT, null)
                childAtts.setAtt(MUTATION_FROM_PARENT_FLAG, true)
                childAtts.setAtt(DbRecordsControlAtts.RELEASE_CHILD, true)
                if (disableEvents) {
                    childAtts.setAtt(DbRecordsControlAtts.DISABLE_EVENTS, true)
                }
                if (disableAudit) {
                    childAtts.setAtt(DbRecordsControlAtts.DISABLE_AUDIT, true)
                }
                recordsService.mutate(childAtts)
            }
        }

        @JvmStatic
        fun updateParentRefOfChildren(
            recordsService: RecordsService,
            parentRef: EntityRef,
            attId: String,
            childRefs: Collection<EntityRef>,
            add: Boolean,
            disableEvents: Boolean = false,
            disableAudit: Boolean = false
        ) {
            for (childRef in childRefs) {
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
                    if (disableEvents) {
                        childAtts.setAtt(DbRecordsControlAtts.DISABLE_EVENTS, true)
                    }
                    if (disableAudit) {
                        childAtts.setAtt(DbRecordsControlAtts.DISABLE_AUDIT, true)
                    }
                    recordsService.mutate(childAtts)
                }
            }
        }
    }

    fun preProcessContentAtts(
        recAttributes: ObjectData,
        recToMutate: DbEntity,
        columns: List<EcosAttColumnDef>,
        contentStorage: EcosContentStorageConfig?,
        creatorRefId: Long
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
                    contentStorage,
                    creatorRefId
                )
            } else if (DbRecordsUtils.isStoredInAssocsTable(column.attribute.type)) {
                val assocValue = recAttributes[column.attribute.id]
                val convertedValue = preProcessContentAssocBeforeMutate(
                    recToMutate.extId,
                    column.attribute.id,
                    assocValue,
                    recToMutate.workspace
                )
                if (convertedValue !== assocValue) {
                    recAttributes[column.attribute.id] = convertedValue
                }
            }
        }
    }

    fun preProcessContentAssocBeforeMutate(
        recordId: String,
        attId: String,
        value: DataValue,
        workspaceId: Long?
    ): DataValue {
        if (value.isNull()) {
            return value
        }
        if (value.isArray()) {
            if (value.size() == 0) {
                return value
            }
            val result = DataValue.createArr()
            value.forEach { result.add(preProcessContentAssocBeforeMutate(recordId, attId, it, workspaceId)) }
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

            if (typeInfo.workspaceScope == WorkspaceScope.PRIVATE) {
                val wsExtId = if (workspaceId == null || workspaceId < 0) {
                    DbRecord.WS_DEFAULT
                } else {
                    ctx.tableCtx.getWorkspaceService().getWorkspaceExtIdById(workspaceId)
                }
                if (wsExtId.isNotBlank()) {
                    childAttributes[RecordConstants.ATT_WORKSPACE] = wsExtId
                }
            }

            val childRef = ctx.recordsService.create(typeInfo.sourceId, childAttributes)
            return DataValue.createStr(childRef.toString())
        }
        return value
    }

    fun replaceRefsById(recAttributes: ObjectData, columns: List<EcosAttColumnDef>) {

        val entityRefAtts = columns.filter {
            AttributeType.isAssocLike(it.attribute.type)
        }.map { it.attribute.id }
            .toMutableList()

        DbRecord.GLOBAL_ATTS.values.forEach { att ->
            if (AttributeType.isAssocLike(att.type) && recAttributes.has(att.id)) {
                entityRefAtts.add(att.id)
            }
        }

        if (entityRefAtts.isNotEmpty()) {
            val entityRefs = mutableSetOf<EntityRef>()
            for (attId in entityRefAtts) {
                if (recAttributes.has(attId)) {
                    extractRecordRefs(recAttributes[attId], entityRefs)
                } else {
                    OperationType.entries.forEach { op ->
                        val attValue = recAttributes[op.prefix + attId]
                        extractRecordRefs(attValue, entityRefs)
                    }
                }
            }
            val idByRef = mutableMapOf<EntityRef, Long>()
            if (entityRefs.isNotEmpty()) {
                val refsList = entityRefs.toList()
                val refsId = ctx.recordRefService.getOrCreateIdByEntityRefs(refsList)
                for ((idx, ref) in refsList.withIndex()) {
                    idByRef[ref] = refsId[idx]
                }
            }
            entityRefAtts.forEach { attId ->
                if (recAttributes.has(attId)) {
                    recAttributes[attId] = replaceRecordRefsToId(recAttributes[attId], idByRef)
                } else {
                    OperationType.entries.forEach { op ->
                        val attWithPrefix = op.prefix + attId
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

    private fun mutateParentRef(ref: EntityRef, atts: ObjectData) {

        if (ref.getAppName() == AppName.ALFRESCO) {
            return
        }
        ctx.recordsService.mutate(RecordAtts(ref, atts))
    }

    fun processParentAfterMutation(
        recBeforeSave: DbEntity,
        recAfterSave: DbEntity,
        attributes: ObjectData,
        disableEvents: Boolean
    ) {
        val hasParent = attributes.has(RecordConstants.ATT_PARENT)
        val hasParentAtt = attributes.has(RecordConstants.ATT_PARENT_ATT)
        if (!hasParent && !hasParentAtt) {
            // parent reference doesn't changed
            return
        }

        val parentAttBeforeId = recBeforeSave.attributes[RecordConstants.ATT_PARENT_ATT] as? Long ?: -1L
        val parentRefIdBefore = recBeforeSave.attributes[RecordConstants.ATT_PARENT] as? Long
        val parentAttAfterId = recAfterSave.attributes[RecordConstants.ATT_PARENT_ATT] as? Long ?: -1L
        val parentRefIdAfter = recAfterSave.attributes[RecordConstants.ATT_PARENT] as? Long

        if (parentRefIdBefore == parentRefIdAfter && parentAttBeforeId == parentAttAfterId) {
            // parent reference doesn't changed
            return
        }
        val isNewParentNotEmpty = parentRefIdAfter != null && parentRefIdAfter != -1L

        val currentRef = ctx.recordRefService.getEntityRefById(recAfterSave.refId)
        if (EntityRef.isEmpty(currentRef)) {
            error("Current ref is empty. RecordRef: ${ctx.getGlobalRef(recAfterSave.extId)}")
        }

        val parentAttBefore = ctx.assocsService.getAttById(parentAttBeforeId)
        val parentAttAfter = ctx.assocsService.getAttById(parentAttAfterId)

        val parentRefBefore = parentRefIdBefore?.let {
            ctx.recordRefService.getEntityRefById(it)
        } ?: EntityRef.EMPTY
        val parentRefAfter = if (parentRefIdAfter == parentRefIdBefore) {
            parentRefBefore
        } else {
            parentRefIdAfter?.let {
                ctx.recordRefService.getEntityRefById(it)
            } ?: EntityRef.EMPTY
        }

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

        val isMutationFromParent = attributes.get(MUTATION_FROM_PARENT_FLAG, false)

        if (parentRefIdBefore != parentRefIdAfter) {

            // child was moved from one parent to another or parent was removed

            if (parentRefBefore.isNotEmpty() && parentAttBefore.isNotEmpty()) {
                // update previous parent only when _parent was changed to non-empty value
                // because _parent == null with MUTATION_FROM_PARENT_FLAG performed only by previous parent
                // and mutation of previous parent is not required from child
                if (!isMutationFromParent || isNewParentNotEmpty) {
                    val atts = ObjectData.create()
                    atts[OperationType.ATT_REMOVE.prefix + parentAttBefore] = currentRef
                    atts[MUTATION_FROM_CHILD_FLAG] = true
                    if (disableEvents) {
                        atts[DbRecordsControlAtts.DISABLE_EVENTS] = true
                    }
                    mutateParentRef(parentRefBefore, atts)
                }
            }

            if (parentRefAfter.isNotEmpty() && !isMutationFromParent) {
                val atts = ObjectData.create()
                atts[OperationType.ATT_ADD.prefix + parentAttAfter] = currentRef
                atts[MUTATION_FROM_CHILD_FLAG] = true
                if (disableEvents) {
                    atts[DbRecordsControlAtts.DISABLE_EVENTS] = true
                }
                mutateParentRef(parentRefAfter, atts)
            }
        } else if (!isMutationFromParent) {

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
                if (disableEvents) {
                    atts[DbRecordsControlAtts.DISABLE_EVENTS] = true
                }
                mutateParentRef(parentRefAfter, atts)
            }
        }
    }

    /**
     * A child telling its parent it is gone, for an attribute whose links are **parked**: the
     * attribute has stopped being an association and a column migration holds its links in
     * `ed_associations_backup`.
     *
     * The notification is an ordinary `att_remove` and there is nothing to remove it from - the
     * attribute the parent has now holds text, or numbers, or nothing the reference means anything
     * to. What has to happen instead is that the **snapshot** stops naming the record: it is what a
     * later return of the type restores from, and a link restored to a deleted record is a dangling
     * reference nobody asked for.
     *
     * The parked copy is also what tells a legitimate notification from a mistaken one. An
     * `att_remove` under [MUTATION_FROM_CHILD_FLAG] naming an attribute that is not a child
     * association is an error - it always was - unless this record really does hold that
     * attribute's links parked, and that question is the same query as the removal. So nothing is
     * forgotten: either a row is dropped and the attribute is handled here, or the caller is left to
     * refuse the mutation exactly as before.
     *
     * The operation is taken out of [recAttributes] rather than let through, because the parent's
     * value is no longer a set of links: applying it would quietly edit a user's text - and only
     * when the conversion happened to produce a string equal to the reference, which is a difference
     * between attribute types and not a rule anybody could rely on.
     *
     * @return the attributes handled this way, for [validateChildAssocs] to pass over.
     */
    fun forgetParkedChildLinks(
        recAttributes: ObjectData,
        entityToMutate: DbEntity,
        columns: List<EcosAttColumnDef>
    ): Set<String> {
        if (!recAttributes.get(MUTATION_FROM_CHILD_FLAG, false) || entityToMutate.refId <= 0) {
            return emptySet()
        }
        val prefix = OperationType.ATT_REMOVE.prefix
        val columnsById = columns.associateBy { it.attribute.id }
        val handled = LinkedHashSet<String>()
        val backupService = ctx.tableCtx.getSchemaCtx().assocBackupService
        for (name in recAttributes.fieldNamesList()) {
            if (!name.startsWith(prefix)) {
                continue
            }
            val att = name.substring(prefix.length)
            if (DbRecordsUtils.isChildAssocAttribute(columnsById[att]?.attribute)) {
                continue
            }
            val attributeId = ctx.assocsService.getIdForAtt(att)
            if (attributeId == -1L) {
                continue
            }
            // A single reference and a list of them, because the notification carries one target
            // per deleted child and the same key can carry several: `asList` answers an empty list
            // for a scalar, which would silently forget nothing.
            val value = recAttributes[name]
            val targetRefs = if (value.isArray()) {
                value.asList(EntityRef::class.java)
            } else {
                listOf(value.getAs(EntityRef::class.java) ?: EntityRef.EMPTY)
            }
            val targetIds = targetRefs
                .filter { EntityRef.isNotEmpty(it) }
                .map { ctx.recordRefService.getIdByEntityRef(it) }
                .filter { it != -1L }
            if (backupService.forgetTargets(entityToMutate.refId, attributeId, targetIds) == 0) {
                continue
            }
            recAttributes.remove(name)
            handled.add(att)
        }
        return handled
    }

    fun validateChildAssocs(
        attributes: ObjectData,
        changedByOperationsAtts: Set<String>,
        recExtId: String,
        columns: List<EcosAttColumnDef>,
        parkedAtts: Set<String> = emptySet()
    ) {
        if (!attributes.get(MUTATION_FROM_CHILD_FLAG, false)) {
            return
        }
        if (changedByOperationsAtts.isEmpty() && parkedAtts.isEmpty()) {
            error(
                "Mutation from child without operations... " +
                    "RecordRef: '${ctx.getGlobalRef(recExtId)}'"
            )
        }
        val columnsById = columns.associateBy { it.attribute.id }
        changedByOperationsAtts.forEach {
            val column = columnsById[it]
            if (column == null ||
                column.attribute.type != AttributeType.ASSOC ||
                !column.attribute.config.get("child", false)
            ) {
                error("'$it' is not a child association. RecordRef: '${ctx.getGlobalRef(recExtId)}'")
            }
        }
    }

    fun processChildrenAfterMutation(
        recBeforeSave: DbEntity,
        recAfterSave: DbEntity,
        attributes: ObjectData,
        columns: List<EcosAttColumnDef>,
        assocsValues: Map<String, DbAssocAttValuesContainer>,
        disableEvents: Boolean
    ) {

        if (recAfterSave.refId < 0) {
            return
        }

        if (attributes.get(MUTATION_FROM_CHILD_FLAG, false)) {
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
            updateParentRefOfChildren(
                parentRef,
                attId,
                children.map {
                    childRefsById[it] ?: error("Child ref doesn't found by id. Refs: $childRefsById id: $it")
                },
                add,
                disableEvents
            )
        }

        childrenChanges.forEach {
            addOrRemoveParentRef.invoke(it.key, it.value.removed, false)
            addOrRemoveParentRef.invoke(it.key, it.value.added, true)
        }
    }

    /**
     * The back-reference of a child association: `_parent` and `_parentAtt` on each child, written
     * through an ordinary mutation carrying [MUTATION_FROM_PARENT_FLAG] so that the child does not
     * turn round and mutate the parent back.
     *
     * A named function rather than the local lambda it used to be, because the background column
     * migration creates child links too and has to make them the same way this dao does. Three
     * user-visible answers depend on the back-reference: `DbRecordsDeleteDao` removes the parent's
     * link to a deleted child only through `_parent`/`_parentAtt`, a `_parent` predicate reads the
     * `__parent` column rather than `ed_associations`, and
     * [DefaultDbPermsComponent][ru.citeck.ecos.data.sql.records.perms.DefaultDbPermsComponent]
     * treats a record with no `_parent` as world-readable. Sharing the code keeps the two paths from
     * drifting.
     *
     * **[add] `= false` deletes the child**, because `DbRecordsDao.mutate` reads `_parent = null`
     * under [MUTATION_FROM_PARENT_FLAG] as "the parent has let this child go". Only a caller that
     * really is dropping the child may pass `false`.
     *
     * **A caller that means "take the back-reference back" wants [releaseChildren] instead**, which
     * says so explicitly and leaves the record where it is. The difference is not a nuance: an
     * attribute that stops being a child association parks its links and keeps its children, and a
     * background transfer undoing its own write must not answer that with a deletion.
     */
    fun updateParentRefOfChildren(
        parentRef: EntityRef,
        attId: String,
        childRefs: Collection<EntityRef>,
        add: Boolean,
        disableEvents: Boolean = false
    ) {
        updateParentRefOfChildren(ctx.recordsService, parentRef, attId, childRefs, add, disableEvents)
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
