package ru.citeck.ecos.data.sql.records.dao.delete

import ru.citeck.ecos.context.lib.auth.AuthContext
import ru.citeck.ecos.data.sql.ecostype.DbEcosModelService
import ru.citeck.ecos.data.sql.records.DbRecordsUtils
import ru.citeck.ecos.data.sql.records.dao.DbRecordsDaoCtx
import ru.citeck.ecos.data.sql.records.dao.mutate.RecMutAssocHandler
import ru.citeck.ecos.data.sql.records.dao.mutate.operation.OperationType
import ru.citeck.ecos.data.sql.records.utils.DbAttValueUtils
import ru.citeck.ecos.data.sql.repo.find.DbFindPage
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType
import ru.citeck.ecos.model.lib.type.dto.TypeInfo
import ru.citeck.ecos.records2.RecordConstants
import ru.citeck.ecos.records3.record.dao.delete.DelStatus
import ru.citeck.ecos.webapp.api.constants.AppName
import ru.citeck.ecos.webapp.api.entity.EntityRef

class DbRecordsDeleteDao(var ctx: DbRecordsDaoCtx) {

    companion object {
        const val ASSOC_FORCE_DELETION_FLAG = "__isAssocForceDeletion"
    }

    private val typeService = ctx.ecosTypeService
    private val dataService = ctx.dataService
    private val contentService = ctx.contentService

    fun delete(
        recordIds: List<String>,
        txnRecordsInDeletion: MutableSet<EntityRef>,
        notifyParent: Boolean
    ): List<DelStatus> {

        val isForceDeletion = isTempStorage()

        for (recordId in recordIds) {

            val globalRef = ctx.getGlobalRef(recordId)
            if (!txnRecordsInDeletion.add(globalRef)) {
                continue
            }
            try {
                deleteRecord(recordId, isForceDeletion, txnRecordsInDeletion, notifyParent)
            } finally {
                txnRecordsInDeletion.remove(globalRef)
            }
        }

        return recordIds.map { DelStatus.OK }
    }

    private fun deleteRecord(
        recordId: String,
        isForceDeletion: Boolean,
        txnRecordsInDeletion: MutableSet<EntityRef>,
        notifyParent: Boolean
    ) {

        val entity = dataService.findByExtId(recordId) ?: return
        val entityGlobalRef = ctx.recordRefService.getEntityRefById(entity.refId)

        if (notifyParent) {

            val parentRefId = entity.attributes[RecordConstants.ATT_PARENT] as? Long
            val parentAttId = entity.attributes[RecordConstants.ATT_PARENT_ATT] as? Long
            val parentAtt = ctx.assocsService.getAttById(parentAttId ?: -1L)

            if (parentRefId != null && parentAtt.isNotBlank()) {
                val parentRef = ctx.recordRefService.getEntityRefById(parentRefId)
                if (parentRef.getAppName() != AppName.ALFRESCO &&
                    EntityRef.isNotEmpty(parentRef) &&
                    !txnRecordsInDeletion.contains(parentRef)
                ) {

                    ctx.recordsService.mutate(
                        parentRef,
                        mapOf(
                            RecMutAssocHandler.MUTATION_FROM_CHILD_FLAG to true,
                            "${OperationType.ATT_REMOVE.prefix}$parentAtt" to entityGlobalRef,
                            ASSOC_FORCE_DELETION_FLAG to isForceDeletion
                        )
                    )
                }
            }
        }
        val pageSize = 100
        fun findSrcAssocs(page: Int): List<Pair<EntityRef, String>> {
            val assocs = ctx.assocsService.getSourceAssocs(
                entity.refId,
                "",
                DbFindPage(pageSize * page, pageSize)
            ).entities.filter { !it.child }

            val assocSrcRefs = ctx.recordRefService.getEntityRefsByIdsMap(
                assocs.map { it.sourceId }
            )
            return assocs.mapNotNull {
                val ref = assocSrcRefs[it.sourceId]
                if (ref == null) {
                    null
                } else {
                    ref to it.attribute
                }
            }
        }

        // if user has permissions to delete node, then source assocs may be deleted in system context
        AuthContext.runAsSystem {
            var page = 0
            var sourceAssocs = findSrcAssocs(page)
            while (sourceAssocs.isNotEmpty()) {
                sourceAssocs.forEach {
                    if (!txnRecordsInDeletion.contains(it.first)) {
                        ctx.recordsService.mutate(
                            it.first,
                            mapOf(
                                "${OperationType.ATT_REMOVE.prefix}${it.second}" to entityGlobalRef,
                                ASSOC_FORCE_DELETION_FLAG to isForceDeletion
                            )
                        )
                    }
                }
                sourceAssocs = findSrcAssocs(++page)
            }
        }

        val meta = ctx.getEntityMeta(entity)
        val childrenIdsToDelete = LinkedHashSet<Long>()
        meta.allAttributes.forEach {
            if (DbRecordsUtils.isChildAssocAttribute(it.value)) {
                DbAttValueUtils.collectLongValues(entity.attributes[it.key], childrenIdsToDelete)
            }
        }

        ctx.remoteActionsClient?.deleteRemoteAssocs(ctx.tableCtx, meta.globalRef, isForceDeletion)

        if (childrenIdsToDelete.isNotEmpty()) {
            val refsToDelete = ctx.recordRefService.getEntityRefsByIds(childrenIdsToDelete.toList())
            ctx.recordsService.delete(refsToDelete)
        }
        if (isForceDeletion) {
            meta.allAttributes.forEach { (attId, attDef) ->
                if (attDef.type == AttributeType.CONTENT) {
                    DbAttValueUtils.forEachLongValue(entity.attributes[attId]) {
                        contentService!!.removeContent(it)
                    }
                }
            }
            dataService.forceDelete(entity)
        } else {
            dataService.delete(entity)
        }
        ctx.assocsService.removeAssocs(entity.refId, isForceDeletion)
        ctx.recEventsHandler.emitDeleteEvent(entity, meta)
    }

    private fun isTempStorage(): Boolean {
        val typeId = ctx.config.typeRef.getLocalId()
        if (typeId.isEmpty()) {
            return false
        }
        var isTempStorage = false
        var typeDef: TypeInfo? = typeService.getTypeInfoNotNull(typeId)
        var iterations = 5
        while (typeDef != null && --iterations > 0) {
            if (typeDef.id == DbEcosModelService.TYPE_ID_TEMP_FILE) {
                isTempStorage = true
                break
            }
            typeDef = typeService.getTypeInfo(typeDef.parentRef.getLocalId())
        }
        return isTempStorage
    }
}
