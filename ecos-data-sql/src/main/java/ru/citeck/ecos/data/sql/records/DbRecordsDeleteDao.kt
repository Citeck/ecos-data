package ru.citeck.ecos.data.sql.records

import ru.citeck.ecos.commons.data.ObjectData
import ru.citeck.ecos.data.sql.ecostype.DbEcosModelService
import ru.citeck.ecos.data.sql.records.dao.DbRecordsDaoCtx
import ru.citeck.ecos.data.sql.records.dao.mutate.RecMutAssocHandler
import ru.citeck.ecos.data.sql.records.dao.mutate.operation.OperationType
import ru.citeck.ecos.data.sql.records.utils.DbAttValueUtils
import ru.citeck.ecos.data.sql.repo.find.DbFindPage
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType
import ru.citeck.ecos.model.lib.type.dto.TypeInfo
import ru.citeck.ecos.records2.RecordConstants
import ru.citeck.ecos.records3.record.atts.dto.RecordAtts
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

    fun delete(recordIds: List<String>): List<DelStatus> {

        val isForceDeletion = isTempStorage()

        for (recordId in recordIds) {

            val entity = dataService.findByExtId(recordId) ?: continue
            val entityGlobalRef = ctx.recordRefService.getEntityRefById(entity.refId)

            val parentRefId = entity.attributes[RecordConstants.ATT_PARENT] as? Long
            val parentAtt = entity.attributes[RecordConstants.ATT_PARENT_ATT] as? String

            if (parentRefId != null && !parentAtt.isNullOrBlank()) {
                val parentRef = ctx.recordRefService.getEntityRefById(parentRefId)
                if (parentRef.getAppName() != AppName.ALFRESCO && EntityRef.isNotEmpty(parentRef)) {
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

            var page = 0
            var sourceAssocs = findSrcAssocs(page)
            while (sourceAssocs.isNotEmpty()) {
                sourceAssocs.forEach {
                    ctx.recordsService.mutate(
                        it.first,
                        mapOf(
                            "${OperationType.ATT_REMOVE.prefix}${it.second}" to entityGlobalRef,
                            ASSOC_FORCE_DELETION_FLAG to isForceDeletion
                        )
                    )
                }
                sourceAssocs = findSrcAssocs(++page)
            }

            val meta = ctx.getEntityMeta(entity)
            val childrenIdsToDelete = LinkedHashSet<Long>()
            meta.allAttributes.forEach {
                if (DbRecordsUtils.isChildAssocAttribute(it.value)) {
                    DbAttValueUtils.collectLongValues(entity.attributes[it.key], childrenIdsToDelete)
                }
            }

            if (childrenIdsToDelete.isNotEmpty()) {
                val refsToDelete = ctx.recordRefService.getEntityRefsByIds(childrenIdsToDelete.toList())
                ctx.recordsService.mutate(
                    refsToDelete.map {
                        RecordAtts(
                            it,
                            ObjectData.create()
                                .set(RecordConstants.ATT_PARENT, null)
                                .set(RecordConstants.ATT_PARENT_ATT, null)
                                .set(RecMutAssocHandler.MUTATION_FROM_PARENT_FLAG, true)
                        )
                    }
                )
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

            ctx.recEventsHandler.emitDeleteEvent(entity, meta)
        }

        return recordIds.map { DelStatus.OK }
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
