package ru.citeck.ecos.data.sql.records

import ru.citeck.ecos.data.sql.ecostype.DbEcosModelService
import ru.citeck.ecos.data.sql.records.dao.DbRecordsDaoCtx
import ru.citeck.ecos.data.sql.records.dao.mutate.operation.OperationType
import ru.citeck.ecos.data.sql.records.utils.DbAttValueUtils
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType
import ru.citeck.ecos.model.lib.type.dto.TypeInfo
import ru.citeck.ecos.records2.RecordConstants
import ru.citeck.ecos.records3.record.dao.delete.DelStatus
import ru.citeck.ecos.webapp.api.constants.AppName
import ru.citeck.ecos.webapp.api.entity.EntityRef

class DbRecordsDeleteDao(var ctx: DbRecordsDaoCtx) {

    private val typeService = ctx.ecosTypeService
    private val dataService = ctx.dataService
    private val contentService = ctx.contentService

    fun delete(recordIds: List<String>): List<DelStatus> {

        val forceDeleteRequired = isTempStorage()

        for (recordId in recordIds) {
            dataService.findByExtId(recordId)?.let { entity ->

                val parentRefId = entity.attributes[RecordConstants.ATT_PARENT] as? Long
                val parentAtt = entity.attributes[RecordConstants.ATT_PARENT_ATT] as? String

                if (parentRefId != null && !parentAtt.isNullOrBlank()) {
                    val childRef = ctx.recordRefService.getEntityRefById(entity.refId)
                    val parentRef = ctx.recordRefService.getEntityRefById(parentRefId)
                    if (parentRef.getAppName() != AppName.ALFRESCO && EntityRef.isEmpty(parentRef)) {
                        ctx.recordsService.mutate(
                            parentRef,
                            mapOf(
                                "${OperationType.ATT_REMOVE.prefix}$parentAtt" to childRef
                            )
                        )
                    }
                }
                val meta = ctx.getEntityMeta(entity)
                val idsToDelete = LinkedHashSet<Long>()
                meta.allAttributes.forEach {
                    if (DbRecordsUtils.isChildAssocAttribute(it.value)) {
                        DbAttValueUtils.collectLongValues(entity.attributes[it.key], idsToDelete)
                    }
                }
                if (idsToDelete.isNotEmpty()) {
                    val refsToDelete = ctx.recordRefService.getEntityRefsByIds(idsToDelete.toList())
                    ctx.recordsService.delete(refsToDelete)
                }
                if (forceDeleteRequired) {
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
