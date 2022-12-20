package ru.citeck.ecos.data.sql.records

import ru.citeck.ecos.data.sql.ecostype.DbEcosTypeService
import ru.citeck.ecos.data.sql.records.dao.DbRecordsDaoCtx
import ru.citeck.ecos.data.sql.records.dao.atts.DbRecord
import ru.citeck.ecos.data.sql.records.listener.DbRecordDeletedEvent
import ru.citeck.ecos.data.sql.records.utils.DbAttValueUtils
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
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

        // todo: add children deletion
        val typesInfo = HashMap<String, DbRecordDeleteTypeInfo>()
        for (recordId in recordIds) {
            dataService.findByExtId(recordId)?.let { entity ->

                val parentRefId = entity.attributes[RecordConstants.ATT_PARENT] as? Long
                val parentAtt = entity.attributes[RecordConstants.ATT_PARENT_ATT] as? String

                if (parentRefId != null && !parentAtt.isNullOrBlank()) {
                    val childRef = ctx.recordRefService.getRecordRefById(entity.refId)
                    val parentRef = ctx.recordRefService.getRecordRefById(parentRefId)
                    if (parentRef.getAppName() != AppName.ALFRESCO && EntityRef.isEmpty(parentRef)) {
                        ctx.recordsService.mutate(
                            parentRef,
                            mapOf(
                                "att_rem_$parentAtt" to childRef
                            )
                        )
                    }
                }

                if (forceDeleteRequired) {
                    val info = typesInfo.computeIfAbsent(entity.type) { getRecordTypeInfo(it) }
                    info.contentAtts.forEach { contentAtt ->
                        DbAttValueUtils.forEachLongValue(entity.attributes[contentAtt]) {
                            contentService!!.removeContent(it)
                        }
                    }
                    dataService.forceDelete(entity)
                } else {
                    dataService.delete(entity)
                }
                val typeInfo = typeService.getTypeInfo(entity.type)
                if (typeInfo != null) {
                    val event = DbRecordDeletedEvent(DbRecord(ctx, entity), typeInfo)
                    ctx.listeners.forEach {
                        it.onDeleted(event)
                    }
                }
            }
        }

        return recordIds.map { DelStatus.OK }
    }

    private fun isTempStorage(): Boolean {
        val typeId = ctx.config.typeRef.id
        if (typeId.isEmpty()) {
            return false
        }
        var isTempStorage = false
        var typeDef: TypeInfo? = typeService.getTypeInfoNotNull(typeId)
        var iterations = 5
        while (typeDef != null && --iterations > 0) {
            if (typeDef.id == DbEcosTypeService.TYPE_ID_TEMP_FILE) {
                isTempStorage = true
                break
            }
            typeDef = typeService.getTypeInfo(typeDef.parentRef.getLocalId())
        }
        return isTempStorage
    }

    private fun getRecordTypeInfo(typeId: String): DbRecordDeleteTypeInfo {
        val contentAtts = mutableListOf<String>()
        val childAssocs = mutableListOf<AttributeDef>()
        forEachAttribute(typeId) {
            if (it.type == AttributeType.CONTENT) {
                contentAtts.add(it.id)
            }
            if (it.type == AttributeType.ASSOC) {
                if (it.config["child"].asBoolean(false)) {
                    childAssocs.add(it)
                }
            }
        }
        return DbRecordDeleteTypeInfo(contentAtts, childAssocs)
    }

    private fun forEachAttribute(typeId: String, action: (AttributeDef) -> Unit) {
        val typeInfo = typeService.getTypeInfoNotNull(typeId)
        typeInfo.model.attributes.forEach { action(it) }
        typeInfo.model.systemAttributes.forEach { action(it) }
    }

    private class DbRecordDeleteTypeInfo(
        val contentAtts: List<String>,
        val childAssocs: List<AttributeDef>
    )
}
