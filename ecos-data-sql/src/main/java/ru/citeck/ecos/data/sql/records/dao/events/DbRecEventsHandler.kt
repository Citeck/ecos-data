package ru.citeck.ecos.data.sql.records.dao.events

import ru.citeck.ecos.data.sql.records.dao.DbRecordsDaoCtx
import ru.citeck.ecos.data.sql.records.dao.atts.DbRecord
import ru.citeck.ecos.data.sql.records.listener.DbRecordChangedEvent
import ru.citeck.ecos.data.sql.records.listener.DbRecordCreatedEvent
import ru.citeck.ecos.data.sql.records.listener.DbRecordDraftStatusChangedEvent
import ru.citeck.ecos.data.sql.records.listener.DbRecordStatusChangedEvent
import ru.citeck.ecos.data.sql.repo.entity.DbEntity
import ru.citeck.ecos.model.lib.status.constants.StatusConstants
import ru.citeck.ecos.model.lib.status.dto.StatusDef
import ru.citeck.ecos.model.lib.type.dto.TypeInfo

class DbRecEventsHandler(private val ctx: DbRecordsDaoCtx) {

    fun emitEventsAfterMutation(before: DbEntity, after: DbEntity, isNewRecord: Boolean) {

        if (ctx.listeners.isEmpty()) {
            return
        }

        val typeInfo = ctx.ecosTypeService.getTypeInfo(after.type) ?: error("Entity with unknown type: " + after.type)

        if (isNewRecord) {
            val event = DbRecordCreatedEvent(DbRecord(ctx, after), typeInfo)
            ctx.listeners.forEach {
                it.onCreated(event)
            }
            return
        }

        val recBefore = DbRecord(ctx, before)
        val recAfter = DbRecord(ctx, after)
        val attsBefore = mutableMapOf<String, Any?>()
        val attsAfter = mutableMapOf<String, Any?>()

        val attsDef = typeInfo.model.attributes
        attsDef.forEach {
            attsBefore[it.id] = recBefore.getAtt(it.id)
            attsAfter[it.id] = recAfter.getAtt(it.id)
        }

        if (attsBefore != attsAfter) {
            val recChangedEvent = DbRecordChangedEvent(recAfter, typeInfo, attsBefore, attsAfter)
            ctx.listeners.forEach {
                it.onChanged(recChangedEvent)
            }
        }

        val statusBefore = ctx.recordsService.getAtt(recBefore, StatusConstants.ATT_STATUS_STR).asText()
        val statusAfter = ctx.recordsService.getAtt(recAfter, StatusConstants.ATT_STATUS_STR).asText()

        if (statusBefore != statusAfter) {

            val statusBeforeDef = getStatusDef(statusBefore, typeInfo)
            val statusAfterDef = getStatusDef(statusAfter, typeInfo)

            val statusChangedEvent = DbRecordStatusChangedEvent(recAfter, typeInfo, statusBeforeDef, statusAfterDef)
            ctx.listeners.forEach {
                it.onStatusChanged(statusChangedEvent)
            }
        }

        val isDraftBefore = before.attributes[DbRecord.COLUMN_IS_DRAFT.name] as? Boolean
        val isDraftAfter = after.attributes[DbRecord.COLUMN_IS_DRAFT.name] as? Boolean
        if (isDraftBefore != null && isDraftAfter != null && isDraftBefore != isDraftAfter) {
            val event = DbRecordDraftStatusChangedEvent(recAfter, typeInfo, isDraftBefore, isDraftAfter)
            ctx.listeners.forEach {
                it.onDraftStatusChanged(event)
            }
        }
    }

    private fun getStatusDef(id: String, typeInfo: TypeInfo): StatusDef {
        if (id.isBlank()) {
            return StatusDef.create {}
        }
        return typeInfo.model.statuses.firstOrNull { it.id == id } ?: StatusDef.create {
            withId(id)
        }
    }
}
