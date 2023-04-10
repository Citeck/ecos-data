package ru.citeck.ecos.data.sql.records.dao.events

import ru.citeck.ecos.data.sql.records.dao.DbEntityMeta
import ru.citeck.ecos.data.sql.records.dao.DbRecordsDaoCtx
import ru.citeck.ecos.data.sql.records.dao.atts.DbRecord
import ru.citeck.ecos.data.sql.records.listener.*
import ru.citeck.ecos.data.sql.repo.entity.DbEntity
import ru.citeck.ecos.model.lib.status.constants.StatusConstants
import ru.citeck.ecos.model.lib.status.dto.StatusDef
import ru.citeck.ecos.model.lib.type.dto.TypeInfo

class DbRecEventsHandler(private val ctx: DbRecordsDaoCtx) {

    fun emitDeleteEvent(entity: DbEntity, meta: DbEntityMeta) {
        val event = DbRecordDeletedEvent(
            meta.localRef,
            meta.globalRef,
            meta.isDraft,
            DbRecord(ctx, entity),
            meta.typeInfo,
            meta.aspectsInfo
        )
        ctx.listeners.forEach {
            it.onDeleted(event)
        }
    }

    fun emitEventsAfterMutation(before: DbEntity, after: DbEntity, isNewRecord: Boolean) {

        if (ctx.listeners.isEmpty()) {
            return
        }

        val meta = ctx.getEntityMeta(after)
        val typeInfo = meta.typeInfo
        val aspectsInfo = meta.aspectsInfo
        val localRef = meta.localRef
        val globalRef = meta.globalRef
        val isDraft = meta.isDraft

        if (isNewRecord) {
            val event = DbRecordCreatedEvent(
                localRef,
                globalRef,
                isDraft,
                DbRecord(ctx, after),
                typeInfo,
                aspectsInfo
            )
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
            val recChangedEvent = DbRecordChangedEvent(
                localRef,
                globalRef,
                isDraft,
                recAfter,
                typeInfo,
                aspectsInfo,
                attsBefore,
                attsAfter
            )
            ctx.listeners.forEach {
                it.onChanged(recChangedEvent)
            }
        }

        val contentBefore = recBefore.getDefaultContent()
        val contentAfter = recAfter.getDefaultContent()

        if (contentBefore != null || contentAfter != null) {

            if (attsBefore[DbRecord.ATT_CONTENT_VERSION] != attsAfter[DbRecord.ATT_CONTENT_VERSION] ||
                contentBefore?.getContentDbData()?.getUri() != contentAfter?.getContentDbData()?.getUri()
            ) {

                val contentChangedEvent = DbRecordContentChangedEvent(
                    localRef,
                    globalRef,
                    isDraft,
                    recAfter,
                    typeInfo,
                    aspectsInfo,
                    contentBefore,
                    contentAfter,
                    attsBefore,
                    attsAfter
                )
                ctx.listeners.forEach {
                    it.onContentChanged(contentChangedEvent)
                }
            }
        }

        val statusBefore = ctx.recordsService.getAtt(recBefore, StatusConstants.ATT_STATUS_STR).asText()
        val statusAfter = ctx.recordsService.getAtt(recAfter, StatusConstants.ATT_STATUS_STR).asText()

        if (statusBefore != statusAfter) {

            val statusBeforeDef = getStatusDef(statusBefore, typeInfo)
            val statusAfterDef = getStatusDef(statusAfter, typeInfo)

            val statusChangedEvent = DbRecordStatusChangedEvent(
                localRef,
                globalRef,
                isDraft,
                recAfter,
                typeInfo,
                aspectsInfo,
                statusBeforeDef,
                statusAfterDef
            )
            ctx.listeners.forEach {
                it.onStatusChanged(statusChangedEvent)
            }
        }

        val isDraftBefore = before.attributes[DbRecord.COLUMN_IS_DRAFT.name] as? Boolean
        val isDraftAfter = after.attributes[DbRecord.COLUMN_IS_DRAFT.name] as? Boolean
        if (isDraftBefore != null && isDraftAfter != null && isDraftBefore != isDraftAfter) {
            val event = DbRecordDraftStatusChangedEvent(
                localRef,
                globalRef,
                isDraft,
                recAfter,
                typeInfo,
                aspectsInfo,
                isDraftBefore,
                isDraftAfter
            )
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
