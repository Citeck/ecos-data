package ru.citeck.ecos.data.sql.records.dao.events

import ru.citeck.ecos.data.sql.records.dao.DbRecordsDaoCtx
import ru.citeck.ecos.data.sql.records.dao.atts.DbRecord
import ru.citeck.ecos.data.sql.records.listener.*
import ru.citeck.ecos.data.sql.repo.entity.DbEntity
import ru.citeck.ecos.model.lib.aspect.dto.AspectInfo
import ru.citeck.ecos.model.lib.status.constants.StatusConstants
import ru.citeck.ecos.model.lib.status.dto.StatusDef
import ru.citeck.ecos.model.lib.type.dto.TypeInfo
import ru.citeck.ecos.webapp.api.entity.EntityRef

class DbRecEventsHandler(private val ctx: DbRecordsDaoCtx) {

    private fun getLongsList(value: Any?): List<Long> {
        value ?: return emptyList()
        if (value is Long) {
            return listOf(value)
        }
        if (value is Iterable<*>) {
            return value.mapNotNull { it as? Long }
        }
        return emptyList()
    }

    fun emitDeleteEvent(entity: DbEntity) {
        val meta = getEntityMeta(entity)
        val event = DbRecordDeletedEvent(
            meta.localRef,
            meta.globalRef,
            meta.draft,
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

        val (
            typeInfo,
            aspectsInfo,
            localRef,
            globalRef,
            isDraft
        ) = getEntityMeta(after)

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
            if (contentBefore?.getContentDbData()?.getUri() != contentAfter?.getContentDbData()?.getUri()) {
                val contentChangedEvent = DbRecordContentChangedEvent(
                    localRef,
                    globalRef,
                    isDraft,
                    recAfter,
                    typeInfo,
                    aspectsInfo,
                    contentBefore,
                    contentAfter
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

    private fun getEntityMeta(entity: DbEntity): EntityMeta {

        val typeInfo = ctx.ecosTypeService.getTypeInfoNotNull(entity.type)

        val aspectsIds = getLongsList(entity.attributes[DbRecord.ATT_ASPECTS])
        val aspectsRefs = ctx.recordRefService.getEntityRefsByIds(aspectsIds).toMutableSet()
        typeInfo.aspects.forEach {
            aspectsRefs.add(it.ref)
        }
        val aspectsInfo = ctx.ecosTypeService.getAspectsInfo(aspectsRefs)

        val localRef = ctx.getLocalRef(entity.extId)
        val globalRef = ctx.getGlobalRef(entity.extId)

        val isDraft = entity.attributes[DbRecord.COLUMN_IS_DRAFT.name] == true

        return EntityMeta(typeInfo, aspectsInfo, localRef, globalRef, isDraft)
    }

    private fun getStatusDef(id: String, typeInfo: TypeInfo): StatusDef {
        if (id.isBlank()) {
            return StatusDef.create {}
        }
        return typeInfo.model.statuses.firstOrNull { it.id == id } ?: StatusDef.create {
            withId(id)
        }
    }

    private data class EntityMeta(
        val typeInfo: TypeInfo,
        val aspectsInfo: List<AspectInfo>,
        val localRef: EntityRef,
        val globalRef: EntityRef,
        val draft: Boolean
    )
}
