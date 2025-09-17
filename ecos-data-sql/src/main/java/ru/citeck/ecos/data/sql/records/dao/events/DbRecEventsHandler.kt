package ru.citeck.ecos.data.sql.records.dao.events

import ru.citeck.ecos.data.sql.records.DbRecordsUtils
import ru.citeck.ecos.data.sql.records.assocs.DbAssocRefsDiff
import ru.citeck.ecos.data.sql.records.dao.DbEntityMeta
import ru.citeck.ecos.data.sql.records.dao.DbRecordsDaoCtx
import ru.citeck.ecos.data.sql.records.dao.atts.DbRecord
import ru.citeck.ecos.data.sql.records.dao.atts.content.DbDefaultLocalContentValue
import ru.citeck.ecos.data.sql.records.listener.*
import ru.citeck.ecos.data.sql.repo.entity.DbEntity
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.status.constants.StatusConstants
import ru.citeck.ecos.model.lib.status.dto.StatusDef
import ru.citeck.ecos.model.lib.type.dto.TypeInfo
import ru.citeck.ecos.records2.RecordConstants
import ru.citeck.ecos.records3.record.atts.value.AttValue
import ru.citeck.ecos.webapp.api.entity.toEntityRef

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

    fun emitEventsAfterMutation(
        before: DbEntity,
        after: DbEntity,
        meta: DbEntityMeta,
        isNewRecord: Boolean,
        assocsDiff: List<DbAssocRefsDiff>
    ) {

        if (ctx.listeners.isEmpty()) {
            return
        }

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

        val nonSystemChangedAttsData = getChangedAttsData(
            recBefore,
            recAfter,
            assocsDiff,
            meta.nonSystemAtts
        )
        val systemChangedAttsData = getChangedAttsData(
            recBefore,
            recAfter,
            assocsDiff,
            meta.systemAtts
        )

        if (nonSystemChangedAttsData.hasDiff() || systemChangedAttsData.hasDiff()) {
            val recChangedEvent = DbRecordChangedEvent(
                localRef,
                globalRef,
                isDraft,
                recAfter,
                typeInfo,
                aspectsInfo,
                nonSystemChangedAttsData.before,
                nonSystemChangedAttsData.after,
                nonSystemChangedAttsData.assocs,
                systemChangedAttsData.before,
                systemChangedAttsData.after,
                systemChangedAttsData.assocs
            )
            ctx.listeners.forEach {
                it.onChanged(recChangedEvent)
            }
        }

        val nonSystemAttsBefore = nonSystemChangedAttsData.before
        val nonSystemAttsAfter = nonSystemChangedAttsData.after

        val contentBefore = recBefore.getDefaultContent("")
        val contentAfter = recAfter.getDefaultContent("")

        fun isEqualContentData(before: AttValue?, after: AttValue?): Boolean {
            val localDataBefore = if (before is DbDefaultLocalContentValue) {
                before.getContentDbData()
            } else {
                null
            }
            val localDataAfter = if (after is DbDefaultLocalContentValue) {
                after.getContentDbData()
            } else {
                null
            }
            return localDataBefore?.getDataKey() == localDataAfter?.getDataKey() &&
                localDataBefore?.getStorageRef() == localDataAfter?.getStorageRef()
        }

        if (contentBefore != null || contentAfter != null) {

            if (nonSystemAttsBefore[DbRecord.ATT_CONTENT_VERSION] != nonSystemAttsAfter[DbRecord.ATT_CONTENT_VERSION] ||
                !isEqualContentData(contentBefore, contentAfter)
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
                    nonSystemAttsBefore,
                    nonSystemAttsAfter
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
        val parentBefore = before.attributes[RecordConstants.ATT_PARENT] as? Long
        val parentAttBefore = before.attributes[RecordConstants.ATT_PARENT_ATT] as? Long
        val parentAfter = after.attributes[RecordConstants.ATT_PARENT] as? Long
        val parentAttAfter = after.attributes[RecordConstants.ATT_PARENT_ATT] as? Long

        if (parentAfter != parentBefore || parentAttAfter != parentAttBefore) {
            val atts = ctx.recordsService.getAtts(
                listOf(recBefore, recAfter),
                mapOf(
                    "parentRef" to "_parent?id",
                    "parentAtt" to "_parentAtt"
                )
            )
            val event = DbRecordParentChangedEvent(
                localRef,
                globalRef,
                isDraft,
                recAfter,
                typeInfo,
                aspectsInfo,
                parentBefore = atts[0].getAtt("parentRef").asText().toEntityRef(),
                parentAfter = atts[1].getAtt("parentRef").asText().toEntityRef(),
                parentAttBefore = atts[0].getAtt("parentAtt").asText(),
                parentAttAfter = atts[1].getAtt("parentAtt").asText(),
            )
            ctx.listeners.forEach {
                it.onParentChanged(event)
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

    private fun getChangedAttsData(
        recBefore: DbRecord,
        recAfter: DbRecord,
        assocsDiff: List<DbAssocRefsDiff>,
        attributes: Map<String, AttributeDef>
    ): ChangedAttsData {

        if (attributes.isEmpty()) {
            return ChangedAttsData.EMPTY
        }

        val attsBefore = mutableMapOf<String, Any?>()
        val attsAfter = mutableMapOf<String, Any?>()

        attributes.values.forEach {
            if (!DbRecordsUtils.isAssocLikeAttribute(it)) {
                attsBefore[it.id] = recBefore.getAtt(it.id)
                attsAfter[it.id] = recAfter.getAtt(it.id)
            }
        }

        val filteredAssocsDiff = assocsDiff.filter {
            attributes.containsKey(it.assocId)
        }

        return ChangedAttsData(attsBefore, attsAfter, filteredAssocsDiff)
    }

    private fun getStatusDef(id: String, typeInfo: TypeInfo): StatusDef {
        if (id.isBlank()) {
            return StatusDef.create {}
        }
        return typeInfo.model.statuses.firstOrNull { it.id == id } ?: StatusDef.create {
            withId(id)
        }
    }

    private class ChangedAttsData(
        val before: Map<String, Any?>,
        val after: Map<String, Any?>,
        val assocs: List<DbAssocRefsDiff>
    ) {
        companion object {
            val EMPTY = ChangedAttsData(emptyMap(), emptyMap(), emptyList())
        }

        fun hasDiff(): Boolean {
            return before != after || assocs.isNotEmpty()
        }

        fun isEmpty(): Boolean {
            return before.isEmpty() && after.isEmpty() && assocs.isEmpty()
        }
    }
}
