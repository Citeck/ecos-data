package ru.citeck.ecos.data.sql.records.refs

import io.github.oshai.kotlinlogging.KotlinLogging
import ru.citeck.ecos.data.sql.context.DbSchemaContext
import ru.citeck.ecos.data.sql.service.DbDataServiceConfig
import ru.citeck.ecos.data.sql.service.DbDataServiceImpl
import ru.citeck.ecos.data.sql.service.DbIdMappingService
import ru.citeck.ecos.txn.lib.TxnContext
import ru.citeck.ecos.webapp.api.entity.EntityRef
import ru.citeck.ecos.webapp.api.entity.toEntityRef
import java.time.Instant
import java.util.UUID

class DbRecordRefService(
    private val appName: String,
    schemaCtx: DbSchemaContext
) : DbIdMappingService<DbRecordRefEntity>(
    DbDataServiceImpl(
        DbRecordRefEntity::class.java,
        DbDataServiceConfig.create {
            withTable(DbRecordRefEntity.TABLE)
        },
        schemaCtx
    )
) {

    companion object {
        private val log = KotlinLogging.logger {}
    }

    private val moveHistDataService = DbDataServiceImpl(
        DbRecordRefMoveHistoryEntity::class.java,
        DbDataServiceConfig.create {
            withTable(DbRecordRefMoveHistoryEntity.TABLE)
        },
        schemaCtx
    )

    fun migrateRefIfExists(fromRef: EntityRef, toRef: EntityRef, migratedBy: Long): Boolean {
        val fromRefEntity = findByExtId(
            fixEntityRef(fromRef).toString()
        ) ?: return false
        return moveRef(fromRefEntity, toRef, migratedBy)
    }

    fun getMovedToRef(fromRef: EntityRef): EntityRef {
        val movedToId = findByExtId(fromRef.toString())?.movedTo ?: return EntityRef.EMPTY
        return getEntityRefById(movedToId)
    }

    /**
     * Migrates the record ref and returns the before/after values.
     * Returns null if migration is not required.
     */
    fun moveRef(fromRefId: Long, toRef: EntityRef, migratedBy: Long): Pair<EntityRef, EntityRef>? {
        val fromRefEntity = findById(fromRefId) ?: error(
            "Ref with id $fromRefId is not registered. " +
                "Migration to $toRef is not allowed."
        )
        val refBefore = EntityRef.valueOf(fromRefEntity.extId)
        if (!moveRef(fromRefEntity, toRef, migratedBy)) {
            return null
        }
        return refBefore to EntityRef.valueOf(fromRefEntity.extId)
    }

    private fun moveRef(fromRefEntity: DbRecordRefEntity, toRef: EntityRef, movedBy: Long): Boolean {

        val fromRef = EntityRef.valueOf(fromRefEntity.extId)
        val fixedToRef = fixEntityRef(
            toRef.withDefault(
                appName = fromRef.getAppName(),
                sourceId = fromRef.getSourceId()
            )
        )

        val targetRefStr = fixedToRef.toString()
        if (fromRefEntity.extId == targetRefStr) {
            // nothing to migrate
            return false
        }
        log.info { "Migrate ref from $fromRef to $fixedToRef" }

        val toRefEntity = findByExtId(fixedToRef.toString())
        if (toRefEntity != null) {
            // In db already registered new ref, but maybe we want
            // to return ref back to previous value? Let's check.
            if (toRefEntity.movedTo != fromRefEntity.id) {
                error(
                    "Ref $targetRefStr is already registered. " +
                        "Migration from '${fromRefEntity.extId}' is not allowed."
                )
            }

            val fromRefExtId = fromRefEntity.extId
            val toRefExtId = toRefEntity.extId

            // avoid unique extId constraint limit
            toRefEntity.extId = UUID.randomUUID().toString()
            save(toRefEntity)

            fromRefEntity.extId = toRefExtId
            save(fromRefEntity)

            toRefEntity.extId = fromRefExtId
            save(toRefEntity)

            invalidateById(fromRefEntity.id)
            invalidateById(toRefEntity.id)
        } else {

            val fromRefStr = fromRefEntity.extId

            fromRefEntity.extId = targetRefStr
            save(fromRefEntity)

            val oldRefEntity = DbRecordRefEntity()
            oldRefEntity.extId = fromRef.toString()
            oldRefEntity.movedTo = fromRefEntity.id
            save(oldRefEntity)

            val historyEntity = DbRecordRefMoveHistoryEntity()
            historyEntity.movedBy = movedBy
            historyEntity.movedFrom = fromRefStr
            historyEntity.movedTo = targetRefStr
            historyEntity.movedAt = Instant.now()
            historyEntity.refId = fromRefEntity.id

            moveHistDataService.save(historyEntity)

            invalidateById(fromRefEntity.id)
        }

        return true
    }

    /**
     * Get or create record identifier for reference
     */
    fun getOrCreateIdByEntityRef(ref: EntityRef): Long {
        return getOrCreateId(fixEntityRef(ref).toString())
    }

    /**
     * Get or create record references
     */
    fun getOrCreateIdByEntityRefsMap(refs: Collection<EntityRef>): Map<EntityRef, Long> {
        val fixedRefs = refs.map { fixEntityRef(it) }
        val idsByExtId = getOrCreateIds(fixedRefs.map { it.toString() })
        val result = LinkedHashMap<EntityRef, Long>()
        for (ref in fixedRefs) {
            idsByExtId[ref.toString()]?.let { result[ref] = it }
        }
        return result
    }

    /**
     * Get or create record references
     */
    fun getOrCreateIdByEntityRefs(refs: List<EntityRef>): List<Long> {
        val fixedRefs = refs.map { fixEntityRef(it) }
        val idsByExtId = getOrCreateIds(fixedRefs.map { it.toString() })
        return fixedRefs.map { idsByExtId[it.toString()] ?: error("Ref not found: $it") }
    }

    fun getIdByEntityRef(ref: EntityRef): Long {
        return getIdByExtId(fixEntityRef(ref).toString())
    }

    /**
     * Get record identifiers for references
     */
    fun getIdByEntityRefsMap(refs: Collection<EntityRef>): Map<EntityRef, Long> {
        val refsList = refs as? List<EntityRef> ?: refs.toList()
        val fixedRefs = refsList.map { fixEntityRef(it) }
        val idsByExtId = getIdsByExtIds(fixedRefs.map { it.toString() })
        val result = LinkedHashMap<EntityRef, Long>()
        for (ref in fixedRefs) {
            val id = idsByExtId[ref.toString()]
            if (id != null) {
                result[ref] = id
            }
        }
        return result
    }

    /**
     * Get record identifiers for references or -1 if reference is not registered
     */
    fun getIdByEntityRefs(refs: List<EntityRef>): List<Long> {
        val fixedRefs = refs.map { fixEntityRef(it) }
        val idsByExtId = getIdsByExtIds(fixedRefs.map { it.toString() })
        return fixedRefs.map { idsByExtId[it.toString()] ?: -1 }
    }

    fun getEntityRefsByIdsMap(ids: Collection<Long>): Map<Long, EntityRef> {
        return getExtIdsByIds(ids).mapValues { EntityRef.valueOf(it.value.toEntityRef()) }
    }

    fun getEntityRefById(id: Long): EntityRef {
        val extId = getExtIdById(id)
        if (extId.isEmpty()) {
            error("Ref doesn't found for id $id")
        }
        return EntityRef.valueOf(extId.toEntityRef())
    }

    fun getEntityRefsByIds(ids: List<Long>): List<EntityRef> {
        val extIdsByIds = getExtIdsByIds(ids)
        return ids.map {
            val extId = extIdsByIds[it] ?: error("Ref doesn't found for id $it")
            EntityRef.valueOf(extId.toEntityRef())
        }
    }

    fun createTableIfNotExists() {
        TxnContext.doInTxn {
            runMigrations(mock = false, diff = true)
        }
    }

    private fun fixEntityRef(entityRef: EntityRef): EntityRef {
        if (entityRef.getAppName().isNotBlank()) {
            return entityRef
        }
        if (entityRef.getLocalId().startsWith("workspace://SpacesStore/")) {
            return EntityRef.create("alfresco", "", entityRef.getLocalId())
        }
        return entityRef.withDefaultAppName(appName)
    }
}
