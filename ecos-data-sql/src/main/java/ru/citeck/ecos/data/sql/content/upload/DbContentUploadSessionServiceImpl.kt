package ru.citeck.ecos.data.sql.content.upload

import io.github.oshai.kotlinlogging.KotlinLogging
import ru.citeck.ecos.data.sql.context.DbSchemaContext
import ru.citeck.ecos.data.sql.repo.find.DbFindPage
import ru.citeck.ecos.data.sql.repo.find.DbFindQuery
import ru.citeck.ecos.data.sql.repo.find.DbFindSort
import ru.citeck.ecos.data.sql.service.DbDataService
import ru.citeck.ecos.data.sql.service.DbDataServiceConfig
import ru.citeck.ecos.data.sql.service.DbDataServiceImpl
import ru.citeck.ecos.records2.predicate.model.Predicates
import java.time.Instant
import java.util.UUID

class DbContentUploadSessionServiceImpl(
    schemaCtx: DbSchemaContext
) : DbContentUploadSessionService {

    companion object {
        private val log = KotlinLogging.logger {}
    }

    private val dataService: DbDataService<DbContentUploadSessionEntity> = DbDataServiceImpl(
        DbContentUploadSessionEntity::class.java,
        DbDataServiceConfig.create {
            withTable(DbContentUploadSessionEntity.TABLE)
            withStoreTableMeta(true)
        },
        schemaCtx
    )

    override fun create(entity: DbContentUploadSessionEntity): DbContentUploadSessionEntity {
        entity.extId = UUID.randomUUID().toString()
        val now = Instant.now()
        entity.created = now
        entity.modified = now
        return dataService.save(entity)
    }

    override fun findByExtId(extId: String): DbContentUploadSessionEntity? {
        return dataService.findByExtId(extId)
    }

    override fun countActiveByCreator(creatorRefId: Long, expiredBefore: Instant): Long {
        return dataService.getCount(
            Predicates.and(
                Predicates.eq(DbContentUploadSessionEntity.CREATOR, creatorRefId),
                Predicates.eq(DbContentUploadSessionEntity.STATUS, ContentUploadSessionStatus.ACTIVE.name),
                Predicates.ge(DbContentUploadSessionEntity.MODIFIED, expiredBefore)
            )
        )
    }

    override fun advanceOffset(
        extId: String,
        expectedOffset: Long,
        newOffset: Long,
        digestState: ByteArray,
        storageState: String
    ): Boolean {
        return dataService.updateByExtIdIfMatches(
            extId,
            mapOf(
                DbContentUploadSessionEntity.STATUS to ContentUploadSessionStatus.ACTIVE.name,
                DbContentUploadSessionEntity.CONFIRMED_OFFSET to expectedOffset
            ),
            mapOf(
                DbContentUploadSessionEntity.CONFIRMED_OFFSET to newOffset,
                DbContentUploadSessionEntity.DIGEST_STATE to digestState,
                DbContentUploadSessionEntity.STORAGE_STATE to storageState,
                DbContentUploadSessionEntity.MODIFIED to Instant.now()
            )
        )
    }

    override fun takeOverCompletion(extId: String, expectedModified: Instant): Boolean {
        return dataService.updateByExtIdIfMatches(
            extId,
            mapOf(
                DbContentUploadSessionEntity.STATUS to ContentUploadSessionStatus.COMPLETING.name,
                DbContentUploadSessionEntity.MODIFIED to expectedModified
            ),
            mapOf(
                DbContentUploadSessionEntity.STATUS to ContentUploadSessionStatus.COMPLETING.name,
                DbContentUploadSessionEntity.MODIFIED to Instant.now()
            )
        )
    }

    override fun releaseCompletion(extId: String, expectedModified: Instant, releasedModified: Instant): Boolean {
        return dataService.updateByExtIdIfMatches(
            extId,
            mapOf(
                DbContentUploadSessionEntity.STATUS to ContentUploadSessionStatus.COMPLETING.name,
                DbContentUploadSessionEntity.MODIFIED to expectedModified
            ),
            mapOf(DbContentUploadSessionEntity.MODIFIED to releasedModified)
        )
    }

    override fun updateStatus(
        extId: String,
        expectedStatus: ContentUploadSessionStatus,
        newStatus: ContentUploadSessionStatus,
        dataKey: String?,
        entityRef: String?,
        expectedModified: Instant?
    ): Boolean {
        return updateStatus(extId, expectedStatus.name, newStatus, dataKey, entityRef, expectedModified)
    }

    override fun updateStatus(
        extId: String,
        expectedStatus: String,
        newStatus: ContentUploadSessionStatus,
        dataKey: String?,
        entityRef: String?,
        expectedModified: Instant?
    ): Boolean {
        val newValues = LinkedHashMap<String, Any?>()
        newValues[DbContentUploadSessionEntity.STATUS] = newStatus.name
        newValues[DbContentUploadSessionEntity.MODIFIED] = Instant.now()
        if (dataKey != null) {
            newValues[DbContentUploadSessionEntity.DATA_KEY] = dataKey
        }
        if (entityRef != null) {
            newValues[DbContentUploadSessionEntity.ENTITY_REF] = entityRef
        }
        val expected = LinkedHashMap<String, Any?>()
        expected[DbContentUploadSessionEntity.STATUS] = expectedStatus
        if (expectedModified != null) {
            expected[DbContentUploadSessionEntity.MODIFIED] = expectedModified
        }
        return dataService.updateByExtIdIfMatches(extId, expected, newValues)
    }

    override fun retire(extId: String, expectedStatus: String): Boolean {
        return dataService.updateByExtIdIfMatches(
            extId,
            mapOf(DbContentUploadSessionEntity.STATUS to expectedStatus),
            mapOf(
                DbContentUploadSessionEntity.STATUS to ContentUploadSessionStatus.ABORTED.name,
                // the handles into the storage go, not just the status: see the interface
                DbContentUploadSessionEntity.STORAGE_STATE to "",
                DbContentUploadSessionEntity.DATA_KEY to "",
                DbContentUploadSessionEntity.MODIFIED to Instant.now()
            )
        )
    }

    override fun delete(extId: String) {
        dataService.delete(Predicates.eq(DbContentUploadSessionEntity.EXT_ID, extId))
    }

    override fun findExpired(expiredBefore: Instant, max: Int): List<DbContentUploadSessionEntity> {
        return dataService.find(
            DbFindQuery.create {
                withPredicate(Predicates.lt(DbContentUploadSessionEntity.MODIFIED, expiredBefore))
                withSortBy(listOf(DbFindSort(DbContentUploadSessionEntity.MODIFIED, true)))
            },
            DbFindPage(0, max)
        ).entities
    }

    override fun cleanupExpired(
        expiredBefore: Instant,
        batch: Int,
        abortAction: (DbContentUploadSessionEntity) -> Boolean
    ): Int {
        var deletedCount = 0
        for (entity in findExpired(expiredBefore, batch)) {
            // Per-row isolation: findExpired sorts oldest-first, so without this a single session
            // whose abortAction (or delete) throws would poison the whole batch on every subsequent
            // run, forever. A throw is treated exactly like `false` - the row stays for the next run.
            try {
                // Only DONE owns no storage-side upload any more - its object became a content
                // row. Everything else still may, ABORTED included: an ABORTED row survives
                // DbChunkedUploadService.abort only when the storage-side abort failed, which is
                // exactly the case that needs the retry. A status this code never wrote is handled
                // like a live one for the same reason. What a row owns is decided by abortAction
                // from the row itself (a retired row carries no handles at all - see retire), never
                // from the status here.
                val done = ContentUploadSessionStatus.ofOrNull(entity.status) == ContentUploadSessionStatus.DONE
                val canDelete = done || abortAction(entity)
                if (canDelete) {
                    delete(entity.extId)
                    deletedCount++
                }
            } catch (e: Exception) {
                log.warn(e) { "Cleanup of expired upload session '${entity.extId}' failed. It will be retried later" }
            }
        }
        return deletedCount
    }

    override fun createTableIfNotExists() {
        dataService.runMigrations(emptyList(), mock = false, diff = true)
    }

    override fun resetColumnsCache() {
        dataService.resetColumnsCache()
    }
}
