package ru.citeck.ecos.data.sql.test.content

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import ru.citeck.ecos.context.lib.ctx.GlobalEcosContext
import ru.citeck.ecos.data.sql.content.upload.ContentUploadSessionStatus
import ru.citeck.ecos.data.sql.content.upload.DbContentUploadSessionEntity
import ru.citeck.ecos.data.sql.content.upload.DbContentUploadSessionService
import ru.citeck.ecos.data.sql.context.DbDataSourceContext
import ru.citeck.ecos.data.sql.context.DbSchemaContext
import ru.citeck.ecos.data.sql.datasource.DbDataSource
import ru.citeck.ecos.data.sql.domain.migration.DbMigrationService
import ru.citeck.ecos.data.sql.service.DbDataServiceConfig
import ru.citeck.ecos.data.sql.service.DbDataServiceFactory
import ru.citeck.ecos.data.sql.service.DbDataServiceImpl
import ru.citeck.ecos.test.commons.EcosWebAppApiMock
import ru.citeck.ecos.txn.lib.TxnContext
import ru.citeck.ecos.txn.lib.manager.EcosTxnProps
import ru.citeck.ecos.txn.lib.manager.TransactionManagerImpl
import java.time.Duration
import java.time.Instant
import java.util.concurrent.atomic.AtomicLong

/**
 * Backend-AGNOSTIC contract test for [DbContentUploadSessionService]: create, atomic conditional
 * updates ([DbContentUploadSessionService.advanceOffset] / [DbContentUploadSessionService.updateStatus]),
 * expiry lookup and creator-scoped counting. Concrete subclasses live in each backend module:
 *  - `ecos-data-sql-pg`  -> PG / Testcontainers
 *  - `ecos-data-inmem`   -> the in-memory backend
 */
abstract class UploadSessionServiceContractTest {

    companion object {
        private val creatorCounter = AtomicLong()
    }

    private lateinit var schemaCtx: DbSchemaContext

    /**
     * Create the storage backend factory (the single seam that selects the backend).
     */
    protected abstract fun createDataServiceFactory(): DbDataServiceFactory

    /**
     * Create a fresh data source compatible with the factory above.
     */
    protected abstract fun createDataSource(): DbDataSource

    private fun createService(): DbContentUploadSessionService {

        val webAppApi = EcosWebAppApiMock("test")
        val txnManager = TransactionManagerImpl()
        txnManager.init(webAppApi, EcosTxnProps())
        TxnContext.setManager(txnManager)

        val dsCtx = DbDataSourceContext(
            createDataSource(),
            createDataServiceFactory(),
            DbMigrationService(),
            webAppApi,
            GlobalEcosContext.getContext()
        )
        schemaCtx = dsCtx.getSchemaContext("ecos-data-upload-session-contract-test-schema")
        return schemaCtx.uploadSessionService
    }

    /**
     * Ages a session by [age], so it reads as idle for that long. Writes `__modified` directly
     * because the service stamps it with `Instant.now()` on every write.
     */
    private fun backdateModified(uploadId: String, age: Duration) {
        val sessions = DbDataServiceImpl(
            DbContentUploadSessionEntity::class.java,
            DbDataServiceConfig.create {
                withTable(DbContentUploadSessionEntity.TABLE)
                withStoreTableMeta(true)
            },
            schemaCtx
        )
        val status = schemaCtx.uploadSessionService.findByExtId(uploadId)?.status
            ?: error("Upload session is not found: '$uploadId'")
        val updated = sessions.updateByExtIdIfMatches(
            uploadId,
            mapOf(DbContentUploadSessionEntity.STATUS to status),
            mapOf(DbContentUploadSessionEntity.MODIFIED to Instant.now().minus(age))
        )
        assertThat(updated).describedAs("backdated __modified of '%s'", uploadId).isTrue()
    }

    private fun newSession(declaredSize: Long, chunkSize: Long): DbContentUploadSessionEntity {
        val entity = DbContentUploadSessionEntity()
        entity.creator = creatorCounter.get()
        entity.status = ContentUploadSessionStatus.ACTIVE.name
        entity.declaredSize = declaredSize
        entity.chunkSize = chunkSize
        entity.confirmedOffset = 0
        return entity
    }

    private fun ba(value: Int): ByteArray = byteArrayOf(value.toByte())

    /**
     * How far back [backdateModified] ages a session to make it read as idle.
     */
    private val idleAge: Duration = Duration.ofHours(1)

    /**
     * A cut-off between a session backdated by [idleAge] and one created just now: the first is
     * expired against it, the second is not. `Instant.now()` would not do - a session created a
     * moment ago is already older than that.
     */
    private fun afterBackdatedSessions(): Instant = Instant.now().minus(idleAge.dividedBy(2))

    /**
     * A cut-off nothing can be older than, i.e. every existing session is expired.
     */
    private fun everythingExpired(): Instant = Instant.now().plusSeconds(3600)

    /**
     * A cut-off everything is newer than, i.e. no session is expired.
     */
    private fun nothingExpired(): Instant = Instant.now().minusSeconds(3600)

    @Test
    fun `create generates extId and persists provided fields`() {
        val service = createService()

        val entity = newSession(declaredSize = 100, chunkSize = 10)
        entity.recordMeta = """{"name":"file.bin","mimeType":"application/octet-stream"}"""

        val saved = service.create(entity)

        assertThat(saved.extId).isNotBlank()
        assertThat(saved.id).isNotEqualTo(DbContentUploadSessionEntity.NEW_REC_ID)

        val found = service.findByExtId(saved.extId) ?: error("not found by extId")
        assertThat(found.declaredSize).isEqualTo(100)
        assertThat(found.chunkSize).isEqualTo(10)
        // stored verbatim: this service persists the payload, it does not interpret it
        assertThat(found.recordMeta).isEqualTo("""{"name":"file.bin","mimeType":"application/octet-stream"}""")
        assertThat(found.status).isEqualTo(ContentUploadSessionStatus.ACTIVE.name)
    }

    @Test
    fun `findByExtId returns null for unknown extId`() {
        val service = createService()
        assertThat(service.findByExtId("unknown-ext-id")).isNull()
    }

    @Test
    fun `advanceOffset is atomic`() {
        val service = createService()

        val s = service.create(newSession(declaredSize = 100, chunkSize = 10))

        assertThat(service.advanceOffset(s.extId, 0, 10, ba(1), "st1")).isTrue()
        // the loser of the race (same expectedOffset=0) is refused:
        assertThat(service.advanceOffset(s.extId, 0, 10, ba(2), "st2")).isFalse()

        assertThat(service.findByExtId(s.extId)!!.confirmedOffset).isEqualTo(10)
        assertThat(service.findByExtId(s.extId)!!.storageState).isEqualTo("st1")
        assertThat(service.findByExtId(s.extId)!!.digestState).isEqualTo(ba(1))

        // and the winning move can be followed by the next chunk, keyed off the new offset:
        assertThat(service.advanceOffset(s.extId, 10, 20, ba(3), "st3")).isTrue()
        assertThat(service.findByExtId(s.extId)!!.confirmedOffset).isEqualTo(20)
    }

    @Test
    fun `advanceOffset fails on non-ACTIVE session`() {
        val service = createService()
        val s = service.create(newSession(declaredSize = 100, chunkSize = 10))

        assertThat(
            service.updateStatus(s.extId, ContentUploadSessionStatus.ACTIVE, ContentUploadSessionStatus.ABORTED)
        ).isTrue()
        assertThat(service.advanceOffset(s.extId, 0, 10, ba(1), "st1")).isFalse()
        assertThat(service.findByExtId(s.extId)!!.confirmedOffset).isEqualTo(0)
    }

    @Test
    fun `advanceOffset renews the idle TTL`() {
        val service = createService()
        val s = service.create(newSession(declaredSize = 100, chunkSize = 10))
        backdateModified(s.extId, idleAge)

        assertThat(service.findExpired(afterBackdatedSessions(), 10).map { it.extId }).containsExactly(s.extId)

        assertThat(service.advanceOffset(s.extId, 0, 10, ba(1), "st1")).isTrue()

        assertThat(service.findExpired(afterBackdatedSessions(), 10)).isEmpty()
    }

    @Test
    fun `status transitions are conditional`() {
        val service = createService()
        val s = service.create(newSession(100, 10))

        assertThat(
            service.updateStatus(
                s.extId,
                ContentUploadSessionStatus.ACTIVE,
                ContentUploadSessionStatus.COMPLETING,
                dataKey = "b/2026/01/01/x"
            )
        ).isTrue()
        // the same transition cannot be applied twice:
        assertThat(
            service.updateStatus(
                s.extId,
                ContentUploadSessionStatus.ACTIVE,
                ContentUploadSessionStatus.COMPLETING
            )
        ).isFalse()

        assertThat(service.findByExtId(s.extId)!!.status).isEqualTo(ContentUploadSessionStatus.COMPLETING.name)
        assertThat(service.findByExtId(s.extId)!!.dataKey).isEqualTo("b/2026/01/01/x")

        assertThat(
            service.updateStatus(
                s.extId,
                ContentUploadSessionStatus.COMPLETING,
                ContentUploadSessionStatus.DONE,
                entityRef = "emodel/temp-file@1"
            )
        ).isTrue()

        val done = service.findByExtId(s.extId)!!
        assertThat(done.status).isEqualTo(ContentUploadSessionStatus.DONE.name)
        assertThat(done.entityRef).isEqualTo("emodel/temp-file@1")
        // dataKey set on the previous transition must survive the DONE transition:
        assertThat(done.dataKey).isEqualTo("b/2026/01/01/x")
    }

    @Test
    fun `updateStatus renews the idle TTL`() {
        val service = createService()
        val s = service.create(newSession(declaredSize = 100, chunkSize = 10))
        backdateModified(s.extId, idleAge)

        assertThat(service.findExpired(afterBackdatedSessions(), 10).map { it.extId }).containsExactly(s.extId)

        // this is what keeps a session that is being finalized alive against the sweeper: completion
        // makes no chunk writes, so the COMPLETING transitions are its only source of `modified`
        assertThat(
            service.updateStatus(
                s.extId,
                ContentUploadSessionStatus.ACTIVE,
                ContentUploadSessionStatus.COMPLETING
            )
        ).isTrue()

        assertThat(service.findExpired(afterBackdatedSessions(), 10)).isEmpty()
    }

    @Test
    fun `findExpired returns only stale sessions`() {
        val service = createService()

        val stale = service.create(newSession(100, 10))
        val fresh = service.create(newSession(100, 10))
        backdateModified(stale.extId, idleAge)

        val expired = service.findExpired(afterBackdatedSessions(), 10)

        assertThat(expired.map { it.extId }).containsExactly(stale.extId)
        assertThat(expired.map { it.extId }).doesNotContain(fresh.extId)
    }

    @Test
    fun `findExpired respects the max limit`() {
        val service = createService()
        repeat(3) {
            service.create(newSession(100, 10))
        }
        assertThat(service.findExpired(everythingExpired(), 2)).hasSize(2)
    }

    @Test
    fun `countActiveByCreator counts only ACTIVE`() {
        val service = createService()
        val creator = creatorCounter.incrementAndGet()

        fun create(status: ContentUploadSessionStatus): DbContentUploadSessionEntity {
            val entity = newSession(100, 10)
            entity.creator = creator
            entity.status = status.name
            return service.create(entity)
        }

        create(ContentUploadSessionStatus.ACTIVE)
        create(ContentUploadSessionStatus.ACTIVE)
        create(ContentUploadSessionStatus.DONE)
        create(ContentUploadSessionStatus.ABORTED)

        assertThat(service.countActiveByCreator(creator, nothingExpired())).isEqualTo(2)
        assertThat(service.countActiveByCreator(creator + 1, nothingExpired())).isEqualTo(0)
    }

    @Test
    fun `countActiveByCreator ignores expired sessions`() {
        val service = createService()
        val creator = creatorCounter.incrementAndGet()

        fun create(): DbContentUploadSessionEntity {
            val entity = newSession(100, 10)
            entity.creator = creator
            return service.create(entity)
        }

        val expired = create()
        create()
        backdateModified(expired.extId, idleAge)

        // An abandoned session (closed browser tab, dead network) stays ACTIVE until the sweeper job
        // runs, but it is already unusable - it must not keep occupying the per-user session cap.
        assertThat(service.countActiveByCreator(creator, afterBackdatedSessions())).isEqualTo(1)
        // the row itself is untouched - only the count ignores it
        assertThat(service.findByExtId(expired.extId)).isNotNull
    }

    @Test
    fun `delete removes the session`() {
        val service = createService()
        val s = service.create(newSession(100, 10))

        service.delete(s.extId)

        assertThat(service.findByExtId(s.extId)).isNull()
    }

    @Test
    fun `cleanupExpired deletes expired ACTIVE session only when abortAction returns true`() {
        val service = createService()
        val keep = service.create(newSession(100, 10))
        val remove = service.create(newSession(100, 10))

        val abortedExtIds = mutableListOf<String>()
        val deletedCount = service.cleanupExpired(everythingExpired(), 10) { entity ->
            abortedExtIds.add(entity.extId)
            entity.extId == remove.extId
        }

        assertThat(deletedCount).isEqualTo(1)
        assertThat(abortedExtIds).containsExactlyInAnyOrder(keep.extId, remove.extId)
        assertThat(service.findByExtId(remove.extId)).isNull()
        assertThat(service.findByExtId(keep.extId)).isNotNull()
    }

    @Test
    fun `cleanupExpired deletes an expired DONE session without calling abortAction`() {
        val service = createService()

        val done = service.create(
            newSession(100, 10).apply { status = ContentUploadSessionStatus.DONE.name }
        )

        val abortActionCalls = mutableListOf<String>()
        val deletedCount = service.cleanupExpired(everythingExpired(), 10) { entity ->
            abortActionCalls.add(entity.extId)
            true
        }

        assertThat(deletedCount).isEqualTo(1)
        assertThat(abortActionCalls).isEmpty()
        assertThat(service.findByExtId(done.extId)).isNull()
    }

    @Test
    fun `cleanupExpired retries the storage-side abort of an expired ABORTED session`() {
        val service = createService()

        // An ABORTED row survives DbChunkedUploadService.abort only when the storage-side abort
        // failed, so the sweeper is the retry - skipping abortAction here would leak the upload.
        val aborted = service.create(
            newSession(100, 10).apply { status = ContentUploadSessionStatus.ABORTED.name }
        )

        val abortActionCalls = mutableListOf<String>()
        val deletedCount = service.cleanupExpired(everythingExpired(), 10) { entity ->
            abortActionCalls.add(entity.extId)
            true
        }

        assertThat(abortActionCalls).containsExactly(aborted.extId)
        assertThat(deletedCount).isEqualTo(1)
        assertThat(service.findByExtId(aborted.extId)).isNull()
    }

    @Test
    fun `cleanupExpired keeps an ABORTED session whose storage-side abort fails again`() {
        val service = createService()

        val aborted = service.create(
            newSession(100, 10).apply { status = ContentUploadSessionStatus.ABORTED.name }
        )

        val deletedCount = service.cleanupExpired(everythingExpired(), 10) { false }

        assertThat(deletedCount).isEqualTo(0)
        assertThat(service.findByExtId(aborted.extId)).isNotNull
    }

    @Test
    fun `retire aborts the session and drops the handles it held into the storage`() {
        val service = createService()

        val session = service.create(
            newSession(100, 10).apply {
                storageState = "multipart-upload-id"
                dataKey = "b/2026/01/01/x"
            }
        )
        backdateModified(session.extId, idleAge)

        assertThat(service.retire(session.extId, ContentUploadSessionStatus.ACTIVE.name)).isTrue()

        val retired = service.findByExtId(session.extId) ?: error("not found")
        assertThat(retired.status).isEqualTo(ContentUploadSessionStatus.ABORTED.name)
        // both handles go: what the row no longer names, no sweep will try to release again
        assertThat(retired.storageState).isBlank()
        assertThat(retired.dataKey).isBlank()
        // and it stops being the oldest row in the queue, so it cannot hold up the batch either
        assertThat(service.findExpired(afterBackdatedSessions(), 10)).isEmpty()
    }

    @Test
    fun `retire expects the status as it is stored`() {
        val service = createService()

        val session = service.create(
            newSession(100, 10).apply { storageState = "multipart-upload-id" }
        )

        assertThat(service.retire(session.extId, ContentUploadSessionStatus.COMPLETING.name)).isFalse()

        val untouched = service.findByExtId(session.extId) ?: error("not found")
        assertThat(untouched.status).isEqualTo(ContentUploadSessionStatus.ACTIVE.name)
        assertThat(untouched.storageState).isEqualTo("multipart-upload-id")
    }

    @Test
    fun `cleanupExpired isolates a throwing abortAction to its own row`() {
        val service = createService()

        // findExpired sorts oldest-first, so the throwing row is processed first: without per-row
        // isolation it would abort the whole batch on every run, forever.
        val throwing = service.create(newSession(100, 10))
        val healthy = service.create(newSession(100, 10))
        backdateModified(throwing.extId, idleAge.multipliedBy(2))
        backdateModified(healthy.extId, idleAge)

        val abortActionCalls = mutableListOf<String>()
        val deletedCount = service.cleanupExpired(everythingExpired(), 10) { entity ->
            abortActionCalls.add(entity.extId)
            if (entity.extId == throwing.extId) {
                throw RuntimeException("storage-side abort blew up")
            }
            true
        }

        assertThat(abortActionCalls).containsExactly(throwing.extId, healthy.extId)
        assertThat(deletedCount).isEqualTo(1)
        // a throw is treated exactly like `false`: the row survives for the next run
        assertThat(service.findByExtId(throwing.extId)).isNotNull
        assertThat(service.findByExtId(healthy.extId)).isNull()
    }

    @Test
    fun `cleanupExpired does not touch live sessions`() {
        val service = createService()
        val live = service.create(newSession(100, 10))

        val deletedCount = service.cleanupExpired(nothingExpired(), 10) { true }

        assertThat(deletedCount).isEqualTo(0)
        assertThat(service.findByExtId(live.extId)).isNotNull()
    }
}
