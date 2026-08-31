package ru.citeck.ecos.data.sql.content.upload

import java.time.Instant

/**
 * Schema-level persistence for chunked-upload sessions ([DbContentUploadSessionEntity]). Orchestration
 * (validating chunk order, driving the storage SPI, moving a session through its status machine end
 * to end) is out of scope here - this service only offers the storage primitives that orchestration
 * needs, including the atomic conditional updates ([advanceOffset], [updateStatus]) required to make
 * concurrent chunk uploads / retries safe.
 *
 * A session has no stored expiry: it is expired when its `modified` is older than the cut-off the
 * caller passes in, i.e. `now - idleTimeout` for the idle timeout configured in the application.
 */
interface DbContentUploadSessionService {

    /**
     * Persists a new session. Generates a fresh [DbContentUploadSessionEntity.extId] (UUID v4) and
     * sets `created`/`modified`; all other fields, [DbContentUploadSessionEntity.status] included,
     * are taken from [entity] as provided by the caller.
     */
    fun create(entity: DbContentUploadSessionEntity): DbContentUploadSessionEntity

    fun findByExtId(extId: String): DbContentUploadSessionEntity?

    /**
     * ACTIVE sessions of [creatorRefId] that are not expired, i.e. modified at or after
     * [expiredBefore]. Expired ones are already unusable and must not keep occupying the per-user
     * session cap.
     */
    fun countActiveByCreator(creatorRefId: Long, expiredBefore: Instant): Long

    /**
     * Atomically advances the confirmed offset: succeeds only if the session is ACTIVE and its
     * current `confirmedOffset` still equals [expectedOffset]. Returns `true` iff exactly one row
     * was updated - `false` means a race (a concurrent/retried request already advanced the offset)
     * or the session isn't ACTIVE. Refreshes `modified`, so it also renews the idle TTL.
     */
    fun advanceOffset(
        extId: String,
        expectedOffset: Long,
        newOffset: Long,
        digestState: ByteArray,
        storageState: String
    ): Boolean

    /**
     * Atomically transitions the session's status, e.g. ACTIVE -> COMPLETING, COMPLETING -> DONE
     * (+ [entityRef]), or `*` -> ABORTED. Succeeds only if the current status still equals
     * [expectedStatus]. [dataKey] / [entityRef], when non-null, are written together with the status
     * change (populated on COMPLETING / DONE respectively).
     *
     * [expectedModified], when given, is compared too. A status alone does not identify *whose*
     * COMPLETING a row is in: a completer whose lease was taken over still finds the status it
     * wrote, and would go on writing over its successor. A caller holding a lease passes the
     * `modified` it owns so that losing the lease turns into a lost CAS.
     *
     * Refreshes `modified`, exactly as [advanceOffset] does: a transition renews the idle TTL, and
     * the transition into COMPLETING is what starts the completion lease of the caller that won it -
     * a completion makes no chunk writes, so its transitions are the session's only other source of
     * `modified`. A caller that has to know the value it now owns re-reads the row.
     */
    fun updateStatus(
        extId: String,
        expectedStatus: ContentUploadSessionStatus,
        newStatus: ContentUploadSessionStatus,
        dataKey: String? = null,
        entityRef: String? = null,
        expectedModified: Instant? = null
    ): Boolean

    /**
     * Takes a stale `COMPLETING` session over: succeeds only while the row is still `COMPLETING`
     * *and* its `modified` still equals [expectedModified], then refreshes `modified` so the taker
     * owns the lease. Expecting `modified` is what makes this single-winner - a CAS on the status
     * alone would let every concurrent taker through, since they all find the same `COMPLETING`.
     *
     * `false` means someone else got there first (or the session moved on) and the caller must
     * re-read the row rather than assume anything about it.
     */
    fun takeOverCompletion(extId: String, expectedModified: Instant): Boolean

    /**
     * Hands a `COMPLETING` lease back: succeeds only while the row is still `COMPLETING` *and* its
     * `modified` still equals [expectedModified], then sets `modified` to [releasedModified], which
     * the caller picks far enough in the past to make the session immediately takeable again.
     *
     * Expecting `modified` is what keeps this safe to call from any failure path: a caller whose
     * lease has already been taken over - or which lost the final `COMPLETING -> DONE` CAS - simply
     * loses this CAS too and releases nothing.
     */
    fun releaseCompletion(extId: String, expectedModified: Instant, releasedModified: Instant): Boolean

    /**
     * The same conditional transition, expecting the status exactly as it is stored: a value this
     * code did not write and may not be able to parse - a row from an older or newer version, or a
     * manual edit. Callers holding a [ContentUploadSessionStatus] should use the overload above.
     */
    fun updateStatus(
        extId: String,
        expectedStatus: String,
        newStatus: ContentUploadSessionStatus,
        dataKey: String? = null,
        entityRef: String? = null,
        expectedModified: Instant? = null
    ): Boolean

    /**
     * Retires a session nobody will work on again: transitions it to ABORTED - expecting
     * [expectedStatus] exactly as it is stored, like the overload above - and clears the two handles
     * the row carries into the storage, [DbContentUploadSessionEntity.storageState] and
     * [DbContentUploadSessionEntity.dataKey].
     *
     * Clearing them is the point. A session's status says what the *session* is doing, not whether
     * anything in the storage still belongs to it, and ABORTED alone conflates two rows that need
     * opposite treatment: one whose storage-side abort failed (its multipart upload is still open
     * and has to be retried) and one the cleanup job has given up on (it must be dropped without
     * another storage call, or it is retried forever and, being the oldest row that never leaves,
     * starves every newer expired session out of the batch-capped [findExpired] queue). A row with
     * no handles left states "nothing here belongs to me any more" in the data rather than in a
     * status the same value has to mean something else in.
     *
     * What was left behind is knowingly leaked, so the caller is expected to log it: an open
     * multipart upload falls to the bucket lifecycle rule, an assembled object has to be removed by
     * hand.
     */
    fun retire(extId: String, expectedStatus: String): Boolean

    fun delete(extId: String)

    /**
     * Sessions last modified before [expiredBefore], oldest first, capped at [max] rows.
     */
    fun findExpired(expiredBefore: Instant, max: Int): List<DbContentUploadSessionEntity>

    /**
     * Cleans up expired sessions ([findExpired]) for the external cleanup job (living in emodel).
     * A DONE session is deleted directly, without calling [abortAction]: its storage-side upload
     * was assembled into a content row and there is nothing left to release. Every other expired
     * session is handed to [abortAction], which performs the storage-side abort: `true` means the
     * row may be deleted now, `false` means it's left in place for a subsequent run to retry.
     * ABORTED is included on purpose - such a row exists only because the storage-side abort failed
     * once already, so it still owns the multipart upload that abort was meant to release. A row the
     * job has finished with carries no handles ([retire]) and is answered `true` by [abortAction]
     * without a storage call. Returns the number of deleted rows.
     */
    fun cleanupExpired(
        expiredBefore: Instant,
        batch: Int,
        abortAction: (DbContentUploadSessionEntity) -> Boolean
    ): Int

    /**
     * Creates the session table if it is missing. Called from the schema migrations so that the
     * table exists before any request needs it: without this the very first `init` in a schema would
     * run DDL from inside a user request, and two concurrent first inits would race each other over
     * `CREATE TABLE` / `CREATE INDEX`.
     */
    fun createTableIfNotExists()

    fun resetColumnsCache()
}
