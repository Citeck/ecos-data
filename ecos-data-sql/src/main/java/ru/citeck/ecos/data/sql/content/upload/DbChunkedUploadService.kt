package ru.citeck.ecos.data.sql.content.upload

import io.github.oshai.kotlinlogging.KotlinLogging
import ru.citeck.ecos.commons.json.Json
import ru.citeck.ecos.commons.mime.MimeTypes
import ru.citeck.ecos.commons.utils.digest.ResumableSha256
import ru.citeck.ecos.context.lib.auth.AuthContext
import ru.citeck.ecos.context.lib.auth.AuthUser
import ru.citeck.ecos.data.sql.content.ContentMimeTypeDetector
import ru.citeck.ecos.data.sql.content.storage.ChunkedInitMeta
import ru.citeck.ecos.data.sql.content.storage.ChunkedUploadGoneException
import ru.citeck.ecos.data.sql.context.DbSchemaContext
import ru.citeck.ecos.txn.lib.TxnContext
import ru.citeck.ecos.webapp.api.content.ContentRange
import ru.citeck.ecos.webapp.api.entity.EntityRef
import ru.citeck.ecos.webapp.api.mime.MimeType
import java.io.ByteArrayInputStream
import java.io.InputStream
import java.time.Duration
import java.time.Instant
import java.util.UUID

/**
 * The state machine of a chunked upload: the session row, the digest carried across chunks, the
 * storage-side upload behind it, and the content row the assembled object becomes.
 *
 * Schema-level, like every state it touches. The session rows
 * ([DbContentUploadSessionService]), the content rows ([DbSchemaContext.contentService]) and the
 * storage service all belong to a [DbSchemaContext] rather than to any one records dao, so an
 * upload is served identically whichever dao of the schema the caller came through - and none of
 * these calls has a reason to reach a records dao at all.
 *
 * What does belong to a dao is decided outside: which content storage a type uploads to, and what
 * record the finished upload becomes. A session is therefore started through
 * [ru.citeck.ecos.data.sql.records.dao.content.DbRecordsContentDao.chunkedUploadInit], which resolves
 * the storage, and completed through its `chunkedUploadComplete`, which passes [complete] the
 * record-creating step as a lambda so that the record and the `COMPLETING -> DONE` write stay in one
 * transaction.
 */
class DbChunkedUploadService(private val schemaCtx: DbSchemaContext) {

    companion object {
        private val log = KotlinLogging.logger {}

        /**
         * Pushed past a whole [ChunkedUploadPolicy.completionLease] when a lease is handed back, so
         * that the retry which follows is on the takeover side of the boundary whatever the clock
         * did in between.
         */
        private const val RELEASE_MARGIN_SECONDS = 1L
    }

    /**
     * Opens a session for an upload into [storageRef], or answers `supported=false` when that
     * storage cannot chunk - in which case nothing is left behind: no session row, and no
     * storage-side upload.
     */
    fun init(req: ChunkedUploadInitReq, storageRef: EntityRef, policy: ChunkedUploadPolicy): ChunkedUploadInitResp {

        val currentUser = AuthContext.getCurrentUser()

        // A non-positive size has no valid chunking at all, and would otherwise fail much later
        // inside registerContent's own size guard, as an HTTP 500 instead of a 400.
        if (req.size <= 0) {
            throw IllegalArgumentException("Declared upload size must be positive, but was ${req.size}")
        }

        if (!schemaCtx.contentStorageService.isChunkedSupported(storageRef)) {
            log.info {
                "Chunked upload init rejected (storage-not-supported): user=$currentUser type=${req.ecosType}"
            }
            return ChunkedUploadInitResp(false, "storage-not-supported", "", 0)
        }

        val creatorRefId = getOrCreateUserRefId(currentUser)
        val activeCount = schemaCtx.uploadSessionService.countActiveByCreator(
            creatorRefId,
            expiredBefore(policy.sessionIdleTimeout)
        )
        if (activeCount >= policy.maxActiveSessionsPerUser) {
            log.info { "Chunked upload init rejected (too-many-sessions): user=$currentUser" }
            return ChunkedUploadInitResp(false, "too-many-sessions", "", 0)
        }

        val initResult = schemaCtx.contentStorageService.chunkedInit(storageRef, ChunkedInitMeta(req.size))
        // A non-positive chunkSize is not a usable chunking contract (chunkIndex is offset/chunkSize,
        // and the expected chunk length is min(chunkSize, ...)), so a storage answering with one is
        // treated exactly like a storage without the capability instead of creating a broken session.
        if (!initResult.supported || initResult.chunkSize <= 0) {
            if (initResult.supported) {
                log.warn {
                    "Chunked upload storage reported supported=true with a non-positive " +
                        "chunkSize=${initResult.chunkSize}, treating it as not supported: storage=$storageRef"
                }
                // the storage already opened an upload for us - release it instead of leaking it,
                // since no session will ever reference this state
                try {
                    schemaCtx.contentStorageService.chunkedAbort(storageRef, initResult.state)
                } catch (e: Exception) {
                    log.warn(e) { "Failed to abort a chunked upload rejected for a non-positive chunkSize" }
                }
            }
            log.info {
                "Chunked upload init rejected (storage-not-supported): user=$currentUser type=${req.ecosType}"
            }
            return ChunkedUploadInitResp(false, "storage-not-supported", "", 0)
        }

        val entity = DbContentUploadSessionEntity()
        entity.creator = creatorRefId
        entity.status = ContentUploadSessionStatus.ACTIVE.name
        entity.declaredSize = req.size
        entity.confirmedOffset = 0
        entity.chunkSize = initResult.chunkSize
        entity.recordMeta = Json.mapper.toString(
            ChunkedUploadRecordMeta(
                ecosType = req.ecosType,
                name = req.name,
                // Normalised here rather than at completion, so that the session, the info answered
                // by getInfo and the content row registered at the end all carry the same value. A
                // mime type the caller made up is not parseable, and a content row holding one makes
                // every later read of its mimeType (previewInfo included) throw instead of degrading,
                // so it is brought to the same canonical form the single-shot path stores via
                // DbContentService.uploadContent.
                mimeType = MimeTypes.parseOrBin(req.mimeType).toString(),
                encoding = req.encoding ?: "",
                attributes = req.attributes
            )
        ) ?: "{}"
        entity.storageRef = schemaCtx.recordRefService.getOrCreateIdByEntityRef(storageRef)
        entity.storageState = initResult.state
        entity.digestState = ResumableSha256.create().getEncodedState()

        // the session must outlive the caller's own (possibly rolled-back) request transaction
        val saved = schemaCtx.doInNewTxn {
            schemaCtx.uploadSessionService.create(entity)
        }

        log.info {
            "Chunked upload init: uploadId=${saved.extId} user=$currentUser " +
                "type=${req.ecosType} size=${req.size} chunkSize=${saved.chunkSize}"
        }

        return ChunkedUploadInitResp(true, "", saved.extId, saved.chunkSize)
    }

    fun writeChunk(
        uploadId: String,
        offset: Long,
        content: InputStream,
        contentLength: Long,
        policy: ChunkedUploadPolicy
    ): ChunkedUploadChunkResp {

        val currentUser = AuthContext.getCurrentUser()

        val session = schemaCtx.uploadSessionService.findByExtId(uploadId)
            ?: throw ContentUploadSessionNotFoundException(uploadId)

        // The owner check must run before any status check: otherwise a probe with a foreign
        // uploadId could tell an existing session from a missing one by the error it gets back.
        if (session.creator != getOrCreateUserRefId(currentUser)) {
            throw ContentUploadSessionNotFoundException(uploadId)
        }

        if (statusOf(session) != ContentUploadSessionStatus.ACTIVE ||
            isExpired(session, policy)
        ) {
            throw ContentUploadSessionGoneException(uploadId)
        }

        // Every declared byte is already confirmed, so there is no chunk left to place: `expected`
        // below would be 0 and a zero-length part would be handed to the storage. Answered as a
        // DUPLICATE rather than an error - the caller has sent everything, and `complete` is what it
        // should call next.
        if (session.confirmedOffset >= session.declaredSize) {
            drainQuietly(content, contentLength)
            return ChunkedUploadChunkResp(ChunkOutcome.DUPLICATE, session.confirmedOffset)
        }

        if (offset < session.confirmedOffset) {
            drainQuietly(content, contentLength)
            return ChunkedUploadChunkResp(ChunkOutcome.DUPLICATE, session.confirmedOffset)
        }
        if (offset != session.confirmedOffset) {
            drainQuietly(content, contentLength)
            log.warn {
                "Chunked upload chunk CONFLICT: uploadId=$uploadId user=$currentUser " +
                    "offset=$offset expected=${session.confirmedOffset}"
            }
            return ChunkedUploadChunkResp(ChunkOutcome.CONFLICT, session.confirmedOffset)
        }

        val expected = minOf(session.chunkSize, session.declaredSize - offset)
        if (contentLength != expected) {
            drainQuietly(content, contentLength)
            throw IllegalArgumentException(
                "Invalid chunk length for uploadId='$uploadId': expected $expected but got $contentLength"
            )
        }

        val digest = restoreDigest(uploadId, currentUser, session)
        val bounded = DigestingBoundedInputStream(content, expected, digest)

        val storageRef = schemaCtx.recordRefService.getEntityRefById(session.storageRef)
        val chunkIndex = (offset / session.chunkSize).toInt()

        val newStorageState = try {
            schemaCtx.contentStorageService.chunkedWriteChunk(
                storageRef,
                session.storageState,
                chunkIndex,
                bounded,
                expected
            )
        } catch (e: ChunkedUploadGoneException) {
            log.warn(e) {
                "Chunked upload storage reports Gone on writeChunk: uploadId=$uploadId user=$currentUser offset=$offset"
            }
            markSessionAbortedBestEffort(uploadId, ContentUploadSessionStatus.ACTIVE)
            throw ContentUploadSessionGoneException(uploadId)
        } catch (e: Exception) {
            log.warn(e) { "Chunked upload storage error on writeChunk: uploadId=$uploadId user=$currentUser offset=$offset" }
            throw e
        }

        if (bounded.bytesRead != expected || content.read() != -1) {
            throw IllegalArgumentException(
                "Chunk body for uploadId='$uploadId' doesn't match the declared length of $expected bytes"
            )
        }

        val advanced = schemaCtx.doInNewTxn {
            schemaCtx.uploadSessionService.advanceOffset(
                uploadId,
                offset,
                offset + expected,
                digest.getEncodedState(),
                newStorageState
            )
        }

        if (!advanced) {
            val latest = schemaCtx.uploadSessionService.findByExtId(uploadId)
                ?: throw ContentUploadSessionNotFoundException(uploadId)
            log.warn {
                "Chunked upload chunk CONFLICT (race): uploadId=$uploadId user=$currentUser offset=$offset"
            }
            return ChunkedUploadChunkResp(ChunkOutcome.CONFLICT, latest.confirmedOffset)
        }

        return ChunkedUploadChunkResp(ChunkOutcome.ACCEPTED, offset + expected)
    }

    fun getInfo(uploadId: String, policy: ChunkedUploadPolicy): ChunkedUploadInfo {

        val session = requireOwnedSession(uploadId, AuthContext.getCurrentUser(), policy)
        val recordMeta = recordMetaOf(session)

        return ChunkedUploadInfo(
            status = statusOf(session).name,
            offset = session.confirmedOffset,
            size = session.declaredSize,
            chunkSize = session.chunkSize,
            name = recordMeta.name,
            mimeType = recordMeta.mimeType,
            entityRef = if (session.entityRef.isBlank()) null else EntityRef.valueOf(session.entityRef)
        )
    }

    /**
     * Assembles the upload in the storage, registers the resulting object as a content row and hands
     * it to [createRecord], which turns it into a record.
     *
     * [createRecord] runs inside the transaction that also writes `COMPLETING -> DONE`, so a crash
     * between the record and that write cannot leave a live record behind a session that never
     * reaches DONE: nothing is committed and the retry lands here again. It may refuse the upload by
     * throwing - the transaction, the content row included, rolls back with it.
     */
    fun complete(
        uploadId: String,
        policy: ChunkedUploadPolicy,
        createRecord: (ChunkedUploadRecordData) -> EntityRef
    ): EntityRef {

        val currentUser = AuthContext.getCurrentUser()

        var session = requireOwnedSession(uploadId, currentUser, policy)

        if (session.confirmedOffset != session.declaredSize) {
            throw ContentUploadSizeMismatchException(session.confirmedOffset, session.declaredSize)
        }

        if (statusOf(session) == ContentUploadSessionStatus.DONE) {
            log.info { "Chunked upload complete (already done): uploadId=$uploadId user=$currentUser" }
            return EntityRef.valueOf(session.entityRef)
        }

        if (statusOf(session) == ContentUploadSessionStatus.ACTIVE) {
            // Only the winner of this CAS may run the storage-complete/register/create body
            // below: two concurrent completes would otherwise each complete the storage-side
            // upload and create a record for it. A caller that arrives when the session is
            // already COMPLETING never reaches this branch at all - see takeOverStaleCompletion,
            // which lets it in only once the previous completer is taken to be dead.
            val wonTransition = schemaCtx.doInNewTxn {
                schemaCtx.uploadSessionService.updateStatus(
                    uploadId,
                    ContentUploadSessionStatus.ACTIVE,
                    ContentUploadSessionStatus.COMPLETING
                )
            }
            // the conditional update bumps the row's update version, so the copy above is stale
            session = schemaCtx.uploadSessionService.findByExtId(uploadId)
                ?: throw ContentUploadSessionNotFoundException(uploadId)

            if (!wonTransition) {
                if (statusOf(session) == ContentUploadSessionStatus.DONE) {
                    log.info { "Chunked upload complete (already done): uploadId=$uploadId user=$currentUser" }
                    return EntityRef.valueOf(session.entityRef)
                }
                // The winner is still working (status is COMPLETING). Simplest deterministic
                // behavior: tell this caller to retry rather than let it race the winner's own
                // storage-complete/register/create - there's nothing useful to return yet.
                log.warn {
                    "Chunked upload complete CONFLICT (concurrent complete in progress): " +
                        "uploadId=$uploadId user=$currentUser"
                }
                throw ContentUploadCompletionInProgressException(uploadId)
            }
            log.info { "Chunked upload completing: uploadId=$uploadId user=$currentUser" }
        } else if (statusOf(session) == ContentUploadSessionStatus.COMPLETING) {
            session = takeOverStaleCompletion(uploadId, currentUser, session, policy)
            if (statusOf(session) == ContentUploadSessionStatus.DONE) {
                // the completer this caller was about to take over finished first
                return EntityRef.valueOf(session.entityRef)
            }
        }

        if (statusOf(session) != ContentUploadSessionStatus.COMPLETING) {
            throw ContentUploadSessionGoneException(uploadId)
        }

        // The lease this caller owns. Every write it makes on the row refreshes it, and the catch at
        // the end hands it back: a completion that fails on this very thread has no completer left
        // to protect, so its client must be able to retry at once instead of waiting the lease out.
        var ownedModified = session.modified

        try {
            return completeOwnedSession(uploadId, currentUser, session, createRecord) { ownedModified = it }
        } catch (e: Throwable) {
            releaseCompletionBestEffort(uploadId, currentUser, ownedModified, policy)
            throw e
        }
    }

    /**
     * The completion body, run by the caller that owns the session's `COMPLETING` lease.
     *
     * [ownedModifiedUpdated] is called with the row's new `modified` whenever this body writes to
     * the row, so that [complete]'s failure path releases the lease it actually owns rather than the
     * one it started with.
     */
    private fun completeOwnedSession(
        uploadId: String,
        currentUser: String,
        sessionAtStart: DbContentUploadSessionEntity,
        createRecord: (ChunkedUploadRecordData) -> EntityRef,
        ownedModifiedUpdated: (Instant) -> Unit
    ): EntityRef {

        var session = sessionAtStart
        // The lease claimed before this body was entered. Both writes below expect it, so a completer
        // whose lease was taken over - its storage-side assembly outran the lease - loses the CAS
        // instead of writing over the successor that now owns the session.
        var ownedModified = session.modified
        val storageRef = schemaCtx.recordRefService.getEntityRefById(session.storageRef)

        var dataKey = session.dataKey
        if (dataKey.isBlank()) {
            dataKey = try {
                schemaCtx.contentStorageService.chunkedComplete(storageRef, session.storageState)
            } catch (e: ChunkedUploadGoneException) {
                log.warn(e) { "Chunked upload storage reports Gone on complete: uploadId=$uploadId user=$currentUser" }
                markSessionAbortedBestEffort(
                    uploadId,
                    ContentUploadSessionStatus.COMPLETING,
                    expectedModified = ownedModified
                )
                throw ContentUploadSessionGoneException(uploadId)
            } catch (e: Exception) {
                log.warn(e) { "Chunked upload storage error on complete: uploadId=$uploadId user=$currentUser" }
                throw e
            }
            val dataKeyStored = schemaCtx.doInNewTxn {
                schemaCtx.uploadSessionService.updateStatus(
                    extId = uploadId,
                    expectedStatus = ContentUploadSessionStatus.COMPLETING,
                    newStatus = ContentUploadSessionStatus.COMPLETING,
                    dataKey = dataKey,
                    expectedModified = ownedModified
                )
            }
            if (!dataKeyStored) {
                // The session was quarantined, aborted, deleted or taken over by another completer
                // while the storage was assembling the object, so nothing may be built on it.
                // Reported here rather than at the final DONE transition, where the same condition
                // would only surface after a record was created and rolled back. The assembled
                // object is orphaned - the dataKey never reached the row, so the cleanup job cannot
                // reclaim it either; log it so it is greppable.
                log.warn {
                    "Chunked upload lost its session while the storage was assembling the object, " +
                        "leaving it orphaned: uploadId=$uploadId user=$currentUser " +
                        "storage=$storageRef dataKey=$dataKey"
                }
                throw ContentUploadSessionGoneException(uploadId)
            }
            // a transition refreshes `modified`, so that write renewed the lease: from here on the
            // one to expect - and to hand back on failure - is the new one
            session = schemaCtx.uploadSessionService.findByExtId(uploadId)
                ?: throw ContentUploadSessionNotFoundException(uploadId)
            ownedModified = session.modified
            ownedModifiedUpdated(ownedModified)
        }

        val digest = restoreDigest(uploadId, currentUser, session)
        val sha256 = digest.finishHex()
        val recordMeta = recordMetaOf(session)
        val sessionSize = session.declaredSize
        val sessionCreator = session.creator

        val contentMeta = resolveUploadedContentMeta(storageRef, dataKey, recordMeta.name, recordMeta.mimeType)

        val entityRef = TxnContext.doInTxn {
            val contentService = schemaCtx.contentService
            val existing = contentService.findContentByStorageAndDataKey(storageRef, dataKey)
            val contentId = existing?.getDbId() ?: contentService.registerContent(
                contentMeta.name,
                contentMeta.mimeType.toString(),
                recordMeta.encoding.ifBlank { null },
                storageRef,
                dataKey,
                sha256,
                sessionSize,
                sessionCreator
            ).getDbId()

            val ref = createRecord(
                ChunkedUploadRecordData(
                    contentId = contentId,
                    ecosType = recordMeta.ecosType,
                    name = recordMeta.name,
                    attributes = recordMeta.attributes
                )
            )
            if (existing == null) {
                // contentId is a throwaway row we just registered and immediately cloned into ref's
                // own content attribute - remove it. When `existing` was non-null, contentId belongs
                // to some other, already-live row (another record, or a content-addressable storage
                // dedup landing on the same dataKey) that must not be touched here.
                contentService.removeContent(contentId)
            }

            // Expects the lease, like every other write this body makes: a completer whose session
            // was taken over while it was creating the record still finds COMPLETING there, and a
            // CAS on the status alone would let it finish the successor's session with its own
            // record - leaving the successor to roll back a record that was already good.
            val statusUpdated = schemaCtx.uploadSessionService.updateStatus(
                uploadId,
                ContentUploadSessionStatus.COMPLETING,
                ContentUploadSessionStatus.DONE,
                entityRef = ref.toString(),
                expectedModified = ownedModified
            )
            if (!statusUpdated) {
                // someone else took the session over while we were creating the record - retryable,
                // and this transaction (record creation included) rolls back with the throw
                throw ContentUploadCompletionInProgressException(uploadId)
            }

            ref
        }

        log.info { "Chunked upload complete: uploadId=$uploadId user=$currentUser entityRef=$entityRef" }

        return entityRef
    }

    fun abort(uploadId: String, policy: ChunkedUploadPolicy) {

        val currentUser = AuthContext.getCurrentUser()

        val session = requireOwnedSession(uploadId, currentUser, policy)

        if (statusOf(session) == ContentUploadSessionStatus.DONE) {
            log.info { "Chunked upload abort skipped (already done): uploadId=$uploadId user=$currentUser" }
            return
        }

        // Claim the transition before touching the storage. Aborting the storage-side upload
        // first would let a cancel race a concurrent complete: the completer's assembled object
        // would be orphaned - no ed_content row, no session row - and invisible to the cleanup job.
        // Only ACTIVE is abortable; a session already in COMPLETING belongs to the completer, and
        // one in DONE/ABORTED needs nothing from us.
        val wonTransition = schemaCtx.doInNewTxn {
            schemaCtx.uploadSessionService.updateStatus(
                uploadId,
                ContentUploadSessionStatus.ACTIVE,
                ContentUploadSessionStatus.ABORTED
            )
        }
        if (!wonTransition) {
            // No-op rather than an error: DELETE answers 204|404, and every lost-CAS outcome
            // here already satisfies the caller's intent (the session is no longer an in-flight
            // upload of theirs) or belongs to a request that must not be interrupted. A session
            // left in COMPLETING by a crashed completer is swept by the cleanup job.
            log.info {
                "Chunked upload abort skipped (session is not ACTIVE anymore): " +
                    "uploadId=$uploadId user=$currentUser status=${session.status}"
            }
            return
        }

        // The CAS above constrains the status only, so a chunk write that committed between the read
        // at the top of this method and the transition left a newer storage state behind. Re-read it
        // instead of aborting with a stale one.
        val current = schemaCtx.uploadSessionService.findByExtId(uploadId)
        if (current == null) {
            log.info { "Chunked upload abort skipped (session is already deleted): uploadId=$uploadId user=$currentUser" }
            return
        }

        val storageRef = schemaCtx.recordRefService.getEntityRefById(current.storageRef)

        val storageAbortSucceeded = try {
            schemaCtx.contentStorageService.chunkedAbort(storageRef, current.storageState)
            true
        } catch (_: ChunkedUploadGoneException) {
            // The upload we wanted to abort is already gone - that's the outcome we wanted, not an
            // error, so treat it exactly like a successful storage-side abort.
            log.info { "Chunked upload storage already gone on abort (treated as success): uploadId=$uploadId user=$currentUser" }
            true
        } catch (e: Exception) {
            log.warn(e) { "Chunked upload storage error on abort: uploadId=$uploadId user=$currentUser" }
            false
        }

        if (storageAbortSucceeded) {
            // the row otherwise stays ABORTED for the sweeper job to clean up later
            schemaCtx.doInNewTxn {
                schemaCtx.uploadSessionService.delete(uploadId)
            }
        }

        log.info { "Chunked upload aborted: uploadId=$uploadId user=$currentUser" }
    }

    /**
     * Name and mime type to store for an assembled chunked upload, resolved exactly the way the
     * single-shot path resolves them for the same input - otherwise the same file would be stored
     * differently depending only on whether it was big enough to be chunked.
     *
     * A client-stated type that boils down to `application/octet-stream` (blank, unparseable, or
     * literally that) carries no information, so the assembled object's own first bytes are asked
     * instead; anything more specific is trusted as it stands and no bytes are read. Either way the
     * file name is then completed from the resulting type.
     *
     * Without a [ContentMimeTypeDetector] neither step is possible, and the client-stated type and
     * name are stored untouched.
     */
    private fun resolveUploadedContentMeta(
        storageRef: EntityRef,
        dataKey: String,
        name: String,
        clientMimeType: String
    ): UploadedContentMeta {

        val detector = schemaCtx.dataSourceCtx.mimeTypeDetector
            ?: return UploadedContentMeta(name, MimeTypes.parseOrBin(clientMimeType))

        var mimeType = MimeTypes.parseOrBin(clientMimeType)
        if (mimeType.isBin()) {
            mimeType = detectMimeType(detector, storageRef, dataKey, name) ?: mimeType
        }
        return UploadedContentMeta(normalizeUploadedContentName(detector, name, mimeType), mimeType)
    }

    /**
     * The type of the assembled object as its own first [ContentMimeTypeDetector.getPrefixSize] bytes
     * describe it, or null when nothing could be told about it.
     *
     * The two ways this can go wrong are not the same thing and are not treated as one. The prefix is
     * read here, outside any detector code, so a storage that cannot serve it back is a storage
     * failure and propagates: this runs before the record-creating transaction, with the dataKey
     * already persisted and the session still re-enterable, so the caller gets a retryable error
     * rather than a row typed `application/octet-stream` for good. The detector is then handed an
     * in-memory buffer, so whatever it fails on says nothing about the bytes - which are stored and
     * accepted by now - and only means the upload keeps the type the client gave it, whether the
     * content was simply unrecognizable or the implementation broke its no-throw contract.
     */
    private fun detectMimeType(
        detector: ContentMimeTypeDetector,
        storageRef: EntityRef,
        dataKey: String,
        name: String
    ): MimeType? {

        val prefixSize = detector.getPrefixSize()
        if (prefixSize <= 0) {
            // ContentRange would reject a non-positive length with an IllegalArgumentException, which
            // the webmvc content controller answers as a 400 - a misconfigured detector reported to
            // the client as its own mistake, mid-completion. Keep the client-stated type instead,
            // exactly as the detector-less path does.
            log.warn {
                "Content type detector answers a non-positive prefix size ($prefixSize) and is " +
                    "skipped, the client-stated type is kept: storage=$storageRef dataKey=$dataKey name='$name'"
            }
            return null
        }

        val prefix = schemaCtx.contentStorageService.readContent(
            storageRef,
            dataKey,
            ContentRange(0, prefixSize.toLong())
        ) { it.readNBytes(prefixSize) }

        // a detector that fails answers null here, exactly as one that recognized nothing does -
        // see SafeContentMimeTypeDetector, which every detector reaching this point is wrapped in
        return detector.detect(name, ByteArrayInputStream(prefix))
    }

    /**
     * A file name is given one when it is missing entirely, and an extension when it has none and
     * the type is specific enough to name one. A name that already contains a dot anywhere is left
     * alone, extension or not.
     */
    private fun normalizeUploadedContentName(
        detector: ContentMimeTypeDetector,
        name: String,
        mimeType: MimeType
    ): String {

        var result = name
        if (result.isBlank()) {
            result = UUID.randomUUID().toString()
        }

        if (!result.contains(".") && !mimeType.isBin()) {
            val extension = detector.getExtension(mimeType)
            if (extension.isNotBlank()) {
                result = "$result$extension"
            }
        }
        return result
    }

    private data class UploadedContentMeta(
        val name: String,
        val mimeType: MimeType
    )

    /**
     * Decides whether a `complete` that found the session already `COMPLETING` may run the
     * completion body, and hands back the session to run it on.
     *
     * Entering `COMPLETING` from `COMPLETING` is only ever legitimate as crash recovery. The
     * ordinary case is a client or gateway retry issued while the first completion is still
     * running - a completion holds the connection for the whole storage-side assembly, so a
     * timeout there is routine - and letting that retry through would run a second
     * `chunkedComplete`/`registerContent`/`createRecord` beside the live completer. On a storage
     * that refuses a repeated complete (S3 answers `NoSuchUpload` for a multipart upload it has
     * already consumed, which arrives here as [ChunkedUploadGoneException]) the retry would then
     * mark the session ABORTED and destroy a completion that was about to succeed, losing a fully
     * transferred file behind a 410.
     *
     * So a live lease - a row written inside [ChunkedUploadPolicy.completionLease] - is answered
     * with [ContentUploadCompletionInProgressException], exactly as the loser of the
     * `ACTIVE -> COMPLETING` CAS is. Past the lease the previous completer is taken to be dead and
     * the session is taken over, single-winner, by a CAS that expects the `modified` this caller
     * read: two callers arriving together at an abandoned session cannot both win it.
     */
    private fun takeOverStaleCompletion(
        uploadId: String,
        currentUser: String,
        session: DbContentUploadSessionEntity,
        policy: ChunkedUploadPolicy
    ): DbContentUploadSessionEntity {

        if (!session.modified.isBefore(expiredBefore(policy.completionLease))) {
            log.warn {
                "Chunked upload complete CONFLICT (completion already in progress): " +
                    "uploadId=$uploadId user=$currentUser"
            }
            throw ContentUploadCompletionInProgressException(uploadId)
        }

        val tookOver = schemaCtx.doInNewTxn {
            schemaCtx.uploadSessionService.takeOverCompletion(uploadId, session.modified)
        }
        val current = schemaCtx.uploadSessionService.findByExtId(uploadId)
            ?: throw ContentUploadSessionNotFoundException(uploadId)

        if (!tookOver) {
            if (statusOf(current) == ContentUploadSessionStatus.DONE) {
                log.info { "Chunked upload complete (already done): uploadId=$uploadId user=$currentUser" }
                return current
            }
            // Another caller took the abandoned session over between our read and our CAS, or it
            // moved on under us. Either way this caller has nothing to complete right now.
            log.warn {
                "Chunked upload complete CONFLICT (stale completion taken over by another request): " +
                    "uploadId=$uploadId user=$currentUser"
            }
            throw ContentUploadCompletionInProgressException(uploadId)
        }

        log.warn {
            "Chunked upload resumes a completion abandoned for longer than the completion lease: " +
                "uploadId=$uploadId user=$currentUser"
        }
        return current
    }

    /**
     * Hands back the `COMPLETING` lease of a completion that failed on this thread, by moving the
     * row's `modified` a full [ChunkedUploadPolicy.completionLease] into the past so the next
     * `complete` may take the session over at once.
     *
     * The CAS expects the `modified` this caller owns, so a lease that has already moved on - taken
     * over by someone else, or advanced by the winner of the final `COMPLETING -> DONE` transition -
     * is left alone. Best-effort: a failure here must not mask the error being propagated.
     *
     * This runs on the way out of a failed completion, including one that failed *because the thread
     * was interrupted*, so the interrupt is taken off the thread for the duration of the release and
     * put back afterwards: the database work below would otherwise consume it and the caller would
     * come back from an interrupted completion with a clear flag.
     */
    private fun releaseCompletionBestEffort(
        uploadId: String,
        currentUser: String,
        ownedModified: Instant,
        policy: ChunkedUploadPolicy
    ) {
        val wasInterrupted = Thread.interrupted()
        try {
            val released = schemaCtx.doInNewTxn {
                schemaCtx.uploadSessionService.releaseCompletion(
                    uploadId,
                    ownedModified,
                    // a whole lease plus a margin: landing exactly on the boundary
                    // takeOverStaleCompletion tests would leave the retry at the mercy of a clock
                    // that did not advance between the release and the retry's own reading of `now`
                    expiredBefore(policy.completionLease.plusSeconds(RELEASE_MARGIN_SECONDS))
                )
            }
            if (released) {
                log.info {
                    "Chunked upload released its completion lease after a failure: " +
                        "uploadId=$uploadId user=$currentUser"
                }
            }
        } catch (e: Exception) {
            log.warn(e) { "Failed to release the completion lease of upload '$uploadId'" }
        } finally {
            if (wasInterrupted) {
                Thread.currentThread().interrupt()
            }
        }
    }

    /**
     * Best-effort CAS transition to ABORTED, used right before rethrowing a storage-level
     * [ChunkedUploadGoneException] as the session-level [ContentUploadSessionGoneException] - so
     * every later call on this uploadId answers Gone consistently instead of leaving the row
     * ACTIVE/COMPLETING until the cleanup job eventually sweeps it. This is best-effort only: a
     * failure here (including losing the CAS to a concurrent transition) must not mask the Gone,
     * so it is swallowed - the caller throws [ContentUploadSessionGoneException] regardless.
     *
     * [expectedModified] is passed by a caller holding a `COMPLETING` lease. Without it the CAS
     * would match on the status alone, and a completer whose storage call outran its lease could
     * abort the session its successor is completing right now - losing a fully transferred file
     * behind a 410, which is the very outcome the lease exists to prevent.
     */
    private fun markSessionAbortedBestEffort(
        uploadId: String,
        expectedStatus: ContentUploadSessionStatus,
        expectedModified: Instant? = null
    ) {
        try {
            schemaCtx.doInNewTxn {
                schemaCtx.uploadSessionService.updateStatus(
                    uploadId,
                    expectedStatus,
                    ContentUploadSessionStatus.ABORTED,
                    expectedModified = expectedModified
                )
            }
        } catch (e: Exception) {
            log.warn(e) { "Failed to best-effort mark upload '$uploadId' ABORTED after a storage-level Gone" }
        }
    }

    /**
     * What the session says the finished upload should become. A payload this version cannot read -
     * a row written by another version, or a manual edit - is treated as a session that no longer
     * exists: without it nothing can be built, and every alternative (an empty type, an empty name)
     * would build the wrong thing instead of saying so.
     */
    private fun recordMetaOf(session: DbContentUploadSessionEntity): ChunkedUploadRecordMeta {
        val meta = try {
            Json.mapper.read(session.recordMeta, ChunkedUploadRecordMeta::class.java)
        } catch (e: Exception) {
            log.error(e) { "Chunked upload session carries an unreadable record meta: uploadId=${session.extId}" }
            null
        }
        return meta ?: throw ContentUploadSessionGoneException(session.extId)
    }

    /**
     * The session's status as this version understands it. A stored value it cannot interpret is,
     * to the client, a session that no longer exists - the cleanup job retires such a row on its
     * own account.
     */
    private fun statusOf(session: DbContentUploadSessionEntity): ContentUploadSessionStatus {
        return ContentUploadSessionStatus.ofOrNull(session.status)
            ?: throw ContentUploadSessionGoneException(session.extId)
    }

    /**
     * A session is expired once it has been idle for longer than
     * [ChunkedUploadPolicy.sessionIdleTimeout]: every write on it refreshes `modified`.
     *
     * A terminal session never expires for as long as its row exists: DONE keeps answering with its
     * [DbContentUploadSessionEntity.entityRef], which is what makes a retried [complete] return the
     * same record instead of an error, and ABORTED is refused on its own account rather than for its
     * age. How long the row exists is decided outside this service - the cleanup job deletes terminal
     * sessions too once `modified` passes the cut-off, after which the same call answers not-found.
     */
    private fun isExpired(session: DbContentUploadSessionEntity, policy: ChunkedUploadPolicy): Boolean {
        if (statusOf(session).isTerminal) {
            return false
        }
        return session.modified.isBefore(expiredBefore(policy.sessionIdleTimeout))
    }

    /**
     * The cut-off a session's `modified` is compared against: it is still live while it was modified
     * at or after this instant, and expired once it is older. The subtraction is what turns a
     * duration into that boundary - `now - idleTimeout` is the oldest `modified` still considered
     * live.
     *
     * The idle timeout is not persisted on the session, so the currently configured value applies to
     * sessions that are already in flight. It is guaranteed positive by [ChunkedUploadPolicy], which
     * validates it where it is constructed - a non-positive one would put this cut-off at or after
     * `now` and make every session, including one created a millisecond ago, read as expired.
     */
    private fun expiredBefore(sessionIdleTimeout: Duration): Instant {
        return Instant.now().minus(sessionIdleTimeout)
    }

    /**
     * Restores the resumable SHA-256 from a session's persisted `digest_state`.
     *
     * [ResumableSha256.restore] validates the state hard and reports a truncated / foreign /
     * malformed one as an [IllegalArgumentException] - the right contract for a general-purpose
     * library, but the wrong signal for this call site: a corrupt row is a server-side fault the
     * client did not cause and cannot recover from by retrying, and the webmvc content controller
     * maps [IllegalArgumentException] to a 400 carrying the exception message (i.e. the digest state
     * layout) in the response body. Translate it into an [IllegalStateException], which has no
     * `@ResponseStatus` and therefore falls through to the generic 500 handler.
     */
    private fun restoreDigest(
        uploadId: String,
        currentUser: String,
        session: DbContentUploadSessionEntity
    ): ResumableSha256 {
        return try {
            ResumableSha256.restore(session.digestState)
        } catch (e: IllegalArgumentException) {
            log.error(e) {
                "Corrupted chunked upload digest state: uploadId=$uploadId user=$currentUser " +
                    "status=${session.status} offset=${session.confirmedOffset} " +
                    "digestStateSize=${session.digestState.size}"
            }
            // deliberately free of any digest internals - this message reaches the client
            throw IllegalStateException("Corrupted digest state of the upload session '$uploadId'")
        }
    }

    private fun requireOwnedSession(
        uploadId: String,
        currentUser: String,
        policy: ChunkedUploadPolicy
    ): DbContentUploadSessionEntity {
        val session = schemaCtx.uploadSessionService.findByExtId(uploadId)
            ?: throw ContentUploadSessionNotFoundException(uploadId)
        // Owner-check first - see the comment in writeChunk for why.
        if (session.creator != getOrCreateUserRefId(currentUser)) {
            throw ContentUploadSessionNotFoundException(uploadId)
        }
        if (statusOf(session) == ContentUploadSessionStatus.ABORTED ||
            isExpired(session, policy)
        ) {
            throw ContentUploadSessionGoneException(uploadId)
        }
        return session
    }

    /**
     * The record-ref id of [userName], created on first use - the same id
     * [ru.citeck.ecos.data.sql.records.dao.DbRecordsDaoCtx.getOrCreateUserRefId] resolves, since both
     * go through the schema's own record-ref table.
     */
    private fun getOrCreateUserRefId(userName: String): Long {
        val personRef = schemaCtx.authoritiesApi.getPersonRef(userName.ifBlank { AuthUser.ANONYMOUS })
        return schemaCtx.recordRefService.getOrCreateIdByEntityRef(personRef)
    }

    /**
     * Reads and discards up to [contentLength] bytes of a chunk body this request is not going to
     * store (a duplicate, a conflicting offset, a wrong length). The body has to leave the socket
     * before the response is written, otherwise the connection cannot be reused and some clients
     * report a broken pipe instead of reading the answer.
     *
     * Best effort by design: the answer is already decided, so a drain that fails changes nothing
     * about it and must not replace it with an error. It is logged rather than swallowed, because a
     * consistent failure here is a symptom worth seeing - at debug level, since a client that hung
     * up mid-body is the ordinary reason for it.
     */
    private fun drainQuietly(content: InputStream, contentLength: Long) {
        try {
            val buf = ByteArray(8192)
            var remaining = contentLength
            while (remaining > 0) {
                val toRead = minOf(buf.size.toLong(), remaining).toInt()
                val n = content.read(buf, 0, toRead)
                if (n <= 0) {
                    // < 0 is the end of the body; 0 would mean no progress and must not spin
                    break
                }
                remaining -= n
            }
        } catch (e: Exception) {
            log.debug(e) { "Failed to drain the body of a chunk that is not stored" }
        }
    }

    /**
     * Reads at most [limit] bytes from [source] and feeds every byte read into [digest], mirroring
     * the remote storage transport's own `content.copyTo(output)` (which reads until EOF - bounding
     * the stream here is what makes it stop exactly at the chunk boundary). [bytesRead] lets the
     * caller confirm the full [limit] was actually consumed (a short read means the underlying
     * stream had less data than declared).
     *
     * **Single pass only.** [digest] is advanced as the bytes go by, so this stream cannot be read
     * twice: a second pass would feed the same bytes into the digest again (and find nothing left to
     * read). That makes it unsafe as a replayable request body - the transport handed this stream
     * must consume it exactly once and must not retry the request by invoking the body writer a
     * second time. A transport that needs retries has to re-enter [writeChunk] instead: that
     * restores the digest from the session row and builds a fresh stream over a fresh body.
     */
    private class DigestingBoundedInputStream(
        private val source: InputStream,
        private val limit: Long,
        private val digest: ResumableSha256
    ) : InputStream() {

        var bytesRead: Long = 0
            private set

        override fun read(): Int {
            if (bytesRead >= limit) {
                return -1
            }
            val b = source.read()
            if (b >= 0) {
                digest.update(byteArrayOf(b.toByte()), 0, 1)
                bytesRead++
            }
            return b
        }

        override fun read(b: ByteArray, off: Int, len: Int): Int {
            if (bytesRead >= limit) {
                return -1
            }
            val toRead = minOf(len.toLong(), limit - bytesRead).toInt()
            val n = source.read(b, off, toRead)
            if (n > 0) {
                digest.update(b, off, n)
                bytesRead += n
            }
            return n
        }
    }
}
