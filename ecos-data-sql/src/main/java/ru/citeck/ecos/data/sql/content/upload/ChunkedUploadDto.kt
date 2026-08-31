package ru.citeck.ecos.data.sql.content.upload

import ru.citeck.ecos.commons.data.ObjectData
import ru.citeck.ecos.webapp.api.entity.EntityRef
import java.time.Duration

/**
 * Server-side limits every chunked call is evaluated against. Kept apart from
 * [ChunkedUploadInitReq] on purpose: those are the caller's own data, while these come from the
 * application configuration and must never be taken from a client request.
 *
 * Nothing here is persisted on the session, so a change of the application setting applies at once
 * to sessions that are already in flight. Every value is validated on construction, i.e. at the
 * boundary that produces them, so every call site downstream may rely on them being sane: a
 * non-positive [sessionIdleTimeout] would put the expiry cut-off at or after `now` and make every
 * session - including one created a millisecond ago - read as expired, and a non-positive
 * [maxActiveSessionsPerUser] would reject every single init with `too-many-sessions`.
 *
 * [completionLease] is how long a `COMPLETING` session is assumed to still have a live completer
 * behind it. A second `complete` arriving inside that window - the ordinary shape of a client or
 * gateway retry, since a completion holds the connection for the whole storage-side assembly - is
 * answered with `ContentUploadCompletionInProgressException` instead of being let into the
 * completion body beside the completer that is already running there. Once the window has passed
 * with no write on the row, the previous completer is taken to have died and the next `complete`
 * takes the session over.
 *
 * So the value trades two failures against each other: shorter than the slowest storage-side
 * assembly and a live completion can be taken over; longer than necessary and recovery from a
 * crashed completion waits that much longer.
 *
 * It may be at most half of [sessionIdleTimeout]. A lease is handed back by moving `modified` a
 * whole lease into the past, and `modified` is also the session's idle clock, so a lease closer to
 * the idle timeout than that would leave a released session so near expiry that the retry the
 * release exists to enable answers Gone instead. That bound is also why [sessionIdleTimeout] has a
 * floor of its own ([MIN_SESSION_IDLE_TIMEOUT]): under it "positive" and "at most half the idle
 * timeout" cannot both hold, and the caller would be refused over a lease it never named.
 */
data class ChunkedUploadPolicy(
    val maxActiveSessionsPerUser: Int,
    val sessionIdleTimeout: Duration,
    val completionLease: Duration = minOf(MAX_DEFAULT_COMPLETION_LEASE, sessionIdleTimeout.dividedBy(2))
) {
    companion object {
        /**
         * The cap on the derived default: long enough for a storage to assemble a large multipart
         * upload, short enough that a completion killed mid-flight is retried within one client
         * session. The default is derived from [sessionIdleTimeout] rather than fixed so that a
         * caller which never mentions the lease cannot be refused for it - a fixed default longer
         * than a short idle timeout would fail the `require` below over a knob that caller never set.
         */
        @JvmField
        val MAX_DEFAULT_COMPLETION_LEASE: Duration = Duration.ofMinutes(10)

        /**
         * The floor under [sessionIdleTimeout]. A completion lease has to be positive and at most
         * half of the idle timeout, so an idle timeout below a couple of clock units admits no valid
         * lease at all - and the caller would then be refused with a complaint about a lease it
         * never set. One second is far below anything a file upload could want and still leaves a
         * lease with room to exist, so the misconfiguration is named for what it is.
         */
        @JvmField
        val MIN_SESSION_IDLE_TIMEOUT: Duration = Duration.ofSeconds(1)
    }

    init {
        require(maxActiveSessionsPerUser > 0) {
            "Max active sessions per user must be positive, but was $maxActiveSessionsPerUser"
        }
        require(sessionIdleTimeout >= MIN_SESSION_IDLE_TIMEOUT) {
            "Session idle timeout must be at least $MIN_SESSION_IDLE_TIMEOUT, but was $sessionIdleTimeout: " +
                "a shorter one leaves no room for a completion lease, which must be positive and at " +
                "most half of it"
        }
        require(!completionLease.isZero && !completionLease.isNegative) {
            "Completion lease must be positive, but was $completionLease"
        }
        require(completionLease <= sessionIdleTimeout.dividedBy(2)) {
            "Completion lease ($completionLease) must be at most half of the session idle timeout " +
                "($sessionIdleTimeout): releasing a lease moves the session a whole lease closer to " +
                "expiry, and a lease longer than that would retire the session instead of freeing it"
        }
    }
}

/**
 * Request to start a new chunked upload session. [ecosType] decides both the content storage the
 * bytes are uploaded to
 * ([ru.citeck.ecos.data.sql.records.dao.content.DbRecordsContentDao.getContentStorage]) and the kind
 * of record created on
 * [ru.citeck.ecos.data.sql.records.dao.content.DbRecordsContentDao.chunkedUploadComplete]. Both are
 * decisions of a records dao, which is why a session is started through one - everything after that
 * is [DbChunkedUploadService]'s own business.
 *
 * [ecosType] is mandatory and must be a type this DAO serves, exactly like on the single-shot
 * upload: there, the caller (`EcosContentServiceImpl`) resolves the type's `sourceId` to an
 * application and a records DAO *before* any byte is written, and calls `uploadFile` on that DAO.
 * The chunked API expects the same of its caller for `init` and for `complete`, the two calls that
 * decide what record is created and where. The calls in between are schema-level and may be served
 * by any DAO of the same schema - see
 * [ru.citeck.ecos.data.sql.records.dao.content.DbRecordsContentDao.chunkedUploadInit].
 */
data class ChunkedUploadInitReq(
    val ecosType: String,
    val name: String,
    val mimeType: String,
    val encoding: String?,
    val size: Long,
    /**
     * Applied to the record the upload becomes, `_workspace` included: a workspace is one of these
     * attributes and not a field of its own, because nothing between `init` and `complete` looks at
     * it - it shapes the record and only the record.
     */
    val attributes: ObjectData
)

/**
 * Everything a finished upload needs to become a record, held on the session as one JSON payload
 * ([DbContentUploadSessionEntity.recordMeta]) rather than as columns of its own.
 *
 * The session table is the state machine of a *transfer*: who owns it, how far it got, what the
 * storage handed back, when it was last touched. None of the values here take part in that - they
 * are written once at `init`, read once at `complete`, and only [name] / [mimeType] are ever looked
 * at in between, to be reported back by [DbChunkedUploadService.getInfo]. A column per value bought
 * nothing for it and cost a chance to write one and never read it, which is exactly what happened
 * to the workspace this payload now carries inside [attributes].
 *
 * [mimeType] is stored normalised (see [DbChunkedUploadService.init]), so the session, the info and
 * the content row registered at the end all name the same type.
 */
data class ChunkedUploadRecordMeta(
    val ecosType: String = "",
    val name: String = "",
    val mimeType: String = "",
    val encoding: String = "",
    val attributes: ObjectData = ObjectData.create()
)

/**
 * [reason] is one of: `""` (supported), `"storage-not-supported"`, `"too-many-sessions"`.
 */
data class ChunkedUploadInitResp(
    val supported: Boolean,
    val reason: String,
    val uploadId: String,
    val chunkSize: Long
)

enum class ChunkOutcome {
    ACCEPTED,
    DUPLICATE,
    CONFLICT
}

data class ChunkedUploadChunkResp(
    val outcome: ChunkOutcome,
    val offset: Long
)

/**
 * What [DbChunkedUploadService.complete] hands to the caller that knows how to turn a finished
 * upload into a record: the content row it registered for the assembled object, and the session
 * metadata that decides what record is made of it.
 */
data class ChunkedUploadRecordData(
    val contentId: Long,
    val ecosType: String,
    val name: String,
    val attributes: ObjectData
)

data class ChunkedUploadInfo(
    val status: String,
    val offset: Long,
    val size: Long,
    val chunkSize: Long,
    val name: String,
    /**
     * Normalised - see [ChunkedUploadInitReq.mimeType].
     */
    val mimeType: String,
    val entityRef: EntityRef?
)

/**
 * No session found for the given `uploadId`, or the caller is not its owner.
 */
class ContentUploadSessionNotFoundException(id: String) : RuntimeException("Upload session not found: '$id'")

/**
 * The session is `ABORTED`, expired, or otherwise no longer usable for the requested operation.
 */
class ContentUploadSessionGoneException(id: String) : RuntimeException("Upload session is gone: '$id'")

/**
 * [chunkedUploadComplete] was called before all bytes were confirmed.
 */
class ContentUploadSizeMismatchException(val offset: Long, val size: Long) :
    RuntimeException("Upload is incomplete: confirmed offset $offset does not match declared size $size")

/**
 * Another request is already completing this session (it won the `ACTIVE -> COMPLETING` transition,
 * or took the session over between this request's own transition and its final `COMPLETING -> DONE`
 * write). Nothing useful can be returned yet - the caller should simply retry.
 *
 * The REST layer maps this to **409** (`complete` answers `200 | 409 | 404 | 410`); without a
 * dedicated type it would be indistinguishable from any other [IllegalStateException] and surface
 * as an HTTP 500.
 */
class ContentUploadCompletionInProgressException(val uploadId: String) :
    RuntimeException("Upload session '$uploadId' is already being completed by another request")
