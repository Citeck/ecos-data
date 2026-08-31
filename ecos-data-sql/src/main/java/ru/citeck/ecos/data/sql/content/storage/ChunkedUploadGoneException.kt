package ru.citeck.ecos.data.sql.content.storage

/**
 * The stable marker string that ecos-content's own
 * `ru.citeck.ecos.content.service.storage.ChunkedUploadGoneException` always starts its message
 * with, when the underlying storage (e.g. an S3 multipart upload) no longer exists - most notably
 * once the bucket's `AbortIncompleteMultipartUpload` lifecycle rule has swept it away, but also on
 * any other "no such upload" condition.
 *
 * This is a **wire-level contract** between ecos-content and its callers, not a shared Kotlin type:
 * ecos-data must not depend on ecos-content, so the literal is duplicated here rather than pulled in
 * as a compile-time dependency. [ru.citeck.ecos.data.sql.content.storage.remote.EcosContentRemoteStorage]
 * looks for this marker anywhere in a failed remote call's cause chain and, if found, rethrows
 * [ChunkedUploadGoneException] (this module's own type) so the rest of ecos-data can react to it
 * without knowing anything about the wire format.
 */
const val CHUNKED_UPLOAD_GONE_MARKER = "CHUNKED_UPLOAD_GONE"

/**
 * Thrown by [ru.citeck.ecos.data.sql.content.storage.EcosContentChunkedStorage] implementations
 * (currently only [ru.citeck.ecos.data.sql.content.storage.remote.EcosContentRemoteStorage]) when
 * the storage reports that a chunked upload's backing no longer exists - see
 * [CHUNKED_UPLOAD_GONE_MARKER]. Callers (see
 * [ru.citeck.ecos.data.sql.records.dao.content.DbRecordsContentDao]) map this onto the
 * session-level `ContentUploadSessionGoneException`.
 */
class ChunkedUploadGoneException(message: String, cause: Throwable? = null) : RuntimeException(message, cause)
