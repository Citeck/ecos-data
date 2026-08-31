package ru.citeck.ecos.data.sql.content.upload

import ru.citeck.ecos.data.sql.dto.DbColumnConstraint.*
import ru.citeck.ecos.data.sql.dto.DbColumnType
import ru.citeck.ecos.data.sql.repo.entity.annotation.ColumnType
import ru.citeck.ecos.data.sql.repo.entity.annotation.Constraints
import ru.citeck.ecos.data.sql.repo.entity.annotation.Index
import ru.citeck.ecos.data.sql.repo.entity.annotation.Indexes
import java.time.Instant

/**
 * Persistent state of a chunked content upload session. One row per in-flight (or recently
 * finished/aborted) upload, keyed by [extId] (the client-facing `uploadId`, a UUID v4).
 *
 * Every column here takes part in the *transfer*: who owns it, what was declared, how far it got,
 * what the storage handed back, when it was last touched. What the upload will become once it is
 * complete - type, name, mime type, encoding, record attributes - is not a set of columns but a
 * single opaque payload, [recordMeta]: it is written once at `init` and read once at `complete`,
 * and nothing in between queries, sorts or compares it.
 *
 * A session expires when [modified] is older than the idle timeout the application applies at
 * query time - the timeout itself is not stored.
 */
/*
 * Indexes follow the three queries this table actually serves:
 *  - every call but the sweeper looks a session up by its client-facing upload id (unique);
 *  - the per-user session cap counts by creator and status
 *    ([DbContentUploadSessionService.countActiveByCreator]), so `creator` has to lead - an index
 *    leading with `status` would still have to walk every ACTIVE session of every user;
 *  - the sweeper filters and sorts by `modified` alone
 *    ([DbContentUploadSessionService.findExpired]), across all statuses, so it needs `modified`
 *    as the leading column of its own index.
 */
@Indexes(
    Index(columns = [DbContentUploadSessionEntity.EXT_ID], unique = true),
    Index(columns = [DbContentUploadSessionEntity.CREATOR, DbContentUploadSessionEntity.STATUS]),
    Index(columns = [DbContentUploadSessionEntity.MODIFIED])
)
class DbContentUploadSessionEntity {

    companion object {

        const val TABLE = "ed_content_upload_session"

        const val NEW_REC_ID = -1L

        const val ID = "id"
        const val EXT_ID = "__ext_id"
        const val CREATOR = "__creator"
        const val STATUS = "__status"
        const val DECLARED_SIZE = "__declared_size"
        const val CONFIRMED_OFFSET = "__confirmed_offset"
        const val CHUNK_SIZE = "__chunk_size"
        const val RECORD_META = "__record_meta"
        const val STORAGE_REF = "__storage_ref"
        const val STORAGE_STATE = "__storage_state"
        const val DIGEST_STATE = "__digest_state"
        const val DATA_KEY = "__data_key"
        const val ENTITY_REF = "__entity_ref"
        const val CREATED = "__created"
        const val MODIFIED = "__modified"
    }

    @Constraints(PRIMARY_KEY)
    var id: Long = NEW_REC_ID

    @Constraints(NOT_NULL)
    var extId: String = ""

    @Constraints(NOT_NULL)
    var creator: Long = -1

    /**
     * Name of a [ContentUploadSessionStatus] - the entity mapper has no enum support.
     */
    @Constraints(NOT_NULL)
    var status: String = ""

    @Constraints(NOT_NULL)
    var declaredSize: Long = 0

    @Constraints(NOT_NULL)
    var confirmedOffset: Long = 0

    @Constraints(NOT_NULL)
    var chunkSize: Long = 0

    /**
     * JSON-encoded [ChunkedUploadRecordMeta]: what the upload will become once it is complete
     * (type, name, mime type, encoding, record attributes). One payload rather than a column each,
     * because none of it takes part in the transfer this table drives - see that class.
     *
     * Deliberately not named `attributes`: the mapper treats a field of that name as the entity's
     * dynamic additional-attributes map.
     */
    var recordMeta: String = "{}"

    @Constraints(NOT_NULL)
    var storageRef: Long = -1

    /**
     * Opaque, storage-implementation-specific state (e.g. multipart-upload id).
     */
    var storageState: String = ""

    /**
     * [ru.citeck.ecos.commons.utils.digest.ResumableSha256.getEncodedState] snapshot.
     */
    @ColumnType(DbColumnType.BINARY)
    var digestState: ByteArray = ByteArray(0)

    /**
     * Filled in once the session moves to COMPLETING.
     */
    var dataKey: String = ""

    /**
     * Filled in once the session moves to DONE.
     */
    var entityRef: String = ""

    var created: Instant = Instant.EPOCH

    var modified: Instant = Instant.EPOCH
}
