package ru.citeck.ecos.data.sql.content.storage

import ru.citeck.ecos.webapp.api.entity.EntityRef
import java.io.InputStream

/**
 * [size] is the total size of the content being uploaded. The storage needs it to pick a part size
 * that fits its own part-count limit (a large enough file cannot be split into its minimal parts).
 */
data class ChunkedInitMeta(val size: Long)

/**
 * [chunkSize] is the effective chunk size chosen by the storage. It is binding: the client must
 * send chunks of exactly this size (except the last one).
 *
 * [state] is the handle of the upload the storage opened, to be handed back to it on every later
 * call of this upload; the storage returns an updated one from every chunk write. It is empty
 * exactly when [supported] is false - no upload was opened, so there is nothing to name.
 */
data class ChunkedInitResult(val supported: Boolean, val state: String, val chunkSize: Long)

/**
 * A chunked upload is addressed by the storage alone: [storageRef] names the storage record, and the
 * storage owns every setting the upload needs (bucket, part size, ...). Unlike
 * [EcosContentStorage.uploadContent] this takes no per-type `storageConfig` - the remote content
 * application resolves all of it from the storage id, so passing one would only mean re-resolving
 * the ecos type on every single chunk to build a value nobody reads.
 */
interface EcosContentChunkedStorage {

    fun chunkedInit(storageRef: EntityRef, meta: ChunkedInitMeta): ChunkedInitResult

    fun chunkedWriteChunk(
        storageRef: EntityRef,
        state: String,
        chunkIndex: Int,
        content: InputStream,
        contentLength: Long
    ): String

    fun chunkedComplete(storageRef: EntityRef, state: String): String

    fun chunkedAbort(storageRef: EntityRef, state: String)
}
