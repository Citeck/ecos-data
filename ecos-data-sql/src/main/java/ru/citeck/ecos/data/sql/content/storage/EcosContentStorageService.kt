package ru.citeck.ecos.data.sql.content.storage

import ru.citeck.ecos.commons.data.ObjectData
import ru.citeck.ecos.webapp.api.content.ContentRange
import ru.citeck.ecos.webapp.api.entity.EntityRef
import java.io.InputStream
import java.io.OutputStream

interface EcosContentStorageService {

    fun resetColumnsCache()

    fun uploadContent(storageRef: EntityRef, storageConfig: ObjectData, action: (OutputStream) -> Unit): String

    /**
     * Read a byte range of the stored content.
     */
    fun <T> readContent(
        storageRef: EntityRef,
        path: String,
        range: ContentRange,
        action: (InputStream) -> T
    ): T

    fun deleteContent(storageRef: EntityRef, path: String)

    // CHUNKED

    fun isChunkedSupported(storageRef: EntityRef): Boolean

    /**
     * See [EcosContentChunkedStorage] for why the chunked calls take no `storageConfig`.
     */
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
