package ru.citeck.ecos.data.sql.content.storage.remote

import ru.citeck.ecos.commons.data.ObjectData
import ru.citeck.ecos.data.sql.content.storage.CHUNKED_UPLOAD_GONE_MARKER
import ru.citeck.ecos.data.sql.content.storage.ChunkedInitMeta
import ru.citeck.ecos.data.sql.content.storage.ChunkedInitResult
import ru.citeck.ecos.data.sql.content.storage.ChunkedUploadGoneException
import ru.citeck.ecos.data.sql.content.storage.EcosContentChunkedStorage
import ru.citeck.ecos.data.sql.content.storage.EcosContentStorage
import ru.citeck.ecos.webapp.api.content.ContentRange
import ru.citeck.ecos.webapp.api.entity.EntityRef
import ru.citeck.ecos.webapp.api.web.client.EcosWebClientApi
import java.io.InputStream
import java.io.OutputStream

class EcosContentRemoteStorage(
    private val webClient: EcosWebClientApi
) : EcosContentStorage,
    EcosContentChunkedStorage {

    companion object {
        const val CONTENT_STORAGE_UPLOAD = "/content/storage/upload"
        const val CONTENT_STORAGE_DOWNLOAD = "/content/storage/download"
        const val CONTENT_STORAGE_DELETE = "/content/storage/delete"

        /**
         * First api version of [CONTENT_STORAGE_DOWNLOAD] which understands a byte range.
         */
        const val DOWNLOAD_RANGE_API_VERSION = 1

        const val CONTENT_STORAGE_CHUNK_INIT = "/content/storage/chunk-init"
        const val CONTENT_STORAGE_CHUNK_WRITE = "/content/storage/chunk-write"
        const val CONTENT_STORAGE_CHUNK_COMPLETE = "/content/storage/chunk-complete"
        const val CONTENT_STORAGE_CHUNK_ABORT = "/content/storage/chunk-abort"
    }

    override fun uploadContent(
        storageRef: EntityRef,
        storageConfig: ObjectData,
        content: (OutputStream) -> Unit
    ): String {

        if (storageRef.getAppName().isBlank()) {
            error("Content storage config is mandatory")
        }
        val appName = storageRef.getAppName()
        validateApiPath(appName, CONTENT_STORAGE_UPLOAD)

        val apiHeaders = UploadReqHeaders(
            storageRef.getLocalId(),
            storageRef.getSourceId(),
            storageConfig
        )

        val response = webClient.newRequest()
            .targetApp(appName)
            .path(CONTENT_STORAGE_UPLOAD)
            .headers(apiHeaders)
            .body { content.invoke(it.getOutputStream()) }
            .executeSync { it.getBodyReader().readDto(UploadRespBody::class.java) }

        return response.dataKey
    }

    /**
     * There is one read path: the whole content is the range that starts at zero and lasts until
     * the end, and it travels in the very same fields as any other range.
     *
     * What the target supports is decided once, by the api version of the download path
     * ([DOWNLOAD_RANGE_API_VERSION] and above cut the range out themselves), and the same call both
     * validates the path and negotiates that version. A target below it receives the range anyway -
     * unknown body fields are ignored by the mapper, so the request stays readable for it - and the
     * range is applied to the response stream instead: the same bytes reach the caller, only without
     * the transfer saving.
     */
    override fun <T> readContent(
        storageRef: EntityRef,
        dataKey: String,
        range: ContentRange,
        action: (InputStream) -> T
    ): T {

        val appName = storageRef.getAppName()
        val apiVersion = resolveApiVersion(appName, CONTENT_STORAGE_DOWNLOAD, DOWNLOAD_RANGE_API_VERSION)

        val rangeSupportedByTarget = apiVersion >= DOWNLOAD_RANGE_API_VERSION

        return webClient.newRequest()
            .targetApp(appName)
            .path(CONTENT_STORAGE_DOWNLOAD)
            .version(apiVersion)
            .body {
                it.writeDto(
                    DownloadReqBody(
                        storageId = storageRef.getLocalId(),
                        storageSourceId = storageRef.getSourceId(),
                        dataKey = dataKey,
                        offset = range.offset,
                        length = range.length
                    )
                )
            }.executeSync {
                val stream = it.getBodyReader().getInputStream()
                if (rangeSupportedByTarget) {
                    action.invoke(stream)
                } else {
                    action.invoke(range.apply(stream))
                }
            }
    }

    override fun deleteContent(storageRef: EntityRef, dataKey: String) {

        validateApiPath(storageRef.getAppName(), CONTENT_STORAGE_DELETE)

        webClient.newRequest()
            .targetApp(storageRef.getAppName())
            .path(CONTENT_STORAGE_DELETE)
            .body {
                it.writeDto(
                    DeleteReqBody(
                        storageRef.getLocalId(),
                        storageRef.getSourceId(),
                        dataKey
                    )
                )
            }
            .executeSync { it.getBodyReader().readDto(DeleteRespBody::class.java) }
    }

    fun isChunkedSupported(storageRef: EntityRef): Boolean {
        val appName = requireAppName(storageRef)
        return webClient.getApiVersion(appName, CONTENT_STORAGE_CHUNK_INIT, 0) >= 0
    }

    override fun chunkedInit(storageRef: EntityRef, meta: ChunkedInitMeta): ChunkedInitResult {
        val appName = requireAppName(storageRef)

        val response = webClient.newRequest()
            .targetApp(appName)
            .path(CONTENT_STORAGE_CHUNK_INIT)
            .headers(ChunkedInitReqHeaders(storageRef.getLocalId()))
            .body {
                it.writeDto(
                    ChunkedInitReqBody(meta.size)
                )
            }
            .executeSync { it.getBodyReader().readDto(ChunkedInitRespBody::class.java) }

        return ChunkedInitResult(response.supported, response.state, response.chunkSize)
    }

    override fun chunkedWriteChunk(
        storageRef: EntityRef,
        state: String,
        chunkIndex: Int,
        content: InputStream,
        contentLength: Long
    ): String {
        val appName = requireAppName(storageRef)
        return translateGoneFailure {
            webClient.newRequest()
                .targetApp(appName)
                .path(CONTENT_STORAGE_CHUNK_WRITE)
                .headers(ChunkWriteReqHeaders(storageRef.getLocalId(), state, chunkIndex, contentLength))
                .body { content.copyTo(it.getOutputStream()) }
                .executeSync { it.getBodyReader().readDto(ChunkWriteRespBody::class.java) }
                .state
        }
    }

    override fun chunkedComplete(storageRef: EntityRef, state: String): String {
        val appName = requireAppName(storageRef)
        return translateGoneFailure {
            webClient.newRequest()
                .targetApp(appName)
                .path(CONTENT_STORAGE_CHUNK_COMPLETE)
                .headers(ChunkCompleteReqHeaders(storageRef.getLocalId()))
                .body { it.writeDto(ChunkCompleteReqBody(state)) }
                .executeSync { it.getBodyReader().readDto(ChunkCompleteRespBody::class.java) }
                .dataKey
        }
    }

    override fun chunkedAbort(storageRef: EntityRef, state: String) {
        val appName = requireAppName(storageRef)
        translateGoneFailure {
            webClient.newRequest()
                .targetApp(appName)
                .path(CONTENT_STORAGE_CHUNK_ABORT)
                .headers(ChunkAbortReqHeaders(storageRef.getLocalId()))
                .body { it.writeDto(ChunkAbortReqBody(state)) }
                .executeSync { it.getBodyReader().readDto(ChunkAbortRespBody::class.java) }
        }
    }

    /**
     * Runs [action] and, if it fails, inspects the whole cause chain for
     * [CHUNKED_UPLOAD_GONE_MARKER]: the marker may sit on a nested cause and may appear anywhere in
     * a wrapper's message. A match is rethrown as [ChunkedUploadGoneException] with the original as
     * its cause; anything else is rethrown unchanged. [chunkedInit] does not go through this:
     * nothing exists yet, so it cannot hit a swept upload.
     */
    private fun <T> translateGoneFailure(action: () -> T): T {
        return try {
            action()
        } catch (e: Exception) {
            if (hasGoneMarker(e)) {
                throw ChunkedUploadGoneException(
                    "Chunked upload storage reports the upload is gone ($CHUNKED_UPLOAD_GONE_MARKER)",
                    e
                )
            }
            throw e
        }
    }

    private fun hasGoneMarker(error: Throwable): Boolean {
        var current: Throwable? = error
        var guard = 0
        while (current != null && guard++ < 32) {
            if (current.message?.contains(CHUNKED_UPLOAD_GONE_MARKER) == true) {
                return true
            }
            current = current.cause
        }
        return false
    }

    private fun requireAppName(storageRef: EntityRef): String {
        if (storageRef.getAppName().isBlank()) {
            error("Content storage config is mandatory")
        }
        return storageRef.getAppName()
    }

    private fun validateApiPath(targetApp: String, path: String) {
        resolveApiVersion(targetApp, path, 0)
    }

    /**
     * The api version to call [path] of [targetApp] with, at most [maxVersion]. Validating the path
     * and negotiating the version is the same question asked once: every answer that is not a
     * version is one of the negative sentinels below, and each of them is fatal.
     */
    private fun resolveApiVersion(targetApp: String, path: String, maxVersion: Int): Int {
        val version = webClient.getApiVersion(targetApp, path, maxVersion)
        when (version) {
            EcosWebClientApi.AV_PATH_NOT_SUPPORTED -> {
                error(
                    "Target app '$targetApp' doesn't support content uploading. " +
                        "You should add EcosWebExecutor with $path path in your webapp."
                )
            }

            EcosWebClientApi.AV_APP_NOT_AVAILABLE -> {
                error("Target app '$targetApp' is not available")
            }

            EcosWebClientApi.AV_VERSION_NOT_SUPPORTED -> {
                error("Target app '$targetApp' has incompatible API version form path $path")
            }
        }
        return version
    }

    private data class DeleteReqBody(
        val storageId: String,
        val storageSourceId: String,
        val dataKey: String
    )

    private data class DeleteRespBody(
        val result: Boolean
    )

    /**
     * [offset] and [length] carry the range exactly as [ContentRange] holds it: [length] is a byte
     * count, or [ContentRange.LENGTH_TO_END] (`-1`) to read until the end of the content. Reading
     * everything is `offset = 0, length = -1` and is not a separate shape of the request.
     *
     * Both are always sent. A target that predates ranges ignores them, because the mapper behind
     * the body reader has `FAIL_ON_UNKNOWN_PROPERTIES` disabled, and answers with the whole content
     * as it always did.
     */
    internal data class DownloadReqBody(
        val storageId: String,
        val storageSourceId: String,
        val dataKey: String,
        val offset: Long,
        val length: Long
    )

    private data class UploadReqHeaders(
        val storageId: String,
        val storageSourceId: String,
        val storageConfig: ObjectData
    )

    private data class UploadRespBody(
        val dataKey: String
    )

    internal data class ChunkedInitReqHeaders(
        val storageId: String
    )

    internal data class ChunkedInitReqBody(
        val size: Long
    )

    /**
     * [state] is the handle of the upload the storage opened for us, echoed back to it on every
     * later call of this upload. It is empty exactly when [supported] is false, because then no
     * upload was opened: ecos-content's `ContentChunkInitWebExecutor` answers
     * `supported=false, state="", chunkSize=0` for a storage that cannot chunk, and an absent handle
     * is the empty string on this wire, never a null.
     */
    internal data class ChunkedInitRespBody(
        val supported: Boolean,
        val state: String = "",
        val chunkSize: Long = 0
    )

    internal data class ChunkWriteReqHeaders(
        val storageId: String,
        val state: String,
        val chunkIndex: Int,
        val contentLength: Long
    )

    internal data class ChunkWriteRespBody(
        val state: String
    )

    internal data class ChunkCompleteReqHeaders(
        val storageId: String
    )

    internal data class ChunkCompleteReqBody(
        val state: String
    )

    internal data class ChunkCompleteRespBody(
        val dataKey: String
    )

    internal data class ChunkAbortReqHeaders(
        val storageId: String
    )

    internal data class ChunkAbortReqBody(
        val state: String
    )

    internal data class ChunkAbortRespBody(
        val result: Boolean
    )
}
