package ru.citeck.ecos.data.sql.content.storage.remote

import ru.citeck.ecos.commons.data.ObjectData
import ru.citeck.ecos.data.sql.content.storage.EcosContentDataUrl
import ru.citeck.ecos.data.sql.content.storage.EcosContentStorage
import ru.citeck.ecos.data.sql.content.storage.EcosContentStorageConfig
import ru.citeck.ecos.webapp.api.web.client.EcosWebClientApi
import java.io.InputStream
import java.io.OutputStream

class EcosContentRemoteStorage(
    private val webClient: EcosWebClientApi
) : EcosContentStorage {

    companion object {
        const val CONTENT_STORAGE_UPLOAD = "/content/storage/upload"
        const val CONTENT_STORAGE_DOWNLOAD = "/content/storage/download"
        const val CONTENT_STORAGE_DELETE = "/content/storage/delete"
    }

    override fun uploadContent(storage: EcosContentStorageConfig, content: (OutputStream) -> Unit): EcosContentDataUrl {

        if (storage.ref.getAppName().isBlank()) {
            error("Content storage config is mandatory")
        }
        val appName = storage.ref.getAppName()
        validateApiPath(appName, CONTENT_STORAGE_UPLOAD)

        val apiHeaders = UploadReqHeaders(
            storage.ref.getLocalId(),
            storage.ref.getSourceId(),
            storage.config
        )

        val response = webClient.newRequest()
            .targetApp(appName)
            .path(CONTENT_STORAGE_UPLOAD)
            .headers(apiHeaders)
            .body { content.invoke(it.getOutputStream()) }
            .executeSync { it.getBodyReader().readDto(UploadRespBody::class.java) }

        return EcosContentDataUrl(appName, response.path)
    }

    override fun <T> readContent(url: EcosContentDataUrl, action: (InputStream) -> T): T {

        validateApiPath(url.appName, CONTENT_STORAGE_DOWNLOAD)

        return webClient.newRequest()
            .targetApp(url.appName)
            .path(CONTENT_STORAGE_DOWNLOAD)
            .body { it.writeDto(DownloadReqBody(url.contentPath)) }
            .executeSync { action.invoke(it.getBodyReader().getInputStream()) }
    }

    override fun deleteContent(url: EcosContentDataUrl) {

        validateApiPath(url.appName, CONTENT_STORAGE_DELETE)

        webClient.newRequest()
            .targetApp(url.appName)
            .path(CONTENT_STORAGE_DELETE)
            .body { it.writeDto(DeleteReqBody(url.contentPath)) }
            .executeSync { it.getBodyReader().readDto(DeleteRespBody::class.java) }
    }

    private fun validateApiPath(targetApp: String, path: String) {
        when (webClient.getApiVersion(targetApp, path, 0)) {
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
    }

    private data class DeleteReqBody(
        val path: String
    )

    private data class DeleteRespBody(
        val result: Boolean
    )

    private data class DownloadReqBody(
        val path: String
    )

    private data class UploadReqHeaders(
        val storageId: String,
        val storageSourceId: String,
        val storageConfig: ObjectData
    )

    private data class UploadRespBody(
        val path: String
    )
}
