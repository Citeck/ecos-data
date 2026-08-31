package ru.citeck.ecos.data.sql.content.storage.remote

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import ru.citeck.ecos.commons.json.Json
import ru.citeck.ecos.webapp.api.content.ContentRange
import ru.citeck.ecos.webapp.api.entity.EntityRef
import ru.citeck.ecos.webapp.api.promise.Promise
import ru.citeck.ecos.webapp.api.web.EcosWebHeaders
import ru.citeck.ecos.webapp.api.web.body.BodyContentType
import ru.citeck.ecos.webapp.api.web.body.EcosWebBodyReader
import ru.citeck.ecos.webapp.api.web.body.EcosWebBodyWriter
import ru.citeck.ecos.webapp.api.web.client.EcosWebClientApi
import ru.citeck.ecos.webapp.api.web.client.EcosWebClientReq
import ru.citeck.ecos.webapp.api.web.client.EcosWebClientResp
import java.io.ByteArrayInputStream
import java.io.File
import java.io.InputStream
import java.io.OutputStream
import java.nio.file.Path

class EcosContentRemoteStorageRangeTest {

    companion object {
        private const val DATA_KEY = "data-key-1"
        private const val WHOLE_CONTENT = "0123456789"
    }

    private val storageRef = EntityRef.create("emodel", "content-storage", "some-storage")

    @Test
    fun `a target which supports the range api receives the range in the body and the negotiated version`() {
        // the target returns only the requested bytes
        val webClient = RecordingWebClient(responseBytes = "34567".toByteArray(), serverMaxVersion = 1)
        val storage = EcosContentRemoteStorage(webClient)

        val result = storage.readContent(storageRef, DATA_KEY, ContentRange(3, 5)) { String(it.readBytes()) }

        assertThat(webClient.capturedTargetApp).isEqualTo("emodel")
        assertThat(webClient.capturedPath).isEqualTo(EcosContentRemoteStorage.CONTENT_STORAGE_DOWNLOAD)
        assertThat(webClient.capturedVersion).isEqualTo(EcosContentRemoteStorage.DOWNLOAD_RANGE_API_VERSION)

        val body = webClient.capturedDto as EcosContentRemoteStorage.DownloadReqBody
        assertThat(body.offset).isEqualTo(3L)
        assertThat(body.length).isEqualTo(5L)

        assertThat(Json.mapper.toStringNotNull(body)).isEqualTo(
            """{"storageId":"some-storage","storageSourceId":"content-storage",""" +
                """"dataKey":"data-key-1","offset":3,"length":5}"""
        )

        // the target already sliced the content, so the response must be handed over untouched
        assertThat(result).isEqualTo("34567")
    }

    @Test
    fun `a read of the whole content sends the unbounded range and asks for the api version once`() {
        val webClient = RecordingWebClient(responseBytes = WHOLE_CONTENT.toByteArray(), serverMaxVersion = 1)
        val storage = EcosContentRemoteStorage(webClient)

        val result = storage.readContent(storageRef, DATA_KEY, ContentRange.UNBOUNDED) { String(it.readBytes()) }

        // one probe, not two: validating the path and negotiating the version is the same question
        assertThat(webClient.apiVersionProbes).containsExactly(EcosContentRemoteStorage.DOWNLOAD_RANGE_API_VERSION)
        assertThat(webClient.capturedVersion).isEqualTo(EcosContentRemoteStorage.DOWNLOAD_RANGE_API_VERSION)

        // reading everything is a range like any other: offset 0, length "until the end"
        assertThat(Json.mapper.toStringNotNull(webClient.capturedDto!!)).isEqualTo(
            """{"storageId":"some-storage","storageSourceId":"content-storage",""" +
                """"dataKey":"data-key-1","offset":0,"length":-1}"""
        )

        assertThat(result).isEqualTo(WHOLE_CONTENT)
    }

    @Test
    fun `a range which lasts until the end of the content is sent as a length of -1`() {
        val webClient = RecordingWebClient(responseBytes = "456789".toByteArray(), serverMaxVersion = 1)
        val storage = EcosContentRemoteStorage(webClient)

        val result = storage.readContent(storageRef, DATA_KEY, ContentRange.from(4)) { String(it.readBytes()) }

        val body = webClient.capturedDto as EcosContentRemoteStorage.DownloadReqBody
        assertThat(body.offset).isEqualTo(4L)
        assertThat(body.length).isEqualTo(ContentRange.LENGTH_TO_END)

        assertThat(Json.mapper.toStringNotNull(body)).isEqualTo(
            """{"storageId":"some-storage","storageSourceId":"content-storage",""" +
                """"dataKey":"data-key-1","offset":4,"length":-1}"""
        )

        assertThat(result).isEqualTo("456789")
    }

    @Test
    fun `a target without the range api is sent the range anyway and the range is applied locally`() {
        // an old target ignores the fields it doesn't know and answers with the whole content
        val webClient = RecordingWebClient(responseBytes = WHOLE_CONTENT.toByteArray(), serverMaxVersion = 0)
        val storage = EcosContentRemoteStorage(webClient)

        val result = storage.readContent(storageRef, DATA_KEY, ContentRange(3, 5)) { String(it.readBytes()) }

        assertThat(webClient.capturedVersion).isEqualTo(0)
        assertThat(Json.mapper.toStringNotNull(webClient.capturedDto!!)).isEqualTo(
            """{"storageId":"some-storage","storageSourceId":"content-storage",""" +
                """"dataKey":"data-key-1","offset":3,"length":5}"""
        )

        // correct bytes anyway, just without the transfer saving
        assertThat(result).isEqualTo("34567")
    }

    @Test
    fun `an old target still serves a to-the-end range through the local fallback`() {
        val webClient = RecordingWebClient(responseBytes = WHOLE_CONTENT.toByteArray(), serverMaxVersion = 0)
        val storage = EcosContentRemoteStorage(webClient)

        val result = storage.readContent(storageRef, DATA_KEY, ContentRange.from(7)) { String(it.readBytes()) }

        assertThat(webClient.capturedVersion).isEqualTo(0)
        assertThat(result).isEqualTo("789")
    }

    @Test
    fun `a range read fails when the target app is not available`() {
        val webClient = RecordingWebClient(fixedApiVersion = EcosWebClientApi.AV_APP_NOT_AVAILABLE)
        val storage = EcosContentRemoteStorage(webClient)

        assertThatThrownBy {
            storage.readContent(storageRef, DATA_KEY, ContentRange(3, 5)) { it.readBytes() }
        }.hasMessage("Target app 'emodel' is not available")

        assertThat(webClient.capturedTargetApp).isNull()
    }

    @Test
    fun `a range read fails when the download path is not supported`() {
        val webClient = RecordingWebClient(fixedApiVersion = EcosWebClientApi.AV_PATH_NOT_SUPPORTED)
        val storage = EcosContentRemoteStorage(webClient)

        assertThatThrownBy {
            storage.readContent(storageRef, DATA_KEY, ContentRange(3, 5)) { it.readBytes() }
        }.hasMessageContaining("Target app 'emodel' doesn't support content uploading")

        assertThat(webClient.capturedTargetApp).isNull()
    }

    @Test
    fun `a range read fails when the download path api version is not supported`() {
        val webClient = RecordingWebClient(fixedApiVersion = EcosWebClientApi.AV_VERSION_NOT_SUPPORTED)
        val storage = EcosContentRemoteStorage(webClient)

        assertThatThrownBy {
            storage.readContent(storageRef, DATA_KEY, ContentRange(3, 5)) { it.readBytes() }
        }.hasMessageContaining("has incompatible API version")

        assertThat(webClient.capturedTargetApp).isNull()
    }

    // ---- test doubles ----

    /**
     * [serverMinVersion]/[serverMaxVersion] reproduce the arithmetic of the real client:
     * a version above the target's minimum is answered with the lowest of the two maximums.
     * [fixedApiVersion] short-circuits that to return one of the negative sentinels.
     */
    private class RecordingWebClient(
        private val responseBytes: ByteArray = ByteArray(0),
        private val serverMinVersion: Int = 0,
        private val serverMaxVersion: Int = 0,
        private val fixedApiVersion: Int? = null
    ) : EcosWebClientApi {

        var capturedTargetApp: String? = null
        var capturedPath: String? = null
        var capturedVersion: Int? = null
        var capturedDto: Any? = null

        val apiVersionProbes = mutableListOf<Int>()

        override fun getApiVersion(targetApp: String, path: String, maxVersion: Int): Int {
            apiVersionProbes.add(maxVersion)
            fixedApiVersion?.let { return it }
            if (serverMinVersion > maxVersion) {
                return EcosWebClientApi.AV_VERSION_NOT_SUPPORTED
            }
            return minOf(maxVersion, serverMaxVersion)
        }

        override fun newRequest(): EcosWebClientReq = RecordingReq()

        private inner class RecordingReq : EcosWebClientReq {

            override fun targetApp(targetApp: String): EcosWebClientReq {
                capturedTargetApp = targetApp
                return this
            }

            override fun path(path: String): EcosWebClientReq {
                capturedPath = path
                return this
            }

            override fun version(version: Int): EcosWebClientReq {
                capturedVersion = version
                return this
            }

            override fun headers(headers: Any): EcosWebClientReq = this

            override fun header(key: String, value: Any?): EcosWebClientReq = this

            override fun body(body: (EcosWebBodyWriter) -> Unit): EcosWebClientReq {
                body(RecordingBodyWriter())
                return this
            }

            override fun <T> executeSync(response: (EcosWebClientResp) -> T): T {
                return response(RecordingResp())
            }

            override fun <T> execute(response: (EcosWebClientResp) -> T): Promise<T> {
                error("execute is not used in tests")
            }
        }

        private inner class RecordingBodyWriter : EcosWebBodyWriter {

            override fun writeDto(value: Any) {
                capturedDto = value
            }

            override fun getOutputStream(): OutputStream = error("not used")
            override fun start(type: BodyContentType) = error("not used")
            override fun writeText(text: String) = error("not used")
            override fun writeStream(stream: InputStream): Long = error("not used")
            override fun writeBytes(bytes: ByteArray) = error("not used")
            override fun writeFile(file: File) = error("not used")
            override fun writeFile(file: Path) = error("not used")
        }

        private inner class RecordingResp : EcosWebClientResp {
            override fun getHeaders(): EcosWebHeaders = error("not used")
            override fun getBodyReader(): EcosWebBodyReader = RecordingBodyReader()
        }

        private inner class RecordingBodyReader : EcosWebBodyReader {
            override fun <T : Any> readDto(type: Class<out T>): T = error("not used")
            override fun <T : Any> readDtoOrNull(type: Class<out T>): T? = error("not used")
            override fun getType(): BodyContentType = error("not used")
            override fun getInputStream(): InputStream = ByteArrayInputStream(responseBytes)
        }
    }
}
