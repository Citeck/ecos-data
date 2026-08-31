package ru.citeck.ecos.data.sql.content.storage.remote

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import ru.citeck.ecos.commons.json.Json
import ru.citeck.ecos.data.sql.content.storage.ChunkedInitMeta
import ru.citeck.ecos.data.sql.content.storage.ChunkedUploadGoneException
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
import java.io.ByteArrayOutputStream
import java.io.File
import java.io.InputStream
import java.io.OutputStream
import java.nio.file.Path

class EcosContentRemoteStorageChunkedTest {

    private val storageRef = EntityRef.create("emodel", "content-storage", "some-storage")

    @Test
    fun `chunkedInit sends storageId header and meta body, parses response`() {
        val webClient = RecordingWebClient(
            response = EcosContentRemoteStorage.ChunkedInitRespBody(true, "state-1", 4096)
        )
        val storage = EcosContentRemoteStorage(webClient)

        val meta = ChunkedInitMeta(12345L)
        val result = storage.chunkedInit(storageRef, meta)

        assertThat(webClient.capturedTargetApp).isEqualTo("emodel")
        assertThat(webClient.capturedPath).isEqualTo(EcosContentRemoteStorage.CONTENT_STORAGE_CHUNK_INIT)

        val headers = webClient.capturedHeaders as EcosContentRemoteStorage.ChunkedInitReqHeaders
        assertThat(headers.storageId).isEqualTo("some-storage")

        val body = webClient.capturedDto as EcosContentRemoteStorage.ChunkedInitReqBody
        assertThat(body.size).isEqualTo(12345L)
        // the wire contract is exactly {"size": <long>} - the storage picks the chunk size itself
        assertThat(Json.mapper.toStringNotNull(body)).isEqualTo("{\"size\":12345}")

        assertThat(result.supported).isTrue()
        assertThat(result.state).isEqualTo("state-1")
        assertThat(result.chunkSize).isEqualTo(4096L)
    }

    @Test
    fun `an unsupported chunk-init response body is readable with an empty state and with none at all`() {
        // ecos-content answers `ChunkInitResponseDto(false, "", 0)` whenever the resolved storage is
        // not a ChunkedContentStorage: no upload was opened, so there is no handle to name, and an
        // absent handle is the empty string on this wire. RecordingWebClient below hands DTOs back
        // without ever serializing them, so only a round trip through the mapper the real body
        // reader uses (WebBodyReaderImpl -> Json.cborMapper for a CBOR body) can pin that such a
        // body is readable at all.
        listOf(
            """{"supported":false,"state":"","chunkSize":0}""",
            """{"supported":false}"""
        ).forEach { wireDoc ->
            val wireBody = Json.cborMapper.toBytesNotNull(Json.mapper.readDataNotNull(wireDoc))

            val parsed = Json.cborMapper.readNotNull(
                wireBody,
                EcosContentRemoteStorage.ChunkedInitRespBody::class.java
            )

            assertThat(parsed.supported).describedAs(wireDoc).isFalse()
            assertThat(parsed.state).describedAs(wireDoc).isEmpty()
            assertThat(parsed.chunkSize).describedAs(wireDoc).isEqualTo(0L)
        }
    }

    @Test
    fun `chunkedInit passes an unsupported response through as an empty-state result`() {
        val webClient = RecordingWebClient(
            response = EcosContentRemoteStorage.ChunkedInitRespBody(false, "", 0)
        )
        val storage = EcosContentRemoteStorage(webClient)

        val result = storage.chunkedInit(storageRef, ChunkedInitMeta(12345L))

        // the caller (DbRecordsContentDao) falls back to a single POST on supported=false, and
        // there is no upload behind such an answer, so the state stays empty
        assertThat(result.supported).isFalse()
        assertThat(result.state).isEmpty()
        assertThat(result.chunkSize).isEqualTo(0L)
    }

    @Test
    fun `chunkedWriteChunk sends headers and streams bytes, parses new state`() {
        val webClient = RecordingWebClient(response = EcosContentRemoteStorage.ChunkWriteRespBody("state-2"))
        val storage = EcosContentRemoteStorage(webClient)

        val bytes = "hello chunk".toByteArray()
        val newState = storage.chunkedWriteChunk(
            storageRef,
            "state-1",
            2,
            ByteArrayInputStream(bytes),
            bytes.size.toLong()
        )

        assertThat(webClient.capturedTargetApp).isEqualTo("emodel")
        assertThat(webClient.capturedPath).isEqualTo(EcosContentRemoteStorage.CONTENT_STORAGE_CHUNK_WRITE)

        val headers = webClient.capturedHeaders as EcosContentRemoteStorage.ChunkWriteReqHeaders
        assertThat(headers.storageId).isEqualTo("some-storage")
        assertThat(headers.state).isEqualTo("state-1")
        assertThat(headers.chunkIndex).isEqualTo(2)
        assertThat(headers.contentLength).isEqualTo(bytes.size.toLong())

        assertThat(webClient.capturedOutputBytes).isEqualTo(bytes)
        assertThat(newState).isEqualTo("state-2")
    }

    @Test
    fun `chunkedComplete sends storageId-only headers plus a state body, and parses dataKey`() {
        val webClient = RecordingWebClient(response = EcosContentRemoteStorage.ChunkCompleteRespBody("data-key-1"))
        val storage = EcosContentRemoteStorage(webClient)

        val dataKey = storage.chunkedComplete(storageRef, "state-3")

        assertThat(webClient.capturedTargetApp).isEqualTo("emodel")
        assertThat(webClient.capturedPath).isEqualTo(EcosContentRemoteStorage.CONTENT_STORAGE_CHUNK_COMPLETE)

        // Headers carry only storageId; `state` travels in the body - ecos-content's
        // ContentChunkCompleteWebExecutor reads it via bodyReader.readDto(...)
        val headers = webClient.capturedHeaders as EcosContentRemoteStorage.ChunkCompleteReqHeaders
        assertThat(headers.storageId).isEqualTo("some-storage")

        val body = webClient.capturedDto as EcosContentRemoteStorage.ChunkCompleteReqBody
        assertThat(body.state).isEqualTo("state-3")

        assertThat(dataKey).isEqualTo("data-key-1")
    }

    @Test
    fun `chunkedAbort sends storageId-only headers plus a state body to the abort path`() {
        val webClient = RecordingWebClient(response = EcosContentRemoteStorage.ChunkAbortRespBody(true))
        val storage = EcosContentRemoteStorage(webClient)

        storage.chunkedAbort(storageRef, "state-4")

        assertThat(webClient.capturedTargetApp).isEqualTo("emodel")
        assertThat(webClient.capturedPath).isEqualTo(EcosContentRemoteStorage.CONTENT_STORAGE_CHUNK_ABORT)

        val headers = webClient.capturedHeaders as EcosContentRemoteStorage.ChunkAbortReqHeaders
        assertThat(headers.storageId).isEqualTo("some-storage")

        val body = webClient.capturedDto as EcosContentRemoteStorage.ChunkAbortReqBody
        assertThat(body.state).isEqualTo("state-4")
    }

    @Test
    fun `chunkedWriteChunk translates a remote failure carrying the gone marker into ChunkedUploadGoneException`() {
        val remoteFailure = RuntimeException("CHUNKED_UPLOAD_GONE: no such upload")
        val webClient = RecordingWebClient(response = Unit, failure = remoteFailure)
        val storage = EcosContentRemoteStorage(webClient)

        assertThatThrownBy {
            storage.chunkedWriteChunk(
                storageRef,
                "state-1",
                0,
                ByteArrayInputStream(ByteArray(0)),
                0
            )
        }.isInstanceOf(ChunkedUploadGoneException::class.java)
            .hasCause(remoteFailure)
    }

    @Test
    fun `chunkedWriteChunk finds the gone marker on a nested cause`() {
        val innermost = RuntimeException("CHUNKED_UPLOAD_GONE: multipart upload does not exist")
        val remoteFailure = RuntimeException("web executor call failed", innermost)
        val webClient = RecordingWebClient(response = Unit, failure = remoteFailure)
        val storage = EcosContentRemoteStorage(webClient)

        assertThatThrownBy {
            storage.chunkedWriteChunk(
                storageRef,
                "state-1",
                0,
                ByteArrayInputStream(ByteArray(0)),
                0
            )
        }.isInstanceOf(ChunkedUploadGoneException::class.java)
            .hasCause(remoteFailure)
    }

    @Test
    fun `chunkedWriteChunk rethrows a remote failure unchanged when no cause carries the gone marker`() {
        val remoteFailure = RuntimeException("connection reset")
        val webClient = RecordingWebClient(response = Unit, failure = remoteFailure)
        val storage = EcosContentRemoteStorage(webClient)

        assertThatThrownBy {
            storage.chunkedWriteChunk(
                storageRef,
                "state-1",
                0,
                ByteArrayInputStream(ByteArray(0)),
                0
            )
        }.isSameAs(remoteFailure)
    }

    @Test
    fun `chunkedComplete translates a remote failure carrying the gone marker into ChunkedUploadGoneException`() {
        val remoteFailure = RuntimeException("CHUNKED_UPLOAD_GONE: no such upload")
        val webClient = RecordingWebClient(response = Unit, failure = remoteFailure)
        val storage = EcosContentRemoteStorage(webClient)

        assertThatThrownBy {
            storage.chunkedComplete(storageRef, "state-3")
        }.isInstanceOf(ChunkedUploadGoneException::class.java)
            .hasCause(remoteFailure)
    }

    @Test
    fun `chunkedComplete rethrows a remote failure unchanged when no cause carries the gone marker`() {
        val remoteFailure = RuntimeException("connection reset")
        val webClient = RecordingWebClient(response = Unit, failure = remoteFailure)
        val storage = EcosContentRemoteStorage(webClient)

        assertThatThrownBy {
            storage.chunkedComplete(storageRef, "state-3")
        }.isSameAs(remoteFailure)
    }

    @Test
    fun `chunkedAbort translates a remote failure carrying the gone marker into ChunkedUploadGoneException`() {
        val remoteFailure = RuntimeException("CHUNKED_UPLOAD_GONE: no such upload")
        val webClient = RecordingWebClient(response = Unit, failure = remoteFailure)
        val storage = EcosContentRemoteStorage(webClient)

        assertThatThrownBy {
            storage.chunkedAbort(storageRef, "state-4")
        }.isInstanceOf(ChunkedUploadGoneException::class.java)
            .hasCause(remoteFailure)
    }

    @Test
    fun `chunkedAbort rethrows a remote failure unchanged when no cause carries the gone marker`() {
        val remoteFailure = RuntimeException("connection reset")
        val webClient = RecordingWebClient(response = Unit, failure = remoteFailure)
        val storage = EcosContentRemoteStorage(webClient)

        assertThatThrownBy {
            storage.chunkedAbort(storageRef, "state-4")
        }.isSameAs(remoteFailure)
    }

    @Test
    fun `chunkedInit does not translate a remote failure even when it carries the gone marker`() {
        // chunkedInit cannot hit an already-swept upload - nothing exists yet - so it must not
        // gain the same translation as the other three chunked calls.
        val remoteFailure = RuntimeException("CHUNKED_UPLOAD_GONE: no such upload")
        val webClient = RecordingWebClient(response = Unit, failure = remoteFailure)
        val storage = EcosContentRemoteStorage(webClient)

        assertThatThrownBy {
            storage.chunkedInit(
                storageRef,
                ChunkedInitMeta(1L)
            )
        }.isSameAs(remoteFailure)
    }

    @Test
    fun `isChunkedSupported returns false when path is not supported`() {
        val webClient = RecordingWebClient(
            response = Unit,
            apiVersion = EcosWebClientApi.AV_PATH_NOT_SUPPORTED
        )
        val storage = EcosContentRemoteStorage(webClient)

        assertThat(storage.isChunkedSupported(storageRef)).isFalse()
    }

    @Test
    fun `isChunkedSupported returns true when path is supported`() {
        val webClient = RecordingWebClient(response = Unit, apiVersion = 0)
        val storage = EcosContentRemoteStorage(webClient)

        assertThat(storage.isChunkedSupported(storageRef)).isTrue()
    }

    @Test
    fun `chunkedInit fails fast when storage app name is blank`() {
        val webClient = RecordingWebClient(response = Unit)
        val storage = EcosContentRemoteStorage(webClient)
        val blankAppNameRef = EntityRef.create("", "content-storage", "some-storage")

        assertThatThrownBy {
            storage.chunkedInit(
                blankAppNameRef,
                ChunkedInitMeta(1L)
            )
        }.hasMessage("Content storage config is mandatory")

        assertThat(webClient.capturedTargetApp).isNull()
    }

    @Test
    fun `isChunkedSupported fails fast when storage app name is blank`() {
        val webClient = RecordingWebClient(response = Unit)
        val storage = EcosContentRemoteStorage(webClient)
        val blankAppNameRef = EntityRef.create("", "content-storage", "some-storage")

        assertThatThrownBy {
            storage.isChunkedSupported(blankAppNameRef)
        }.hasMessage("Content storage config is mandatory")
    }

    // ---- test doubles ----

    private class RecordingWebClient(
        private val response: Any,
        private val apiVersion: Int = 0,
        /**
         * When set, [EcosWebClientReq.executeSync] throws this instead of returning [response].
         */
        private val failure: Throwable? = null
    ) : EcosWebClientApi {

        var capturedTargetApp: String? = null
        var capturedPath: String? = null
        var capturedHeaders: Any? = null
        var capturedDto: Any? = null
        var capturedOutputBytes: ByteArray? = null

        override fun getApiVersion(targetApp: String, path: String, maxVersion: Int): Int = apiVersion

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

            override fun version(version: Int): EcosWebClientReq = this

            override fun headers(headers: Any): EcosWebClientReq {
                capturedHeaders = headers
                return this
            }

            override fun header(key: String, value: Any?): EcosWebClientReq = this

            override fun body(body: (EcosWebBodyWriter) -> Unit): EcosWebClientReq {
                body(RecordingBodyWriter())
                return this
            }

            override fun <T> executeSync(response: (EcosWebClientResp) -> T): T {
                failure?.let { throw it }
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

            override fun getOutputStream(): OutputStream {
                val out = ByteArrayOutputStream()
                capturedOutputBytes = null
                return object : OutputStream() {
                    override fun write(b: Int) {
                        out.write(b)
                        capturedOutputBytes = out.toByteArray()
                    }

                    override fun write(b: ByteArray, off: Int, len: Int) {
                        out.write(b, off, len)
                        capturedOutputBytes = out.toByteArray()
                    }
                }
            }

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

            @Suppress("UNCHECKED_CAST")
            override fun <T : Any> readDto(type: Class<out T>): T = response as T

            override fun <T : Any> readDtoOrNull(type: Class<out T>): T? = readDto(type)

            override fun getType(): BodyContentType = error("not used")
            override fun getInputStream(): InputStream = error("not used")
        }
    }
}
