package ru.citeck.ecos.data.sql.test.content

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import ru.citeck.ecos.commons.data.ObjectData
import ru.citeck.ecos.commons.json.Json
import ru.citeck.ecos.commons.mime.MimeTypes
import ru.citeck.ecos.commons.utils.io.IOUtils
import ru.citeck.ecos.context.lib.auth.AuthContext
import ru.citeck.ecos.data.sql.content.ContentMimeTypeDetector
import ru.citeck.ecos.data.sql.content.DbEcosContentData
import ru.citeck.ecos.data.sql.content.entity.DbContentEntity
import ru.citeck.ecos.data.sql.content.storage.EcosContentChunkedStorage
import ru.citeck.ecos.data.sql.content.storage.EcosContentStorage
import ru.citeck.ecos.data.sql.content.upload.ChunkOutcome
import ru.citeck.ecos.data.sql.content.upload.ChunkedUploadChunkResp
import ru.citeck.ecos.data.sql.content.upload.ChunkedUploadInitReq
import ru.citeck.ecos.data.sql.content.upload.ChunkedUploadPolicy
import ru.citeck.ecos.data.sql.content.upload.ChunkedUploadRecordMeta
import ru.citeck.ecos.data.sql.content.upload.ContentUploadCompletionInProgressException
import ru.citeck.ecos.data.sql.content.upload.ContentUploadSessionGoneException
import ru.citeck.ecos.data.sql.content.upload.ContentUploadSessionNotFoundException
import ru.citeck.ecos.data.sql.content.upload.ContentUploadSessionStatus
import ru.citeck.ecos.data.sql.content.upload.ContentUploadSizeMismatchException
import ru.citeck.ecos.data.sql.content.upload.DbChunkedUploadService
import ru.citeck.ecos.data.sql.content.upload.DbContentUploadSessionEntity
import ru.citeck.ecos.data.sql.dto.DbTableRef
import ru.citeck.ecos.data.sql.records.dao.content.DbRecordsContentDao
import ru.citeck.ecos.data.sql.service.DbDataServiceConfig
import ru.citeck.ecos.data.sql.service.DbDataServiceImpl
import ru.citeck.ecos.data.sql.test.records.DbRecordsTestBase
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType
import ru.citeck.ecos.model.lib.type.dto.TypeContentConfig
import ru.citeck.ecos.model.lib.type.dto.TypeInfo
import ru.citeck.ecos.model.lib.type.dto.TypeModelDef
import ru.citeck.ecos.model.lib.type.dto.WorkspaceScope
import ru.citeck.ecos.model.lib.utils.ModelUtils
import ru.citeck.ecos.records2.RecordConstants
import ru.citeck.ecos.records3.record.dao.impl.proxy.RecordsDaoProxy
import ru.citeck.ecos.records3.record.request.RequestContext
import ru.citeck.ecos.txn.lib.TxnContext
import ru.citeck.ecos.webapp.api.entity.EntityRef
import java.io.ByteArrayInputStream
import java.security.MessageDigest
import java.time.Duration
import java.time.Instant
import java.util.concurrent.CountDownLatch
import java.util.concurrent.CyclicBarrier
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

/**
 * Contract test for the records-level chunked-upload orchestration
 * ([ru.citeck.ecos.data.sql.records.dao.content.DbRecordsContentDao.chunkedUploadInit] and its
 * `writeChunk`/`getInfo`/`complete`/`abort` siblings, reached through
 * [ru.citeck.ecos.data.sql.records.DbRecordsDao.getContentDao]).
 *
 * Runs backend-neutral through [DbRecordsTestBase] (default in-mem; PG/inmem runner subclasses in
 * `ecos-data-sql-pg`/`ecos-data-inmem` just re-trigger this suite under their own package so the
 * modules' `dependenciesToScan` surefire config picks it up - see
 * `InMemChunkedUploadContractTest`/`PgChunkedUploadContractTest`).
 *
 * [EcosContentStorageServiceImpl][ru.citeck.ecos.data.sql.content.storage.EcosContentStorageServiceImpl]
 * routes every non-LOCAL storage ref through a real remote webapp call, which isn't available in this
 * harness. Test seam: [ru.citeck.ecos.data.sql.test.records.DataMockFactory.contentStorageServiceOverride]
 * lets us swap in [TestChunkedStorageService] (wrapping the in-memory [FakeChunkedStorage]) for the
 * schema shared by every DAO this factory creates - set from `init` so it's in place before
 * [ru.citeck.ecos.data.sql.test.records.DataMockFactory.setUp] (a superclass `@BeforeEach`) runs.
 */
open class ChunkedUploadContractTest : DbRecordsTestBase() {

    companion object {
        private val FAKE_STORAGE_REF = EntityRef.create("fake-app", "content-storage", "FAKE")
        private const val CHUNK_SIZE = 8L

        /**
         * Distinctive, non-empty storage config. The chunked calls do not carry one at all (see
         * [EcosContentChunkedStorage]); this is what the type declares, and it still reaches the
         * single-shot [EcosContentStorage.uploadContent] the chunked path is compared against.
         */
        private val FAKE_STORAGE_CONFIG = ObjectData.create().set("bucket", "fake-bucket")

        private val IDLE_TIMEOUT: Duration = Duration.ofMinutes(30)

        /**
         * The policy every test uses unless it is specifically about a limit.
         */
        private val POLICY = ChunkedUploadPolicy(
            maxActiveSessionsPerUser = 10,
            sessionIdleTimeout = IDLE_TIMEOUT
        )

        private const val BIN_MIME_TYPE = "application/octet-stream"

        private const val TYPE_DEFAULT_WORKSPACE = "temp-file-default-ws"

        private const val UPLOAD_WORKSPACE = "chunked-ws"

        /**
         * A random uuid carrying the png extension, i.e. what a blank name has to become - as
         * opposed to the bare ".png" a name completed in the other order would leave behind.
         */
        private const val UUID_WITH_PNG_EXTENSION =
            "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\\.png"

        /**
         * The PNG signature followed by a few filler bytes - a payload a real detector recognizes,
         * so the same input can be fed to both this suite's fake detector and a real one.
         */
        private val PNG_BYTES = byteArrayOf(
            0x89.toByte(), 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A,
            0x00, 0x00, 0x00, 0x0D, 0x49, 0x48, 0x44, 0x52
        )

        /**
         * A type served by a dao standing in a different schema than the one the upload sessions
         * live in.
         */
        private const val OTHER_SCHEMA = "other-records-schema"
        private const val OTHER_SCHEMA_TYPE_ID = "other-schema-type"
        private const val OTHER_SCHEMA_SRC_ID = "other-schema-src"

        /**
         * Types differing only in the shape of their source id: no prefix, this application's own
         * prefix, another application's prefix, and a local part carrying the delimiter itself.
         */
        private const val BARE_SRC_TYPE_ID = "bare-src-type"
        private const val OWN_PREFIX_TYPE_ID = "own-prefix-type"
        private const val NESTED_SRC_TYPE_ID = "nested-src-type"
        private const val NESTED_SRC_ID = "nested/src"
        private const val FOREIGN_PREFIX_TYPE_ID = "foreign-prefix-type"
        private const val FOREIGN_APP_NAME = "other-app"
    }

    private val fakeStorage = FakeChunkedStorage(CHUNK_SIZE, FAKE_STORAGE_REF)

    /**
     * Installed once, for the cases that need a detector at all, and re-aimed per test by
     * [installDetector].
     */
    private val detectorHolder = SwappableMimeTypeDetector()

    /**
     * A chunked upload is started and finished through a records dao - those two calls are about
     * the type and the record - and this suite drives the temp-file one unless a case says otherwise.
     */
    private val contentDao: DbRecordsContentDao
        get() = tempCtx.dao.getContentDao()

    /**
     * Everything between init and complete is schema-level and is asked of the schema directly, not
     * of a dao: the session row, the digest and the storage-side upload belong to [DbSchemaContext].
     */
    private val uploadService: DbChunkedUploadService
        get() = currentSchemaCtx().chunkedUploadService

    init {
        contentStorageServiceOverride = TestChunkedStorageService(fakeStorage, FAKE_STORAGE_REF)
    }

    /**
     * Set by a test that deliberately runs a single-shot upload of its own, which switches off
     * [assertOrchestrationDidNotUseSingleShotUpload] for that test.
     */
    private var expectSingleShotUploads = false

    /**
     * The chunked orchestration assembles the payload inside the storage
     * ([EcosContentChunkedStorage.chunkedInit]/`chunkedWriteChunk`/`chunkedComplete`) and then only
     * registers the resulting `dataKey`, so it must never buffer the bytes back through
     * [EcosContentStorage.uploadContent]. Tests that do not upload single-shot themselves therefore
     * expect that method to stay untouched.
     */
    @AfterEach
    fun assertOrchestrationDidNotUseSingleShotUpload() {
        if (expectSingleShotUploads) {
            return
        }
        assertThat(fakeStorage.singleShotUploadCount)
            .describedAs("single-shot storage uploads performed by the chunked-upload orchestration")
            .isEqualTo(0)
    }

    private fun useFakeStorageForTempFileType() {
        useFakeStorageForType(TEMP_FILE_TYPE_ID)
    }

    /**
     * Points [typeId]'s content storage at [fakeStorage] and (re-)declares its usual
     * `name`/`content` attributes. Idempotent: a test that deliberately breaks the type's model
     * repairs it by calling this again.
     */
    private fun useFakeStorageForType(typeId: String) {
        updateType(typeId) {
            it.withContentConfig(fakeStorageContentConfig())
            it.withModel(contentTypeModel())
        }
    }

    private fun fakeStorageContentConfig(): TypeContentConfig {
        return TypeContentConfig.create()
            .withStorageRef(FAKE_STORAGE_REF)
            .withStorageConfig(FAKE_STORAGE_CONFIG)
            .build()
    }

    private fun contentTypeModel(): TypeModelDef {
        return TypeModelDef.create()
            .withAttributes(
                listOf(
                    AttributeDef.create().withId("name").build(),
                    AttributeDef.create().withId("content").withType(AttributeType.CONTENT).build()
                )
            ).build()
    }

    private fun initReq(
        size: Long = 20,
        ecosType: String = TEMP_FILE_TYPE_ID,
        mimeType: String = "application/octet-stream",
        name: String = "file.bin",
        attributes: ObjectData = ObjectData.create()
    ) = ChunkedUploadInitReq(
        ecosType = ecosType,
        name = name,
        mimeType = mimeType,
        encoding = null,
        size = size,
        attributes = attributes
    )

    private fun policy(
        maxActiveSessionsPerUser: Int = 10,
        sessionIdleTimeout: Duration = IDLE_TIMEOUT
    ) = ChunkedUploadPolicy(maxActiveSessionsPerUser, sessionIdleTimeout)

    private fun sha256Hex(bytes: ByteArray): String {
        return MessageDigest.getInstance("SHA-256").digest(bytes).joinToString("") { "%02x".format(it) }
    }

    private fun <T> asUser(user: String, action: () -> T): T {
        return AuthContext.runAs(user) { RequestContext.doWithCtx { action() } }
    }

    /**
     * Direct access to the schema-level services (upload sessions, content) shared by every DAO.
     */
    private fun currentSchemaCtx() = tempCtx.dataService.getTableContext().getSchemaCtx()

    /**
     * Ages a session by [age], so it reads as idle for that long. Writes `__modified` directly
     * because the session service stamps it with `Instant.now()` on every write.
     */
    private fun backdateModified(uploadId: String, age: Duration) {
        updateSessionRow(
            uploadId,
            DbContentUploadSessionEntity.MODIFIED to Instant.now().minus(age),
            describedAs = "backdated __modified"
        )
    }

    /**
     * Writes [mimeType] into the session's record meta as it stands, bypassing the normalisation
     * [DbRecordsContentDao.chunkedUploadInit] applies to it.
     */
    private fun overwriteSessionMimeType(uploadId: String, mimeType: String) {
        val meta = storedRecordMeta(uploadId).copy(mimeType = mimeType)
        updateSessionRow(
            uploadId,
            DbContentUploadSessionEntity.RECORD_META to (Json.mapper.toString(meta) ?: "{}"),
            describedAs = "overwrote the mime type of __record_meta"
        )
    }

    /**
     * The record meta of a live session, as it is stored.
     */
    private fun storedRecordMeta(uploadId: String): ChunkedUploadRecordMeta {
        val session = currentSchemaCtx().uploadSessionService.findByExtId(uploadId)
            ?: error("Upload session is not found: '$uploadId'")
        return Json.mapper.read(session.recordMeta, ChunkedUploadRecordMeta::class.java)
            ?: error("Upload session '$uploadId' carries an unreadable record meta")
    }

    /**
     * The content row as it stands, read straight off the table: [DbEcosContentData] canonicalizes
     * the mime type on the way out and so reports the same value whatever the row holds, as long as
     * it parses at all.
     */
    private fun storedContentRow(entityRef: EntityRef): DbContentEntity {
        val contentData = tempCtx.dao.getContent(entityRef.getLocalId(), "content") as? DbEcosContentData
            ?: error("Content is not found for '$entityRef'")
        val rows = DbDataServiceImpl(
            DbContentEntity::class.java,
            DbDataServiceConfig.create {
                withTable(DbContentEntity.TABLE)
                withStoreTableMeta(true)
            },
            currentSchemaCtx()
        )
        return rows.findById(contentData.getDbId()) ?: error("Content row is not found for '$entityRef'")
    }

    private fun storedMimeType(entityRef: EntityRef): String {
        return storedContentRow(entityRef).mimeType
    }

    private fun storedName(entityRef: EntityRef): String {
        return storedContentRow(entityRef).name
    }

    /**
     * Writes [column] straight onto the session row, bypassing the session service.
     */
    private fun updateSessionRow(uploadId: String, column: Pair<String, Any>, describedAs: String) {
        val schemaCtx = currentSchemaCtx()
        val sessions = DbDataServiceImpl(
            DbContentUploadSessionEntity::class.java,
            DbDataServiceConfig.create {
                withTable(DbContentUploadSessionEntity.TABLE)
                withStoreTableMeta(true)
            },
            schemaCtx
        )
        val status = schemaCtx.uploadSessionService.findByExtId(uploadId)?.status
            ?: error("Upload session is not found: '$uploadId'")
        val updated = schemaCtx.doInNewTxn {
            sessions.updateByExtIdIfMatches(
                uploadId,
                mapOf(DbContentUploadSessionEntity.STATUS to status),
                mapOf(column)
            )
        }
        assertThat(updated).describedAs("%s of '%s'", describedAs, uploadId).isTrue()
    }

    /**
     * Pushes every chunk of [bytes] through in as few chunks as the fake storage allows and answers
     * the upload id, leaving the completion to the caller.
     */
    private fun uploadChunksOf(bytes: ByteArray, mimeType: String, name: String): String {
        return asUser("user1") {
            val init = contentDao.chunkedUploadInit(
                initReq(size = bytes.size.toLong(), mimeType = mimeType, name = name),
                POLICY
            )
            assertThat(init.supported).isTrue()
            var offset = 0
            while (offset < bytes.size) {
                val length = minOf(init.chunkSize, (bytes.size - offset).toLong()).toInt()
                uploadService.writeChunk(
                    init.uploadId,
                    offset.toLong(),
                    ByteArrayInputStream(bytes, offset, length),
                    length.toLong(),
                    POLICY
                )
                offset += length
            }
            init.uploadId
        }
    }

    /**
     * Runs a whole chunked upload of [bytes] in as few chunks as the fake storage allows and
     * answers the created record.
     */
    private fun uploadChunked(bytes: ByteArray, mimeType: String, name: String): EntityRef {
        val uploadId = uploadChunksOf(bytes, mimeType, name)
        return asUser("user1") { contentDao.chunkedUploadComplete(uploadId, POLICY) }
    }

    private fun installDetector(detector: RecordingMimeTypeDetector): RecordingMimeTypeDetector {
        detectorHolder.target = detector
        return detector
    }

    @Test
    fun `without a detector the client mime type and name are kept and the content is never read`() {
        useFakeStorageForTempFileType()
        assertThat(dataSourceCtx.mimeTypeDetector).isNull()

        val entityRef = uploadChunked(PNG_BYTES, mimeType = "", name = "file")

        assertThat(storedMimeType(entityRef)).isEqualTo(BIN_MIME_TYPE)
        assertThat(storedName(entityRef)).isEqualTo("file")
        assertThat(fakeStorage.readCount)
            .describedAs("content reads performed by the chunked-upload orchestration")
            .isEqualTo(0)
    }

    /**
     * The cases that need a [ContentMimeTypeDetector] behind the data source context. It is wired in
     * from this class's own construction, which JUnit runs before the enclosing class's `@BeforeEach`
     * builds that context - the enclosing class itself keeps the detector-less default.
     */
    @Nested
    inner class WithMimeTypeDetector {

        init {
            mimeTypeDetectorOverride = detectorHolder
        }

        @Test
        fun `a client mime type carrying no information is replaced by the detected one`() {
            useFakeStorageForTempFileType()

            // blank is what a browser sends for an extension it doesn't know, and an unparseable one is
            // normalised to application/octet-stream on the session row - both leave the bytes as the
            // only source of the type
            listOf("", "definitely-not-a-mime-type", BIN_MIME_TYPE).forEach { clientMimeType ->

                val detector = installDetector(RecordingMimeTypeDetector())
                detector.result = MimeTypes.IMG_PNG
                detector.extension = ".png"

                val entityRef = uploadChunked(PNG_BYTES, mimeType = clientMimeType, name = "file")

                assertThat(storedMimeType(entityRef))
                    .describedAs("mime type stored for client input '%s'", clientMimeType)
                    .isEqualTo("image/png")
                assertThat(storedName(entityRef))
                    .describedAs("name stored for client input '%s'", clientMimeType)
                    .isEqualTo("file.png")
                assertThat(detector.detectCalls).hasSize(1)
            }
        }

        @Test
        fun `a detector that recognizes nothing leaves the client mime type and the name alone`() {
            useFakeStorageForTempFileType()

            // "no idea" may be said either way round, and both must mean the same thing here
            listOf(null, MimeTypes.APP_BIN).forEach { detected ->

                val detector = installDetector(RecordingMimeTypeDetector())
                detector.result = detected
                detector.extension = ".png"

                val entityRef = uploadChunked(PNG_BYTES, mimeType = "", name = "file")

                assertThat(storedMimeType(entityRef))
                    .describedAs("mime type stored when the detector answered '%s'", detected)
                    .isEqualTo(BIN_MIME_TYPE)
                assertThat(storedName(entityRef))
                    .describedAs("name stored when the detector answered '%s'", detected)
                    .isEqualTo("file")
                assertThat(detector.detectCalls).hasSize(1)
            }
        }

        @Test
        fun `a client mime type that says something is trusted and the content is not read for it`() {
            useFakeStorageForTempFileType()

            val detector = installDetector(RecordingMimeTypeDetector())
            detector.result = MimeTypes.IMG_PNG
            detector.extension = ".pdf"

            val entityRef = uploadChunked(PNG_BYTES, mimeType = "application/pdf", name = "file")

            assertThat(storedMimeType(entityRef)).isEqualTo("application/pdf")
            assertThat(detector.detectCalls).describedAs("detect calls").isEmpty()
            assertThat(fakeStorage.readCount)
                .describedAs("content reads performed by the chunked-upload orchestration")
                .isEqualTo(0)
            // the name is still completed from the type the client itself stated, exactly as the
            // single-shot path completes it
            assertThat(detector.extensionCalls).containsExactly(MimeTypes.APP_PDF)
            assertThat(storedName(entityRef)).isEqualTo("file.pdf")
        }

        @Test
        fun `a name that already contains a dot is never extended`() {
            useFakeStorageForTempFileType()

            val detector = installDetector(RecordingMimeTypeDetector())
            detector.result = MimeTypes.IMG_PNG
            detector.extension = ".png"

            val entityRef = uploadChunked(PNG_BYTES, mimeType = "", name = "archive.tar.gz")

            assertThat(storedMimeType(entityRef)).isEqualTo("image/png")
            assertThat(storedName(entityRef)).isEqualTo("archive.tar.gz")
            assertThat(detector.extensionCalls).describedAs("getExtension calls").isEmpty()
        }

        @Test
        fun `an extension the detector doesn't know leaves the name alone`() {
            useFakeStorageForTempFileType()

            val detector = installDetector(RecordingMimeTypeDetector())
            detector.result = MimeTypes.IMG_PNG
            detector.extension = ""

            val entityRef = uploadChunked(PNG_BYTES, mimeType = "", name = "file")

            assertThat(storedMimeType(entityRef)).isEqualTo("image/png")
            assertThat(storedName(entityRef)).isEqualTo("file")
        }

        @Test
        fun `a detector breaking its no-throw contract cannot cost the caller the upload`() {
            useFakeStorageForTempFileType()

            val detector = installDetector(RecordingMimeTypeDetector())
            detector.detectFailure = RuntimeException("detector is broken")
            detector.extensionFailure = RuntimeException("detector is broken")

            val entityRef = uploadChunked(PNG_BYTES, mimeType = "", name = "file")

            assertThat(storedMimeType(entityRef)).isEqualTo(BIN_MIME_TYPE)
            assertThat(storedName(entityRef)).isEqualTo("file")

            val readBytes =
                tempCtx.dao.getContent(entityRef.getLocalId(), "content")?.readContent { IOUtils.readAsBytes(it) }
            assertThat(readBytes).isEqualTo(PNG_BYTES)

            // the same for a detector that only breaks while naming the extension: the type it did
            // recognize still lands on the row
            val secondDetector = installDetector(RecordingMimeTypeDetector())
            secondDetector.result = MimeTypes.IMG_PNG
            secondDetector.extensionFailure = RuntimeException("detector is broken")

            val secondRef = uploadChunked(PNG_BYTES, mimeType = "", name = "file")

            assertThat(storedMimeType(secondRef)).isEqualTo("image/png")
            assertThat(storedName(secondRef)).isEqualTo("file")
        }

        /**
         * The two places a completion calls the detector from. A failure that is not the detector
         * answering "I don't recognize this" has to be treated the same way at both of them.
         */
        private val detectorCallSites = mapOf<String, (RecordingMimeTypeDetector, Throwable) -> Unit>(
            "detect" to { detector, failure -> detector.detectFailure = failure },
            "getExtension" to { detector, failure -> detector.extensionFailure = failure }
        )

        /**
         * Completes a chunked upload of [PNG_BYTES] under a detector that recognizes the content but
         * throws [failure] from the call site [injectFailure] picks.
         */
        private fun completeWithFailingDetector(
            failure: Throwable,
            injectFailure: (RecordingMimeTypeDetector, Throwable) -> Unit
        ): CompletionOutcome {

            val detector = installDetector(RecordingMimeTypeDetector())
            // recognized and named, so getExtension is reached too - "file" carries no dot yet
            detector.result = MimeTypes.IMG_PNG
            detector.extension = ".png"
            injectFailure(detector, failure)

            val uploadId = uploadChunksOf(PNG_BYTES, mimeType = "", name = "file")
            return asUser("user1") {
                val error = runCatching { contentDao.chunkedUploadComplete(uploadId, POLICY) }.exceptionOrNull()
                // read and cleared right where it would have been set, so nothing running afterwards
                // - the rest of this test included - inherits an interrupt
                CompletionOutcome(error, Thread.interrupted())
            }
        }

        /**
         * An [Error] is not a detector reporting an unrecognizable content, it is the call not
         * having happened at all. Absorbing it would store a type nobody ever computed, and the
         * bytes are still there to be typed correctly on a retry.
         */
        @Test
        fun `an Error from the detector fails the completion instead of being absorbed into a type`() {
            useFakeStorageForTempFileType()

            detectorCallSites.forEach { (callSite, injectFailure) ->

                val failure = StackOverflowError("detector is broken beyond giving an answer")
                val outcome = completeWithFailingDetector(failure, injectFailure)

                assertThat(outcome.error)
                    .describedAs("completion whose detector threw an Error from %s", callSite)
                    .isSameAs(failure)
                assertThat(outcome.callerInterrupted)
                    .describedAs("interrupt flag of the caller after an Error from %s", callSite)
                    .isFalse()
                assertThat(records.query(tempCtx.createQuery()).getRecords())
                    .describedAs("records created while %s was failing", callSite)
                    .isEmpty()
            }
        }

        /**
         * A thread interrupted while the detector runs is being cancelled; that says nothing about
         * the content, so it is not softened into "type unknown". Catching the exception clears the
         * flag, so the completion has to put it back - otherwise the cancellation is invisible to
         * everything above and the caller runs on.
         */
        @Test
        fun `an interrupted detector fails the completion and leaves the interrupt visible`() {
            useFakeStorageForTempFileType()

            detectorCallSites.forEach { (callSite, injectFailure) ->

                val failure = InterruptedException("the caller was cancelled")
                val outcome = completeWithFailingDetector(failure, injectFailure)

                assertThat(outcome.error)
                    .describedAs("completion whose detector was interrupted in %s", callSite)
                    .isSameAs(failure)
                assertThat(outcome.callerInterrupted)
                    .describedAs("interrupt flag of the caller after %s was interrupted", callSite)
                    .isTrue()
                assertThat(records.query(tempCtx.createQuery()).getRecords())
                    .describedAs("records created while %s was interrupted", callSite)
                    .isEmpty()
            }
        }

        @Test
        fun `the detector is given the file name and exactly the first bytes of the content`() {
            useFakeStorageForTempFileType()

            // one chunk holds the whole payload, so a content longer than the prefix doesn't turn into
            // thousands of round trips
            val bytes = ByteArray(detectorHolder.getPrefixSize() + 100) { (it % 251).toByte() }
            fakeStorage.chunkSize = bytes.size.toLong()

            val detector = installDetector(RecordingMimeTypeDetector())
            detector.result = MimeTypes.IMG_PNG
            detector.extension = ".png"

            uploadChunked(bytes, mimeType = "", name = "no-extension-here")

            assertThat(detector.detectCalls).hasSize(1)
            val call = detector.detectCalls[0]
            assertThat(call.name).isEqualTo("no-extension-here")
            assertThat(call.content).isEqualTo(bytes.copyOf(detectorHolder.getPrefixSize()))
        }

        @Test
        fun `a content shorter than the prefix reaches the detector whole`() {
            useFakeStorageForTempFileType()

            val detector = installDetector(RecordingMimeTypeDetector())
            detector.result = MimeTypes.IMG_PNG
            detector.extension = ".png"

            uploadChunked(PNG_BYTES, mimeType = "", name = "file")

            assertThat(detector.detectCalls).hasSize(1)
            assertThat(detector.detectCalls[0].content).isEqualTo(PNG_BYTES)
        }

        @Test
        fun `a blank name becomes a uuid before it is given an extension`() {
            useFakeStorageForTempFileType()

            val detector = installDetector(RecordingMimeTypeDetector())
            detector.result = MimeTypes.IMG_PNG
            detector.extension = ".png"

            val entityRef = uploadChunked(PNG_BYTES, mimeType = "", name = "")

            assertThat(storedMimeType(entityRef)).isEqualTo("image/png")
            // ".png" is not blank, so a name filled in after the extension step rather than before it
            // would be stored as an extension-only hidden file instead of a named one
            assertThat(storedName(entityRef)).matches(UUID_WITH_PNG_EXTENSION)
        }

        @Test
        fun `a detector answering a non-positive prefix size is skipped, not turned into a 400`() {
            useFakeStorageForTempFileType()

            val detector = installDetector(RecordingMimeTypeDetector())
            detector.prefixSizeAnswer = 0
            detector.result = MimeTypes.IMG_PNG
            detector.extension = ".png"

            val uploadId = uploadChunksOf(PNG_BYTES, mimeType = BIN_MIME_TYPE, name = "file.bin")

            // Reading the prefix as ContentRange(0, 0) throws an IllegalArgumentException, which the
            // webmvc content controller answers as a 400: a misconfigured detector reported to the
            // client as its own mistake, in the middle of a completion it cannot retry out of. The
            // completion must go through instead, keeping the type the client stated - exactly what
            // the detector-less path does.
            val completed = asUser("user1") { runCatching { contentDao.chunkedUploadComplete(uploadId, POLICY) } }
            assertThat(completed.exceptionOrNull())
                .describedAs("completion under a detector with no prefix to read")
                .isNull()

            val entityRef = completed.getOrThrow()
            assertThat(storedName(entityRef)).isEqualTo("file.bin")
        }

        @Test
        fun `a storage that can't serve the content back fails the completion instead of mistyping it`() {
            useFakeStorageForTempFileType()

            val detector = installDetector(RecordingMimeTypeDetector())
            detector.result = MimeTypes.IMG_PNG
            detector.extension = ".png"

            val uploadId = uploadChunksOf(PNG_BYTES, mimeType = "", name = "file")

            fakeStorage.readFailure = RuntimeException("storage is unavailable")
            val error = asUser("user1") {
                runCatching { contentDao.chunkedUploadComplete(uploadId, POLICY) }.exceptionOrNull()
            }
            assertThat(error).describedAs("completion over an unreadable storage").isNotNull
            assertThat(error!!).hasStackTraceContaining("storage is unavailable")

            // nothing was stored under the type the client couldn't name
            assertThat(records.query(tempCtx.createQuery()).getRecords()).isEmpty()

            // and the completion stays re-enterable: the assembled object's key is already on the
            // session, which still belongs to this completer, so the client's retry finishes it
            val session = currentSchemaCtx().uploadSessionService.findByExtId(uploadId)!!
            assertThat(session.status).isEqualTo("COMPLETING")
            assertThat(session.dataKey).isNotBlank()

            fakeStorage.readFailure = null
            // no backdating: the failed completion released its lease as it threw, so the client's
            // retry is served at once rather than after the lease
            val entityRef = asUser("user1") { contentDao.chunkedUploadComplete(uploadId, POLICY) }

            assertThat(storedMimeType(entityRef)).isEqualTo("image/png")
            assertThat(storedName(entityRef)).isEqualTo("file.png")
        }
    }

    @Test
    fun `a mime type the client made up is normalised on the session, on the info and on the finished record`() {
        useFakeStorageForTempFileType()

        val bytes = ByteArray(4) { it.toByte() }

        val entityRef = asUser("user1") {
            val init = contentDao.chunkedUploadInit(initReq(size = 4, mimeType = "definitely-not-a-mime-type"), POLICY)
            assertThat(init.supported).isTrue()

            assertThat(storedRecordMeta(init.uploadId).mimeType).isEqualTo(BIN_MIME_TYPE)

            uploadService.writeChunk(init.uploadId, 0, ByteArrayInputStream(bytes), 4, POLICY)

            val info = uploadService.getInfo(init.uploadId, POLICY)
            assertThat(info.mimeType).isEqualTo(BIN_MIME_TYPE)

            contentDao.chunkedUploadComplete(init.uploadId, POLICY)
        }

        // Reading any of these on a record whose content row holds an unparseable mime type throws
        // InvalidMimeType out of EcosContentDataImpl.getMimeType instead of degrading, and the row is
        // already committed by then - so the value must never reach the row in the first place.
        assertThat(records.getAtt(entityRef, "content.mimeType").asText()).isEqualTo(BIN_MIME_TYPE)
        assertThat(records.getAtt(entityRef, "_content.mimeType").asText()).isEqualTo(BIN_MIME_TYPE)
        assertThat(records.getAtt(entityRef, "content.previewInfo.originalMimeType").asText())
            .isEqualTo(BIN_MIME_TYPE)
        assertThat(records.getAtt(entityRef, "_content.previewInfo.originalMimeType").asText())
            .isEqualTo(BIN_MIME_TYPE)
    }

    @Test
    fun `a mime type left un-normalised on the session row does not reach the content row`() {
        useFakeStorageForTempFileType()

        val bytes = ByteArray(4) { it.toByte() }

        listOf(
            "definitely-not-a-mime-type" to BIN_MIME_TYPE,
            "TEXT/Plain;  Charset=UTF-8" to "text/plain; charset=UTF-8"
        ).forEach { (rawMimeType, expectedMimeType) ->

            val entityRef = asUser("user1") {
                val init = contentDao.chunkedUploadInit(initReq(size = 4), POLICY)
                uploadService.writeChunk(init.uploadId, 0, ByteArrayInputStream(bytes), 4, POLICY)
                // complete takes the mime type off the session row, so the row is the last place a
                // raw value can enter the content row from
                overwriteSessionMimeType(init.uploadId, rawMimeType)
                contentDao.chunkedUploadComplete(init.uploadId, POLICY)
            }

            assertThat(storedMimeType(entityRef))
                .describedAs("mime type stored for session row value '%s'", rawMimeType)
                .isEqualTo(expectedMimeType)
        }
    }

    @Test
    fun `the client mime type is stored in its canonical form, the same one uploadFile stores`() {
        useFakeStorageForTempFileType()
        expectSingleShotUploads = true

        val bytes = ByteArray(4) { it.toByte() }

        // A blank one (what a browser sends for a type it doesn't know), an unparseable one, one
        // whose case and parameter spacing only the canonical form fixes, one that is already
        // canonical, and one whose parameters arrive out of order.
        val cases = listOf(
            "" to BIN_MIME_TYPE,
            "definitely-not-a-mime-type" to BIN_MIME_TYPE,
            "TEXT/Plain;  Charset=UTF-8" to "text/plain; charset=UTF-8",
            "application/pdf" to "application/pdf",
            "application/pdf; b=2; a=1" to "application/pdf; a=1; b=2"
        )

        cases.forEach { (rawMimeType, expectedMimeType) ->

            val chunkedRef = asUser("user1") {
                val init = contentDao.chunkedUploadInit(initReq(size = 4, mimeType = rawMimeType), POLICY)
                assertThat(init.supported).isTrue()
                uploadService.writeChunk(init.uploadId, 0, ByteArrayInputStream(bytes), 4, POLICY)
                contentDao.chunkedUploadComplete(init.uploadId, POLICY)
            }
            val singleShotRef = asUser("user1") {
                contentDao.uploadFile(
                    ecosType = TEMP_FILE_TYPE_ID,
                    name = "file.bin",
                    mimeType = rawMimeType
                ) { it.writeBytes(bytes) }
            }

            assertThat(storedMimeType(chunkedRef))
                .describedAs("mime type stored by the chunked upload for client input '%s'", rawMimeType)
                .isEqualTo(expectedMimeType)
            assertThat(storedMimeType(singleShotRef))
                .describedAs("mime type stored by uploadFile for client input '%s'", rawMimeType)
                .isEqualTo(expectedMimeType)
        }
    }

    @Test
    fun `full cycle - init, write chunks, complete produces a temp-file record with correct content and sha256`() {
        useFakeStorageForTempFileType()

        val bytes = ByteArray(20) { it.toByte() }

        val entityRef = asUser("user1") {
            val init = contentDao.chunkedUploadInit(initReq(), POLICY)
            assertThat(init.supported).isTrue()
            assertThat(init.reason).isEmpty()
            assertThat(init.chunkSize).isEqualTo(CHUNK_SIZE)
            assertThat(init.uploadId).isNotBlank()

            val chunk0 =
                uploadService.writeChunk(init.uploadId, 0, ByteArrayInputStream(bytes, 0, 8), 8, POLICY)
            assertThat(chunk0.outcome).isEqualTo(ChunkOutcome.ACCEPTED)
            assertThat(chunk0.offset).isEqualTo(8)

            val chunk1 =
                uploadService.writeChunk(init.uploadId, 8, ByteArrayInputStream(bytes, 8, 8), 8, POLICY)
            assertThat(chunk1.outcome).isEqualTo(ChunkOutcome.ACCEPTED)
            assertThat(chunk1.offset).isEqualTo(16)

            val chunk2 = uploadService.writeChunk(
                init.uploadId,
                16,
                ByteArrayInputStream(bytes, 16, 4),
                4,
                POLICY
            )
            assertThat(chunk2.outcome).isEqualTo(ChunkOutcome.ACCEPTED)
            assertThat(chunk2.offset).isEqualTo(20)

            val info = uploadService.getInfo(init.uploadId, POLICY)
            assertThat(info.status).isEqualTo("ACTIVE")
            assertThat(info.offset).isEqualTo(20)
            assertThat(info.size).isEqualTo(20)

            contentDao.chunkedUploadComplete(init.uploadId, POLICY)
        }

        assertThat(entityRef.getSourceId()).isEqualTo(tempCtx.dao.getId())

        val readBytes =
            tempCtx.dao.getContent(entityRef.getLocalId(), "content")?.readContent { IOUtils.readAsBytes(it) }
        assertThat(readBytes).isEqualTo(bytes)

        val sha256Att = records.getAtt(entityRef, "content.sha256").asText()
        assertThat(sha256Att).isEqualTo(sha256Hex(bytes))
    }

    @Test
    fun `init rejects a type with no content attribute before creating a session`() {
        useFakeStorageForTempFileType()
        updateType(TEMP_FILE_TYPE_ID) {
            it.withModel(TypeModelDef.create { withAttributes(emptyList()) })
        }

        asUser("user1") {
            val creatorRefId = tempCtx.dao.getRecordsDaoCtx().getOrCreateUserRefId("user1")
            val activeBefore = currentSchemaCtx().uploadSessionService
                .countActiveByCreator(creatorRefId, Instant.now().minus(IDLE_TIMEOUT))

            val error = runCatching { contentDao.chunkedUploadInit(initReq(size = 8), POLICY) }.exceptionOrNull()
            assertThat(error).isInstanceOf(IllegalArgumentException::class.java)
            assertThat(error!!.message).contains("content")

            // no orphan session row, and no storage-side upload left open
            assertThat(
                currentSchemaCtx().uploadSessionService
                    .countActiveByCreator(creatorRefId, Instant.now().minus(IDLE_TIMEOUT))
            ).isEqualTo(activeBefore)
            assertThat(fakeStorage.activeCount).isEqualTo(0)
        }
    }

    /**
     * A dao in another schema is refused like any other dao that is not this one: what is compared
     * is the source id, and the schema is simply one of the reasons two daos are not the same.
     */
    @Test
    fun `init rejects a type served by a dao in another schema`() {
        useFakeStorageForTempFileType()
        registerType(
            TypeInfo.create()
                .withId(OTHER_SCHEMA_TYPE_ID)
                .withSourceId(OTHER_SCHEMA_SRC_ID)
                .withContentConfig(fakeStorageContentConfig())
                .withModel(contentTypeModel())
                .build()
        )
        createRecordsDao(
            DbTableRef(OTHER_SCHEMA, "other-records-table"),
            ModelUtils.getTypeRef(OTHER_SCHEMA_TYPE_ID),
            OTHER_SCHEMA_SRC_ID
        )

        asUser("user1") {
            val error = runCatching {
                contentDao.chunkedUploadInit(initReq(size = 8, ecosType = OTHER_SCHEMA_TYPE_ID), POLICY)
            }.exceptionOrNull()

            assertThat(error).isNotNull
            assertThat(error!!.message)
                .contains(OTHER_SCHEMA_TYPE_ID)
                .contains(OTHER_SCHEMA_SRC_ID)
                .contains(TEMP_FILE_TYPE_ID)

            assertThat(fakeStorage.activeCount).isEqualTo(0)
        }
    }

    /**
     * A type's source id may or may not carry an app name prefix, and the local part may itself
     * contain the delimiter. Only the first delimiter separates the two, exactly as [EntityRef]
     * parses a ref, and only this application's own prefix may be stripped - what the local part is
     * then compared against is this dao's own source id.
     */
    @Test
    fun `init accepts a type of this dao however its source id is spelled`() {
        useFakeStorageForTempFileType()

        registerTypeWithSrcId(BARE_SRC_TYPE_ID, TEMP_FILE_TYPE_ID)
        registerTypeWithSrcId(OWN_PREFIX_TYPE_ID, "$APP_NAME${EntityRef.APP_NAME_DELIMITER}$TEMP_FILE_TYPE_ID")
        val foreignSrcId = "$FOREIGN_APP_NAME${EntityRef.APP_NAME_DELIMITER}$TEMP_FILE_TYPE_ID"
        registerTypeWithSrcId(FOREIGN_PREFIX_TYPE_ID, foreignSrcId)

        asUser("user1") {
            listOf(BARE_SRC_TYPE_ID, OWN_PREFIX_TYPE_ID).forEach { typeId ->
                val init = contentDao.chunkedUploadInit(initReq(size = 8, ecosType = typeId), POLICY)
                assertThat(init.supported)
                    .describedAs("init accepted for type '$typeId'")
                    .isTrue()
                uploadService.abort(init.uploadId, POLICY)
            }

            val error = runCatching {
                contentDao.chunkedUploadInit(initReq(size = 8, ecosType = FOREIGN_PREFIX_TYPE_ID), POLICY)
            }.exceptionOrNull()

            assertThat(error).isNotNull
            assertThat(error!!.message)
                .contains(FOREIGN_PREFIX_TYPE_ID)
                .contains(foreignSrcId)
                .contains("another application")

            // refused before the storage side is touched
            assertThat(fakeStorage.activeCount).isEqualTo(0)
        }
    }

    /**
     * The dao that receives the call is the dao that will create the record, so a type whose records
     * another dao serves is refused even when that dao stands in this very schema and could have
     * been reached: the record would otherwise be written here, under a type whose records are
     * expected to live in the other dao's table.
     */
    @Test
    fun `init rejects a type served by another dao of this schema`() {
        useFakeStorageForTempFileType()
        useFakeStorageForType(ATTACHMENT_TYPE_ID)

        asUser("user1") {
            val error = runCatching {
                contentDao.chunkedUploadInit(initReq(size = 8, ecosType = ATTACHMENT_TYPE_ID), POLICY)
            }.exceptionOrNull()

            assertThat(error).isNotNull
            assertThat(error!!.message)
                .contains(ATTACHMENT_TYPE_ID)
                .contains(TEMP_FILE_TYPE_ID)

            assertThat(fakeStorage.activeCount).isEqualTo(0)
            assertThat(records.query(attachmentCtx.createQuery()).getRecords()).isEmpty()
            assertThat(records.query(tempCtx.createQuery()).getRecords()).isEmpty()
        }
    }

    /**
     * The type is checked again at completion, because the record is created there and the type's
     * source id may be reconfigured while the session is in flight - a session that started on a
     * type this dao served must not quietly produce a record of a type that has moved away.
     */
    @Test
    fun `complete rejects a type that moved to another dao while the session was in flight`() {
        useFakeStorageForTempFileType()
        useFakeStorageForType(ATTACHMENT_TYPE_ID)
        registerTypeWithSrcId(BARE_SRC_TYPE_ID, TEMP_FILE_TYPE_ID)

        val bytes = ByteArray(4) { (it + 1).toByte() }
        val uploadId = asUser("user1") {
            val init = contentDao.chunkedUploadInit(initReq(size = 4, ecosType = BARE_SRC_TYPE_ID), POLICY)
            assertThat(init.supported).isTrue()
            uploadService.writeChunk(init.uploadId, 0, ByteArrayInputStream(bytes), 4, POLICY)
            init.uploadId
        }

        registerTypeWithSrcId(BARE_SRC_TYPE_ID, ATTACHMENT_TYPE_ID)

        asUser("user1") {
            val error = runCatching { contentDao.chunkedUploadComplete(uploadId, POLICY) }.exceptionOrNull()

            assertThat(error).isNotNull
            assertThat(error!!.message)
                .contains(BARE_SRC_TYPE_ID)
                .contains(ATTACHMENT_TYPE_ID)

            assertThat(records.query(tempCtx.createQuery()).getRecords()).isEmpty()
            assertThat(records.query(attachmentCtx.createQuery()).getRecords()).isEmpty()
        }
    }

    /**
     * A [RecordsDaoProxy] is refused rather than unwrapped: its source id is not this dao's, so the
     * record it would have produced is not this dao's to create. The local part keeps every
     * delimiter after the first, which is what makes the comparison see `nested/src` and not
     * `nested`.
     */
    @Test
    fun `a proxied source id is refused rather than unwrapped`() {
        useFakeStorageForTempFileType()
        useFakeStorageForType(ATTACHMENT_TYPE_ID)

        val proxiedSrcId = "$APP_NAME${EntityRef.APP_NAME_DELIMITER}$NESTED_SRC_ID"
        registerTypeWithSrcId(NESTED_SRC_TYPE_ID, proxiedSrcId)
        records.register(RecordsDaoProxy(NESTED_SRC_ID, attachmentCtx.dao.getId(), null))

        asUser("user1") {
            val error = runCatching {
                contentDao.chunkedUploadInit(initReq(size = 8, ecosType = NESTED_SRC_TYPE_ID), POLICY)
            }.exceptionOrNull()

            assertThat(error).isNotNull
            assertThat(error!!.message)
                .contains(NESTED_SRC_TYPE_ID)
                .contains(proxiedSrcId)
                .contains(TEMP_FILE_TYPE_ID)

            assertThat(fakeStorage.activeCount).isEqualTo(0)
            assertThat(records.query(attachmentCtx.createQuery()).getRecords()).isEmpty()
        }
    }

    private fun registerTypeWithSrcId(typeId: String, sourceId: String) {
        registerType(
            TypeInfo.create()
                .withId(typeId)
                .withSourceId(sourceId)
                .withContentConfig(fakeStorageContentConfig())
                .withModel(contentTypeModel())
                .build()
        )
    }

    @Test
    fun `full cycle with a payload spanning several SHA-256 blocks - digest state round-trips through the DB at every chunk`() {
        useFakeStorageForTempFileType()

        // 300 bytes over 8-byte chunks (37 full chunks + a 4-byte tail) crosses the SHA-256 64-byte
        // block boundary several times: the cumulative byte counts of 64, 128, 192 and 256 land
        // exactly on a block boundary, so the digest_state persisted to and restored from the DB at
        // that chunk carries an advanced H with an empty pending tail, while every chunk boundary in
        // between (e.g. 72, 136...) persists an advanced H together with a non-empty pending tail.
        // Neither case is reachable with the 20-byte payload used by the basic full-cycle test above,
        // since it never crosses even one 64-byte block - the persisted state there always carries the
        // untouched initial H and only the pending tail varies.
        val bytes = ByteArray(300) { it.toByte() }

        val entityRef = asUser("user1") {
            val init = contentDao.chunkedUploadInit(initReq(size = bytes.size.toLong()), POLICY)
            assertThat(init.supported).isTrue()

            var offset = 0
            while (offset < bytes.size) {
                val chunkLen = minOf(CHUNK_SIZE.toInt(), bytes.size - offset)
                val resp = uploadService.writeChunk(
                    init.uploadId,
                    offset.toLong(),
                    ByteArrayInputStream(bytes, offset, chunkLen),
                    chunkLen.toLong(),
                    POLICY
                )
                assertThat(resp.outcome).isEqualTo(ChunkOutcome.ACCEPTED)
                offset += chunkLen
            }
            assertThat(offset).isEqualTo(bytes.size)

            val info = uploadService.getInfo(init.uploadId, POLICY)
            assertThat(info.offset).isEqualTo(bytes.size.toLong())

            contentDao.chunkedUploadComplete(init.uploadId, POLICY)
        }

        val readBytes =
            tempCtx.dao.getContent(entityRef.getLocalId(), "content")?.readContent { IOUtils.readAsBytes(it) }
        assertThat(readBytes).isEqualTo(bytes)

        val sha256Att = records.getAtt(entityRef, "content.sha256").asText()
        assertThat(sha256Att).isEqualTo(sha256Hex(bytes))
    }

    @Test
    fun `duplicate chunk write is a no-op and does not corrupt the upload`() {
        useFakeStorageForTempFileType()
        val bytes = ByteArray(20) { (it + 1).toByte() }

        val entityRef = asUser("user1") {
            val init = contentDao.chunkedUploadInit(initReq(), POLICY)

            val first =
                uploadService.writeChunk(init.uploadId, 0, ByteArrayInputStream(bytes, 0, 8), 8, POLICY)
            assertThat(first.outcome).isEqualTo(ChunkOutcome.ACCEPTED)

            // resend the same first chunk, as a client would after a lost response
            val duplicate =
                uploadService.writeChunk(init.uploadId, 0, ByteArrayInputStream(bytes, 0, 8), 8, POLICY)
            assertThat(duplicate.outcome).isEqualTo(ChunkOutcome.DUPLICATE)
            assertThat(duplicate.offset).isEqualTo(8)

            uploadService.writeChunk(init.uploadId, 8, ByteArrayInputStream(bytes, 8, 8), 8, POLICY)
            uploadService.writeChunk(init.uploadId, 16, ByteArrayInputStream(bytes, 16, 4), 4, POLICY)

            contentDao.chunkedUploadComplete(init.uploadId, POLICY)
        }

        val readBytes =
            tempCtx.dao.getContent(entityRef.getLocalId(), "content")?.readContent { IOUtils.readAsBytes(it) }
        assertThat(readBytes).isEqualTo(bytes)
    }

    /**
     * Every declared byte is confirmed already, so there is no chunk left to place: the expected
     * length of a chunk at this offset is zero, and a zero-length part must never reach the storage.
     * The caller is told the same thing a resent chunk is told - it has sent everything, `complete`
     * is what comes next.
     */
    @Test
    fun `a chunk written when every byte is already confirmed is a duplicate and never reaches the storage`() {
        useFakeStorageForTempFileType()
        val bytes = ByteArray(4) { (it + 1).toByte() }

        val entityRef = asUser("user1") {
            val init = contentDao.chunkedUploadInit(initReq(size = 4), POLICY)
            uploadService.writeChunk(init.uploadId, 0, ByteArrayInputStream(bytes), 4, POLICY)

            val writesBefore = fakeStorage.chunkWriteCount
            val resp = uploadService.writeChunk(
                init.uploadId,
                4,
                ByteArrayInputStream(ByteArray(0)),
                0,
                POLICY
            )

            assertThat(resp.outcome).isEqualTo(ChunkOutcome.DUPLICATE)
            assertThat(resp.offset).isEqualTo(4)
            assertThat(fakeStorage.chunkWriteCount)
                .describedAs("chunk writes performed by the storage")
                .isEqualTo(writesBefore)

            contentDao.chunkedUploadComplete(init.uploadId, POLICY)
        }

        val readBytes =
            tempCtx.dao.getContent(entityRef.getLocalId(), "content")?.readContent { IOUtils.readAsBytes(it) }
        assertThat(readBytes).isEqualTo(bytes)
    }

    @Test
    fun `writing ahead of the confirmed offset returns CONFLICT`() {
        useFakeStorageForTempFileType()
        asUser("user1") {
            val init = contentDao.chunkedUploadInit(initReq(), POLICY)
            val resp = uploadService.writeChunk(
                init.uploadId,
                8,
                ByteArrayInputStream(ByteArray(8)),
                8,
                POLICY
            )
            assertThat(resp.outcome).isEqualTo(ChunkOutcome.CONFLICT)
            assertThat(resp.offset).isEqualTo(0)
        }
    }

    @Test
    fun `chunk write races - the loser gets CONFLICT and the final content is not corrupted`() {
        useFakeStorageForTempFileType()

        val bytes = ByteArray(20) { (it + 3).toByte() }

        val entityRef = asUser("racer") {
            val init = contentDao.chunkedUploadInit(initReq(size = 20), POLICY)

            fakeStorage.writeBarrier = CyclicBarrier(2)
            val executor = Executors.newFixedThreadPool(2)
            try {
                val futures = (0 until 2).map {
                    executor.submit<ChunkedUploadChunkResp> {
                        asUser("racer") {
                            uploadService.writeChunk(
                                init.uploadId,
                                0,
                                ByteArrayInputStream(bytes, 0, 8),
                                8,
                                POLICY
                            )
                        }
                    }
                }
                val results = futures.map { it.get(10, TimeUnit.SECONDS) }

                val accepted = results.filter { it.outcome == ChunkOutcome.ACCEPTED }
                val conflicted = results.filter { it.outcome == ChunkOutcome.CONFLICT }

                assertThat(accepted).hasSize(1)
                assertThat(conflicted).hasSize(1)
                assertThat(accepted.single().offset).isEqualTo(8)
                assertThat(conflicted.single().offset).isEqualTo(8)
            } finally {
                fakeStorage.writeBarrier = null
                executor.shutdownNow()
            }

            // Finish the upload. Both racers wrote the same chunkIndex 0, so if the storage
            // double appended their bytes instead of overwriting by index, the final content would
            // be 8 bytes too long.
            uploadService.writeChunk(init.uploadId, 8, ByteArrayInputStream(bytes, 8, 8), 8, POLICY)
            uploadService.writeChunk(init.uploadId, 16, ByteArrayInputStream(bytes, 16, 4), 4, POLICY)

            contentDao.chunkedUploadComplete(init.uploadId, POLICY)
        }

        val readBytes =
            tempCtx.dao.getContent(entityRef.getLocalId(), "content")?.readContent { IOUtils.readAsBytes(it) }
        assertThat(readBytes).isEqualTo(bytes)
    }

    @Test
    fun `invalid chunk length is rejected and the offset does not move`() {
        useFakeStorageForTempFileType()
        asUser("user1") {
            val init = contentDao.chunkedUploadInit(initReq(), POLICY)

            val ex = runCatching {
                uploadService.writeChunk(
                    init.uploadId,
                    0,
                    ByteArrayInputStream(ByteArray(5)),
                    5,
                    POLICY
                )
            }.exceptionOrNull()
            assertThat(ex).isInstanceOf(IllegalArgumentException::class.java)

            val info = uploadService.getInfo(init.uploadId, POLICY)
            assertThat(info.offset).isEqualTo(0)
        }
    }

    @Test
    fun `a corrupt digest state fails the next chunk with IllegalStateException and leaks nothing`() {
        useFakeStorageForTempFileType()
        asUser("user1") {
            val bytes = ByteArray(20) { it.toByte() }
            val init = contentDao.chunkedUploadInit(initReq(), POLICY)
            uploadService.writeChunk(init.uploadId, 0, ByteArrayInputStream(bytes, 0, 8), 8, POLICY)

            corruptDigestState(init.uploadId)

            val ex = runCatching {
                uploadService.writeChunk(init.uploadId, 8, ByteArrayInputStream(bytes, 8, 8), 8, POLICY)
            }.exceptionOrNull()

            assertCorruptDigestStateFailure(ex, init.uploadId)
        }
    }

    @Test
    fun `a corrupt digest state fails complete with IllegalStateException and leaks nothing`() {
        useFakeStorageForTempFileType()
        asUser("user1") {
            val init = contentDao.chunkedUploadInit(initReq(size = 8), POLICY)
            uploadService.writeChunk(init.uploadId, 0, ByteArrayInputStream(ByteArray(8)), 8, POLICY)

            corruptDigestState(init.uploadId)

            val ex = runCatching { contentDao.chunkedUploadComplete(init.uploadId, POLICY) }.exceptionOrNull()

            assertCorruptDigestStateFailure(ex, init.uploadId)
        }
    }

    /**
     * Replaces the session's persisted `digest_state` with a truncated one - exactly the kind of row
     * [ru.citeck.ecos.commons.utils.digest.ResumableSha256.restore] rejects - while leaving the
     * confirmed offset (and everything else) where it was.
     */
    private fun corruptDigestState(uploadId: String) {
        val schemaCtx = currentSchemaCtx()
        val session = schemaCtx.uploadSessionService.findByExtId(uploadId)!!
        assertThat(session.digestState).isNotEmpty()
        val corrupted = session.digestState.copyOf(session.digestState.size - 1)
        val written = schemaCtx.doInNewTxn {
            schemaCtx.uploadSessionService.advanceOffset(
                uploadId,
                session.confirmedOffset,
                session.confirmedOffset,
                corrupted,
                session.storageState
            )
        }
        assertThat(written).describedAs("corrupted digest_state written").isTrue()
    }

    /**
     * A corrupt `digest_state` is a server-side fault, not a bad request: it must surface as an
     * [IllegalStateException] (a 500 through the webmvc content controller) rather than the
     * [IllegalArgumentException] `restore` raises - which that controller would turn into a 400
     * whose body spells out the digest state layout.
     */
    private fun assertCorruptDigestStateFailure(ex: Throwable?, uploadId: String) {
        assertThat(ex).isInstanceOf(IllegalStateException::class.java)
        assertThat(ex).isNotInstanceOf(IllegalArgumentException::class.java)
        val message = ex!!.message ?: ""
        assertThat(message).contains(uploadId)
        assertThat(message).doesNotContain("SHA-256", "byte count", "version", "bytes")
    }

    @Test
    fun `complete before all bytes are confirmed throws ContentUploadSizeMismatchException`() {
        useFakeStorageForTempFileType()
        asUser("user1") {
            val init = contentDao.chunkedUploadInit(initReq(size = 20), POLICY)
            uploadService.writeChunk(init.uploadId, 0, ByteArrayInputStream(ByteArray(8)), 8, POLICY)

            val ex = runCatching { contentDao.chunkedUploadComplete(init.uploadId, POLICY) }.exceptionOrNull()
            assertThat(ex).isInstanceOf(ContentUploadSizeMismatchException::class.java)
            ex as ContentUploadSizeMismatchException
            assertThat(ex.offset).isEqualTo(8)
            assertThat(ex.size).isEqualTo(20)
        }
    }

    @Test
    fun `repeated complete returns the same EntityRef without re-completing storage`() {
        useFakeStorageForTempFileType()

        asUser("user1") {
            val init = contentDao.chunkedUploadInit(initReq(size = 8), POLICY)
            uploadService.writeChunk(init.uploadId, 0, ByteArrayInputStream(ByteArray(8)), 8, POLICY)

            val first = contentDao.chunkedUploadComplete(init.uploadId, POLICY)
            val second = contentDao.chunkedUploadComplete(init.uploadId, POLICY)

            assertThat(second).isEqualTo(first)
            assertThat(fakeStorage.completedCount).isEqualTo(1)
        }
    }

    @Test
    fun `concurrent complete - only the CAS winner runs the completion body, the loser is told to retry`() {
        useFakeStorageForTempFileType()

        val bytes = ByteArray(8)
        val uploadId = asUser("completer") {
            val init = contentDao.chunkedUploadInit(initReq(size = 8), POLICY)
            uploadService.writeChunk(init.uploadId, 0, ByteArrayInputStream(bytes), 8, POLICY)
            init.uploadId
        }

        // Force the CAS winner to pause inside the storage-complete call, well past the point where
        // it already claimed ACTIVE -> COMPLETING, so the loser's own (fast, failing) CAS attempt and
        // re-read reliably observe "still COMPLETING" rather than racing to see the winner's DONE.
        fakeStorage.completeDelayMs = 300
        val executor = Executors.newFixedThreadPool(2)
        try {
            val startGate = CountDownLatch(2)
            val futures = (0 until 2).map {
                executor.submit<Result<EntityRef>> {
                    asUser("completer") {
                        startGate.countDown()
                        startGate.await(5, TimeUnit.SECONDS)
                        runCatching { contentDao.chunkedUploadComplete(uploadId, POLICY) }
                    }
                }
            }
            val results = futures.map { it.get(10, TimeUnit.SECONDS) }

            val succeeded = results.filter { it.isSuccess }
            val failed = results.filter { it.isFailure }

            assertThat(succeeded).hasSize(1)
            assertThat(failed).hasSize(1)
            assertThat(failed.single().exceptionOrNull())
                .isInstanceOf(ContentUploadCompletionInProgressException::class.java)

            // exactly one temp-file record was created - the loser must not have run the
            // register/create body concurrently with the winner
            val allTempRecords = records.query(tempCtx.createQuery()).getRecords()
            assertThat(allTempRecords).hasSize(1)
            assertThat(allTempRecords.single()).isEqualTo(succeeded.single().getOrThrow())

            // The record count alone does not pin the property this test is named for: the final
            // COMPLETING -> DONE CAS would also leave exactly one record if the loser had run the
            // whole body and been rolled back at the very end. What only the early throw produces
            // is a storage that was never asked to complete twice.
            assertThat(fakeStorage.chunkedCompleteCalls)
                .describedAs("storage completes attempted by both callers")
                .isEqualTo(1)
        } finally {
            fakeStorage.completeDelayMs = 0
            executor.shutdownNow()
        }
    }

    @Test
    fun `complete resumes cleanly after a crash inside the record-creation transaction`() {
        useFakeStorageForTempFileType()
        val bytes = ByteArray(8)

        asUser("user1") {
            val init = contentDao.chunkedUploadInit(initReq(size = 8), POLICY)
            uploadService.writeChunk(init.uploadId, 0, ByteArrayInputStream(bytes), 8, POLICY)

            // Force the record-creation half of complete() to fail *after* chunkedComplete/dataKey
            // have already committed, by temporarily breaking the temp-file type's content attribute
            // - createRecordForContent's own validation then throws partway through the atomic
            // register+create+DONE transaction, simulating a crash in that window.
            updateType(TEMP_FILE_TYPE_ID) {
                it.withModel(TypeModelDef.create { withAttributes(emptyList()) })
            }

            val firstAttempt = runCatching { contentDao.chunkedUploadComplete(init.uploadId, POLICY) }
            assertThat(firstAttempt.isFailure).isTrue()

            // dataKey must already be committed (from the earlier, independent transaction) even
            // though the record was never created
            val midSession = currentSchemaCtx().uploadSessionService.findByExtId(init.uploadId)!!
            assertThat(midSession.status).isEqualTo("COMPLETING")
            assertThat(midSession.dataKey).isNotBlank()
            assertThat(midSession.entityRef).isBlank()
            assertThat(fakeStorage.completedCount).isEqualTo(1)

            // repair the type and retry - immediately, with no waiting out the completion lease:
            // the failed attempt threw on this very thread, so it handed its lease back on the way
            // out and there is no completer left for the lease to protect.
            useFakeStorageForTempFileType()
            val entityRef = contentDao.chunkedUploadComplete(init.uploadId, POLICY)

            // storage-complete was NOT called again (dataKey was already set and is resumed, not
            // redone), and exactly one temp-file record / live content row exists - no dangling
            // content from the failed first attempt
            assertThat(fakeStorage.completedCount).isEqualTo(1)

            val allTempRecords = records.query(tempCtx.createQuery()).getRecords()
            assertThat(allTempRecords).hasSize(1)
            assertThat(allTempRecords.single()).isEqualTo(entityRef)

            val readBytes =
                tempCtx.dao.getContent(entityRef.getLocalId(), "content")?.readContent { IOUtils.readAsBytes(it) }
            assertThat(readBytes).isEqualTo(bytes)
        }
    }

    @Test
    fun `complete resume does not corrupt an already-completed record's content row`() {
        useFakeStorageForTempFileType()
        val bytes = ByteArray(8)

        asUser("user1") {
            val init = contentDao.chunkedUploadInit(initReq(size = 8), POLICY)
            uploadService.writeChunk(init.uploadId, 0, ByteArrayInputStream(bytes), 8, POLICY)

            val schemaCtx = currentSchemaCtx()

            // Replay by hand what chunkedUploadComplete does - the ACTIVE -> COMPLETING
            // transition, the storage-complete/dataKey step, then a fully committed
            // register+create - while never writing the final COMPLETING -> DONE status/entityRef.
            // That leaves a session whose (storageRef, dataKey) row already exists, which is the
            // case the idempotency check in complete() has to survive: a content-addressable
            // backend can also dedup two unrelated uploads onto one dataKey.
            schemaCtx.uploadSessionService.updateStatus(
                init.uploadId,
                ContentUploadSessionStatus.ACTIVE,
                ContentUploadSessionStatus.COMPLETING
            )
            val session = schemaCtx.uploadSessionService.findByExtId(init.uploadId)!!
            val storageRef = schemaCtx.recordRefService.getEntityRefById(session.storageRef)
            val chunkedStorage = schemaCtx.contentStorageService
            val dataKey = chunkedStorage.chunkedComplete(storageRef, session.storageState)
            schemaCtx.uploadSessionService.updateStatus(
                init.uploadId,
                ContentUploadSessionStatus.COMPLETING,
                ContentUploadSessionStatus.COMPLETING,
                dataKey = dataKey
            )

            // registerContent/mutate(clone)/removeContent are only durable together inside a real
            // platform transaction (same as the production code's own TxnContext.doInTxn block) -
            // do the same here rather than calling the schema services naked.
            val firstRecordRef = TxnContext.doInTxn {
                val registered = schemaCtx.contentService.registerContent(
                    "file.bin",
                    "application/octet-stream",
                    null,
                    storageRef,
                    dataKey,
                    sha256Hex(bytes),
                    bytes.size.toLong(),
                    session.creator
                )
                val ref = AuthContext.runAsSystem {
                    tempCtx.createRecord("content" to registered.getDbId(), "name" to "file.bin")
                }
                // this content record is now cloned into ref's own content attribute - the pre-clone
                // row is redundant, exactly as chunkedUploadComplete's own commit would leave it
                schemaCtx.contentService.removeContent(registered.getDbId())
                ref
            }
            // status/entityRef deliberately left at COMPLETING/blank - as if the process crashed here

            val beforeRetryBytes =
                tempCtx.dao.getContent(firstRecordRef.getLocalId(), "content")?.readContent { IOUtils.readAsBytes(it) }
            assertThat(beforeRetryBytes).isEqualTo(bytes)

            // as above: the hand-replayed completion left the session in COMPLETING, so the resume
            // is a takeover and only happens once the lease has lapsed
            backdateModified(init.uploadId, POLICY.completionLease.plusMinutes(1))
            val entityRef = contentDao.chunkedUploadComplete(init.uploadId, POLICY)
            assertThat(entityRef).isNotEqualTo(firstRecordRef)

            // the pre-existing record's content must still be intact - not deleted out from under
            // it by the retry reusing/removing the wrong (storageRef, dataKey) row
            val firstRecordBytesAfterRetry = runCatching {
                tempCtx.dao.getContent(firstRecordRef.getLocalId(), "content")?.readContent { IOUtils.readAsBytes(it) }
            }
            assertThat(firstRecordBytesAfterRetry.getOrNull()).isEqualTo(bytes)

            val retryBytes =
                tempCtx.dao.getContent(entityRef.getLocalId(), "content")?.readContent { IOUtils.readAsBytes(it) }
            assertThat(retryBytes).isEqualTo(bytes)
        }
    }

    @Test
    fun `write, getInfo, complete and abort on someone else's session throw NotFound regardless of session status`() {
        useFakeStorageForTempFileType()

        val activeUploadId = asUser("owner-active") {
            contentDao.chunkedUploadInit(initReq(size = 20), POLICY).uploadId
        }
        // a still-existing ABORTED row (as if the storage-side abort had failed and left it for the
        // sweeper job) - status/existence must not leak to a non-owner before the owner-check runs
        val abortedUploadId = asUser("owner-aborted") {
            val id = contentDao.chunkedUploadInit(initReq(size = 20), POLICY).uploadId
            currentSchemaCtx().uploadSessionService.updateStatus(
                id,
                ContentUploadSessionStatus.ACTIVE,
                ContentUploadSessionStatus.ABORTED
            )
            id
        }
        val doneUploadId = asUser("owner-done") {
            val init = contentDao.chunkedUploadInit(initReq(size = 8), POLICY)
            uploadService.writeChunk(init.uploadId, 0, ByteArrayInputStream(ByteArray(8)), 8, POLICY)
            contentDao.chunkedUploadComplete(init.uploadId, POLICY)
            init.uploadId
        }

        asUser("intruder") {
            for (uploadId in listOf(activeUploadId, abortedUploadId, doneUploadId)) {
                assertThat(
                    runCatching {
                        uploadService.writeChunk(
                            uploadId,
                            0,
                            ByteArrayInputStream(ByteArray(8)),
                            8,
                            POLICY
                        )
                    }.exceptionOrNull()
                ).describedAs("writeChunk on %s", uploadId)
                    .isInstanceOf(ContentUploadSessionNotFoundException::class.java)

                assertThat(
                    runCatching { uploadService.getInfo(uploadId, POLICY) }.exceptionOrNull()
                ).describedAs("getInfo on %s", uploadId).isInstanceOf(ContentUploadSessionNotFoundException::class.java)

                assertThat(
                    runCatching { contentDao.chunkedUploadComplete(uploadId, POLICY) }.exceptionOrNull()
                ).describedAs("complete on %s", uploadId)
                    .isInstanceOf(ContentUploadSessionNotFoundException::class.java)

                assertThat(
                    runCatching { uploadService.abort(uploadId, POLICY) }.exceptionOrNull()
                ).describedAs("abort on %s", uploadId).isInstanceOf(ContentUploadSessionNotFoundException::class.java)
            }
        }
    }

    @Test
    fun `init on LOCAL storage reports storage-not-supported`() {
        // temp-file's contentConfig is left at its default (no explicit storageRef) -> resolves to LOCAL
        asUser("user1") {
            val resp = contentDao.chunkedUploadInit(initReq(), POLICY)
            assertThat(resp.supported).isFalse()
            assertThat(resp.reason).isEqualTo("storage-not-supported")
            assertThat(resp.uploadId).isEmpty()
        }
    }

    @Test
    fun `init respects maxActiveSessionsPerUser`() {
        useFakeStorageForTempFileType()
        asUser("user1") {
            val first = contentDao.chunkedUploadInit(initReq(), policy(maxActiveSessionsPerUser = 1))
            assertThat(first.supported).isTrue()

            val second = contentDao.chunkedUploadInit(initReq(), policy(maxActiveSessionsPerUser = 1))
            assertThat(second.supported).isFalse()
            assertThat(second.reason).isEqualTo("too-many-sessions")
        }
    }

    @Test
    fun `abort releases the storage-side upload and deletes the session row`() {
        useFakeStorageForTempFileType()
        asUser("user1") {
            val init = contentDao.chunkedUploadInit(initReq(), POLICY)
            uploadService.abort(init.uploadId, POLICY)

            // successful storage-side abort -> the session row is deleted immediately, so a
            // subsequent write on the same uploadId reports NotFound, not Gone
            assertThat(fakeStorage.activeCount).isEqualTo(0)

            val ex = runCatching {
                uploadService.writeChunk(
                    init.uploadId,
                    0,
                    ByteArrayInputStream(ByteArray(8)),
                    8,
                    POLICY
                )
            }.exceptionOrNull()
            assertThat(ex).isInstanceOf(ContentUploadSessionNotFoundException::class.java)
        }
    }

    @Test
    fun `storage-level Gone on writeChunk surfaces as session Gone and aborts the session`() {
        useFakeStorageForTempFileType()
        asUser("user1") {
            val init = contentDao.chunkedUploadInit(initReq(), POLICY)
            fakeStorage.goneOnNextCall = true

            val ex = runCatching {
                uploadService.writeChunk(
                    init.uploadId,
                    0,
                    ByteArrayInputStream(ByteArray(8)),
                    8,
                    POLICY
                )
            }.exceptionOrNull()
            assertThat(ex).isInstanceOf(ContentUploadSessionGoneException::class.java)

            val session = currentSchemaCtx().uploadSessionService.findByExtId(init.uploadId)
            assertThat(session).isNotNull
            assertThat(session!!.status).isEqualTo("ABORTED")

            // every later call now answers Gone consistently, not NotFound or a storage error
            val getInfoEx =
                runCatching { uploadService.getInfo(init.uploadId, POLICY) }.exceptionOrNull()
            assertThat(getInfoEx).isInstanceOf(ContentUploadSessionGoneException::class.java)
        }
    }

    @Test
    fun `storage-level Gone on complete surfaces as session Gone and aborts the session`() {
        useFakeStorageForTempFileType()
        asUser("user1") {
            val init = contentDao.chunkedUploadInit(initReq(size = 8), POLICY)
            uploadService.writeChunk(init.uploadId, 0, ByteArrayInputStream(ByteArray(8)), 8, POLICY)
            fakeStorage.goneOnNextCall = true

            val ex = runCatching {
                contentDao.chunkedUploadComplete(init.uploadId, POLICY)
            }.exceptionOrNull()
            assertThat(ex).isInstanceOf(ContentUploadSessionGoneException::class.java)

            val session = currentSchemaCtx().uploadSessionService.findByExtId(init.uploadId)
            assertThat(session).isNotNull
            assertThat(session!!.status).isEqualTo("ABORTED")
        }
    }

    @Test
    fun `storage-level Gone on abort is treated as success, not an error`() {
        useFakeStorageForTempFileType()
        asUser("user1") {
            val init = contentDao.chunkedUploadInit(initReq(), POLICY)
            fakeStorage.goneOnNextCall = true

            // must not throw - the upload we wanted to abort is already gone, which is the outcome
            // we wanted, exactly as if the storage-side abort had succeeded
            uploadService.abort(init.uploadId, POLICY)

            // treated exactly like a successful storage-side abort -> row deleted immediately
            val session = currentSchemaCtx().uploadSessionService.findByExtId(init.uploadId)
            assertThat(session).isNull()
        }
    }

    /**
     * Expiry is `modified` older than the idle timeout the caller passes in, so an idle session
     * stops occupying the per-user cap - while a live one still holds it.
     */
    @Test
    fun `an idle session stops counting against the per-user session limit`() {
        useFakeStorageForTempFileType()
        val req = initReq(size = 8)
        val singleSessionPolicy = policy(maxActiveSessionsPerUser = 1)
        asUser("user1") {
            val first = contentDao.chunkedUploadInit(req, singleSessionPolicy)
            assertThat(first.supported).isTrue()

            assertThat(contentDao.chunkedUploadInit(req, singleSessionPolicy).reason).isEqualTo("too-many-sessions")

            backdateModified(first.uploadId, IDLE_TIMEOUT.plusMinutes(1))

            assertThat(contentDao.chunkedUploadInit(req, singleSessionPolicy).supported).isTrue()
        }
    }

    /**
     * Liveness does not apply to a terminal status. A DONE session keeps answering with its record
     * however long ago it finished - that is what makes a retried complete after a network failure
     * return the same ref instead of an error - and an ABORTED one is refused on its own account,
     * not for its age.
     */
    @Test
    fun `a terminal session does not expire`() {
        useFakeStorageForTempFileType()
        asUser("user1") {
            val done = contentDao.chunkedUploadInit(initReq(size = 8), POLICY).uploadId
            uploadService.writeChunk(done, 0, ByteArrayInputStream(ByteArray(8)), 8, POLICY)
            val entityRef = contentDao.chunkedUploadComplete(done, POLICY)

            backdateModified(done, IDLE_TIMEOUT.plusMinutes(1))

            assertThat(contentDao.chunkedUploadComplete(done, POLICY)).isEqualTo(entityRef)
            val info = uploadService.getInfo(done, POLICY)
            assertThat(info.status).isEqualTo("DONE")
            assertThat(info.entityRef).isEqualTo(entityRef)

            val aborted = contentDao.chunkedUploadInit(initReq(size = 8), POLICY).uploadId
            currentSchemaCtx().uploadSessionService.updateStatus(
                aborted,
                ContentUploadSessionStatus.ACTIVE,
                ContentUploadSessionStatus.ABORTED
            )
            backdateModified(aborted, IDLE_TIMEOUT.plusMinutes(1))
            assertThat(runCatching { uploadService.getInfo(aborted, POLICY) }.exceptionOrNull())
                .isInstanceOf(ContentUploadSessionGoneException::class.java)
        }
    }

    /**
     * An expired session answers Gone on every operation. Each case is paired with the same
     * operation on a live session under the same idle timeout, so an [isExpired][
     * ru.citeck.ecos.data.sql.records.dao.content.DbRecordsContentDao] that rejected everything
     * could not pass.
     */
    @Test
    fun `an expired session answers Gone on every operation`() {
        useFakeStorageForTempFileType()
        asUser("user1") {

            fun newFilledSession(): String {
                val uploadId = contentDao.chunkedUploadInit(initReq(size = 8), POLICY).uploadId
                uploadService.writeChunk(uploadId, 0, ByteArrayInputStream(ByteArray(8)), 8, POLICY)
                return uploadId
            }

            val operations = listOf<Pair<String, (String) -> Unit>>(
                "writeChunk" to { uploadId ->
                    uploadService.writeChunk(uploadId, 0, ByteArrayInputStream(ByteArray(8)), 8, POLICY)
                },
                "getInfo" to { uploadId -> uploadService.getInfo(uploadId, POLICY) },
                "complete" to { uploadId -> contentDao.chunkedUploadComplete(uploadId, POLICY) },
                "abort" to { uploadId -> uploadService.abort(uploadId, POLICY) }
            )

            for ((name, operation) in operations) {
                val live = newFilledSession()
                assertThat(runCatching { operation(live) }.exceptionOrNull())
                    .describedAs("%s on a live session", name).isNull()

                val expired = newFilledSession()
                backdateModified(expired, IDLE_TIMEOUT.plusMinutes(1))
                assertThat(runCatching { operation(expired) }.exceptionOrNull())
                    .describedAs("%s on an expired session", name)
                    .isInstanceOf(ContentUploadSessionGoneException::class.java)
            }
        }
    }

    @Test
    fun `init rejects a non-positive declared size`() {
        useFakeStorageForTempFileType()
        asUser("user1") {
            for (size in listOf(0L, -1L)) {
                assertThat(
                    runCatching { contentDao.chunkedUploadInit(initReq(size = size), POLICY) }.exceptionOrNull()
                ).describedAs("size %s", size).isInstanceOf(IllegalArgumentException::class.java)
            }
            // rejected before anything was opened on the storage side or persisted
            assertThat(fakeStorage.activeCount).isEqualTo(0)
        }
    }

    @Test
    fun `init rejects a blank ecos type before creating a session`() {
        useFakeStorageForTempFileType()

        asUser("user1") {
            val creatorRefId = tempCtx.dao.getRecordsDaoCtx().getOrCreateUserRefId("user1")
            val activeBefore = currentSchemaCtx().uploadSessionService
                .countActiveByCreator(creatorRefId, Instant.now().minus(IDLE_TIMEOUT))

            val error = runCatching {
                contentDao.chunkedUploadInit(initReq(size = 8, ecosType = ""), POLICY)
            }.exceptionOrNull()

            assertThat(error).isInstanceOf(IllegalArgumentException::class.java)
            assertThat(error!!.message).contains("Ecos type")

            // the type is what the record is made of and there is no fallback for it, so nothing
            // may be left behind - neither a session row nor a storage-side upload
            assertThat(
                currentSchemaCtx().uploadSessionService
                    .countActiveByCreator(creatorRefId, Instant.now().minus(IDLE_TIMEOUT))
            ).isEqualTo(activeBefore)
            assertThat(fakeStorage.activeCount).isEqualTo(0)
        }
    }

    @Test
    fun `init reports storage-not-supported when the storage answers with a non-positive chunkSize`() {
        useFakeStorageForTempFileType()
        fakeStorage.chunkSize = 0

        asUser("user1") {
            val resp = contentDao.chunkedUploadInit(initReq(), POLICY)

            // a zero chunkSize has no usable chunking contract at all (chunkIndex = offset/chunkSize)
            assertThat(resp.supported).isFalse()
            assertThat(resp.reason).isEqualTo("storage-not-supported")
            assertThat(resp.uploadId).isEmpty()
            // and the upload the storage nevertheless opened is released, not leaked
            assertThat(fakeStorage.activeCount).isEqualTo(0)
        }
    }

    @Test
    fun `abort during an in-flight complete is a no-op and cannot destroy the completion`() {
        useFakeStorageForTempFileType()
        val bytes = ByteArray(8) { (it + 11).toByte() }

        val uploadId = asUser("user1") {
            val init = contentDao.chunkedUploadInit(initReq(size = 8), POLICY)
            uploadService.writeChunk(init.uploadId, 0, ByteArrayInputStream(bytes), 8, POLICY)
            init.uploadId
        }

        // Hold the completer inside the storage-complete call, well past the point where it already
        // claimed ACTIVE -> COMPLETING, so the cancel below reliably lands in the dangerous window.
        fakeStorage.completeDelayMs = 800
        val executor = Executors.newSingleThreadExecutor()
        try {
            val completeFuture = executor.submit<EntityRef> {
                asUser("user1") { contentDao.chunkedUploadComplete(uploadId, POLICY) }
            }

            val deadline = System.currentTimeMillis() + 10_000
            while (currentSchemaCtx().uploadSessionService.findByExtId(uploadId)?.status != "COMPLETING") {
                check(System.currentTimeMillis() < deadline) { "complete never reached COMPLETING" }
                Thread.sleep(10)
            }

            // Cancel while the completer owns the session. Without the ACTIVE -> ABORTED CAS gate,
            // this would release the storage-side upload under the completer and delete the session
            // row, orphaning the assembled object with no ed_content row and no session row.
            asUser("user1") { uploadService.abort(uploadId, POLICY) }

            val entityRef = completeFuture.get(20, TimeUnit.SECONDS)

            val session = currentSchemaCtx().uploadSessionService.findByExtId(uploadId)
            assertThat(session).isNotNull
            assertThat(session!!.status).isEqualTo("DONE")
            assertThat(session.entityRef).isEqualTo(entityRef.toString())

            val readBytes =
                tempCtx.dao.getContent(entityRef.getLocalId(), "content")?.readContent { IOUtils.readAsBytes(it) }
            assertThat(readBytes).isEqualTo(bytes)
        } finally {
            fakeStorage.completeDelayMs = 0
            executor.shutdownNow()
        }
    }

    @Test
    fun `a complete arriving while another one is in flight is refused without touching the storage again`() {
        useFakeStorageForTempFileType()
        val bytes = ByteArray(8)

        val uploadId = asUser("user1") {
            val init = contentDao.chunkedUploadInit(initReq(size = 8), POLICY)
            uploadService.writeChunk(init.uploadId, 0, ByteArrayInputStream(bytes), 8, POLICY)
            init.uploadId
        }

        val executor = Executors.newSingleThreadExecutor()
        try {
            // hold the winner inside the storage-side assembly, which is the whole window a client
            // that timed out and retried would land in
            fakeStorage.completeDelayMs = 2000

            val winner = executor.submit<EntityRef> {
                asUser("user1") { contentDao.chunkedUploadComplete(uploadId, POLICY) }
            }

            // Wait until the winner is inside the storage-side assembly, so the retry below is the
            // already-COMPLETING case and not a second ACTIVE -> COMPLETING race. Polling the
            // storage call rather than the status is what makes the call-count assertion below
            // deterministic: the winner writes COMPLETING *before* it enters chunkedComplete, so a
            // poll on the status alone could return while the count is still 0.
            val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10)
            while (fakeStorage.chunkedCompleteCalls == 0) {
                if (System.nanoTime() > deadline) {
                    error("The completion never reached the storage")
                }
                Thread.sleep(10)
            }
            assertThat(currentSchemaCtx().uploadSessionService.findByExtId(uploadId)?.status)
                .isEqualTo("COMPLETING")

            val retry = asUser("user1") {
                runCatching { contentDao.chunkedUploadComplete(uploadId, POLICY) }
            }
            assertThat(retry.exceptionOrNull())
                .describedAs("a retry issued while the first completion is still running")
                .isInstanceOf(ContentUploadCompletionInProgressException::class.java)

            // The retry must be stopped before the storage: a backend that refuses a repeated
            // complete would answer Gone here, and the Gone handler would mark the session ABORTED
            // out from under the completer that is about to succeed.
            assertThat(fakeStorage.chunkedCompleteCalls)
                .describedAs("storage completes attempted while one was in flight")
                .isEqualTo(1)

            val entityRef = winner.get(20, TimeUnit.SECONDS)
            val session = currentSchemaCtx().uploadSessionService.findByExtId(uploadId)!!
            assertThat(session.status).isEqualTo("DONE")
            assertThat(session.entityRef).isEqualTo(entityRef.toString())

            val allTempRecords = records.query(tempCtx.createQuery()).getRecords()
            assertThat(allTempRecords).hasSize(1)

            val readBytes =
                tempCtx.dao.getContent(entityRef.getLocalId(), "content")?.readContent { IOUtils.readAsBytes(it) }
            assertThat(readBytes).isEqualTo(bytes)
        } finally {
            fakeStorage.completeDelayMs = 0
            executor.shutdownNow()
        }
    }

    /**
     * Drives a completer whose storage-side assembly outran its lease: the session is taken over
     * while it is still inside the storage, and it must not write on the session any more. Both
     * writes it can still reach are covered - the dataKey it was about to store, and the ABORTED it
     * marks after a storage-level Gone.
     */
    private fun completeWhileSessionIsTakenOver(goneFromStorage: Boolean): Pair<String, Result<EntityRef>> {
        val bytes = ByteArray(8)

        val uploadId = asUser("user1") {
            val init = contentDao.chunkedUploadInit(initReq(size = 8), POLICY)
            uploadService.writeChunk(init.uploadId, 0, ByteArrayInputStream(bytes), 8, POLICY)
            init.uploadId
        }

        val executor = Executors.newSingleThreadExecutor()
        try {
            fakeStorage.completeDelayMs = 3000
            fakeStorage.goneOnCompleteAfterDelay = goneFromStorage

            val loser = executor.submit<Result<EntityRef>> {
                asUser("user1") { runCatching { contentDao.chunkedUploadComplete(uploadId, POLICY) } }
            }

            val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10)
            while (fakeStorage.chunkedCompleteCalls == 0) {
                if (System.nanoTime() > deadline) {
                    error("The completion never reached the storage")
                }
                Thread.sleep(10)
            }

            // the successor takes the session over while the first completer is still assembling
            backdateModified(uploadId, POLICY.completionLease.plusMinutes(1))
            val takenOver = currentSchemaCtx().uploadSessionService.takeOverCompletion(
                uploadId,
                currentSchemaCtx().uploadSessionService.findByExtId(uploadId)!!.modified
            )
            assertThat(takenOver).describedAs("takeover of the in-flight completion").isTrue()

            return uploadId to loser.get(20, TimeUnit.SECONDS)
        } finally {
            fakeStorage.completeDelayMs = 0
            fakeStorage.goneOnCompleteAfterDelay = false
            executor.shutdownNow()
        }
    }

    @Test
    fun `a completer that lost its lease does not store its dataKey on the successor's session`() {
        useFakeStorageForTempFileType()

        val (uploadId, result) = completeWhileSessionIsTakenOver(goneFromStorage = false)

        assertThat(result.exceptionOrNull())
            .describedAs("completion whose lease was taken over while it assembled")
            .isInstanceOf(ContentUploadSessionGoneException::class.java)

        // the session belongs to its successor now: still COMPLETING, and with none of the loser's
        // work written onto it
        val session = currentSchemaCtx().uploadSessionService.findByExtId(uploadId)!!
        assertThat(session.status).isEqualTo("COMPLETING")
        assertThat(session.dataKey).isBlank()
        assertThat(records.query(tempCtx.createQuery()).getRecords()).isEmpty()
    }

    @Test
    fun `a completer that lost its lease does not abort the successor's session`() {
        useFakeStorageForTempFileType()

        // The loser's storage answers Gone - the shape of an S3 multipart upload consumed by the
        // completion that took over. Aborting on that would retire a session another request is
        // completing right now, losing a fully transferred file behind a 410.
        val (uploadId, result) = completeWhileSessionIsTakenOver(goneFromStorage = true)

        assertThat(result.exceptionOrNull())
            .isInstanceOf(ContentUploadSessionGoneException::class.java)

        val session = currentSchemaCtx().uploadSessionService.findByExtId(uploadId)!!
        assertThat(session.status)
            .describedAs("status of the session the loser tried to abort")
            .isEqualTo("COMPLETING")
    }

    @Test
    fun `a completer that lost its lease does not finish the successor's session`() {
        useFakeStorageForTempFileType()
        val bytes = ByteArray(8)

        val uploadId = asUser("user1") {
            val init = contentDao.chunkedUploadInit(initReq(size = 8), POLICY)
            uploadService.writeChunk(init.uploadId, 0, ByteArrayInputStream(bytes), 8, POLICY)
            init.uploadId
        }

        // The last write of a completion is its own COMPLETING -> DONE, and it happens after the
        // record is built - the longest step a lease can outrun. Taking the session over from inside
        // the record creation puts the loser exactly there: on a CAS matching the status alone it
        // would write DONE onto the successor's session and answer its client with the record the
        // successor is about to roll back.
        val result = asUser("user1") {
            runCatching {
                uploadService.complete(uploadId, POLICY) {
                    backdateModified(uploadId, POLICY.completionLease.plusMinutes(1))
                    val schemaCtx = currentSchemaCtx()
                    val taken = schemaCtx.doInNewTxn {
                        schemaCtx.uploadSessionService.takeOverCompletion(
                            uploadId,
                            schemaCtx.uploadSessionService.findByExtId(uploadId)!!.modified
                        )
                    }
                    assertThat(taken).describedAs("takeover from inside the record creation").isTrue()
                    EntityRef.valueOf("emodel/temp-file@record-of-the-loser")
                }
            }
        }

        assertThat(result.exceptionOrNull())
            .describedAs("completion whose lease was taken over while it was creating the record")
            .isInstanceOf(ContentUploadCompletionInProgressException::class.java)

        val session = currentSchemaCtx().uploadSessionService.findByExtId(uploadId)!!
        assertThat(session.status)
            .describedAs("status of the session the loser tried to finish")
            .isEqualTo("COMPLETING")
        assertThat(session.entityRef)
            .describedAs("entityRef the loser tried to publish on the successor's session")
            .isBlank()
    }

    @Test
    fun `only one of two callers takes over the same abandoned completion`() {
        useFakeStorageForTempFileType()
        val bytes = ByteArray(8)

        asUser("user1") {
            val init = contentDao.chunkedUploadInit(initReq(size = 8), POLICY)
            uploadService.writeChunk(init.uploadId, 0, ByteArrayInputStream(bytes), 8, POLICY)

            // a completion that died right after taking the session, then left alone past the lease
            currentSchemaCtx().uploadSessionService.updateStatus(
                init.uploadId,
                ContentUploadSessionStatus.ACTIVE,
                ContentUploadSessionStatus.COMPLETING
            )
            backdateModified(init.uploadId, POLICY.completionLease.plusMinutes(1))

            val executor = Executors.newFixedThreadPool(2)
            try {
                val barrier = CyclicBarrier(2)
                val futures = (0 until 2).map {
                    executor.submit<Result<EntityRef>> {
                        asUser("user1") {
                            barrier.await(10, TimeUnit.SECONDS)
                            runCatching { contentDao.chunkedUploadComplete(init.uploadId, POLICY) }
                        }
                    }
                }
                val results = futures.map { it.get(20, TimeUnit.SECONDS) }

                // The takeover CAS expects the `modified` each caller read, so only one of them may
                // run the completion body. The record count does not pin that on its own: with a
                // status-only CAS both callers would take the session over, both would complete the
                // storage and build a record, and the loser of the final COMPLETING -> DONE CAS
                // would roll its record back - leaving one record all the same. Only the storage
                // call count separates "one caller ran the body" from "two did and one was undone".
                assertThat(fakeStorage.chunkedCompleteCalls)
                    .describedAs("storage completes attempted by the two takers")
                    .isEqualTo(1)
                assertThat(records.query(tempCtx.createQuery()).getRecords()).hasSize(1)

                // Two successes are legal, and not the same as two completions: the loser of the CAS
                // re-reads the row, and if the winner has already finished it answers the winner's
                // record rather than failing. Which of the two outcomes the loser gets depends on
                // how far the winner got, so that fall-through is tolerated here rather than pinned.
                // What must never happen is a success that made its own record, or a failure of any
                // other kind.
                assertThat(results.count { it.isSuccess }).isGreaterThanOrEqualTo(1)
                assertThat(results.filter { it.isSuccess }.map { it.getOrThrow() }.distinct()).hasSize(1)
                results.filter { it.isFailure }.forEach {
                    assertThat(it.exceptionOrNull())
                        .isInstanceOf(ContentUploadCompletionInProgressException::class.java)
                }
            } finally {
                executor.shutdownNow()
            }
        }
    }

    /**
     * Makes the temp-file type workspace-scoped with a default, so that what a record's `_workspace`
     * ends up holding is decided by the upload rather than by the type having no opinion.
     */
    private fun useWorkspaceScopedTempFileType() {
        useFakeStorageForTempFileType()
        updateType(TEMP_FILE_TYPE_ID) {
            it.withWorkspaceScope(WorkspaceScope.PRIVATE)
            it.withDefaultWorkspace(TYPE_DEFAULT_WORKSPACE)
        }
        // the uploader has to be a member of both, or the mutation refuses the record for the
        // workspace rather than for the workspace it was given
        workspaceService.setUserWorkspaces("user1", setOf(UPLOAD_WORKSPACE, TYPE_DEFAULT_WORKSPACE))
    }

    private fun uploadWholeFile(workspace: String): EntityRef {
        val bytes = ByteArray(8)
        val attributes = ObjectData.create()
        if (workspace.isNotEmpty()) {
            attributes[RecordConstants.ATT_WORKSPACE] = workspace
        }
        return asUser("user1") {
            val init = contentDao.chunkedUploadInit(initReq(size = 8, attributes = attributes), POLICY)
            uploadService.writeChunk(init.uploadId, 0, ByteArrayInputStream(bytes), 8, POLICY)
            contentDao.chunkedUploadComplete(init.uploadId, POLICY)
        }
    }

    @Test
    fun `the workspace declared at init is the workspace of the finished record`() {
        useWorkspaceScopedTempFileType()

        // The session carries the attributes from init, long before the record exists, and a
        // workspace is one of them. Without them reaching the mutation the record silently lands in
        // the type's default workspace instead - or, for a type with no default, the mutation
        // refuses it after the whole file was transferred.
        val entityRef = uploadWholeFile(workspace = UPLOAD_WORKSPACE)

        assertThat(records.getAtt(entityRef, "_workspace?localId").asText()).isEqualTo(UPLOAD_WORKSPACE)
    }

    @Test
    fun `an upload that declares no workspace leaves the workspace to the mutation`() {
        useWorkspaceScopedTempFileType()

        // no `_workspace` attribute at all - not a blank one, which would override the type's
        // default with nothing instead of leaving the mutation to resolve it
        val entityRef = uploadWholeFile(workspace = "")

        assertThat(records.getAtt(entityRef, "_workspace?localId").asText()).isEqualTo(TYPE_DEFAULT_WORKSPACE)
    }

    /**
     * What a completion left behind: what it threw, if anything, and whether the thread that called
     * it came back interrupted.
     */
    private class CompletionOutcome(
        val error: Throwable?,
        val callerInterrupted: Boolean
    )
}
