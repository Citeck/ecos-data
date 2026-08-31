package ru.citeck.ecos.data.sql.test.content

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import ru.citeck.ecos.commons.data.ObjectData
import ru.citeck.ecos.context.lib.ctx.GlobalEcosContext
import ru.citeck.ecos.data.sql.content.DbContentService
import ru.citeck.ecos.data.sql.content.DbContentServiceImpl
import ru.citeck.ecos.data.sql.content.storage.ChunkedInitMeta
import ru.citeck.ecos.data.sql.content.storage.EcosContentStorageConstants
import ru.citeck.ecos.data.sql.content.storage.EcosContentStorageService
import ru.citeck.ecos.data.sql.content.storage.EcosContentStorageServiceFactory
import ru.citeck.ecos.data.sql.context.DbDataSourceContext
import ru.citeck.ecos.data.sql.context.DbSchemaContext
import ru.citeck.ecos.data.sql.datasource.DbDataSource
import ru.citeck.ecos.data.sql.domain.migration.DbMigrationService
import ru.citeck.ecos.data.sql.service.DbDataServiceFactory
import ru.citeck.ecos.test.commons.EcosWebAppApiMock
import ru.citeck.ecos.txn.lib.TxnContext
import ru.citeck.ecos.txn.lib.manager.EcosTxnProps
import ru.citeck.ecos.txn.lib.manager.TransactionManagerImpl
import ru.citeck.ecos.webapp.api.content.ContentRange
import ru.citeck.ecos.webapp.api.entity.EntityRef
import java.io.InputStream
import java.io.OutputStream
import java.util.concurrent.atomic.AtomicLong

/**
 * Backend-AGNOSTIC contract test for [DbContentService.registerContent]: registering already-known
 * content metadata without streaming bytes, and the interaction of the resulting row with
 * [DbContentService.removeContent] / [DbContentService.cloneContent]. A recording fake storage is
 * used in place of the real [EcosContentStorageService] so we can assert that `registerContent`
 * never streams bytes and that `removeContent` only deletes from storage when no other row shares
 * the same (storageRef, dataKey). Concrete subclasses live in each backend module:
 *  - `ecos-data-sql-pg`  -> PG / Testcontainers
 *  - `ecos-data-inmem`   -> the in-memory backend
 */
abstract class ContentServiceRegisterContractTest {

    companion object {
        private val creatorCounter = AtomicLong()
    }

    /**
     * Create the storage backend factory (the single seam that selects the backend).
     */
    protected abstract fun createDataServiceFactory(): DbDataServiceFactory

    /**
     * Create a fresh data source compatible with the factory above.
     */
    protected abstract fun createDataSource(): DbDataSource

    /**
     * Records calls made to the storage layer so tests can assert bytes were never touched.
     */
    private class RecordingContentStorage : EcosContentStorageService {

        val uploadedKeys = mutableListOf<String>()
        val deletedKeys = mutableListOf<Pair<EntityRef, String>>()

        override fun resetColumnsCache() {}

        override fun uploadContent(
            storageRef: EntityRef,
            storageConfig: ObjectData,
            action: (OutputStream) -> Unit
        ): String {
            error("uploadContent must not be called by registerContent")
        }

        override fun <T> readContent(
            storageRef: EntityRef,
            path: String,
            range: ContentRange,
            action: (InputStream) -> T
        ): T {
            error("readContent is not used by this contract test")
        }

        override fun deleteContent(storageRef: EntityRef, path: String) {
            deletedKeys.add(storageRef to path)
        }

        override fun isChunkedSupported(storageRef: EntityRef): Boolean = TODO("Not yet implemented")

        override fun chunkedInit(
            storageRef: EntityRef,
            meta: ChunkedInitMeta
        ) = TODO("Not yet implemented")

        override fun chunkedWriteChunk(
            storageRef: EntityRef,
            state: String,
            chunkIndex: Int,
            content: InputStream,
            contentLength: Long
        ) = TODO("Not yet implemented")

        override fun chunkedComplete(
            storageRef: EntityRef,
            state: String
        ) = TODO("Not yet implemented")

        override fun chunkedAbort(
            storageRef: EntityRef,
            state: String
        ) = TODO("Not yet implemented")
    }

    private fun createSchemaCtx(storage: EcosContentStorageService): DbSchemaContext {
        val webAppApi = EcosWebAppApiMock("test")
        val txnManager = TransactionManagerImpl()
        txnManager.init(webAppApi, EcosTxnProps())
        TxnContext.setManager(txnManager)

        val dsCtx = DbDataSourceContext(
            createDataSource(),
            createDataServiceFactory(),
            DbMigrationService(),
            webAppApi,
            GlobalEcosContext.getContext(),
            contentStorageServiceFactory = EcosContentStorageServiceFactory { storage }
        )
        return dsCtx.getSchemaContext("ecos-data-content-register-contract-test-schema")
    }

    private fun createService(storage: EcosContentStorageService): DbContentService {
        return DbContentServiceImpl(createSchemaCtx(storage))
    }

    @Test
    fun `findContentByStorageAndDataKey answers null for an unknown storage without registering it`() {
        val schemaCtx = createSchemaCtx(RecordingContentStorage())
        val service = DbContentServiceImpl(schemaCtx)

        val unusedStorageRef = EntityRef.valueOf("emodel/storage@never-used-by-any-content")

        assertThat(service.findContentByStorageAndDataKey(unusedStorageRef, "some-key")).isNull()

        // A lookup must not write. Resolving the ref with getOrCreateIdByEntityRef would insert an
        // ed_record_ref row for a storage nothing was ever stored under, on a read path that runs
        // once per chunked completion.
        assertThat(schemaCtx.recordRefService.getIdByEntityRef(unusedStorageRef))
            .describedAs("record-ref id created by a pure lookup")
            .isEqualTo(-1L)
    }

    private fun nextCreator(): Long = creatorCounter.incrementAndGet()

    @Test
    fun `registerContent creates a row without streaming bytes`() {
        val storage = RecordingContentStorage()
        val service = createService(storage)

        val creator = nextCreator()
        val storageRef = EcosContentStorageConstants.LOCAL_CONTENT_STORAGE_REF
        val dataKey = "b/2026/08/25/registered-key"

        val registered = service.registerContent(
            name = "file.bin",
            mimeType = "application/octet-stream",
            encoding = "utf-8",
            storageRef = storageRef,
            dataKey = dataKey,
            sha256 = "deadbeef",
            size = 42,
            creatorRefId = creator
        )

        assertThat(storage.uploadedKeys).isEmpty()
        assertThat(registered.getName()).isEqualTo("file.bin")
        assertThat(registered.getDataKey()).isEqualTo(dataKey)
        assertThat(registered.getStorageRef()).isEqualTo(storageRef)
        assertThat(registered.getSha256()).isEqualTo("deadbeef")
        assertThat(registered.getSize()).isEqualTo(42)

        val found = service.getContent(registered.getDbId()) ?: error("not found by id")
        assertThat(found.getSha256()).isEqualTo("deadbeef")
        assertThat(found.getDataKey()).isEqualTo(dataKey)
    }

    @Test
    fun `registerContent rejects blank sha256 or non-positive size`() {
        val service = createService(RecordingContentStorage())
        val storageRef = EcosContentStorageConstants.LOCAL_CONTENT_STORAGE_REF

        assertThat(
            runCatching {
                service.registerContent(
                    name = "file.bin",
                    mimeType = null,
                    encoding = null,
                    storageRef = storageRef,
                    dataKey = "some-key",
                    sha256 = "",
                    size = 42,
                    creatorRefId = nextCreator()
                )
            }.isFailure
        ).isTrue()

        assertThat(
            runCatching {
                service.registerContent(
                    name = "file.bin",
                    mimeType = null,
                    encoding = null,
                    storageRef = storageRef,
                    dataKey = "some-key",
                    sha256 = "deadbeef",
                    size = 0,
                    creatorRefId = nextCreator()
                )
            }.isFailure
        ).isTrue()
    }

    @Test
    fun `removeContent does not touch storage while another row shares storageRef and dataKey`() {
        val storage = RecordingContentStorage()
        val service = createService(storage)

        val storageRef = EcosContentStorageConstants.LOCAL_CONTENT_STORAGE_REF
        val dataKey = "b/2026/08/25/shared-key"

        val first = service.registerContent(
            name = "first.bin",
            mimeType = "application/octet-stream",
            encoding = null,
            storageRef = storageRef,
            dataKey = dataKey,
            sha256 = "sha-1",
            size = 10,
            creatorRefId = nextCreator()
        )
        val second = service.registerContent(
            name = "second.bin",
            mimeType = "application/octet-stream",
            encoding = null,
            storageRef = storageRef,
            dataKey = dataKey,
            sha256 = "sha-1",
            size = 10,
            creatorRefId = nextCreator()
        )

        service.removeContent(first.getDbId())

        assertThat(storage.deletedKeys).isEmpty()
        assertThat(service.getContent(first.getDbId())).isNull()
        assertThat(service.getContent(second.getDbId())).isNotNull()

        // now the last row sharing (storageRef, dataKey) is removed -> storage delete IS called
        service.removeContent(second.getDbId())
        assertThat(storage.deletedKeys).containsExactly(storageRef to dataKey)
    }

    @Test
    fun `cloneContent works on a registered row`() {
        val storage = RecordingContentStorage()
        val service = createService(storage)

        val storageRef = EcosContentStorageConstants.LOCAL_CONTENT_STORAGE_REF
        val dataKey = "b/2026/08/25/clone-key"

        val original = service.registerContent(
            name = "original.bin",
            mimeType = "application/octet-stream",
            encoding = null,
            storageRef = storageRef,
            dataKey = dataKey,
            sha256 = "sha-clone",
            size = 7,
            creatorRefId = nextCreator()
        )

        val cloneCreator = nextCreator()
        val cloned = service.cloneContent(original.getDbId(), cloneCreator)

        assertThat(cloned.getDbId()).isNotEqualTo(original.getDbId())
        assertThat(cloned.getDataKey()).isEqualTo(dataKey)
        assertThat(cloned.getStorageRef()).isEqualTo(storageRef)
        assertThat(cloned.getSha256()).isEqualTo("sha-clone")
        assertThat(cloned.getSize()).isEqualTo(7)
        assertThat(storage.uploadedKeys).isEmpty()

        // removing the original must not delete storage since the clone still shares the key
        service.removeContent(original.getDbId())
        assertThat(storage.deletedKeys).isEmpty()

        service.removeContent(cloned.getDbId())
        assertThat(storage.deletedKeys).containsExactly(storageRef to dataKey)
    }
}
