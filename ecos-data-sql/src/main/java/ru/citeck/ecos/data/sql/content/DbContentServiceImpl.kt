package ru.citeck.ecos.data.sql.content

import ru.citeck.ecos.data.sql.content.entity.DbContentEntity
import ru.citeck.ecos.data.sql.content.storage.EcosContentStorageService
import ru.citeck.ecos.data.sql.content.storage.local.EcosContentLocalStorage
import ru.citeck.ecos.data.sql.content.writer.EcosContentWriterFactory
import ru.citeck.ecos.data.sql.content.writer.EcosContentWriterImpl
import ru.citeck.ecos.data.sql.service.DbDataService
import ru.citeck.ecos.data.sql.service.DbMigrationsExecutor
import ru.citeck.ecos.webapp.api.content.EcosContentWriter
import java.io.InputStream
import java.io.OutputStream
import java.time.Instant
import java.util.UUID

class DbContentServiceImpl(
    private val dataService: DbDataService<DbContentEntity>,
    private val contentDataService: EcosContentStorageService
) : DbContentService, DbMigrationsExecutor {

    private val contentWriterFactory: EcosContentWriterFactory = ContentWriterFactoryImpl()

    override fun init() {
        contentDataService.init(contentWriterFactory)
    }

    override fun uploadContent(
        name: String?,
        mimeType: String?,
        encoding: String?,
        storage: String?,
        writer: (EcosContentWriter) -> Unit
    ): DbEcosContentData {

        val nnName = (name ?: "").ifBlank { UUID.randomUUID().toString() }
        val nnMimeType = (mimeType ?: "").ifBlank { "application/octet-stream" }
        val nnEncoding = encoding ?: ""
        val nnStorage = (storage ?: "").ifBlank { EcosContentLocalStorage.TYPE }

        var sha256 = ""
        var size = 0L

        val dataUri = contentDataService.uploadContent(nnStorage) {
            writer(it)
            it.getOutputStream().flush()
            sha256 = it.getSha256()
            size = it.getContentSize()
        }

        if (sha256.isEmpty() || size <= 0L) {
            error("Invalid content metadata. Sha256: $sha256 Size: $size")
        }

        val entity = DbContentEntity()

        entity.name = nnName
        entity.mimeType = nnMimeType
        entity.encoding = nnEncoding
        entity.created = Instant.now()
        entity.sha256 = sha256
        entity.size = size
        entity.uri = dataUri

        return EcosContentDataImpl(dataService.save(entity))
    }

    override fun getContent(id: Long): DbEcosContentData? {
        val entity = dataService.findById(id) ?: return null
        return EcosContentDataImpl(entity)
    }

    override fun runMigrations(mock: Boolean, diff: Boolean): List<String> {
        var result = dataService.runMigrations(emptyList(), mock, diff, true)
        if (contentDataService is DbMigrationsExecutor) {
            val newRes = ArrayList(result)
            newRes.addAll(contentDataService.runMigrations(mock, diff))
            result = newRes
        }
        return result
    }

    private inner class EcosContentDataImpl(val entity: DbContentEntity) : DbEcosContentData {

        override fun getDbId(): Long {
            return entity.id
        }

        override fun getCreated(): Instant {
            return entity.created
        }

        override fun getEncoding(): String {
            return entity.encoding
        }

        override fun getMimeType(): String {
            return entity.mimeType
        }

        override fun getName(): String {
            return entity.name
        }

        override fun getSha256(): String {
            return entity.sha256
        }

        override fun getSize(): Long {
            return entity.size
        }

        override fun <T> readContent(action: (InputStream) -> T): T {
            return contentDataService.readContent(entity.uri, action)
        }
    }

    private class ContentWriterFactoryImpl : EcosContentWriterFactory {

        override fun createWriter(output: OutputStream): EcosContentWriter {
            return EcosContentWriterImpl(output)
        }
    }
}
