package ru.citeck.ecos.data.sql.content

import ru.citeck.ecos.data.sql.content.entity.DbContentEntity
import ru.citeck.ecos.data.sql.content.storage.EcosContentStorageService
import ru.citeck.ecos.data.sql.content.stream.EcosContentWriterInputStreamImpl
import ru.citeck.ecos.data.sql.service.DbDataService
import ru.citeck.ecos.data.sql.service.DbMigrationsExecutor
import java.io.InputStream
import java.time.Instant
import java.util.UUID

class DbContentServiceImpl(
    private val dataService: DbDataService<DbContentEntity>,
    private val contentDataService: EcosContentStorageService
) : DbContentService, DbMigrationsExecutor {

    override fun uploadContent(storage: String, data: DbContentUploadData): DbEcosContentData {

        val contentStream = EcosContentWriterInputStreamImpl(data.content)
        val dataUri = contentDataService.uploadContent(storage, contentStream)
        val entity = DbContentEntity()

        entity.created = Instant.now()
        entity.encoding = data.encoding
        entity.mimeType = data.mimeType.ifBlank { "application/octet-stream" }
        entity.sha256 = contentStream.getSha256Digest()
        entity.size = contentStream.getContentSize()
        entity.uri = dataUri
        entity.name = data.name.ifBlank { UUID.randomUUID().toString() }

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
}
