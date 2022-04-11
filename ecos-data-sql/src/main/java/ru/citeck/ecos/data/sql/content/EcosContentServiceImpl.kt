package ru.citeck.ecos.data.sql.content

import ru.citeck.ecos.data.sql.content.data.EcosContentDataService
import ru.citeck.ecos.data.sql.content.entity.DbContentEntity
import ru.citeck.ecos.data.sql.service.DbDataService
import ru.citeck.ecos.data.sql.service.DbMigrationsExecutor
import java.io.InputStream
import java.io.OutputStream
import java.time.Instant

class EcosContentServiceImpl(
    private val dataService: DbDataService<DbContentEntity>,
    private val contentDataService: EcosContentDataService
) : EcosContentService, DbMigrationsExecutor {

    override fun writeContent(storage: String, meta: EcosContentMeta, bytes: ByteArray): EcosContentMeta {
        return writeContent(storage, meta) { it.write(bytes) }
    }

    override fun writeContent(storage: String, meta: EcosContentMeta, action: (OutputStream) -> Unit): EcosContentMeta {

        val dataMeta = contentDataService.writeContent(storage, action)
        val entity = DbContentEntity()

        entity.created = Instant.now()
        entity.encoding = meta.encoding
        entity.mimeType = meta.mimeType
        entity.sha256 = dataMeta.sha256
        entity.size = dataMeta.size
        entity.uri = dataMeta.uri
        entity.name = meta.name

        return entityToMeta(dataService.save(entity))
    }

    override fun getMeta(id: Long): EcosContentMeta? {
        return dataService.findById(id)?.let { entityToMeta(it) }
    }

    override fun <T> readContent(id: Long, action: (EcosContentMeta, InputStream) -> T): T {
        val entity = dataService.findById(id) ?: error("Entity doesn't found by id: '$id'")
        return contentDataService.readContent(entity.uri) { inputStream ->
            action.invoke(entityToMeta(entity), inputStream)
        }
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

    private fun entityToMeta(entity: DbContentEntity): EcosContentMeta {
        return EcosContentMeta.create {
            withId(entity.id)
            withName(entity.name)
            withSha256(entity.sha256)
            withSize(entity.size)
            withMimeType(entity.mimeType)
            withEncoding(entity.encoding)
            withCreated(entity.created)
        }
    }
}
