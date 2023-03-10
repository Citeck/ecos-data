package ru.citeck.ecos.data.sql.content

import ru.citeck.ecos.commons.mime.MimeTypes
import ru.citeck.ecos.context.lib.auth.AuthContext
import ru.citeck.ecos.data.sql.content.entity.DbContentEntity
import ru.citeck.ecos.data.sql.content.storage.EcosContentStorageService
import ru.citeck.ecos.data.sql.content.storage.local.EcosContentLocalStorage
import ru.citeck.ecos.data.sql.repo.find.DbFindPage
import ru.citeck.ecos.data.sql.service.DbDataService
import ru.citeck.ecos.data.sql.service.DbMigrationsExecutor
import ru.citeck.ecos.records2.predicate.model.Predicates
import ru.citeck.ecos.webapp.api.content.EcosContentWriter
import ru.citeck.ecos.webapp.api.mime.MimeType
import java.io.InputStream
import java.net.URI
import java.time.Instant
import java.util.UUID

class DbContentServiceImpl(
    private val dataService: DbDataService<DbContentEntity>,
    private val contentStorageService: EcosContentStorageService
) : DbContentService, DbMigrationsExecutor {

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

        val dataUri = contentStorageService.uploadContent(nnStorage) {
            writer(it)
            it.getOutputStream().flush()
            val meta = it.finish()
            sha256 = meta.getSha256()
            size = meta.getSize()
        }

        if (sha256.isEmpty() || size <= 0L) {
            error("Invalid content metadata. Sha256: $sha256 Size: $size")
        }

        val entity = DbContentEntity()

        entity.name = nnName
        entity.mimeType = nnMimeType
        entity.encoding = nnEncoding
        entity.created = Instant.now()
        entity.creator = AuthContext.getCurrentUser()
        entity.sha256 = sha256
        entity.size = size
        entity.uri = dataUri

        return EcosContentDataImpl(dataService.save(entity))
    }

    override fun getContent(id: Long): DbEcosContentData? {
        val entity = dataService.findById(id) ?: return null
        return EcosContentDataImpl(entity)
    }

    override fun removeContent(id: Long) {
        val entity = dataService.findById(id) ?: return
        dataService.forceDelete(entity)
        val entitiesWithSameUri = dataService.find(
            Predicates.eq(
                DbContentEntity.URI,
                entity.uri
            ),
            emptyList(),
            DbFindPage.ALL
        )
        if (entitiesWithSameUri.entities.isEmpty()) {
            contentStorageService.removeContent(entity.uri)
        }
    }

    override fun cloneContent(id: Long): DbEcosContentData {
        val entity = dataService.findById(id) ?: error("Content doesn't found by id '$id'")
        entity.id = DbContentEntity.NEW_REC_ID
        entity.created = Instant.now()
        entity.creator = AuthContext.getCurrentUser()
        return EcosContentDataImpl(dataService.save(entity))
    }

    override fun runMigrations(mock: Boolean, diff: Boolean): List<String> {
        var result = dataService.runMigrations(emptyList(), mock, diff, true)
        if (contentStorageService is DbMigrationsExecutor) {
            val newRes = ArrayList(result)
            newRes.addAll(contentStorageService.runMigrations(mock, diff))
            result = newRes
        }
        return result
    }

    private inner class EcosContentDataImpl(val entity: DbContentEntity) : DbEcosContentData {

        override fun getDbId(): Long {
            return entity.id
        }

        override fun getUri(): URI {
            return entity.uri
        }

        override fun getCreated(): Instant {
            return entity.created
        }

        override fun getCreator(): String {
            return entity.creator
        }

        override fun getEncoding(): String {
            return entity.encoding
        }

        override fun getMimeType(): MimeType {
            return MimeTypes.parse(entity.mimeType)
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
            return contentStorageService.readContent(entity.uri, action)
        }
    }
}
