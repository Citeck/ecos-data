package ru.citeck.ecos.data.sql.content

import ru.citeck.ecos.commons.mime.MimeTypes
import ru.citeck.ecos.data.sql.content.entity.DbContentEntity
import ru.citeck.ecos.data.sql.content.storage.EcosContentDataUrl
import ru.citeck.ecos.data.sql.content.storage.EcosContentStorageConfig
import ru.citeck.ecos.data.sql.content.storage.EcosContentStorageConstants
import ru.citeck.ecos.data.sql.content.storage.EcosContentStorageService
import ru.citeck.ecos.data.sql.content.writer.EcosContentWriterImpl
import ru.citeck.ecos.data.sql.context.DbSchemaContext
import ru.citeck.ecos.data.sql.repo.find.DbFindPage
import ru.citeck.ecos.data.sql.repo.find.DbFindQuery
import ru.citeck.ecos.data.sql.service.DbDataService
import ru.citeck.ecos.data.sql.service.DbDataServiceConfig
import ru.citeck.ecos.data.sql.service.DbDataServiceImpl
import ru.citeck.ecos.data.sql.service.DbMigrationsExecutor
import ru.citeck.ecos.records2.predicate.model.Predicates
import ru.citeck.ecos.webapp.api.content.EcosContentWriter
import ru.citeck.ecos.webapp.api.entity.EntityRef
import ru.citeck.ecos.webapp.api.mime.MimeType
import java.io.InputStream
import java.time.Instant
import java.util.UUID

class DbContentServiceImpl(
    private val contentStorageService: EcosContentStorageService,
    private val schemaCtx: DbSchemaContext
) : DbContentService, DbMigrationsExecutor {

    private val dataService: DbDataService<DbContentEntity> = DbDataServiceImpl(
        DbContentEntity::class.java,
        DbDataServiceConfig.create {
            withTable(DbContentEntity.TABLE)
            withStoreTableMeta(true)
        },
        schemaCtx
    )

    override fun uploadContent(
        name: String?,
        mimeType: String?,
        encoding: String?,
        storage: EcosContentStorageConfig?,
        creatorRefId: Long,
        content: (EcosContentWriter) -> Unit
    ): DbEcosContentData {

        val nnName = (name ?: "").ifBlank { UUID.randomUUID().toString() }
        val nnMimeType = (mimeType ?: "").ifBlank { MimeTypes.APP_BIN_TEXT }
        val nnEncoding = encoding ?: ""

        var sha256 = ""
        var size = 0L

        val dataUrl = contentStorageService.uploadContent(storage) { output ->
            val writer = EcosContentWriterImpl(output)
            content.invoke(writer)
            writer.getOutputStream().flush()
            val meta = writer.finish()
            sha256 = meta.getSha256()
            size = meta.getSize()
        }

        if (sha256.isEmpty() || size <= 0L) {
            error("Invalid content metadata. Sha256: $sha256 Size: $size")
        }

        var storageRef = storage?.ref ?: EcosContentStorageConstants.LOCAL_CONTENT_STORAGE_REF
        if (storageRef == EcosContentStorageConstants.DEFAULT_CONTENT_STORAGE_REF) {
            storageRef = EcosContentStorageConstants.LOCAL_CONTENT_STORAGE_REF
        }

        val entity = DbContentEntity()

        entity.name = nnName
        entity.mimeType = nnMimeType
        entity.encoding = nnEncoding
        entity.created = Instant.now()
        entity.creator = creatorRefId
        entity.sha256 = sha256
        entity.size = size
        entity.uri = dataUrl.toString()
        entity.storageRef = schemaCtx.recordRefService.getOrCreateIdByEntityRef(storageRef)

        return EcosContentDataImpl(dataService.save(entity))
    }

    override fun getContent(id: Long): DbEcosContentData? {
        if (id < 0) {
            return null
        }
        val entity = dataService.findById(id) ?: return null
        return EcosContentDataImpl(entity)
    }

    override fun removeContent(id: Long) {
        val entity = dataService.findById(id) ?: return
        dataService.forceDelete(entity)
        val entitiesWithSameUri = dataService.find(
            DbFindQuery.create {
                withPredicate(
                    Predicates.eq(
                        DbContentEntity.URI,
                        entity.uri
                    )
                )
            },
            DbFindPage.FIRST
        )
        if (entitiesWithSameUri.entities.isEmpty()) {
            contentStorageService.deleteContent(EcosContentDataUrl.valueOf(entity.uri))
        }
    }

    override fun cloneContent(id: Long, creatorRefId: Long): DbEcosContentData {
        val entity = dataService.findById(id) ?: error("Content doesn't found by id '$id'")
        entity.id = DbContentEntity.NEW_REC_ID
        entity.created = Instant.now()
        entity.creator = creatorRefId
        return EcosContentDataImpl(dataService.save(entity))
    }

    override fun runMigrations(mock: Boolean, diff: Boolean): List<String> {
        var result = dataService.runMigrations(emptyList(), mock, diff)
        if (contentStorageService is DbMigrationsExecutor) {
            val newRes = ArrayList(result)
            newRes.addAll(contentStorageService.runMigrations(mock, diff))
            result = newRes
        }
        return result
    }

    override fun resetColumnsCache() {
        dataService.resetColumnsCache()
    }

    fun getDataService(): DbDataService<DbContentEntity> {
        return dataService
    }

    private inner class EcosContentDataImpl(val entity: DbContentEntity) : DbEcosContentData {

        private val creatorName: String by lazy {
            schemaCtx.recordRefService.getEntityRefById(entity.creator).getLocalId()
        }

        private val dataUrl by lazy { EcosContentDataUrl.valueOf(entity.uri) }
        private val storageRefValue by lazy {
            if (entity.storageRef >= 0) {
                schemaCtx.recordRefService.getEntityRefById(entity.storageRef)
            } else {
                EntityRef.EMPTY
            }
        }

        override fun getDbId(): Long {
            return entity.id
        }

        override fun getUrl(): EcosContentDataUrl {
            return dataUrl
        }

        override fun getCreated(): Instant {
            return entity.created
        }

        override fun getCreator(): String {
            return creatorName
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

        override fun getStorageRef(): EntityRef {
            return EntityRef.valueOf(storageRefValue)
        }

        override fun <T> readContent(action: (InputStream) -> T): T {
            return contentStorageService.readContent(dataUrl, action)
        }
    }
}
