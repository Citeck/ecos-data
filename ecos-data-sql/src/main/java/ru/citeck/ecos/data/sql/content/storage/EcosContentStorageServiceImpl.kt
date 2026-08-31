package ru.citeck.ecos.data.sql.content.storage

import ru.citeck.ecos.commons.data.ObjectData
import ru.citeck.ecos.context.lib.auth.AuthContext
import ru.citeck.ecos.data.sql.content.storage.local.DbContentDataEntity
import ru.citeck.ecos.data.sql.content.storage.local.EcosContentLocalStorage
import ru.citeck.ecos.data.sql.content.storage.remote.EcosContentRemoteStorage
import ru.citeck.ecos.data.sql.context.DbSchemaContext
import ru.citeck.ecos.data.sql.service.DbDataServiceConfig
import ru.citeck.ecos.data.sql.service.DbDataServiceImpl
import ru.citeck.ecos.data.sql.service.DbMigrationsExecutor
import ru.citeck.ecos.webapp.api.EcosWebAppApi
import ru.citeck.ecos.webapp.api.content.ContentRange
import ru.citeck.ecos.webapp.api.entity.EntityRef
import java.io.InputStream
import java.io.OutputStream

class EcosContentStorageServiceImpl(
    webAppApi: EcosWebAppApi,
    schemaCtx: DbSchemaContext
) : EcosContentStorageService, DbMigrationsExecutor {

    private val remoteStorage = EcosContentRemoteStorage(webAppApi.getWebClientApi())
    private val localStorage = EcosContentLocalStorage(
        DbDataServiceImpl(
            DbContentDataEntity::class.java,
            DbDataServiceConfig.create {
                withTable(DbContentDataEntity.TABLE)
                withStoreTableMeta(true)
            },
            schemaCtx
        )
    )

    /**
     * Every storage this service can dispatch to. Only the ones that keep their data in this
     * schema's own tables have migrations to run - see [runMigrations].
     */
    private val storages: List<EcosContentStorage> = listOf(localStorage, remoteStorage)

    override fun uploadContent(
        storageRef: EntityRef,
        storageConfig: ObjectData,
        action: (OutputStream) -> Unit
    ): String {
        val storage = if (storageRef == EcosContentStorageConstants.LOCAL_CONTENT_STORAGE_REF) {
            localStorage
        } else {
            remoteStorage
        }
        return AuthContext.runAsSystem {
            storage.uploadContent(storageRef, storageConfig, action)
        }
    }

    override fun <T> readContent(
        storageRef: EntityRef,
        path: String,
        range: ContentRange,
        action: (InputStream) -> T
    ): T {
        val storage = if (storageRef == EcosContentStorageConstants.LOCAL_CONTENT_STORAGE_REF) {
            localStorage
        } else {
            remoteStorage
        }
        return AuthContext.runAsSystem {
            storage.readContent(storageRef, path, range, action)
        }
    }

    override fun deleteContent(storageRef: EntityRef, path: String) {
        val storage = if (storageRef == EcosContentStorageConstants.LOCAL_CONTENT_STORAGE_REF) {
            localStorage
        } else {
            remoteStorage
        }
        return AuthContext.runAsSystem {
            storage.deleteContent(storageRef, path)
        }
    }

    override fun isChunkedSupported(storageRef: EntityRef): Boolean {
        if (storageRef == EcosContentStorageConstants.LOCAL_CONTENT_STORAGE_REF) {
            return false
        }
        return AuthContext.runAsSystem {
            remoteStorage.isChunkedSupported(storageRef)
        }
    }

    override fun chunkedInit(storageRef: EntityRef, meta: ChunkedInitMeta): ChunkedInitResult {
        if (storageRef == EcosContentStorageConstants.LOCAL_CONTENT_STORAGE_REF) {
            return ChunkedInitResult(false, "", 0)
        }
        return AuthContext.runAsSystem {
            remoteStorage.chunkedInit(storageRef, meta)
        }
    }

    override fun chunkedWriteChunk(
        storageRef: EntityRef,
        state: String,
        chunkIndex: Int,
        content: InputStream,
        contentLength: Long
    ): String {
        if (storageRef == EcosContentStorageConstants.LOCAL_CONTENT_STORAGE_REF) {
            error("Chunked upload is not supported for local content storage")
        }
        return AuthContext.runAsSystem {
            remoteStorage.chunkedWriteChunk(storageRef, state, chunkIndex, content, contentLength)
        }
    }

    override fun chunkedComplete(storageRef: EntityRef, state: String): String {
        if (storageRef == EcosContentStorageConstants.LOCAL_CONTENT_STORAGE_REF) {
            error("Chunked upload is not supported for local content storage")
        }
        return AuthContext.runAsSystem {
            remoteStorage.chunkedComplete(storageRef, state)
        }
    }

    override fun chunkedAbort(storageRef: EntityRef, state: String) {
        if (storageRef == EcosContentStorageConstants.LOCAL_CONTENT_STORAGE_REF) {
            error("Chunked upload is not supported for local content storage")
        }
        AuthContext.runAsSystem {
            remoteStorage.chunkedAbort(storageRef, state)
        }
    }

    override fun runMigrations(mock: Boolean, diff: Boolean): List<String> {
        val result = ArrayList<String>()
        storages.forEach {
            if (it is DbMigrationsExecutor) {
                result.addAll(it.runMigrations(mock, diff))
            }
        }
        return result
    }

    fun getLocalStorageService(): EcosContentLocalStorage {
        return localStorage
    }

    override fun resetColumnsCache() {
        localStorage.getDataService().resetColumnsCache()
    }
}
