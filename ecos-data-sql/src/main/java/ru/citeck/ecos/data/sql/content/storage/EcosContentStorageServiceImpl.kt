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
import ru.citeck.ecos.webapp.api.entity.EntityRef
import java.io.InputStream
import java.io.OutputStream
import java.util.concurrent.ConcurrentHashMap

class EcosContentStorageServiceImpl(
    webAppApi: EcosWebAppApi,
    schemaCtx: DbSchemaContext
) : EcosContentStorageService, DbMigrationsExecutor {

    private val storages: MutableMap<String, EcosContentStorage> = ConcurrentHashMap()

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

    override fun <T> readContent(storageRef: EntityRef, path: String, action: (InputStream) -> T): T {
        val storage = if (storageRef == EcosContentStorageConstants.LOCAL_CONTENT_STORAGE_REF) {
            localStorage
        } else {
            remoteStorage
        }
        return AuthContext.runAsSystem {
            storage.readContent(storageRef, path, action)
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

    override fun runMigrations(mock: Boolean, diff: Boolean): List<String> {
        val result = ArrayList<String>()
        storages.values.forEach {
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
