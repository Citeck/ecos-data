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

    override fun uploadContent(storageConfig: EcosContentStorageConfig?, action: (OutputStream) -> Unit): EcosContentDataUrl {

        val storageRef = storageConfig?.ref ?: EcosContentStorageConstants.LOCAL_CONTENT_STORAGE_REF

        val storage = if (storageRef.getAppName().isBlank() ||
            storageRef == EcosContentStorageConstants.LOCAL_CONTENT_STORAGE_REF ||
            storageRef == EcosContentStorageConstants.DEFAULT_CONTENT_STORAGE_REF
        ) {
            localStorage
        } else {
            remoteStorage
        }
        val config = storageConfig?.config ?: ObjectData.create()
        val nnStorageConfig = EcosContentStorageConfig(storageRef, config)

        return AuthContext.runAsSystem {
            storage.uploadContent(nnStorageConfig, action)
        }
    }

    override fun <T> readContent(url: EcosContentDataUrl, action: (InputStream) -> T): T {
        val storage = if (url.isLocalStorageUrl()) {
            localStorage
        } else {
            remoteStorage
        }
        return AuthContext.runAsSystem {
            storage.readContent(url, action)
        }
    }

    override fun deleteContent(url: EcosContentDataUrl) {
        val storage = if (url.isLocalStorageUrl()) {
            localStorage
        } else {
            remoteStorage
        }
        return AuthContext.runAsSystem {
            storage.deleteContent(url)
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
}
