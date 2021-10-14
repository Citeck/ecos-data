package ru.citeck.ecos.data.sql.content.data

import ru.citeck.ecos.data.sql.content.data.storage.EcosContentStorage
import ru.citeck.ecos.data.sql.service.DbMigrationsExecutor
import java.io.InputStream
import java.io.OutputStream
import java.net.URI
import java.util.concurrent.ConcurrentHashMap

class EcosContentDataServiceImpl : EcosContentDataService, DbMigrationsExecutor {

    private val storages: MutableMap<String, EcosContentStorage> = ConcurrentHashMap()

    override fun writeContent(storage: String, action: (OutputStream) -> Unit): EcosContentDataMeta {
        val contentStorage = getStorage(storage)
        val meta = contentStorage.writeContent(action)
        return EcosContentDataMeta(
            URI(EcosContentDataConstants.URI_SCHEMA + "://" + storage + "/" + meta.path),
            meta.sha256,
            meta.size
        )
    }

    override fun <T> readContent(uri: URI, action: (InputStream) -> T): T {
        val contentStorage = getStorage(uri.authority)
        return contentStorage.readContent(uri.path.substring(1), action)
    }

    private fun getStorage(storage: String): EcosContentStorage {
        return storages[storage] ?: error("Storage is not registered: '$storage'")
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

    fun register(storage: EcosContentStorage) {
        this.storages[storage.getType()] = storage
    }
}
