package ru.citeck.ecos.data.sql.content.storage

import ru.citeck.ecos.commons.utils.NameUtils
import ru.citeck.ecos.data.sql.content.stream.EcosContentWriterInputStream
import ru.citeck.ecos.data.sql.service.DbMigrationsExecutor
import java.io.InputStream
import java.net.URI
import java.util.concurrent.ConcurrentHashMap

class EcosContentStorageServiceImpl : EcosContentStorageService, DbMigrationsExecutor {

    private val nameEscaper = NameUtils.getEscaperWithAllowedChars("/?&=")
    private val storages: MutableMap<String, EcosContentStorage> = ConcurrentHashMap()

    override fun uploadContent(storage: String, content: EcosContentWriterInputStream): URI {

        val delimIdx = storage.indexOf('/')
        val (storageType, storageSubType) = if (delimIdx > 0) {
            storage.substring(0, delimIdx) to storage.substring(delimIdx + 1)
        } else {
            storage to ""
        }

        val contentStorage = getStorage(storageType)
        val storagePath = contentStorage.uploadContent(storageSubType, content)
        return URI(
            EcosContentStorageConstants.URI_SCHEMA +
                "://" + contentStorage.getType() +
                "/" + nameEscaper.escape(storagePath)
        )
    }

    override fun getContent(uri: URI): InputStream {
        val contentStorage = getStorage(uri.authority)
        return contentStorage.getContent(nameEscaper.unescape(uri.path.substring(1)))
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
        register(storage.getType(), storage)
    }

    fun register(type: String, storage: EcosContentStorage) {
        this.storages[type] = storage
    }
}
