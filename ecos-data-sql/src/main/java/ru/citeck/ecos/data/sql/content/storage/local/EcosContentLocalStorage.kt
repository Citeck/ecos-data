package ru.citeck.ecos.data.sql.content.storage.local

import ru.citeck.ecos.data.sql.content.storage.EcosContentDataUrl
import ru.citeck.ecos.data.sql.content.storage.EcosContentStorage
import ru.citeck.ecos.data.sql.content.storage.EcosContentStorageConfig
import ru.citeck.ecos.data.sql.service.DbDataService
import ru.citeck.ecos.data.sql.service.DbMigrationsExecutor
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.InputStream
import java.io.OutputStream
import java.util.*

class EcosContentLocalStorage(
    private val dataService: DbDataService<DbContentDataEntity>
) : EcosContentStorage, DbMigrationsExecutor {

    override fun uploadContent(storage: EcosContentStorageConfig, content: (OutputStream) -> Unit): EcosContentDataUrl {

        val output = ByteArrayOutputStream(10_000)
        content.invoke(output)

        val entity = DbContentDataEntity()
        entity.data = output.toByteArray()
        // temp solution until unique constraint will be removed
        entity.sha256 = UUID.randomUUID().toString()

        return EcosContentDataUrl(
            EcosContentDataUrl.LOCAL_STORAGE_APP_NAME,
            entityToPath(dataService.save(entity))
        )
    }

    private fun entityToPath(entity: DbContentDataEntity): String {
        return entity.id.toString()
    }

    override fun <T> readContent(url: EcosContentDataUrl, action: (InputStream) -> T): T {

        val id = url.contentPath.toLong()
        val entity = dataService.findById(id) ?: error("Content doesn't exists for id: ${url.contentPath}")

        return action(ByteArrayInputStream(entity.data))
    }

    override fun deleteContent(url: EcosContentDataUrl) {
        dataService.forceDelete(url.contentPath.toLong())
    }

    override fun runMigrations(mock: Boolean, diff: Boolean): List<String> {
        return dataService.runMigrations(mock, diff)
    }
}
