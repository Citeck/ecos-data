package ru.citeck.ecos.data.sql.content.storage.local

import ru.citeck.ecos.commons.data.ObjectData
import ru.citeck.ecos.data.sql.content.storage.EcosContentStorage
import ru.citeck.ecos.data.sql.service.DbDataService
import ru.citeck.ecos.data.sql.service.DbMigrationsExecutor
import ru.citeck.ecos.webapp.api.entity.EntityRef
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.InputStream
import java.io.OutputStream
import java.util.*

class EcosContentLocalStorage(
    private val dataService: DbDataService<DbContentDataEntity>
) : EcosContentStorage, DbMigrationsExecutor {

    companion object {
        // legacy prefix. Will be removed in future
        private const val PATH_PREFIX = "ecd://local/"
    }

    override fun uploadContent(
        storageRef: EntityRef,
        storageConfig: ObjectData,
        content: (OutputStream) -> Unit
    ): String {

        val output = ByteArrayOutputStream(10_000)
        content.invoke(output)

        val entity = DbContentDataEntity()
        entity.data = output.toByteArray()
        // temp solution until unique constraint will be removed
        entity.sha256 = UUID.randomUUID().toString()

        return entityToPath(dataService.save(entity))
    }

    private fun entityToPath(entity: DbContentDataEntity): String {
        return PATH_PREFIX + entity.id.toString()
    }

    private fun pathToEntityId(path: String): Long {
        return path.replaceFirst(PATH_PREFIX, "").toLong()
    }

    override fun <T> readContent(storageRef: EntityRef, path: String, action: (InputStream) -> T): T {

        val id = pathToEntityId(path)
        val entity = dataService.findById(id) ?: error("Content doesn't exists for id: $id")

        return action(ByteArrayInputStream(entity.data))
    }

    override fun deleteContent(storageRef: EntityRef, path: String) {
        dataService.forceDelete(pathToEntityId(path))
    }

    override fun runMigrations(mock: Boolean, diff: Boolean): List<String> {
        return dataService.runMigrations(mock, diff)
    }
}
