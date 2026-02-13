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

        return entityToDataKey(dataService.save(entity))
    }

    private fun entityToDataKey(entity: DbContentDataEntity): String {
        return PATH_PREFIX + entity.id.toString()
    }

    private fun dataKeyToEntityIds(path: String): List<Long> {
        return path.replaceFirst(PATH_PREFIX, "")
            .split(",")
            .filter { it.isNotBlank() }
            .map { it.toLong() }
    }

    override fun <T> readContent(storageRef: EntityRef, dataKey: String, action: (InputStream) -> T): T {

        // multiple parts doesn't support yet
        val id = dataKeyToEntityIds(dataKey).first()
        val entity = dataService.findById(id) ?: error("Content doesn't exists for id: $id")

        return action(ByteArrayInputStream(entity.data))
    }

    override fun deleteContent(storageRef: EntityRef, dataKey: String) {
        dataKeyToEntityIds(dataKey).forEach {
            dataService.delete(it)
        }
    }

    override fun runMigrations(mock: Boolean, diff: Boolean): List<String> {
        return dataService.runMigrations(mock, diff)
    }

    fun getDataService(): DbDataService<DbContentDataEntity> {
        return dataService
    }
}
