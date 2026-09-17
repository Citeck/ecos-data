package ru.citeck.ecos.data.sql.content.storage.local

import ru.citeck.ecos.commons.data.ObjectData
import ru.citeck.ecos.data.sql.content.storage.EcosContentStorage
import ru.citeck.ecos.data.sql.service.DbDataService
import ru.citeck.ecos.data.sql.service.DbMigrationsExecutor
import ru.citeck.ecos.webapp.api.content.ContentRange
import ru.citeck.ecos.webapp.api.entity.EntityRef
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.InputStream
import java.io.OutputStream

class EcosContentLocalStorage(
    private val dataService: DbDataService<DbContentDataEntity>
) : EcosContentStorage,
    DbMigrationsExecutor {

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

    /**
     * The blob lives in a single column and is already fully in memory, so the range is served as a
     * window over that array instead of being emulated on top of a stream. The window is clamped to
     * the array bounds: a range which starts at or past the end of the content yields an empty
     * stream, and a range which extends past the end is truncated to the available bytes.
     */
    override fun <T> readContent(
        storageRef: EntityRef,
        dataKey: String,
        range: ContentRange,
        action: (InputStream) -> T
    ): T {

        // multiple parts doesn't support yet
        val id = dataKeyToEntityIds(dataKey).first()
        val entity = dataService.findById(id) ?: error("Content doesn't exists for id: $id")
        val data = entity.data

        if (range.isUnbounded()) {
            return action(ByteArrayInputStream(data))
        }

        val size = data.size.toLong()
        val offset = minOf(range.offset, size)
        val available = size - offset
        val length = if (range.length == ContentRange.LENGTH_TO_END) {
            available
        } else {
            minOf(range.length, available)
        }

        return action(ByteArrayInputStream(data, offset.toInt(), length.toInt()))
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
