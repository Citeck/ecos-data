package ru.citeck.ecos.data.sql.content.storage.local

import ru.citeck.ecos.commons.utils.io.IOUtils
import ru.citeck.ecos.data.sql.content.storage.EcosContentStorage
import ru.citeck.ecos.data.sql.content.stream.EcosContentWriterInputStream
import ru.citeck.ecos.data.sql.repo.find.DbFindPage
import ru.citeck.ecos.data.sql.service.DbDataService
import ru.citeck.ecos.data.sql.service.DbMigrationsExecutor
import ru.citeck.ecos.records2.predicate.model.Predicates
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.InputStream

class EcosContentLocalStorage(
    private val dataService: DbDataService<DbContentDataEntity>
) : EcosContentStorage, DbMigrationsExecutor {

    companion object {
        const val TYPE = "local"
    }

    override fun uploadContent(type: String, content: EcosContentWriterInputStream): String {

        val bytesOut = ByteArrayOutputStream()
        IOUtils.copy(content, bytesOut)
        val byteArray = bytesOut.toByteArray()

        val sha256 = content.getSha256Digest()
        val contentSize = content.getContentSize()

        val data = dataService.find(
            Predicates.and(
                Predicates.eq(DbContentDataEntity.SHA_256, sha256),
                Predicates.eq(DbContentDataEntity.SIZE, contentSize)
            ),
            emptyList(),
            DbFindPage.FIRST
        )

        if (data.entities.isNotEmpty()) {
            return entityToPath(data.entities[0])
        }

        val entity = DbContentDataEntity()
        entity.data = byteArray
        entity.sha256 = sha256
        entity.size = contentSize

        return entityToPath(dataService.save(entity))
    }

    private fun entityToPath(entity: DbContentDataEntity): String {
        return entity.id.toString()
    }

    override fun <T> readContent(path: String, action: (InputStream) -> T): T {

        val id = path.toLong()
        val entity = dataService.findById(id) ?: error("Content doesn't exists for id: $path")

        return action(ByteArrayInputStream(entity.data))
    }

    override fun removeContent(path: String) {
        dataService.forceDelete(path.toLong())
    }

    override fun runMigrations(mock: Boolean, diff: Boolean): List<String> {
        return dataService.runMigrations(mock, diff, true)
    }

    override fun getType() = TYPE
}
