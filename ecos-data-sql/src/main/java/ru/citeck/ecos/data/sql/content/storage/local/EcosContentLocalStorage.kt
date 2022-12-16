package ru.citeck.ecos.data.sql.content.storage.local

import ru.citeck.ecos.data.sql.content.storage.EcosContentStorage
import ru.citeck.ecos.data.sql.content.writer.EcosContentWriterFactory
import ru.citeck.ecos.data.sql.repo.find.DbFindPage
import ru.citeck.ecos.data.sql.service.DbDataService
import ru.citeck.ecos.data.sql.service.DbMigrationsExecutor
import ru.citeck.ecos.records2.predicate.model.Predicates
import ru.citeck.ecos.webapp.api.content.EcosContentWriter
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.InputStream

class EcosContentLocalStorage(
    private val dataService: DbDataService<DbContentDataEntity>
) : EcosContentStorage, DbMigrationsExecutor {

    companion object {
        const val TYPE = "local"
    }

    private lateinit var writerFactory: EcosContentWriterFactory

    override fun init(writerFactory: EcosContentWriterFactory) {
        this.writerFactory = writerFactory
    }

    override fun uploadContent(type: String, writer: (EcosContentWriter) -> Unit): String {

        val bytesOut = ByteArrayOutputStream()
        val contentWriter = writerFactory.createWriter(bytesOut)
        writer(contentWriter)
        val byteArray = bytesOut.toByteArray()

        val sha256 = contentWriter.getSha256()
        val contentSize = contentWriter.getContentSize()

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
