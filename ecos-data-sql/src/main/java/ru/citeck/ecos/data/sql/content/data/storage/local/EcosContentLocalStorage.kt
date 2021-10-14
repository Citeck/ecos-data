package ru.citeck.ecos.data.sql.content.data.storage.local

import ru.citeck.ecos.commons.utils.digest.DigestAlgorithm
import ru.citeck.ecos.commons.utils.digest.DigestUtils
import ru.citeck.ecos.data.sql.content.data.storage.EcosContentStorage
import ru.citeck.ecos.data.sql.content.data.storage.EcosContentStorageMeta
import ru.citeck.ecos.data.sql.repo.find.DbFindPage
import ru.citeck.ecos.data.sql.service.DbDataService
import ru.citeck.ecos.data.sql.service.DbMigrationsExecutor
import ru.citeck.ecos.records2.predicate.model.Predicates
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.InputStream
import java.io.OutputStream

class EcosContentLocalStorage(
    private val dataService: DbDataService<DbContentDataEntity>
) : EcosContentStorage, DbMigrationsExecutor {

    companion object {
        const val TYPE = "local"
    }

    override fun writeContent(action: (OutputStream) -> Unit): EcosContentStorageMeta {

        val bytesOut = ByteArrayOutputStream()
        action.invoke(bytesOut)
        val byteArray = bytesOut.toByteArray()

        val digest = DigestUtils.getDigest(byteArray, DigestAlgorithm.SHA256)

        val data = dataService.find(
            Predicates.and(
                Predicates.eq(DbContentDataEntity.SHA_256, digest.hash),
                Predicates.eq(DbContentDataEntity.SIZE, digest.size)
            ),
            emptyList(),
            DbFindPage.FIRST
        )

        if (data.entities.isNotEmpty()) {
            return entityToMeta(data.entities[0])
        }

        val entity = DbContentDataEntity()
        entity.data = byteArray
        entity.sha256 = digest.hash
        entity.size = digest.size

        return entityToMeta(dataService.save(entity))
    }

    private fun entityToMeta(entity: DbContentDataEntity): EcosContentStorageMeta {
        return EcosContentStorageMeta(
            entity.id.toString(),
            entity.sha256,
            entity.size
        )
    }

    private fun entityToPath(entity: DbContentDataEntity): String {
        return entity.id.toString()
    }

    override fun <T> readContent(path: String, action: (InputStream) -> T): T {

        val id = path.toLong()
        val entity = dataService.findById(id) ?: error("Content doesn't exists for id: $path")

        val bytesIn = ByteArrayInputStream(entity.data)

        return action.invoke(bytesIn)
    }

    override fun runMigrations(mock: Boolean, diff: Boolean): List<String> {
        return dataService.runMigrations(mock, diff, true)
    }

    override fun getType() = TYPE
}
