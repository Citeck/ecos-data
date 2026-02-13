package ru.citeck.ecos.data.sql.trashcan

import ru.citeck.ecos.commons.json.Json
import ru.citeck.ecos.data.sql.context.DbSchemaContext
import ru.citeck.ecos.data.sql.repo.entity.DbEntity
import ru.citeck.ecos.data.sql.repo.find.DbFindPage
import ru.citeck.ecos.data.sql.repo.find.DbFindQuery
import ru.citeck.ecos.data.sql.repo.find.DbFindSort
import ru.citeck.ecos.data.sql.service.DbDataService
import ru.citeck.ecos.data.sql.service.DbDataServiceConfig
import ru.citeck.ecos.data.sql.service.DbDataServiceImpl
import ru.citeck.ecos.data.sql.service.DbMigrationsExecutor
import ru.citeck.ecos.data.sql.trashcan.entity.DbTrashcanEntity
import ru.citeck.ecos.records2.predicate.model.Predicate
import ru.citeck.ecos.records2.predicate.model.Predicates
import ru.citeck.ecos.txn.lib.TxnContext
import java.time.Instant

class DbTrashcanServiceImpl(
    private val schemaCtx: DbSchemaContext
) : DbTrashcanService, DbMigrationsExecutor {

    private val ecosContext = schemaCtx.ecosContext

    private val dataService: DbDataService<DbTrashcanEntity> = DbDataServiceImpl(
        DbTrashcanEntity::class.java,
        DbDataServiceConfig.create {
            withTable(DbTrashcanEntity.TABLE)
            withStoreTableMeta(true)
        },
        schemaCtx
    )

    override fun moveToTrashcan(
        entity: DbEntity,
        sourceTable: String,
        contentIds: List<Long>,
        deletedById: Long,
        deletedAsId: Long
    ) {
        val entityDataMap = LinkedHashMap<String, Any?>()

        entityDataMap["extId"] = entity.extId
        entityDataMap["updVersion"] = entity.updVersion
        entityDataMap["modified"] = entity.modified.toString()
        entityDataMap["modifier"] = entity.modifier
        entityDataMap["created"] = entity.created.toString()
        entityDataMap["creator"] = entity.creator
        entityDataMap["workspace"] = entity.workspace
        entityDataMap["status"] = entity.status
        entityDataMap["attributes"] = entity.attributes

        val trashcanEntity = DbTrashcanEntity()
        trashcanEntity.refId = entity.refId
        trashcanEntity.sourceTable = sourceTable
        trashcanEntity.type = entity.type
        trashcanEntity.name = entity.name
        trashcanEntity.deletedAt = Instant.now()
        trashcanEntity.deletedBy = deletedById
        trashcanEntity.deletedAs = deletedAsId
        trashcanEntity.traceId = ecosContext.get("traceId") as? String ?: ""
        trashcanEntity.txnId = TxnContext.getTxnOrNull()?.getId()?.toString() ?: ""
        trashcanEntity.entityData = Json.mapper.toString(entityDataMap) ?: "{}"
        trashcanEntity.contentIds = contentIds

        dataService.save(trashcanEntity)
    }

    override fun findAllByRefId(refId: Long): List<DbTrashcanEntity> {
        return dataService.find(
            DbFindQuery.create {
                withPredicate(Predicates.eq(DbTrashcanEntity.REF_ID, refId))
                withSortBy(listOf(DbFindSort(DbTrashcanEntity.DELETED_AT, false)))
            },
            DbFindPage.ALL
        ).entities
    }

    override fun findLatestByRefId(refId: Long): DbTrashcanEntity? {
        return dataService.find(
            DbFindQuery.create {
                withPredicate(Predicates.eq(DbTrashcanEntity.REF_ID, refId))
                withSortBy(listOf(DbFindSort(DbTrashcanEntity.DELETED_AT, false)))
            },
            DbFindPage.FIRST
        ).entities.firstOrNull()
    }

    override fun findAll(predicate: Predicate, skipCount: Int, maxItems: Int): List<DbTrashcanEntity> {
        return dataService.find(
            DbFindQuery.create {
                withPredicate(predicate)
                withSortBy(listOf(DbFindSort(DbTrashcanEntity.DELETED_AT, false)))
            },
            DbFindPage(skipCount, maxItems)
        ).entities
    }

    override fun deleteById(id: Long) {
        dataService.delete(id)
    }

    override fun deleteAllByRefId(refId: Long) {
        val entries = dataService.find(
            DbFindQuery.create {
                withPredicate(Predicates.eq(DbTrashcanEntity.REF_ID, refId))
            },
            DbFindPage.ALL
        ).entities

        if (entries.isEmpty()) {
            return
        }

        val contentService = schemaCtx.contentService
        for (entry in entries) {
            for (contentId in entry.contentIds) {
                contentService.removeContent(contentId)
            }
        }

        dataService.delete(entries)
    }

    override fun createTableIfNotExists() {
        dataService.runMigrations(emptyList(), mock = false, diff = true)
    }

    override fun runMigrations(mock: Boolean, diff: Boolean): List<String> {
        return dataService.runMigrations(emptyList(), mock, diff)
    }

    override fun resetColumnsCache() {
        dataService.resetColumnsCache()
    }
}
