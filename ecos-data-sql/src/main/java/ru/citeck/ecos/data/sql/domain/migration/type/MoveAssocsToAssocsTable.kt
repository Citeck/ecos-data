package ru.citeck.ecos.data.sql.domain.migration.type

import mu.KotlinLogging
import ru.citeck.ecos.data.sql.domain.migration.DbDomainMigration
import ru.citeck.ecos.data.sql.domain.migration.DbDomainMigrationContext
import ru.citeck.ecos.data.sql.records.utils.DbAttValueUtils
import ru.citeck.ecos.data.sql.repo.entity.DbEntity
import ru.citeck.ecos.data.sql.repo.find.DbFindPage
import ru.citeck.ecos.data.sql.repo.find.DbFindRes
import ru.citeck.ecos.data.sql.repo.find.DbFindSort
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType
import ru.citeck.ecos.records2.predicate.model.Predicates
import ru.citeck.ecos.txn.lib.TxnContext

class MoveAssocsToAssocsTable : DbDomainMigration {

    companion object {
        private val log = KotlinLogging.logger {}
    }

    override fun run(context: DbDomainMigrationContext) {

        val daoCtx = context.recordsDao.getRecordsDaoCtx()
        val typeRef = context.config.recordsDao.typeRef
        val typeInfo = daoCtx.ecosTypeService.getTypeInfo(typeRef.getLocalId())
        val dataSource = context.schemaContext.dataSourceCtx.dataSource

        if (typeInfo == null) {
            if (typeRef.isNotEmpty()) {
                log.warn { "TypeInfo is not found for ref: $typeRef" }
            } else {
                log.warn { "TypeRef is empty for source '${daoCtx.sourceId}'" }
            }
            return
        }

        val attributes = LinkedHashMap<String, AttributeDef>()
        typeInfo.model.attributes.forEach {
            attributes[it.id] = it
        }
        typeInfo.model.systemAttributes.forEach {
            attributes[it.id] = it
        }

        val targetAssocsAtts = mutableListOf<AttributeDef>()
        val childAssocsAtts = mutableListOf<AttributeDef>()

        for (attDef in attributes.values) {
            if (attDef.type == AttributeType.ASSOC) {
                if (attDef.config["child"].asBoolean()) {
                    childAssocsAtts.add(attDef)
                } else {
                    targetAssocsAtts.add(attDef)
                }
            }
        }

        if (targetAssocsAtts.isEmpty() && childAssocsAtts.isEmpty()) {
            log.warn { "Assoc attributes doesn't found. Migration will be skipped for '${daoCtx.sourceId}'" }
            return
        }

        var lastId = -1L
        fun findImpl(): DbFindRes<DbEntity> {
            val result = context.dataService.find(
                Predicates.gt(DbEntity.ID, lastId),
                listOf(DbFindSort(DbEntity.ID, true)),
                DbFindPage(0, 100),
                true
            )
            if (result.entities.isNotEmpty()) {
                lastId = result.entities.last().id
            }
            return result
        }

        val assocsService = context.recordsDao.getRecordsDaoCtx().assocsService

        var processed = 0

        var findRes = findImpl()
        log.info { "Start migration. Total count: ${findRes.totalCount}" }

        while (findRes.entities.isNotEmpty()) {
            TxnContext.doInNewTxn {
                dataSource.withTransaction(readOnly = false, requiresNew = true) {
                    findRes.entities.forEach { entity ->
                        targetAssocsAtts.forEach {
                            val targetIds = DbAttValueUtils.anyToSetOfLongs(entity.attributes[it.id])
                            if (targetIds.isNotEmpty()) {
                                assocsService.createAssocs(entity.refId, it.id, false, targetIds)
                            }
                        }
                        childAssocsAtts.forEach {
                            val childrenIds = DbAttValueUtils.anyToSetOfLongs(entity.attributes[it.id])
                            if (childrenIds.isNotEmpty()) {
                                assocsService.createAssocs(entity.refId, it.id, true, childrenIds)
                            }
                        }
                    }
                    processed++
                    if (processed.mod(10000) == 0) {
                        log.info { "Processed: $processed" }
                    }
                    findRes = findImpl()
                }
            }
        }

        log.info { "Migration completed. Processed: $processed" }
    }

    override fun getAppliedVersions(): Pair<Int, Int> {
        return 1 to 2
    }
}
