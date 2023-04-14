package ru.citeck.ecos.data.sql.domain.migration.type

import mu.KotlinLogging
import ru.citeck.ecos.data.sql.domain.migration.DbDomainMigration
import ru.citeck.ecos.data.sql.domain.migration.DbDomainMigrationContext
import ru.citeck.ecos.data.sql.records.utils.DbAttValueUtils
import ru.citeck.ecos.data.sql.repo.find.DbFindPage
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType
import ru.citeck.ecos.records2.predicate.model.Predicates

class MoveAssocsToAssocsTable : DbDomainMigration {

    companion object {
        private val log = KotlinLogging.logger {}
    }

    override fun run(context: DbDomainMigrationContext) {

        val daoCtx = context.recordsDao.getRecordsDaoCtx()
        val typeRef = context.config.recordsDao.typeRef
        val typeInfo = daoCtx.ecosTypeService.getTypeInfo(typeRef.getLocalId())

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

        val pageSize = 100
        fun findImpl(page: Int) = context.dataService.find(
            Predicates.alwaysTrue(),
            emptyList(),
            DbFindPage(page * pageSize, pageSize),
            true
        )

        val assocsService = context.recordsDao.getRecordsDaoCtx().assocsService

        var page = 0
        var processed = 0

        var findRes = findImpl(page)
        while (findRes.entities.isNotEmpty()) {
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
            findRes = findImpl(++page)
        }

        log.info { "Migration completed. Processed: $processed" }
    }

    override fun getAppliedVersions(): Pair<Int, Int> {
        return 1 to 2
    }
}
