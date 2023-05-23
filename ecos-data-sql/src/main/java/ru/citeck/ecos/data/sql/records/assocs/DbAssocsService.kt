package ru.citeck.ecos.data.sql.records.assocs

import ru.citeck.ecos.data.sql.context.DbSchemaContext
import ru.citeck.ecos.data.sql.records.attnames.DbEcosAttributesService
import ru.citeck.ecos.data.sql.repo.find.DbFindPage
import ru.citeck.ecos.data.sql.repo.find.DbFindRes
import ru.citeck.ecos.data.sql.repo.find.DbFindSort
import ru.citeck.ecos.data.sql.service.DbDataService
import ru.citeck.ecos.data.sql.service.DbDataServiceConfig
import ru.citeck.ecos.data.sql.service.DbDataServiceImpl
import ru.citeck.ecos.data.sql.service.aggregation.AggregateFunc
import ru.citeck.ecos.records2.predicate.model.Predicate
import ru.citeck.ecos.records2.predicate.model.Predicates
import ru.citeck.ecos.records2.predicate.model.ValuePredicate
import ru.citeck.ecos.txn.lib.TxnContext
import java.time.Instant

class DbAssocsService(
    schemaCtx: DbSchemaContext
) {

    private val dataService: DbDataService<DbAssocEntity> = DbDataServiceImpl(
        DbAssocEntity::class.java,
        DbDataServiceConfig.create {
            withTable(DbAssocEntity.TABLE)
        },
        schemaCtx
    )

    private val attsService = DbEcosAttributesService(schemaCtx)
    private val refsService = schemaCtx.recordRefService

    fun getIdsForAtts(attributes: Collection<String>, createIfNotExists: Boolean = false): Map<String, Long> {
        return attsService.getIdsForAtts(attributes, createIfNotExists)
    }

    fun getIdForAtt(attribute: String, createIfNotExists: Boolean = false): Long {
        return attsService.getIdsForAtts(listOf(attribute), createIfNotExists)[attribute] ?: -1L
    }

    fun createAssocs(
        sourceId: Long,
        attribute: String,
        child: Boolean,
        targetIds: Collection<Long>,
        creatorId: Long
    ): List<Long> {

        val attId = getIdForAtt(attribute, true)

        val existingAssocs = dataService.find(
            Predicates.and(
                Predicates.eq(DbAssocEntity.SOURCE_ID, sourceId),
                Predicates.eq(DbAssocEntity.ATTRIBUTE, attId),
                ValuePredicate(DbAssocEntity.TARGET_ID, ValuePredicate.Type.IN, targetIds),
                Predicates.eq(DbAssocEntity.CHILD, child)
            ),
            emptyList(),
            DbFindPage.ALL
        )
        val assocsToCreate = LinkedHashSet(targetIds)
        existingAssocs.entities.forEach {
            assocsToCreate.remove(it.targetId)
        }
        if (assocsToCreate.isNotEmpty()) {

            val maxIdxFindRes = dataService.find(
                Predicates.and(
                    Predicates.eq(DbAssocEntity.SOURCE_ID, sourceId),
                    Predicates.eq(DbAssocEntity.ATTRIBUTE, attId)
                ),
                emptyList(),
                DbFindPage.FIRST,
                true,
                listOf(DbAssocEntity.SOURCE_ID),
                listOf(AggregateFunc(DbAssocEntity.INDEX, "max", DbAssocEntity.INDEX)),
                emptyMap(),
                false
            )

            var index = if (maxIdxFindRes.entities.isEmpty()) {
                0
            } else {
                maxIdxFindRes.entities[0].index + 1
            }

            dataService.save(
                assocsToCreate.map {
                    val entity = DbAssocEntity()
                    entity.sourceId = sourceId
                    entity.targetId = it
                    entity.attributeId = attId
                    entity.child = child
                    entity.index = index++
                    entity.creator = creatorId
                    entity.created = Instant.now()
                    entity
                }
            )
        }
        return assocsToCreate.toList()
    }

    fun removeAssocs(sourceId: Long, force: Boolean) {
        val predicate = Predicates.eq(DbAssocEntity.SOURCE_ID, sourceId)
        if (force) {
            dataService.forceDelete(predicate)
        } else {
            dataService.delete(predicate)
        }
    }

    fun removeAssocs(sourceId: Long, attribute: String, targetIds: Collection<Long>, force: Boolean): List<Long> {
        if (targetIds.isEmpty()) {
            return emptyList()
        }

        fun createPredicateForIds(ids: Collection<Long>): Predicate {
            return Predicates.and(
                Predicates.eq(DbAssocEntity.SOURCE_ID, sourceId),
                Predicates.eq(DbAssocEntity.ATTRIBUTE, getIdForAtt(attribute)),
                ValuePredicate(DbAssocEntity.TARGET_ID, ValuePredicate.Type.IN, ids)
            )
        }

        val existentAssocs = dataService.findAll(createPredicateForIds(targetIds), force)

        if (existentAssocs.isEmpty()) {
            return emptyList()
        }

        val targetsToRemove = existentAssocs.map { it.targetId }
        val predicate = createPredicateForIds(targetsToRemove)
        if (force) {
            dataService.forceDelete(predicate)
        } else {
            dataService.delete(predicate)
        }
        return targetsToRemove
    }

    fun getTargetAssocs(sourceId: Long, attribute: String, page: DbFindPage): DbFindRes<DbAssocDto> {

        val result = dataService.find(
            Predicates.and(
                Predicates.eq(DbAssocEntity.SOURCE_ID, sourceId),
                Predicates.eq(DbAssocEntity.ATTRIBUTE, getIdForAtt(attribute))
            ),
            listOf(DbFindSort(DbAssocEntity.INDEX, true)),
            page
        )
        return result.withEntities(mapToDto(result.entities))
    }

    fun getSourceAssocs(targetId: Long, attribute: String, page: DbFindPage): DbFindRes<DbAssocDto> {

        val conditions = mutableListOf(
            Predicates.eq(DbAssocEntity.TARGET_ID, targetId)
        )
        if (attribute.isNotEmpty()) {
            conditions.add(Predicates.eq(DbAssocEntity.ATTRIBUTE, getIdForAtt(attribute)))
        }

        val result = dataService.find(
            Predicates.and(conditions),
            listOf(DbFindSort(DbAssocEntity.INDEX, true)),
            page
        )
        return result.withEntities(mapToDto(result.entities))
    }

    fun createTableIfNotExists() {
        TxnContext.doInTxn {
            dataService.runMigrations(mock = false, diff = true)
        }
    }

    fun isAssocsTableExists(): Boolean {
        return dataService.isTableExists()
    }

    fun resetColumnsCache() {
        dataService.resetColumnsCache()
    }

    private fun mapToDto(entities: List<DbAssocEntity>): List<DbAssocDto> {

        val attributeIds = HashSet<Long>()
        entities.forEach { attributeIds.add(it.attributeId) }
        val attsByIds = attsService.getAttsByIds(attributeIds)

        return entities.map {
            DbAssocDto(
                it.sourceId,
                attsByIds[it.attributeId] ?: error("Att is not found: ${it.attributeId}"),
                it.targetId,
                it.child
            )
        }
    }
}
