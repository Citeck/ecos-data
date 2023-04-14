package ru.citeck.ecos.data.sql.records.assocs

import ru.citeck.ecos.context.lib.auth.AuthContext
import ru.citeck.ecos.data.sql.context.DbSchemaContext
import ru.citeck.ecos.data.sql.records.attnames.DbEcosAttributesService
import ru.citeck.ecos.data.sql.repo.find.DbFindPage
import ru.citeck.ecos.data.sql.repo.find.DbFindRes
import ru.citeck.ecos.data.sql.repo.find.DbFindSort
import ru.citeck.ecos.data.sql.service.DbDataService
import ru.citeck.ecos.data.sql.service.DbDataServiceConfig
import ru.citeck.ecos.data.sql.service.DbDataServiceImpl
import ru.citeck.ecos.data.sql.service.aggregation.AggregateFunc
import ru.citeck.ecos.records2.predicate.model.Predicates
import ru.citeck.ecos.records2.predicate.model.ValuePredicate
import ru.citeck.ecos.webapp.api.constants.AppName
import ru.citeck.ecos.webapp.api.entity.EntityRef
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

    fun createAssocs(sourceId: Long, attribute: String, child: Boolean, targetIds: Collection<Long>) {

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
                listOf(AggregateFunc(DbAssocEntity.INDEX, "max", DbAssocEntity.INDEX))
            )

            var index = if (maxIdxFindRes.entities.isEmpty()) {
                0
            } else {
                maxIdxFindRes.entities[0].index + 1
            }

            val creatorName = AuthContext.getCurrentUser().ifBlank { "system" }
            val creatorId = refsService.getOrCreateIdByEntityRef(
                EntityRef.create(AppName.EMODEL, "person", creatorName)
            )
            val created = Instant.now()

            dataService.save(
                assocsToCreate.map {
                    val entity = DbAssocEntity()
                    entity.sourceId = sourceId
                    entity.targetId = it
                    entity.attributeId = attId
                    entity.child = child
                    entity.index = index++
                    entity.creator = creatorId
                    entity.created = created
                    entity
                }
            )
        }
    }

    fun removeAssocs(sourceId: Long, attribute: String, targetIds: Collection<Long>) {
        if (targetIds.isEmpty()) {
            return
        }
        dataService.forceDelete(
            Predicates.and(
                Predicates.eq(DbAssocEntity.SOURCE_ID, sourceId),
                Predicates.eq(DbAssocEntity.ATTRIBUTE, getIdForAtt(attribute)),
                ValuePredicate(DbAssocEntity.TARGET_ID, ValuePredicate.Type.IN, targetIds)
            )
        )
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

        val result = dataService.find(
            Predicates.and(
                Predicates.eq(DbAssocEntity.TARGET_ID, targetId),
                Predicates.eq(DbAssocEntity.ATTRIBUTE, getIdForAtt(attribute))
            ),
            listOf(DbFindSort(DbAssocEntity.INDEX, true)),
            page
        )
        return result.withEntities(mapToDto(result.entities))
    }

    private fun getIdForAtt(attribute: String, createIfNotExists: Boolean = false): Long {
        return attsService.getIdsForAtts(listOf(attribute), createIfNotExists)[attribute] ?: -1L
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
