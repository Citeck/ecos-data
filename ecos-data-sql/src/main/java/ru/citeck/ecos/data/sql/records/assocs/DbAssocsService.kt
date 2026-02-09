package ru.citeck.ecos.data.sql.records.assocs

import ru.citeck.ecos.data.sql.context.DbSchemaContext
import ru.citeck.ecos.data.sql.records.attnames.DbEcosAttributesService
import ru.citeck.ecos.data.sql.records.refs.DbRecordRefEntity
import ru.citeck.ecos.data.sql.repo.entity.DbEntity
import ru.citeck.ecos.data.sql.repo.find.DbFindPage
import ru.citeck.ecos.data.sql.repo.find.DbFindQuery
import ru.citeck.ecos.data.sql.repo.find.DbFindRes
import ru.citeck.ecos.data.sql.repo.find.DbFindSort
import ru.citeck.ecos.data.sql.service.DbDataService
import ru.citeck.ecos.data.sql.service.DbDataServiceConfig
import ru.citeck.ecos.data.sql.service.DbDataServiceImpl
import ru.citeck.ecos.data.sql.service.RawTableJoin
import ru.citeck.ecos.data.sql.service.expression.token.ColumnToken
import ru.citeck.ecos.data.sql.service.expression.token.FunctionToken
import ru.citeck.ecos.data.sql.service.expression.token.ScalarToken
import ru.citeck.ecos.records2.predicate.model.Predicate
import ru.citeck.ecos.records2.predicate.model.Predicates
import ru.citeck.ecos.records2.predicate.model.ValuePredicate
import ru.citeck.ecos.txn.lib.TxnContext
import java.time.Instant

class DbAssocsService(
    private val currentAppName: String,
    private val schemaCtx: DbSchemaContext
) {

    private val dataService: DbDataService<DbAssocEntity> = DbDataServiceImpl(
        DbAssocEntity::class.java,
        DbDataServiceConfig.create {
            withTable(DbAssocEntity.MAIN_TABLE)
        },
        schemaCtx
    )

    private val deletedDataService: DbDataService<DbAssocEntity> = DbDataServiceImpl(
        DbAssocEntity::class.java,
        DbDataServiceConfig.create {
            withTable(DbAssocEntity.DELETED_TABLE)
        },
        schemaCtx
    )

    private val attsService = DbEcosAttributesService(schemaCtx)

    fun getIdsForAtts(attributes: Collection<String>, createIfNotExists: Boolean = false): Map<String, Long> {
        return attsService.getIdsForAtts(attributes, createIfNotExists)
    }

    // todo: move this logic to external service
    fun getIdForAtt(attribute: String, createIfNotExists: Boolean = false): Long {
        if (attribute.isEmpty()) {
            return -1L
        }
        return attsService.getIdsForAtts(listOf(attribute), createIfNotExists)[attribute] ?: -1L
    }

    // todo: move this logic to external service
    fun getAttById(attribute: Long): String {
        if (attribute == -1L) {
            return ""
        }
        return attsService.getAttsByIds(listOf(attribute))[attribute] ?: ""
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
                DbFindQuery.create {
                    withPredicate(
                        Predicates.and(
                            Predicates.eq(DbAssocEntity.SOURCE_ID, sourceId),
                            Predicates.eq(DbAssocEntity.ATTRIBUTE, attId)
                        )
                    )
                    withGroupBy(listOf(DbAssocEntity.SOURCE_ID))
                    addExpression(
                        DbAssocEntity.INDEX,
                        FunctionToken("max", listOf(ColumnToken(DbAssocEntity.INDEX)))
                    )
                },
                DbFindPage.FIRST,
                false
            )

            var index = if (maxIdxFindRes.entities.isEmpty()) {
                0
            } else {
                maxIdxFindRes.entities[0].index + 1
            }

            deletedDataService.delete(
                Predicates.and(
                    Predicates.eq(DbAssocEntity.SOURCE_ID, sourceId),
                    Predicates.eq(DbAssocEntity.ATTRIBUTE, attId),
                    ValuePredicate(DbAssocEntity.TARGET_ID, ValuePredicate.Type.IN, assocsToCreate)
                )
            )

            val entitiesToCreate = assocsToCreate.map {
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
            dataService.save(entitiesToCreate)
        }
        return assocsToCreate.toList()
    }

    /**
     * Finds external source applications that have references to the ref identified by targetId.
     */
    fun findSourceExternalApps(targetId: Long): Set<String> {

        val func = FunctionToken(
            "substringBefore",
            listOf(
                ColumnToken("ref.${DbRecordRefEntity.EXT_ID}"),
                ScalarToken("/")
            )
        )
        val refsTableCtx = schemaCtx.recordRefService.getDataService().getTableContext()

        val query = DbFindQuery.create()
            .withPredicate(Predicates.eq(DbAssocEntity.TARGET_ID, targetId))
            .withRawTableJoins(
                mapOf(
                    "ref" to RawTableJoin(
                        refsTableCtx,
                        Predicates.eq(DbAssocEntity.SOURCE_ID, "ref.${DbEntity.ID}")
                    )
                )
            )
            .withExpressions(mapOf("appName" to func))
            .withGroupBy(listOf("appName"))
            .build()

        val queryRes = dataService.findRaw(
            query,
            DbFindPage(0, 10_000),
            false
        )

        return queryRes.entities
            .asSequence()
            .map { it["appName"].toString() }
            .filter { it.isNotBlank() && it != currentAppName }
            .toSet()
    }

    fun findNonChildrenTargetRecsSrcIds(sourceId: Long): Set<String> {

        val func = FunctionToken(
            "substringBefore",
            listOf(
                ColumnToken("ref.${DbRecordRefEntity.EXT_ID}"),
                ScalarToken("@")
            )
        )
        val refsTableCtx = schemaCtx.recordRefService.getDataService().getTableContext()

        val query = DbFindQuery.create()
            .withPredicate(
                Predicates.and(
                    Predicates.eq(DbAssocEntity.CHILD, false),
                    Predicates.eq(DbAssocEntity.SOURCE_ID, sourceId)
                )
            )
            .withRawTableJoins(
                mapOf(
                    "ref" to RawTableJoin(
                        refsTableCtx,
                        Predicates.eq(DbAssocEntity.TARGET_ID, "ref.${DbEntity.ID}")
                    )
                )
            )
            .withExpressions(mapOf("srcId" to func))
            .withGroupBy(listOf("srcId"))
            .build()

        val queryRes = dataService.findRaw(
            query,
            DbFindPage(0, 10_000),
            false
        )

        return queryRes.entities
            .asSequence()
            .map { it["srcId"].toString() }
            .filter { it.isNotBlank() }
            .toSet()
    }

    fun removeAssocs(sourceId: Long, force: Boolean) {
        deleteByPredicate(Predicates.eq(DbAssocEntity.SOURCE_ID, sourceId), force)
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

        val existentAssocs = dataService.findAll(createPredicateForIds(targetIds))

        if (existentAssocs.isEmpty()) {
            return emptyList()
        }

        val targetsToRemove = existentAssocs.map { it.targetId }
        deleteByPredicate(createPredicateForIds(targetsToRemove), force)
        return targetsToRemove
    }

    private fun deleteByPredicate(predicate: Predicate, force: Boolean) {
        if (force) {
            dataService.delete(predicate)
        } else {
            forEachAssocEntity(predicate, 100) { entities ->
                deletedDataService.save(
                    entities.map {
                        val entity = it.copy()
                        entity.id = DbAssocEntity.NEW_REC_ID
                        entity
                    }
                )
                dataService.delete(entities)
                false
            }
        }
    }

    fun forEachAssoc(predicate: Predicate, action: (List<DbAssocDto>) -> Boolean) {
        forEachAssocEntity(predicate, 100) { action(mapToDto(it)) }
    }

    fun forEachAssoc(predicate: Predicate, batchSize: Int, action: (List<DbAssocDto>) -> Boolean) {
        forEachAssocEntity(predicate, batchSize) { action(mapToDto(it)) }
    }

    private fun forEachAssocEntity(predicate: Predicate, batchSize: Int, action: (List<DbAssocEntity>) -> Boolean) {

        val sort = listOf(DbFindSort(DbAssocEntity.ID, true))
        val page = DbFindPage(0, batchSize)

        var searchRes = dataService.find(predicate, sort, page).entities
        while (searchRes.isNotEmpty()) {
            if (action.invoke(searchRes)) {
                break
            }
            searchRes = dataService.find(
                Predicates.and(
                    predicate,
                    Predicates.gt(DbAssocEntity.ID, searchRes.last().id)
                ),
                sort,
                page
            ).entities
        }
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
        val attId = if (attribute.isEmpty()) {
            null
        } else {
            getIdForAtt(attribute)
        }
        return getSourceAssocs(targetId, attId, page)
    }

    fun getSourceAssocs(targetId: Long, attribute: Long?, page: DbFindPage): DbFindRes<DbAssocDto> {

        if (targetId == -1L || attribute == -1L) {
            return DbFindRes.empty()
        }

        val conditions = mutableListOf(
            Predicates.eq(DbAssocEntity.TARGET_ID, targetId)
        )
        if (attribute != null) {
            conditions.add(Predicates.eq(DbAssocEntity.ATTRIBUTE, attribute))
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
            deletedDataService.runMigrations(mock = false, diff = true)
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
