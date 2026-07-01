package ru.citeck.ecos.data.sql.inmem

import io.github.oshai.kotlinlogging.KotlinLogging
import ru.citeck.ecos.context.lib.auth.AuthContext
import ru.citeck.ecos.data.sql.context.DbTableContext
import ru.citeck.ecos.data.sql.dto.DbColumnType
import ru.citeck.ecos.data.sql.inmem.datasource.InMemDataSource
import ru.citeck.ecos.data.sql.inmem.query.InMemQueryEngine
import ru.citeck.ecos.data.sql.inmem.query.PredicateEvaluator
import ru.citeck.ecos.data.sql.inmem.query.QueryEvalContext
import ru.citeck.ecos.data.sql.inmem.query.RowEvalContext
import ru.citeck.ecos.data.sql.inmem.store.InMemTable
import ru.citeck.ecos.data.sql.repo.DbEntityRepo
import ru.citeck.ecos.data.sql.repo.entity.DbEntity
import ru.citeck.ecos.data.sql.repo.find.DbFindPage
import ru.citeck.ecos.data.sql.repo.find.DbFindQuery
import ru.citeck.ecos.data.sql.repo.find.DbFindRes
import ru.citeck.ecos.data.sql.type.DbTypeUtils
import ru.citeck.ecos.data.sql.type.DbTypesConverter
import ru.citeck.ecos.records2.predicate.model.Predicate
import java.util.UUID

/**
 * In-memory [DbEntityRepo].
 *
 * Mirrors the observable behaviour of [ru.citeck.ecos.data.sql.pg.DbEntityRepoPg] but stores rows in
 * the shared [ru.citeck.ecos.data.sql.inmem.store.InMemStore] instead of SQL:
 *
 *  - **save**: splits entities into inserts ([DbEntity.ID] == [DbEntity.NEW_REC_ID]) and updates;
 *    generates a UUID ext-id for blank non-deleted entities; sets [DbEntity.UPD_VERSION] to 0 on
 *    insert and increments it on update with optimistic-lock (concurrent-modification) checking;
 *    assigns auto-incremented long ids; converts every value to the column's common JVM type
 *    (array thereof for multiple columns, except JSON) exactly like the PG repo's read/write path.
 *  - **insertIfNoConflictByExtId**: returns null when a row with the same ext-id already exists,
 *    otherwise inserts and returns the new id.
 *  - **find**: delegates to [InMemQueryEngine], which honours the full [DbFindQuery] (predicate,
 *    sort, paging, association joins, computed expressions, grouping/aggregation and PG NULL/empty
 *    + perms semantics).
 *  - **delete**: by id list, by entity (extracting [DbEntity.ID]) and by predicate (the latter
 *    evaluated with the same [PredicateEvaluator] the engine uses, removing rows by their internal
 *    store key so id-less tables work).
 */
class InMemEntityRepo : DbEntityRepo {

    companion object {
        private val log = KotlinLogging.logger {}
    }

    private val queryEngine = InMemQueryEngine()

    override fun find(
        context: DbTableContext,
        query: DbFindQuery,
        page: DbFindPage,
        withTotalCount: Boolean
    ): DbFindRes<Map<String, Any?>> {
        return queryEngine.find(context, query, page, withTotalCount)
    }

    override fun save(context: DbTableContext, entities: List<Map<String, Any?>>): List<Map<String, Any?>> {
        if (context.getColumns().isEmpty()) {
            error("Columns is empty")
        }
        val ids = saveImpl(context, entities)
        if (!context.hasIdColumn()) {
            return entities
        }
        val table = getTable(context) ?: error("Table is not initialized: ${context.getTableRef()}")
        return ids.map { id ->
            val row = table.getRowById(id)
                ?: error("Entity with id $id doesn't found after updating")
            LinkedHashMap(row)
        }
    }

    private fun saveImpl(context: DbTableContext, entities: List<Map<String, Any?>>): List<Long> {

        checkAuth(context)

        val hasIdColumn = context.hasIdColumn()
        val table = context.getStoreTable()
        val typesConverter = context.getTypesConverter()
        val resultIds = LongArray(entities.size) { DbEntity.NEW_REC_ID }

        for ((entityIdx, entity) in entities.withIndex()) {

            val entityMap = LinkedHashMap(entity)
            val entityId: Long = if (hasIdColumn) {
                val id = entityMap[DbEntity.ID] as? Long ?: error("ID is a mandatory parameter!")
                entityMap.remove(DbEntity.ID)
                id
            } else {
                DbEntity.NEW_REC_ID
            }

            var extId = entityMap[DbEntity.EXT_ID] as? String ?: ""
            val deleted = entityMap[DbEntity.DELETED] as? Boolean ?: false

            if (deleted && extId.isBlank()) {
                continue
            } else if (extId.isBlank() && !deleted) {
                extId = UUID.randomUUID().toString()
                entityMap[DbEntity.EXT_ID] = extId
            }

            if (entityId == DbEntity.NEW_REC_ID) {
                resultIds[entityIdx] = insertOne(context, table, typesConverter, entityMap)
            } else {
                resultIds[entityIdx] = entityId
                updateOne(context, table, typesConverter, entityId, entityMap)
            }
        }

        return resultIds.toList()
    }

    private fun insertOne(
        context: DbTableContext,
        table: InMemTable,
        typesConverter: DbTypesConverter,
        entity: Map<String, Any?>
    ): Long {
        val atts = LinkedHashMap(entity)
        atts[DbEntity.UPD_VERSION] = 0L
        val id = table.nextId()
        val row = convertRow(context, typesConverter, atts)
        if (context.hasIdColumn()) {
            row[DbEntity.ID] = id
        }
        val extId = row[DbEntity.EXT_ID] as? String ?: ""
        table.putRow(id, extId, row)
        return id
    }

    private fun updateOne(
        context: DbTableContext,
        table: InMemTable,
        typesConverter: DbTypesConverter,
        id: Long,
        entity: Map<String, Any?>
    ) {
        val existing = table.getRowById(id)
            ?: error("Entity with id $id doesn't found for update")

        val atts = LinkedHashMap(entity)
        if (context.hasColumn(DbEntity.UPD_VERSION)) {
            val currentVersion = atts[DbEntity.UPD_VERSION] as? Long
                ?: error("Missing attribute: ${DbEntity.UPD_VERSION}")
            val storedVersion = existing[DbEntity.UPD_VERSION] as? Long ?: 0L
            if (storedVersion != currentVersion) {
                error("Concurrent modification of record with id: $id")
            }
            var newVersion = currentVersion + 1
            if (newVersion >= Int.MAX_VALUE) {
                newVersion = 0L
            }
            atts[DbEntity.UPD_VERSION] = newVersion
        }

        val converted = convertRow(context, typesConverter, atts)
        val newRow = LinkedHashMap(existing)
        newRow.putAll(converted)
        newRow[DbEntity.ID] = id
        val extId = newRow[DbEntity.EXT_ID] as? String ?: ""
        table.putRow(id, extId, newRow)
    }

    override fun insertIfNoConflictByExtId(context: DbTableContext, entity: Map<String, Any?>): Long? {

        checkAuth(context)

        val columns = context.getColumns().filter { entity.containsKey(it.name) }
        if (columns.isEmpty()) {
            error(
                "Can't insert entity into ${context.getTableRef().fullName}: " +
                    "no columns matching entity keys. Entity keys: ${entity.keys}, " +
                    "table columns: ${context.getColumns().map { it.name }}"
            )
        }

        val table = context.getStoreTable()
        val extId = entity[DbEntity.EXT_ID] as? String ?: ""
        if (extId.isNotEmpty() && table.getRowByExtId(extId) != null) {
            return null
        }
        if (!context.hasIdColumn()) {
            insertOne(context, table, context.getTypesConverter(), entity)
            return null
        }
        return insertOne(context, table, context.getTypesConverter(), entity)
    }

    override fun delete(context: DbTableContext, entity: Map<String, Any?>) {
        delete(context, listOf(entity[DbEntity.ID] as Long))
    }

    override fun delete(context: DbTableContext, predicate: Predicate) {
        val table = getTable(context) ?: return
        // Evaluate the predicate with the same PG-fidelity engine used by find, and remove matching
        // rows by their internal store key - this works for id-less tables (ed_associations,
        // ed_read_perms) where DbEntity.ID is absent, which the old DbEntity.ID-based path could not.
        val evalCtx = QueryEvalContext(
            ds(context),
            context,
            DbFindQuery.create { withPredicate(predicate) }
        )
        table.removeRowsBy { row ->
            PredicateEvaluator.eval(predicate, RowEvalContext(row, context, evalCtx))
        }
    }

    override fun delete(context: DbTableContext, entities: List<Long>) {
        val table = getTable(context) ?: return
        entities.forEach { table.removeRow(it) }
    }

    /**
     * Convert each attribute to the column's common JVM type, matching DbEntityRepoPg's read/write
     * normalization (array of base type for multiple columns, except JSON). Values without a column
     * are kept as-is (they are dropped from storage but never read back through column access).
     */
    private fun convertRow(
        context: DbTableContext,
        typesConverter: DbTypesConverter,
        atts: Map<String, Any?>
    ): MutableMap<String, Any?> {
        val result = LinkedHashMap<String, Any?>()
        val columnsByName = context.getColumns().associateBy { it.name }
        for ((key, value) in atts) {
            val column = columnsByName[key]
            if (column == null) {
                // not a persisted column - skip (PG would reject it; the mapper never produces such keys)
                continue
            }
            result[key] = if (value == null) {
                null
            } else {
                var expectedType = column.type.type
                if (column.multiple && column.type != DbColumnType.JSON) {
                    expectedType = DbTypeUtils.getArrayType(expectedType)
                }
                typesConverter.convert(value, expectedType)
            }
        }
        return result
    }

    private fun checkAuth(context: DbTableContext) {
        if (isAuthEnabled(context) && AuthContext.getCurrentUser().isEmpty()) {
            error("Current user is empty. Table: ${context.getTableRef()}")
        }
    }

    private fun isAuthEnabled(context: DbTableContext): Boolean {
        // perms-based auth requires perms subquery support which is out of scope for in-mem;
        // mirror PG only for the "no perms column" case (auth disabled)
        return false
    }

    private fun getTable(context: DbTableContext): InMemTable? {
        return ds(context).getStore().getTable(context.getTableRef())
    }

    private fun DbTableContext.getStoreTable(): InMemTable {
        return ds(this).getStore().getOrCreateTable(this.getTableRef())
    }

    private fun ds(context: DbTableContext): InMemDataSource {
        val dataSource = context.getDataSource()
        return dataSource as? InMemDataSource
            ?: error("InMemEntityRepo requires an InMemDataSource, but got: ${dataSource::class}")
    }
}
