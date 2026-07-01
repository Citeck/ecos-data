package ru.citeck.ecos.data.sql.inmem.query

import ru.citeck.ecos.data.sql.context.DbTableContext
import ru.citeck.ecos.data.sql.dto.DbColumnDef
import ru.citeck.ecos.data.sql.inmem.datasource.InMemDataSource
import ru.citeck.ecos.data.sql.inmem.store.InMemTable
import ru.citeck.ecos.data.sql.records.assocs.DbAssocEntity
import ru.citeck.ecos.data.sql.repo.entity.DbEntity
import ru.citeck.ecos.data.sql.repo.find.DbFindPage
import ru.citeck.ecos.data.sql.repo.find.DbFindQuery
import ru.citeck.ecos.data.sql.repo.find.DbFindRes
import ru.citeck.ecos.data.sql.service.assocs.AssocJoinWithPredicate
import ru.citeck.ecos.data.sql.service.assocs.AssocTableJoin
import ru.citeck.ecos.records2.predicate.model.ValuePredicate

/**
 * In-memory query engine that mirrors the observable behaviour of
 * [ru.citeck.ecos.data.sql.pg.DbEntityRepoPg.find].
 *
 * Unlike a thin `PredicateService` filter, this engine honours the FULL [DbFindQuery]:
 * association table joins, association joins with predicate, association select joins
 * (`assocAtt.targetAtt`), computed SQL expressions, grouping/aggregation and PG NULL/empty
 * semantics. The `ed_associations` join table and the joined target tables are read from the
 * SAME [ru.citeck.ecos.data.sql.inmem.store.InMemStore] (associations reference records by
 * [DbEntity.REF_ID], exactly as in PG), so the join semantics are reproduced without any SQL.
 *
 * The unit of work is a per-row "evaluation context" ([RowEvalContext]) that knows how to resolve
 * any attribute the query can reference (base column, `assocSelectJoin` column, expression alias)
 * and how to test the EXISTS-style association predicates. The predicate, sort, groupBy and result
 * shaping are all expressed against those contexts so they match the SQL the PG repo would emit.
 */
class InMemQueryEngine {

    fun find(
        context: DbTableContext,
        query: DbFindQuery,
        page: DbFindPage,
        withTotalCount: Boolean
    ): DbFindRes<Map<String, Any?>> {

        val columns = context.getColumns()
        if (columns.isEmpty()) {
            return DbFindRes(emptyList(), 0)
        }

        val ds = dataSource(context)
        val table = ds.getStore().getTable(context.getTableRef())
            ?: return DbFindRes(emptyList(), 0)

        val permsFilter = PermsFilter.create(context, query)
        if (permsFilter.alwaysEmpty) {
            return DbFindRes(emptyList(), 0)
        }

        val evalCtx = QueryEvalContext(ds, context, query)

        // 1. filter
        val matched = ArrayList<RowEvalContext>()
        for (row in table.getRows()) {
            val rowCtx = RowEvalContext(row, context, evalCtx)
            if (!permsFilter.test(row)) {
                continue
            }
            if (PredicateEvaluator.eval(query.predicate, rowCtx)) {
                matched.add(rowCtx)
            }
        }

        // 2. group (or pass-through) -> result row contexts
        val resultRows: List<ResultRow> = if (hasGrouping(query)) {
            GroupingProcessor(query, evalCtx).process(matched)
        } else {
            matched.map { PlainResultRow(it, query, evalCtx) }
        }

        // 3. sort
        val sorted = RowSorter.sort(resultRows, query.sortBy, evalCtx)

        // 4. page
        val totalCount = sorted.size.toLong()
        val skip = page.skipCount
        val max = page.maxItems

        if (max == 0) {
            return DbFindRes(emptyList(), if (withTotalCount) totalCount else 0)
        }

        val fromIdx = skip.coerceIn(0, sorted.size)
        val toIdx = if (max < 0) sorted.size else (fromIdx + max).coerceAtMost(sorted.size)
        val pageRows = sorted.subList(fromIdx, toIdx)

        val entities = pageRows.map { it.toResultMap() }

        val resultTotal = if (!withTotalCount || max < 0 || max > entities.size) {
            entities.size.toLong() + skip
        } else {
            totalCount
        }
        return DbFindRes(entities, resultTotal)
    }

    private fun hasGrouping(query: DbFindQuery): Boolean {
        return query.groupBy.isNotEmpty()
    }

    private fun dataSource(context: DbTableContext): InMemDataSource {
        val dataSource = context.getDataSource()
        return dataSource as? InMemDataSource
            ?: error("InMemQueryEngine requires an InMemDataSource, but got: ${dataSource::class}")
    }
}

/**
 * Per-query shared resolution helpers: maps of assoc joins, the assoc table reader, the assoc
 * select-join target tables and expression definitions. Stateless w.r.t. a single source row.
 */
class QueryEvalContext(
    val dataSource: InMemDataSource,
    val context: DbTableContext,
    val query: DbFindQuery
) {

    val assocTableJoins: Map<String, AssocTableJoin> = query.assocTableJoins.associateBy { it.attribute }
    val assocJoinsWithPredicate: Map<String, AssocJoinWithPredicate> =
        query.assocJoinsWithPredicate.associateBy { it.attribute }
    val assocSelectJoins: Map<String, DbTableContext> = query.assocSelectJoins
    val rawTableJoins = query.rawTableJoins
    val expressions = query.expressions

    private val assocTableCache = HashMap<String, InMemTable?>()

    /** The `ed_associations` table for the schema of [tableRef]. */
    fun getAssocTable(tableContext: DbTableContext): InMemTable? {
        val ref = tableContext.getTableRef().withTable(DbAssocEntity.MAIN_TABLE)
        return assocTableCache.getOrPut(ref.fullName) {
            dataSource.getStore().getTable(ref)
        }
    }

    fun getTable(tableContext: DbTableContext): InMemTable? {
        return dataSource.getStore().getTable(tableContext.getTableRef())
    }
}

/**
 * A single source row plus everything needed to resolve any attribute the query may reference.
 * `assocAtt.targetAtt` style names are resolved through [QueryEvalContext.assocSelectJoins]:
 * the target row whose [DbEntity.REF_ID] equals the source row's `assocAtt` column.
 */
class RowEvalContext(
    val row: Map<String, Any?>,
    val tableContext: DbTableContext,
    val queryCtx: QueryEvalContext
) {

    private val resolvedJoinTargets = HashMap<String, Map<String, Any?>?>()

    fun getColumn(name: String): DbColumnDef? = tableContext.getColumnByName(name)

    /**
     * Resolve a "select" attribute value the way PG's SELECT would (base column, joined assoc
     * column or computed expression alias). Returns the raw stored value (already converted to the
     * column's common JVM type by the repo on write).
     */
    fun resolveValue(name: String): Any? {
        if (queryCtx.expressions.containsKey(name)) {
            return ExpressionEvaluator.eval(queryCtx.expressions.getValue(name), this)
        }
        val dotIdx = name.indexOf('.')
        if (dotIdx > 0) {
            val srcAtt = name.substring(0, dotIdx)
            val tgtAtt = name.substring(dotIdx + 1)
            if (queryCtx.rawTableJoins.containsKey(srcAtt)) {
                return resolveRawJoinTarget(srcAtt)?.get(tgtAtt)
            }
            val targetRow = resolveAssocSelectJoinTarget(srcAtt) ?: return null
            return targetRow[tgtAtt]
        }
        return row[name]
    }

    /**
     * Resolve the row of a raw-joined table (LEFT JOIN ... ON src = alias.col). The join condition
     * is a single EQ predicate: one side is the alias-qualified target column, the other the source
     * column.
     */
    private fun resolveRawJoinTarget(alias: String): Map<String, Any?>? {
        return resolvedJoinTargets.getOrPut("raw:$alias") {
            val join = queryCtx.rawTableJoins[alias] ?: return@getOrPut null
            val on = join.on as? ValuePredicate ?: return@getOrPut null
            val attribute = on.getAttribute()
            val valueText = on.getValue().asText()
            val prefix = "$alias."
            val (srcColumn, tgtColumn) = if (valueText.startsWith(prefix)) {
                attribute to valueText.substring(prefix.length)
            } else if (attribute.startsWith(prefix)) {
                valueText to attribute.substring(prefix.length)
            } else {
                return@getOrPut null
            }
            val joinKey = row[srcColumn] ?: return@getOrPut null
            val targetTable = queryCtx.getTable(join.table) ?: return@getOrPut null
            targetTable.getRows().firstOrNull { numbersEqual(it[tgtColumn], joinKey) }
        }
    }

    private fun numbersEqual(a: Any?, b: Any?): Boolean {
        if (a is Number && b is Number) {
            return a.toLong() == b.toLong()
        }
        return a == b
    }

    /** The single-valued assoc-select-join target row referenced by [srcAtt]. */
    private fun resolveAssocSelectJoinTarget(srcAtt: String): Map<String, Any?>? {
        return resolvedJoinTargets.getOrPut(srcAtt) {
            val targetCtx = queryCtx.assocSelectJoins[srcAtt] ?: return@getOrPut null
            val srcRefId = (row[srcAtt] as? Number)?.toLong() ?: return@getOrPut null
            val targetTable = queryCtx.getTable(targetCtx) ?: return@getOrPut null
            targetTable.getRows().firstOrNull { (it[DbEntity.REF_ID] as? Number)?.toLong() == srcRefId }
        }
    }

    /** Column definition for a (possibly joined) select attribute. */
    fun resolveColumnDef(name: String): DbColumnDef? {
        if (queryCtx.expressions.containsKey(name)) {
            return null
        }
        val dotIdx = name.indexOf('.')
        if (dotIdx > 0) {
            val srcAtt = name.substring(0, dotIdx)
            val tgtAtt = name.substring(dotIdx + 1)
            return queryCtx.assocSelectJoins[srcAtt]?.getColumnByName(tgtAtt)
        }
        return tableContext.getColumnByName(name)
    }

    private val refId: Long?
        get() = (row[DbEntity.REF_ID] as? Number)?.toLong()

    /**
     * EXISTS over `ed_associations`: there is an association of [attId] linking this row (as
     * source when [target], else as target) to any of [values] (other side ref-ids).
     */
    fun existsAssoc(attId: Long, target: Boolean, values: Collection<Long>): Boolean {
        if (values.isEmpty()) {
            return false
        }
        val myRefId = refId ?: return false
        val assocTable = queryCtx.getAssocTable(tableContext) ?: return false
        val valueSet = values.toHashSet()
        return assocTable.getRows().any { assoc ->
            val attrId = (assoc[DbAssocEntity.ATTRIBUTE] as? Number)?.toLong()
            if (attrId != attId) {
                return@any false
            }
            val src = (assoc[DbAssocEntity.SOURCE_ID] as? Number)?.toLong()
            val tgt = (assoc[DbAssocEntity.TARGET_ID] as? Number)?.toLong()
            if (target) {
                src == myRefId && tgt != null && valueSet.contains(tgt)
            } else {
                tgt == myRefId && src != null && valueSet.contains(src)
            }
        }
    }

    /**
     * EXISTS over an assoc-join-with-predicate: a target row reachable from this row (via the
     * `ed_associations` table for multi-assocs, or the single src column otherwise) that satisfies
     * the join's inner predicate.
     */
    fun existsAssocJoinWithPredicate(join: AssocJoinWithPredicate): Boolean {
        val targetTable = queryCtx.getTable(join.tableContext) ?: return false
        val targetRefIds: Set<Long> = if (join.multipleAssoc) {
            val myRefId = refId ?: return false
            val assocTable = queryCtx.getAssocTable(tableContext) ?: return false
            assocTable.getRows().asSequence()
                .filter { (it[DbAssocEntity.ATTRIBUTE] as? Number)?.toLong() == join.srcAttributeId }
                .filter { (it[DbAssocEntity.SOURCE_ID] as? Number)?.toLong() == myRefId }
                .mapNotNull { (it[DbAssocEntity.TARGET_ID] as? Number)?.toLong() }
                .toHashSet()
        } else {
            val srcRefId = (row[join.srcColumn] as? Number)?.toLong() ?: return false
            setOf(srcRefId)
        }
        if (targetRefIds.isEmpty()) {
            return false
        }
        val innerCtx = QueryEvalContext(
            queryCtx.dataSource,
            join.tableContext,
            DbFindQuery.create {
                withAssocTableJoins(join.assocTableJoins)
                withAssocJoinWithPredicates(join.assocJoinsWithPredicate)
            }
        )
        return targetTable.getRows().any { targetRow ->
            val tgtRefId = (targetRow[DbEntity.REF_ID] as? Number)?.toLong()
            if (tgtRefId == null || !targetRefIds.contains(tgtRefId)) {
                return@any false
            }
            val targetCtx = RowEvalContext(targetRow, join.tableContext, innerCtx)
            PredicateEvaluator.eval(join.predicate, targetCtx)
        }
    }
}

/** Wraps a sortable / projectable result row (a single matched row, or an aggregated group). */
sealed interface ResultRow {
    fun resolveSortValue(att: String): Any?
    fun toResultMap(): Map<String, Any?>
}

/** Non-grouped row: SELECT base columns + every expression alias (PG `convertRowToMap`). */
class PlainResultRow(
    val rowCtx: RowEvalContext,
    private val query: DbFindQuery,
    private val queryCtx: QueryEvalContext
) : ResultRow {

    override fun resolveSortValue(att: String): Any? {
        if (att == DbEntity.ID) {
            return rowCtx.row[DbEntity.ID]
        }
        return rowCtx.resolveValue(att)
    }

    override fun toResultMap(): Map<String, Any?> {
        val result = LinkedHashMap<String, Any?>()
        for (column in rowCtx.tableContext.getColumns()) {
            result[column.name] = rowCtx.row[column.name]
        }
        for ((alias, _) in query.expressions) {
            result[alias] = rowCtx.resolveValue(alias)
        }
        return result
    }
}
