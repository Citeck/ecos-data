package ru.citeck.ecos.data.sql.inmem.query

import ru.citeck.ecos.data.sql.repo.find.DbFindQuery
import ru.citeck.ecos.data.sql.service.expression.token.CaseToken
import ru.citeck.ecos.data.sql.service.expression.token.ColumnToken
import ru.citeck.ecos.data.sql.service.expression.token.ExpressionToken
import ru.citeck.ecos.data.sql.service.expression.token.FunctionToken
import ru.citeck.ecos.data.sql.service.expression.token.GroupToken

/**
 * Reproduces PG's GROUP BY + aggregate SELECT semantics in memory.
 *
 * Rows are bucketed by the resolved values of the groupBy attributes (each being a base/joined
 * column or an expression alias). `groupBy = ["*"]` collapses everything into a single bucket. The
 * result map of a group contains the groupBy keys (unless they are also computed expressions) and
 * every "select expression" alias - aggregates evaluated over the bucket, non-aggregate expressions
 * evaluated against the first row of the bucket - exactly like
 * [ru.citeck.ecos.data.sql.pg.DbEntityRepoPg.convertRowToMap] for the grouped branch.
 */
class GroupingProcessor(
    private val query: DbFindQuery,
    private val queryCtx: QueryEvalContext
) {

    fun process(rows: List<RowEvalContext>): List<ResultRow> {
        val groupBy = query.groupBy
        val isGroupAll = groupBy.size == 1 && groupBy[0] == "*"

        // Which expression aliases are SELECTed in the grouped result (PG removes non-grouped,
        // non-aggregate plain expressions from the SELECT).
        val selectExpressions = computeSelectExpressions(groupBy)

        if (isGroupAll || groupBy.isEmpty()) {
            if (rows.isEmpty()) {
                return emptyList()
            }
            return listOf(GroupResultRow(emptyList(), rows, query, queryCtx, selectExpressions, groupBy))
        }

        val buckets = LinkedHashMap<List<Any?>, MutableList<RowEvalContext>>()
        for (row in rows) {
            val key = groupBy.map { row.resolveValue(it) }
            buckets.getOrPut(key) { ArrayList() }.add(row)
        }
        return buckets.map { (key, bucketRows) ->
            GroupResultRow(key, bucketRows, query, queryCtx, selectExpressions, groupBy)
        }
    }

    private fun computeSelectExpressions(groupBy: List<String>): Set<String> {
        val result = LinkedHashSet(query.expressions.keys)
        val groupByExpressions = groupBy.mapTo(HashSet<ExpressionToken>()) {
            query.expressions[it] ?: ColumnToken(it)
        }
        query.expressions.forEach { (alias, token) ->
            if (!groupBy.contains(alias) && !isValidForGroupedSelect(groupByExpressions, token)) {
                result.remove(alias)
            }
        }
        return result
    }

    private fun isValidForGroupedSelect(groupBy: Set<ExpressionToken>, token: ExpressionToken?): Boolean {
        token ?: return true
        if (groupBy.contains(token)) {
            return true
        }
        return when (token) {
            is ColumnToken -> groupBy.contains(token)
            is FunctionToken -> if (token.isAggregationFunc()) {
                true
            } else {
                token.args.all { isValidForGroupedSelect(groupBy, it) }
            }
            is GroupToken -> token.tokens.all { isValidForGroupedSelect(groupBy, it) }
            is CaseToken -> token.branches.all {
                isValidForGroupedSelect(groupBy, it.condition) &&
                    isValidForGroupedSelect(groupBy, it.thenResult)
            } && isValidForGroupedSelect(groupBy, token.orElse)
            else -> true
        }
    }
}

/** An aggregated group of source rows. */
class GroupResultRow(
    private val groupKey: List<Any?>,
    private val rows: List<RowEvalContext>,
    private val query: DbFindQuery,
    private val queryCtx: QueryEvalContext,
    private val selectExpressions: Set<String>,
    private val groupBy: List<String>
) : ResultRow {

    override fun resolveSortValue(att: String): Any? {
        // sort by a groupBy column or by an aggregate/expression alias
        if (query.expressions.containsKey(att)) {
            return evalExpressionAlias(att)
        }
        val idx = groupBy.indexOf(att)
        if (idx >= 0) {
            return groupKey[idx]
        }
        // sort by an aggregate not part of groupBy/expressions: evaluate over the bucket
        return null
    }

    override fun toResultMap(): Map<String, Any?> {
        val result = LinkedHashMap<String, Any?>()
        groupBy.forEachIndexed { idx, att ->
            if (att != "*" && !selectExpressions.contains(att) && !query.expressions.containsKey(att)) {
                result[att] = groupKey[idx]
            }
        }
        for (alias in selectExpressions) {
            result[alias] = evalExpressionAlias(alias)
        }
        return result
    }

    private fun evalExpressionAlias(alias: String): Any? {
        val token = query.expressions[alias] ?: return rows.firstOrNull()?.resolveValue(alias)
        if (token is FunctionToken && token.isAggregationFunc()) {
            return AggregateEvaluator.eval(token, rows)
        }
        // non-aggregate expression in a grouped query: evaluate against the first row of the bucket
        return rows.firstOrNull()?.let { ExpressionEvaluator.eval(token, it) }
    }
}

/**
 * Evaluates a top-level aggregate function over a bucket of rows, preserving numeric type the way
 * PostgreSQL does: `max`/`min` return the element value (so an int column stays int), `sum` over
 * integral values stays integral (Long), `avg` is always double, `count` is a Long. This matters
 * because the entity mapper converts the result back to the column type and only knows
 * Long->Int/Long, not Double->Int.
 */
object AggregateEvaluator {

    fun eval(token: FunctionToken, rows: List<RowEvalContext>): Any? {
        val arg = token.args.firstOrNull()
        if (token.name == "count") {
            if (arg == null || isAllFields(arg)) {
                return rows.size.toLong()
            }
            return rows.count { ExpressionEvaluator.eval(arg, it) != null }.toLong()
        }
        val rawValues = rows.mapNotNull { row -> arg?.let { ExpressionEvaluator.eval(it, row) } }
        if (rawValues.isEmpty()) {
            return null
        }
        val doubles = rawValues.map { ExpressionEvaluator.toDouble(it) ?: 0.0 }
        val allIntegral = rawValues.all { it is Int || it is Long }
        return when (token.name) {
            "sum" -> {
                val sum = doubles.sum()
                if (allIntegral) sum.toLong() else sum
            }
            "max" -> rawValues.maxByOrNull { ExpressionEvaluator.toDouble(it) ?: Double.MIN_VALUE }
            "min" -> rawValues.minByOrNull { ExpressionEvaluator.toDouble(it) ?: Double.MAX_VALUE }
            "avg" -> doubles.average()
            else -> null
        }
    }

    private fun isAllFields(token: ExpressionToken): Boolean {
        return token.toString() == "*"
    }
}
