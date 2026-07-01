package ru.citeck.ecos.data.sql.inmem.query

import ru.citeck.ecos.data.sql.records.assocs.DbAssocEntity
import ru.citeck.ecos.data.sql.records.dao.atts.DbExpressionAttsContext
import ru.citeck.ecos.data.sql.repo.entity.DbEntity
import ru.citeck.ecos.data.sql.service.expression.token.CaseToken
import ru.citeck.ecos.data.sql.service.expression.token.CastToken
import ru.citeck.ecos.data.sql.service.expression.token.ColumnToken
import ru.citeck.ecos.data.sql.service.expression.token.ExpressionToken
import ru.citeck.ecos.data.sql.service.expression.token.FunctionToken
import ru.citeck.ecos.data.sql.service.expression.token.GroupToken
import ru.citeck.ecos.data.sql.service.expression.token.NullConditionToken
import ru.citeck.ecos.data.sql.service.expression.token.NullToken
import ru.citeck.ecos.data.sql.service.expression.token.OperatorToken
import ru.citeck.ecos.data.sql.service.expression.token.ScalarToken
import kotlin.math.abs
import kotlin.math.ceil
import kotlin.math.floor
import kotlin.math.sign
import kotlin.math.sqrt

/**
 * Signals that an expression uses a SQL feature the in-memory engine deliberately does not emulate
 * (PG-internal text formatting / timezone-sensitive date math). The engine falls back to `null` for
 * the per-row value, which is the closest faithful behaviour to "this requires a real SQL engine".
 */
class UnsupportedExpressionException(message: String) : RuntimeException(message)

/**
 * Evaluates the supported subset of ecos-data SQL [ExpressionToken]s over a single [RowEvalContext].
 *
 * Supported: arithmetic over the operator tokens, the numeric functions
 * (floor/ceil/round/abs/sign/sqrt/...), the string functions (length/upper/lower/concat/substring
 * /replace/...), coalesce/nullif/greatest/least, CASE, and the scalar/column/group/cast tokens.
 * Aggregation functions (count/sum/min/max/avg) are handled by [GroupingProcessor], not here, except
 * the per-row assoc-aggregation subquery ([DbExpressionAttsContext.AssocAggregationSelectExpression]).
 *
 * Genuinely DB-internal expressions (PG `to_char` format strings, `date_part('epoch')`, `date_trunc`
 * timezone semantics, `interval` arithmetic) throw [UnsupportedExpressionException] -> resolve to
 * null; the corresponding contract tests stay documented skips on the in-mem backend.
 */
object ExpressionEvaluator {

    fun eval(token: ExpressionToken, ctx: RowEvalContext): Any? {
        return try {
            evalToken(token, ctx)
        } catch (e: UnsupportedExpressionException) {
            null
        }
    }

    private fun evalToken(token: ExpressionToken, ctx: RowEvalContext): Any? {
        return when (token) {
            is ScalarToken -> token.value
            is NullToken -> null
            is ColumnToken -> resolveColumn(token.name, ctx)
            is GroupToken -> evalGroup(token, ctx)
            is FunctionToken -> evalFunction(token, ctx)
            is CaseToken -> evalCase(token, ctx)
            is CastToken -> evalCast(token, ctx)
            is DbExpressionAttsContext.AssocAggregationSelectExpression -> evalAssocAgg(token, ctx)
            else -> throw UnsupportedExpressionException("Unsupported token: ${token::class}")
        }
    }

    private fun resolveColumn(name: String, ctx: RowEvalContext): Any? {
        val dotIdx = name.indexOf('.')
        if (dotIdx > 0) {
            return ctx.resolveValue(name)
        }
        return ctx.row[name]
    }

    /** A parenthesised group: single token, an `operand IS [NOT] NULL`, or an arithmetic chain. */
    private fun evalGroup(token: GroupToken, ctx: RowEvalContext): Any? {
        val tokens = token.tokens
        if (tokens.size == 1) {
            return evalToken(tokens[0], ctx)
        }
        // "<operand> IS [NOT] NULL"
        if (tokens.size == 2 && tokens[1] is NullConditionToken) {
            val operand = evalToken(tokens[0], ctx)
            val isNull = (tokens[1] as NullConditionToken).isNull
            return if (isNull) operand == null else operand != null
        }
        return evalArithmetic(tokens, ctx)
    }

    /** Left-to-right evaluation honouring multiplicative precedence over additive. */
    private fun evalArithmetic(tokens: List<ExpressionToken>, ctx: RowEvalContext): Any? {
        // First pass: resolve operands, keep operators.
        val items = ArrayList<Any?>()
        for (t in tokens) {
            if (t is OperatorToken) {
                items.add(t.type)
            } else {
                items.add(evalToken(t, ctx))
            }
        }
        // Multiplicative pass.
        val reduced = ArrayList<Any?>()
        var i = 0
        while (i < items.size) {
            val item = items[i]
            if (item is OperatorToken.Type &&
                (item == OperatorToken.Type.MULTIPLY || item == OperatorToken.Type.DIV || item == OperatorToken.Type.REM)
            ) {
                val left = reduced.removeAt(reduced.size - 1)
                val right = items[i + 1]
                reduced.add(applyOp(item, left, right))
                i += 2
            } else {
                reduced.add(item)
                i++
            }
        }
        // Additive pass.
        var acc = reduced[0]
        var j = 1
        while (j < reduced.size) {
            val op = reduced[j] as OperatorToken.Type
            val right = reduced[j + 1]
            acc = applyOp(op, acc, right)
            j += 2
        }
        return acc
    }

    private val ARITHMETIC_OPS = setOf(
        OperatorToken.Type.PLUS,
        OperatorToken.Type.MINUS,
        OperatorToken.Type.MULTIPLY,
        OperatorToken.Type.DIV,
        OperatorToken.Type.REM
    )

    private fun applyOp(op: OperatorToken.Type, left: Any?, right: Any?): Any? {
        if (op in ARITHMETIC_OPS) {
            val l = toDouble(left) ?: return null
            val r = toDouble(right) ?: return null
            return when (op) {
                OperatorToken.Type.PLUS -> l + r
                OperatorToken.Type.MINUS -> l - r
                OperatorToken.Type.MULTIPLY -> l * r
                OperatorToken.Type.DIV -> if (r == 0.0) null else l / r
                OperatorToken.Type.REM -> if (r == 0.0) null else l % r
                else -> null
            }
        }
        return applyComparison(op, left, right)
    }

    /** Relational/equality/logical operators. NULL operand -> SQL "unknown" (null). */
    private fun applyComparison(op: OperatorToken.Type, left: Any?, right: Any?): Any? {
        if (op == OperatorToken.Type.AND) {
            return toBoolNullable(left) == true && toBoolNullable(right) == true
        }
        if (op == OperatorToken.Type.OR) {
            return toBoolNullable(left) == true || toBoolNullable(right) == true
        }
        if (left == null || right == null) {
            return null
        }
        val cmp = if (left is Number && right is Number) {
            left.toDouble().compareTo(right.toDouble())
        } else {
            left.toString().compareTo(right.toString())
        }
        return when (op) {
            OperatorToken.Type.EQUAL -> cmp == 0
            OperatorToken.Type.NOT_EQUAL -> cmp != 0
            OperatorToken.Type.GREATER -> cmp > 0
            OperatorToken.Type.LESS -> cmp < 0
            OperatorToken.Type.GREATER_OR_EQUAL -> cmp >= 0
            OperatorToken.Type.LESS_OR_EQUAL -> cmp <= 0
            else -> throw UnsupportedExpressionException("Unsupported operator: $op")
        }
    }

    private fun toBoolNullable(value: Any?): Boolean? {
        return when (value) {
            null -> null
            is Boolean -> value
            is Number -> value.toDouble() != 0.0
            else -> null
        }
    }

    private fun evalCase(token: CaseToken, ctx: RowEvalContext): Any? {
        for (branch in token.branches) {
            if (toBoolean(evalToken(branch.condition, ctx))) {
                return evalToken(branch.thenResult, ctx)
            }
        }
        return token.orElse?.let { evalToken(it, ctx) }
    }

    private fun evalCast(token: CastToken, ctx: RowEvalContext): Any? {
        val value = evalToken(token.token, ctx) ?: return null
        return when (token.castTo.lowercase()) {
            "int", "integer", "bigint" -> toDouble(value)?.toLong()
            "double", "numeric", "decimal", "float", "real" -> toDouble(value)
            "text", "varchar" -> value.toString()
            else -> value
        }
    }

    private fun evalFunction(token: FunctionToken, ctx: RowEvalContext): Any? {
        val name = token.name
        if (FunctionToken.AGG_FUNCTIONS.contains(name)) {
            // aggregation outside grouping (single row): handled here for non-grouped expr atts.
            return evalAggregateAsScalar(token, ctx)
        }
        val args = token.args.map { evalToken(it, ctx) }
        return when (name) {
            "floor" -> toDouble(args[0])?.let { floor(it) }
            "ceil", "ceiling" -> toDouble(args[0])?.let { ceil(it) }
            "round" -> toDouble(args[0])?.let { Math.round(it).toDouble() }
            "abs" -> toDouble(args[0])?.let { abs(it) }
            "sign" -> toDouble(args[0])?.let { sign(it) }
            "sqrt" -> toDouble(args[0])?.let { sqrt(it) }
            "trunc" -> toDouble(args[0])?.let { it.toLong().toDouble() }
            "mod" -> {
                val a = toDouble(args[0])
                val b = toDouble(args[1])
                if (a == null || b == null || b == 0.0) null else a % b
            }
            "power" -> {
                val a = toDouble(args[0])
                val b = toDouble(args[1])
                if (a == null || b == null) null else Math.pow(a, b)
            }
            "length", "char_length", "character_length" -> args[0]?.toString()?.length
            "upper" -> args[0]?.toString()?.uppercase()
            "lower" -> args[0]?.toString()?.lowercase()
            "trim", "btrim" -> args[0]?.toString()?.trim()
            "ltrim" -> args[0]?.toString()?.trimStart()
            "rtrim" -> args[0]?.toString()?.trimEnd()
            "initcap" -> args[0]?.toString()?.split(" ")?.joinToString(" ") { w ->
                w.replaceFirstChar { it.uppercase() }
            }
            "concat" -> args.joinToString("") { sqlText(it) }
            "concat_ws" -> {
                val sep = args[0]?.toString() ?: ""
                args.drop(1).filterNotNull().joinToString(sep) { sqlText(it) }
            }
            "replace" -> {
                val src = args[0]?.toString() ?: return null
                src.replace(args[1]?.toString() ?: "", args[2]?.toString() ?: "")
            }
            "substring" -> evalSubstring(args)
            "substringBefore" -> {
                // PG form: substring(s, 0, position('@' IN s)); position=0 when not found, so a
                // missing delimiter yields the empty string, not the whole string.
                val src = args[0]?.toString() ?: return null
                val delim = args[1]?.toString() ?: ""
                val idx = src.indexOf(delim)
                if (idx < 0) "" else src.substring(0, idx)
            }
            "repeat" -> {
                val src = args[0]?.toString() ?: return null
                src.repeat(toDouble(args[1])?.toInt() ?: 0)
            }
            "position", "strpos" -> {
                val haystack = args[0]?.toString() ?: return null
                (haystack.indexOf(args[1]?.toString() ?: "") + 1)
            }
            "coalesce" -> args.firstOrNull { it != null }
            "nullif" -> if (args[0] == args[1]) null else args[0]
            "greatest" -> args.filterNotNull().maxByOrNull { toDouble(it) ?: Double.MIN_VALUE }
            "least" -> args.filterNotNull().minByOrNull { toDouble(it) ?: Double.MAX_VALUE }
            else -> throw UnsupportedExpressionException("Unsupported function: $name")
        }
    }

    private fun evalSubstring(args: List<Any?>): Any? {
        val src = args[0]?.toString() ?: return null
        val from = (toDouble(args.getOrNull(1)) ?: 1.0).toInt()
        val startIdx = (from - 1).coerceAtLeast(0).coerceAtMost(src.length)
        if (args.size <= 2) {
            return src.substring(startIdx)
        }
        val forLen = (toDouble(args[2]) ?: 0.0).toInt()
        val endIdx = (startIdx + forLen).coerceAtMost(src.length)
        return src.substring(startIdx, endIdx.coerceAtLeast(startIdx))
    }

    /** A bare aggregate over a single non-grouped row collapses to that one row's value. */
    private fun evalAggregateAsScalar(token: FunctionToken, ctx: RowEvalContext): Any? {
        val arg = token.args.firstOrNull()
        return when (token.name) {
            "count" -> {
                if (arg == null || arg.toString() == "*") {
                    1L
                } else {
                    if (evalToken(arg, ctx) != null) 1L else 0L
                }
            }
            "sum", "max", "min", "avg" -> toDouble(arg?.let { evalToken(it, ctx) })
            else -> null
        }
    }

    /** Per-row aggregation over all assoc targets (the AssocAggregationSelectExpression subquery). */
    private fun evalAssocAgg(
        token: DbExpressionAttsContext.AssocAggregationSelectExpression,
        ctx: RowEvalContext
    ): Any? {
        val myRefId = (ctx.row[DbEntity.REF_ID] as? Number)?.toLong() ?: return null
        val assocTable = ctx.queryCtx.getAssocTable(ctx.tableContext) ?: return null
        val targetTable = ctx.queryCtx.getTable(token.tableContext) ?: return 0.0
        val targetRefIds = assocTable.getRows().asSequence()
            .filter { (it[DbAssocEntity.ATTRIBUTE] as? Number)?.toLong() == token.attributeId }
            .filter { (it[DbAssocEntity.SOURCE_ID] as? Number)?.toLong() == myRefId }
            .mapNotNull { (it[DbAssocEntity.TARGET_ID] as? Number)?.toLong() }
            .toHashSet()
        val targetRows = targetTable.getRows().filter {
            val ref = (it[DbEntity.REF_ID] as? Number)?.toLong()
            ref != null && targetRefIds.contains(ref)
        }
        val innerCtx = QueryEvalContext(ctx.queryCtx.dataSource, token.tableContext, ctx.queryCtx.query)
        val aggFn = token.expression as? FunctionToken
            ?: throw UnsupportedExpressionException("AssocAgg expects a function")
        val values = targetRows.mapNotNull { tRow ->
            val tCtx = RowEvalContext(tRow, token.tableContext, innerCtx)
            aggFn.args.firstOrNull()?.let { toDouble(evalToken(it, tCtx)) }
        }
        return when (aggFn.name) {
            "sum" -> values.sum()
            "count" -> values.size.toLong()
            "max" -> values.maxOrNull()
            "min" -> values.minOrNull()
            "avg" -> if (values.isEmpty()) null else values.average()
            else -> null
        }
    }

    /** Render a value as PG would inside string functions: whole doubles drop the ".0". */
    private fun sqlText(value: Any?): String {
        if (value == null) {
            return ""
        }
        if (value is Double && value == floor(value) && !value.isInfinite()) {
            return value.toLong().toString()
        }
        return value.toString()
    }

    fun toDouble(value: Any?): Double? {
        return when (value) {
            null -> null
            is Number -> value.toDouble()
            is Boolean -> if (value) 1.0 else 0.0
            is String -> value.toDoubleOrNull()
            else -> null
        }
    }

    private fun toBoolean(value: Any?): Boolean {
        return when (value) {
            is Boolean -> value
            is Number -> value.toDouble() != 0.0
            else -> false
        }
    }
}
