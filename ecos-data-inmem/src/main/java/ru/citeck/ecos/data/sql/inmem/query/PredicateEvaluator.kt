package ru.citeck.ecos.data.sql.inmem.query

import ru.citeck.ecos.commons.data.DataValue
import ru.citeck.ecos.data.sql.dto.DbColumnDef
import ru.citeck.ecos.data.sql.dto.DbColumnType
import ru.citeck.ecos.data.sql.records.utils.DbAttValueUtils
import ru.citeck.ecos.records2.predicate.model.AndPredicate
import ru.citeck.ecos.records2.predicate.model.ComposedPredicate
import ru.citeck.ecos.records2.predicate.model.EmptyPredicate
import ru.citeck.ecos.records2.predicate.model.NotPredicate
import ru.citeck.ecos.records2.predicate.model.OrPredicate
import ru.citeck.ecos.records2.predicate.model.Predicate
import ru.citeck.ecos.records2.predicate.model.ValuePredicate
import ru.citeck.ecos.records2.predicate.model.VoidPredicate
import java.time.Instant
import java.time.LocalDate
import java.time.OffsetDateTime
import java.time.ZoneOffset

/**
 * Evaluates an `ecos-records` [Predicate] against a single [RowEvalContext], reproducing the exact
 * semantics of [ru.citeck.ecos.data.sql.pg.DbEntityRepoPg.toSqlCondition] (the SQL the PG repo would
 * generate). In particular it reproduces PG's NULL/empty behaviour, which the generic
 * `DefaultValueComparator` does not:
 *
 *  - `eq(col, null)`  -> `col IS NULL`
 *  - `not(eq(col, v))` for TEXT/INT/DOUBLE/LONG/DATE -> `IS DISTINCT FROM` (NULL rows included)
 *  - `empty(col)` -> NULL (or `''` for text, empty array for multiple) - 0 is NOT empty
 *  - association predicates -> EXISTS over `ed_associations` / the joined target table
 *
 * An "always-true" composed branch and a [VoidPredicate] contribute nothing (return true) exactly
 * like the SQL builder skips them.
 */
object PredicateEvaluator {

    private val DISTINCT_FROM_TYPES = setOf(
        DbColumnType.TEXT,
        DbColumnType.INT,
        DbColumnType.DOUBLE,
        DbColumnType.LONG,
        DbColumnType.DATE
    )

    fun eval(predicate: Predicate, ctx: RowEvalContext): Boolean {
        return when (predicate) {
            is AndPredicate -> predicate.getPredicates().all { eval(it, ctx) }
            is OrPredicate -> {
                // an "empty"/non-contributing branch is skipped in SQL; an OR with only skipped
                // branches contributes nothing (true). Track whether anything was contributed.
                var anyContributed = false
                var anyTrue = false
                for (inner in predicate.getPredicates()) {
                    val contributed = contributes(inner, ctx)
                    if (!contributed) {
                        continue
                    }
                    anyContributed = true
                    if (eval(inner, ctx)) {
                        anyTrue = true
                    }
                }
                if (!anyContributed) true else anyTrue
            }
            is ComposedPredicate -> predicate.getPredicates().all { eval(it, ctx) }
            is NotPredicate -> evalNot(predicate, ctx)
            is ValuePredicate -> evalValue(predicate, ctx)
            is EmptyPredicate -> evalEmpty(predicate, ctx)
            is VoidPredicate -> true
            else -> true
        }
    }

    /** Whether the predicate would produce any SQL (mirrors toSqlCondition returning true/false). */
    private fun contributes(predicate: Predicate, ctx: RowEvalContext): Boolean {
        return when (predicate) {
            is VoidPredicate -> false
            is ComposedPredicate -> predicate.getPredicates().any { contributes(it, ctx) }
            is NotPredicate -> contributes(predicate.getPredicate(), ctx)
            is ValuePredicate -> resolveColumn(predicate.getAttribute(), ctx) != null ||
                ctx.queryCtx.assocTableJoins.containsKey(predicate.getAttribute()) ||
                ctx.queryCtx.assocJoinsWithPredicate.containsKey(predicate.getAttribute()) ||
                ctx.queryCtx.expressions.containsKey(predicate.getAttribute())
            is EmptyPredicate -> ctx.getColumn(predicate.getAttribute()) != null
            else -> true
        }
    }

    private fun evalNot(predicate: NotPredicate, ctx: RowEvalContext): Boolean {
        val inner = predicate.getPredicate()
        if (!contributes(inner, ctx)) {
            // NOT of a non-contributing predicate produces nothing -> contributes false -> true
            return true
        }
        // PG replaces "= ?" with "IS DISTINCT FROM ?" for not(eq(col, nonNull)) on
        // TEXT/INT/DOUBLE/LONG/DATE columns - this INCLUDES rows where the column is NULL.
        if (isDistinctFromCase(inner, ctx)) {
            val vp = inner as ValuePredicate
            val rowValue = ctx.resolveValue(vp.getAttribute())
            return !valueEquals(ctx.resolveColumnDef(vp.getAttribute()), rowValue, vp.getValue())
        }
        // Otherwise plain SQL NOT: three-valued logic - NOT(unknown) is unknown -> row excluded.
        // The inner predicate is "unknown" exactly when its column/expression value is NULL.
        if (isUnknownComparison(inner, ctx)) {
            return false
        }
        return !eval(inner, ctx)
    }

    /** not(eq(col, nonNull)) on a column whose type uses IS DISTINCT FROM in PG. */
    private fun isDistinctFromCase(inner: Predicate, ctx: RowEvalContext): Boolean {
        if (inner !is ValuePredicate || inner.getType() != ValuePredicate.Type.EQ) {
            return false
        }
        val att = inner.getAttribute()
        if (ctx.queryCtx.assocJoinsWithPredicate.containsKey(att) ||
            ctx.queryCtx.assocTableJoins.containsKey(att) ||
            ctx.queryCtx.expressions.containsKey(att)
        ) {
            return false
        }
        if (inner.getValue().isNull()) {
            return false
        }
        val column = ctx.resolveColumnDef(att) ?: return false
        return !column.multiple && DISTINCT_FROM_TYPES.contains(column.type)
    }

    /** A comparison evaluates to SQL "unknown" (NULL) when the compared row value is NULL. */
    private fun isUnknownComparison(inner: Predicate, ctx: RowEvalContext): Boolean {
        if (inner !is ValuePredicate) {
            return false
        }
        if (inner.getType() == ValuePredicate.Type.EQ && inner.getValue().isNull()) {
            return false
        }
        val att = inner.getAttribute()
        if (ctx.queryCtx.assocJoinsWithPredicate.containsKey(att) ||
            ctx.queryCtx.assocTableJoins.containsKey(att)
        ) {
            return false
        }
        // PG renders boolean eq as "col IS TRUE/IS FALSE", which is two-valued (NULL IS FALSE -> false),
        // so NOT(...) on a NULL boolean is true (the row is included), not unknown.
        if (!ctx.queryCtx.expressions.containsKey(att)) {
            val column = ctx.resolveColumnDef(att)
            if (column != null && column.type == DbColumnType.BOOLEAN) {
                return false
            }
        }
        return ctx.resolveValue(att) == null
    }

    private fun valueEquals(column: DbColumnDef?, rowValue: Any?, value: DataValue): Boolean {
        if (value.isNull()) {
            return rowValue == null
        }
        if (rowValue == null) {
            return false
        }
        column ?: return rowValue.toString() == value.asText()
        return compareSingle(ValuePredicate.Type.EQ, column, rowValue, value)
    }

    private fun evalEmpty(predicate: EmptyPredicate, ctx: RowEvalContext): Boolean {
        val column = ctx.getColumn(predicate.getAttribute()) ?: return false
        val value = ctx.row[predicate.getAttribute()]
        return when {
            column.multiple -> isEmptyArray(value)
            column.type == DbColumnType.TEXT -> value == null || value == ""
            else -> value == null
        }
    }

    private fun isEmptyArray(value: Any?): Boolean {
        return when (value) {
            null -> true
            is Collection<*> -> value.isEmpty()
            else -> if (value::class.java.isArray) {
                java.lang.reflect.Array.getLength(value) == 0
            } else {
                false
            }
        }
    }

    private fun evalValue(predicate: ValuePredicate, ctx: RowEvalContext): Boolean {
        val attribute = predicate.getAttribute()
        val type = predicate.getType()
        val value = predicate.getValue()

        // Association predicates (LONG-typed src column, resolved via the join maps).
        val assocJoin = ctx.queryCtx.assocTableJoins[attribute]
        if (assocJoin != null) {
            if (type != ValuePredicate.Type.EQ &&
                type != ValuePredicate.Type.CONTAINS &&
                type != ValuePredicate.Type.IN
            ) {
                return false
            }
            val longs = DbAttValueUtils.anyToSetOfLongs(value.asJavaObj())
            return ctx.existsAssoc(assocJoin.attId, assocJoin.target, longs)
        }
        val assocJoinWithPred = ctx.queryCtx.assocJoinsWithPredicate[attribute]
        if (assocJoinWithPred != null) {
            if (type != ValuePredicate.Type.EQ &&
                type != ValuePredicate.Type.CONTAINS &&
                type != ValuePredicate.Type.IN
            ) {
                return false
            }
            return ctx.existsAssocJoinWithPredicate(assocJoinWithPred)
        }

        // Expression-typed condition.
        if (ctx.queryCtx.expressions.containsKey(attribute)) {
            val exprValue = ctx.resolveValue(attribute)
            return compareScalar(type, exprValue, value)
        }

        val column = resolveColumn(attribute, ctx) ?: return false
        val rowValue = ctx.resolveValue(attribute)

        if (column.multiple) {
            if (type != ValuePredicate.Type.EQ &&
                type != ValuePredicate.Type.CONTAINS &&
                type != ValuePredicate.Type.IN
            ) {
                return false
            }
            return arrayOverlaps(rowValue, value)
        }
        return compareSingle(type, column, rowValue, value)
    }

    private fun resolveColumn(attribute: String, ctx: RowEvalContext): DbColumnDef? {
        return ctx.resolveColumnDef(attribute)
    }

    /** Single-valued column comparison, matching PG operator semantics. */
    private fun compareSingle(
        type: ValuePredicate.Type,
        column: DbColumnDef,
        rowValue: Any?,
        value: DataValue
    ): Boolean {
        if (type == ValuePredicate.Type.EQ && value.isNull()) {
            return rowValue == null
        }
        if (type == ValuePredicate.Type.IN) {
            if (!value.isArray()) {
                return false
            }
            return value.any { compareSingle(ValuePredicate.Type.EQ, column, rowValue, it) }
        }
        if (rowValue == null) {
            // SQL comparisons with a NULL column value are never true (except handled IS NULL above)
            return false
        }
        return when (column.type) {
            DbColumnType.TEXT -> compareText(type, rowValue.toString(), value)
            DbColumnType.BOOLEAN -> rowValue == value.asBoolean()
            DbColumnType.DATETIME, DbColumnType.DATE -> compareDate(type, column.type, rowValue, value)
            DbColumnType.UUID -> compareEqLike(type, rowValue.toString(), value.asText())
            DbColumnType.DOUBLE -> compareNumber(type, toDouble(rowValue), value.asDouble())
            DbColumnType.INT, DbColumnType.LONG, DbColumnType.BIGSERIAL ->
                compareNumber(type, toDouble(rowValue), value.asLong().toDouble())
            else -> compareEqLike(type, rowValue.toString(), value.asText())
        }
    }

    private fun compareText(type: ValuePredicate.Type, rowValue: String, value: DataValue): Boolean {
        val target = value.asText()
        return when (type) {
            ValuePredicate.Type.EQ -> rowValue == target
            ValuePredicate.Type.CONTAINS -> rowValue.lowercase().contains(target.lowercase())
            ValuePredicate.Type.LIKE -> likeMatch(rowValue, target)
            ValuePredicate.Type.GT -> rowValue > target
            ValuePredicate.Type.GE -> rowValue >= target
            ValuePredicate.Type.LT -> rowValue < target
            ValuePredicate.Type.LE -> rowValue <= target
            else -> false
        }
    }

    private fun compareEqLike(type: ValuePredicate.Type, rowValue: String, target: String): Boolean {
        return when (type) {
            ValuePredicate.Type.EQ -> rowValue == target
            ValuePredicate.Type.CONTAINS -> rowValue.lowercase().contains(target.lowercase())
            ValuePredicate.Type.LIKE -> likeMatch(rowValue, target)
            else -> false
        }
    }

    private fun compareNumber(type: ValuePredicate.Type, rowValue: Double, target: Double): Boolean {
        return when (type) {
            ValuePredicate.Type.EQ, ValuePredicate.Type.CONTAINS, ValuePredicate.Type.LIKE -> rowValue == target
            ValuePredicate.Type.GT -> rowValue > target
            ValuePredicate.Type.GE -> rowValue >= target
            ValuePredicate.Type.LT -> rowValue < target
            ValuePredicate.Type.LE -> rowValue <= target
            else -> false
        }
    }

    private fun compareDate(
        type: ValuePredicate.Type,
        columnType: DbColumnType,
        rowValue: Any?,
        value: DataValue
    ): Boolean {
        val rowInstant = toInstant(rowValue) ?: return false
        val targetInstant = parsePredicateDate(columnType, value) ?: return false
        val cmp = rowInstant.compareTo(targetInstant)
        return when (type) {
            ValuePredicate.Type.EQ, ValuePredicate.Type.CONTAINS, ValuePredicate.Type.LIKE -> cmp == 0
            ValuePredicate.Type.GT -> cmp > 0
            ValuePredicate.Type.GE -> cmp >= 0
            ValuePredicate.Type.LT -> cmp < 0
            ValuePredicate.Type.LE -> cmp <= 0
            else -> false
        }
    }

    private fun parsePredicateDate(columnType: DbColumnType, value: DataValue): Instant? {
        val offsetDateTime: OffsetDateTime = when {
            value.isTextual() -> {
                val txt = value.asText()
                OffsetDateTime.parse(if (!txt.contains('T')) "${txt}T00:00:00Z" else txt)
            }
            value.isNumber() -> OffsetDateTime.ofInstant(Instant.ofEpochMilli(value.asLong()), ZoneOffset.UTC)
            else -> return null
        }
        return if (columnType == DbColumnType.DATE) {
            offsetDateTime.toLocalDate().atStartOfDay(ZoneOffset.UTC).toInstant()
        } else {
            offsetDateTime.toInstant()
        }
    }

    private fun arrayOverlaps(rowValue: Any?, value: DataValue): Boolean {
        val rowItems = toList(rowValue).map { it?.toString() }.toHashSet()
        if (rowItems.isEmpty()) {
            return false
        }
        val targets = if (value.isArray()) {
            value.map { it.asJavaObj()?.toString() }
        } else {
            listOf(value.asJavaObj()?.toString())
        }
        return targets.any { rowItems.contains(it) }
    }

    /** Comparison for a computed expression alias result. */
    private fun compareScalar(type: ValuePredicate.Type, exprValue: Any?, value: DataValue): Boolean {
        if (type == ValuePredicate.Type.EQ && value.isNull()) {
            return exprValue == null
        }
        if (exprValue == null) {
            return false
        }
        if (exprValue is Boolean && value.isBoolean()) {
            return when (type) {
                ValuePredicate.Type.EQ, ValuePredicate.Type.CONTAINS, ValuePredicate.Type.LIKE ->
                    exprValue == value.asBoolean()
                else -> false
            }
        }
        if (exprValue is Number && value.isNumber()) {
            return compareNumber(type, exprValue.toDouble(), value.asDouble())
        }
        if (exprValue is Number && !value.isNumber() && value.asText().toDoubleOrNull() != null) {
            return compareNumber(type, exprValue.toDouble(), value.asText().toDouble())
        }
        return compareText(type, exprValue.toString(), value)
    }

    private fun likeMatch(value: String, pattern: String): Boolean {
        // PG LIKE is case-insensitive here because the column value is wrapped in LOWER() and the
        // param is lowercased before binding. Build a case-insensitive regex; '%'/'_' are wildcards,
        // everything else (incl. backslashes) is matched literally via Pattern.quote.
        val regex = StringBuilder()
        val literal = StringBuilder()
        fun flushLiteral() {
            if (literal.isNotEmpty()) {
                regex.append(java.util.regex.Pattern.quote(literal.toString()))
                literal.setLength(0)
            }
        }
        for (ch in pattern) {
            when (ch) {
                '%' -> {
                    flushLiteral()
                    regex.append(".*")
                }
                '_' -> {
                    flushLiteral()
                    regex.append('.')
                }
                else -> literal.append(ch)
            }
        }
        flushLiteral()
        return Regex(regex.toString(), setOf(RegexOption.IGNORE_CASE, RegexOption.DOT_MATCHES_ALL))
            .matches(value)
    }

    private fun toDouble(value: Any?): Double {
        return when (value) {
            is Number -> value.toDouble()
            is String -> value.toDoubleOrNull() ?: 0.0
            else -> 0.0
        }
    }

    private fun toInstant(value: Any?): Instant? {
        return when (value) {
            is Instant -> value
            is java.sql.Timestamp -> value.toInstant()
            is LocalDate -> value.atStartOfDay(ZoneOffset.UTC).toInstant()
            is java.sql.Date -> value.toLocalDate().atStartOfDay(ZoneOffset.UTC).toInstant()
            is String -> runCatching { Instant.parse(value) }.getOrNull()
            else -> null
        }
    }

    private fun toList(value: Any?): List<Any?> {
        return when (value) {
            null -> emptyList()
            is Collection<*> -> value.toList()
            else -> if (value::class.java.isArray) {
                val length = java.lang.reflect.Array.getLength(value)
                (0 until length).map { java.lang.reflect.Array.get(value, it) }
            } else {
                listOf(value)
            }
        }
    }
}
