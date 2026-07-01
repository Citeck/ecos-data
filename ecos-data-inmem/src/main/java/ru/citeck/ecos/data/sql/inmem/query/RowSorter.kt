package ru.citeck.ecos.data.sql.inmem.query

import ru.citeck.ecos.data.sql.repo.find.DbFindSort
import java.time.Instant
import java.time.LocalDate
import java.time.ZoneOffset

/**
 * Stable multi-key sort matching PG `ORDER BY`. NULLs sort last for ascending and first for
 * descending - PostgreSQL's default `NULLS LAST`/`NULLS FIRST` for ASC/DESC respectively.
 */
object RowSorter {

    fun sort(rows: List<ResultRow>, sortBy: List<DbFindSort>, ctx: QueryEvalContext): List<ResultRow> {
        if (sortBy.isEmpty() || rows.isEmpty()) {
            return rows
        }
        var comparator: Comparator<ResultRow>? = null
        for (sort in sortBy) {
            val keyComparator = Comparator<ResultRow> { a, b ->
                compareValues(a.resolveSortValue(sort.column), b.resolveSortValue(sort.column), sort.ascending)
            }
            comparator = comparator?.then(keyComparator) ?: keyComparator
        }
        return rows.sortedWith(comparator!!)
    }

    private fun compareValues(a: Any?, b: Any?, ascending: Boolean): Int {
        if (a == null && b == null) {
            return 0
        }
        // ascending -> NULLS LAST; descending -> NULLS FIRST (PostgreSQL defaults)
        if (a == null) {
            return if (ascending) 1 else -1
        }
        if (b == null) {
            return if (ascending) -1 else 1
        }
        val cmp = compareNonNull(a, b)
        return if (ascending) cmp else -cmp
    }

    @Suppress("UNCHECKED_CAST")
    private fun compareNonNull(a: Any, b: Any): Int {
        val na = toComparable(a)
        val nb = toComparable(b)
        if (na is Number && nb is Number) {
            return na.toDouble().compareTo(nb.toDouble())
        }
        if (na::class == nb::class && na is Comparable<*>) {
            return (na as Comparable<Any>).compareTo(nb)
        }
        return na.toString().compareTo(nb.toString())
    }

    private fun toComparable(value: Any): Any {
        return when (value) {
            is Instant -> value
            is java.sql.Timestamp -> value.toInstant()
            is LocalDate -> value.atStartOfDay(ZoneOffset.UTC).toInstant()
            is java.sql.Date -> value.toLocalDate().atStartOfDay(ZoneOffset.UTC).toInstant()
            else -> value
        }
    }
}
