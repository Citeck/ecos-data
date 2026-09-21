package ru.citeck.ecos.data.sql.service

import ru.citeck.ecos.data.sql.repo.find.DbFindQuery
import ru.citeck.ecos.data.sql.service.assocs.AssocSelectJoins

/**
 * Whether a name a query groups by, sorts by or selects is something the query can actually read.
 *
 * A name is readable when it is a column of the table, an alias of an expression the query carries,
 * a column behind a registered assoc-select join, or a column of a joined raw table. Anything else
 * would reach the database as a column that is not there.
 *
 * Shared by both backends deliberately: the in-memory backend models what PostgreSQL does, and two
 * copies of this rule are two chances for them to disagree about which query is answerable.
 */
object DbQueryColumns {

    fun isReadable(name: String, columnNames: Set<String>, query: DbFindQuery): Boolean {
        if (columnNames.contains(name) || query.expressions.containsKey(name)) {
            return true
        }
        val dotIdx = name.indexOf('.')
        if (dotIdx == -1) {
            return false
        }
        if (AssocSelectJoins.splitColumn(name, query.assocSelectJoins.keys) != null) {
            return true
        }
        return query.rawTableJoins.containsKey(name.substring(0, dotIdx))
    }

    /**
     * The group-by names the query cannot read. `*` is not a name - it stands for "one group over
     * everything" - so it is never among them.
     */
    fun getUnreadableGroupBy(query: DbFindQuery, columnNames: Set<String>): List<String> {
        if (query.groupBy.isEmpty()) {
            return emptyList()
        }
        return query.groupBy.filter { it != "*" && !isReadable(it, columnNames, query) }
    }

    /** The columns named inside the query's expressions that it cannot read. */
    fun getUnreadableExpressionColumns(query: DbFindQuery, columnNames: Set<String>): List<String> {
        if (query.expressions.isEmpty()) {
            return emptyList()
        }
        val unreadable = LinkedHashSet<String>()
        query.expressions.values.forEach { expression ->
            expression.visitColumns { token ->
                if (!isReadable(token.name, columnNames, query)) {
                    unreadable.add(token.name)
                }
            }
        }
        return unreadable.toList()
    }

    /**
     * Refuses a query naming something it cannot read - loudly, because a caller assembling a query
     * in code has made a mistake rather than asked an unanswerable question. The records DAO above
     * answers the same situation with an empty result, having caught it before it gets here.
     *
     * Both backends call this, and that is the point of it living here: the in-memory backend is
     * there to model what the SQL one does, and a query one of them refuses is not a query the
     * other should answer.
     */
    fun checkQueryIsReadable(query: DbFindQuery, columnNames: Set<String>) {
        val unreadableExpressionColumns = getUnreadableExpressionColumns(query, columnNames)
        if (unreadableExpressionColumns.isNotEmpty()) {
            error("Invalid columns used in expressions: $unreadableExpressionColumns")
        }
        val unreadableGroupBy = getUnreadableGroupBy(query, columnNames)
        if (unreadableGroupBy.isNotEmpty()) {
            error("Grouping by columns $unreadableGroupBy is not allowed")
        }
    }
}
