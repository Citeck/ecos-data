package ru.citeck.ecos.data.sql.repo

import ru.citeck.ecos.data.sql.context.DbTableContext
import ru.citeck.ecos.data.sql.repo.find.DbFindPage
import ru.citeck.ecos.data.sql.repo.find.DbFindQuery
import ru.citeck.ecos.data.sql.repo.find.DbFindRes
import ru.citeck.ecos.records2.predicate.model.Predicate

interface DbEntityRepo {

    fun find(
        context: DbTableContext,
        query: DbFindQuery,
        page: DbFindPage,
        withTotalCount: Boolean
    ): DbFindRes<Map<String, Any?>>

    fun save(context: DbTableContext, entities: List<Map<String, Any?>>): List<Map<String, Any?>>

    fun insertIfNoConflictByExtId(context: DbTableContext, entity: Map<String, Any?>): Long?

    /**
     * Atomic compare-and-set on a single row: assign [newValues] to the row with [extId], but only
     * while every column listed in [expected] still holds the given value. Only those columns are
     * compared - columns missing from [expected] are ignored, so the row may differ from the caller's
     * copy in any other column.
     *
     * Returns true when the row was updated. A false means the update did not apply: either no row
     * has this ext id, or at least one expected column no longer matches. Both are normal outcomes
     * and not an error.
     *
     * Both maps are keyed by database column name and their values are converted to the column type,
     * so a [java.time.Instant] may be passed for a datetime column. An unknown column name is an
     * error, as is an empty [newValues] or an empty [expected] - an empty condition would make this
     * an unconditional overwrite by ext id. The id, ext id and update version columns are reserved
     * and must not appear in either map: the first two identify the row and the last one is
     * incremented by the implementation, keeping the optimistic lock of `save` coherent. Nothing
     * else is written - in particular the audit columns are left alone, so pass them in [newValues]
     * when they matter.
     *
     * The compare and the write are one indivisible step, but the method runs inside whatever
     * transaction the caller has open on [context]'s data source and never opens one of its own. A
     * true is therefore durable only once that transaction commits, and the row stays locked until
     * then.
     */
    fun updateByExtIdIfMatches(
        context: DbTableContext,
        extId: String,
        expected: Map<String, Any?>,
        newValues: Map<String, Any?>
    ): Boolean

    fun delete(context: DbTableContext, entity: Map<String, Any?>)

    fun delete(context: DbTableContext, predicate: Predicate)

    fun delete(context: DbTableContext, entities: List<Long>)
}
