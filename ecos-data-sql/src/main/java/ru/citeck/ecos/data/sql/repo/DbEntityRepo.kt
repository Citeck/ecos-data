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

    /**
     * Inserts [entity] and answers its id, or answers the id of the row that already holds the same
     * ext id - in **one statement**, which is what makes it usable as an id allocator shared by
     * transactions that know nothing about each other.
     *
     * One statement because the two halves cannot be separated. `ON CONFLICT DO NOTHING` returns
     * nothing on a collision, and a `SELECT` that follows it cannot see the row it collided with
     * while the transaction that inserted it is still open - an invisible tuple cannot be read and
     * cannot be locked either, so "insert, then look" degenerates into polling with a sleep in it.
     * `DO UPDATE` waits for the other transaction instead and answers with the row it left behind:
     * its id when it committed, a freshly inserted one when it rolled back. Measured on PostgreSQL
     * 12.6 against a competitor holding its transaction open: both forms wait, and only this one
     * comes back with an id.
     *
     * The `SET` is a no-op assignment of the ext id to itself - the row's only purpose here is to be
     * returned. It is a write all the same, so a collision leaves a dead tuple behind, which is why
     * callers look the id up first and reach this only when it was really missing.
     *
     * Requires a unique index on the ext id column, which is what `ON CONFLICT` resolves the
     * arbiter through.
     *
     * @param extraLongColumns columns of the stored row to answer alongside the id. For a caller
     *        that has to know what the row says about itself - `ed_record_ref.__moved_to`, which
     *        turns the id of a redirect into the id it redirects to - and would otherwise have to
     *        read the row back, once per reference it registers, to learn something the statement
     *        already had in its hands.
     */
    fun insertOrGetByExtId(
        context: DbTableContext,
        entity: Map<String, Any?>,
        extraLongColumns: List<String> = emptyList()
    ): DbInsertOrGetRes

    /**
     * Inserts every entity that does not collide with an existing row on [conflictColumns], and
     * answers the [returningColumn] of the rows actually inserted. A colliding entity is silently
     * left out - it is not an error and not an update.
     *
     * **Why this exists rather than "select, then insert what is missing".** Between that select and
     * that insert another transaction can insert the very same row: the reader cannot see it until
     * it commits, and then the insert fails on the unique index. For "put this row there if it is
     * not there yet" that failure is noise - the row is there, which is all the caller wanted - but
     * it arrives as an aborted transaction, which for a user's mutation is a failed save and for a
     * batch is a rolled-back window. Letting the database skip the collision turns the race into the
     * outcome the caller was asking for.
     *
     * [conflictColumns] must match a unique index of the table, and [returningColumn] must hold a
     * `long` - both are what the callers of this need and what every backend can answer cheaply.
     *
     * Every entity is inserted as a new row: an `id` among its keys is ignored.
     */
    fun insertIfNoConflict(
        context: DbTableContext,
        entities: List<Map<String, Any?>>,
        conflictColumns: List<String>,
        returningColumn: String
    ): List<Long>

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

    /**
     * The id-keyed sibling of [updateByExtIdIfMatches], for tables that carry no ext id at all -
     * [ru.citeck.ecos.data.sql.batch.DbBatchTaskEntity] is the first of these. Everything else about
     * the contract is identical: same reserved columns, same empty-map rejections, same "one
     * indivisible step inside the caller's transaction" durability, same meaning of a false return.
     */
    fun updateByIdIfMatches(
        context: DbTableContext,
        id: Long,
        expected: Map<String, Any?>,
        newValues: Map<String, Any?>
    ): Boolean

    fun delete(context: DbTableContext, entity: Map<String, Any?>)

    fun delete(context: DbTableContext, predicate: Predicate)

    fun delete(context: DbTableContext, entities: List<Long>)
}

/**
 * What [DbEntityRepo.insertOrGetByExtId] answers: the id the ext id is registered under, and the
 * values of the columns the caller asked for, absent when the column is null in the row.
 */
class DbInsertOrGetRes(
    val id: Long,
    val longs: Map<String, Long>
)
