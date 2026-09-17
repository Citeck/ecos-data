package ru.citeck.ecos.data.sql.schema

import ru.citeck.ecos.data.sql.datasource.DbDataSource
import ru.citeck.ecos.data.sql.dto.*
import ru.citeck.ecos.data.sql.dto.fk.DbFkConstraint

interface DbSchemaDao {

    fun addSchemaListener(schema: String, listener: DbSchemaListener)

    fun isTableExists(dataSource: DbDataSource, tableRef: DbTableRef): Boolean

    fun isSchemaExists(dataSource: DbDataSource, schema: String): Boolean

    fun getColumns(dataSource: DbDataSource, tableRef: DbTableRef): List<DbColumnDef>

    fun createTable(dataSource: DbDataSource, tableRef: DbTableRef, columns: List<DbColumnDef>)

    fun addColumns(dataSource: DbDataSource, tableRef: DbTableRef, columns: List<DbColumnDef>)

    fun setColumnType(dataSource: DbDataSource, tableRef: DbTableRef, name: String, multiple: Boolean, newType: DbColumnType)

    fun createIndexes(dataSource: DbDataSource, tableRef: DbTableRef, indexes: List<DbIndexDef>)

    fun createFkConstraints(dataSource: DbDataSource, tableRef: DbTableRef, constraints: List<DbFkConstraint>)

    fun setColumnConstraints(dataSource: DbDataSource, tableRef: DbTableRef, columnName: String, constraints: List<DbColumnConstraint>)

    fun resetCache(dataSource: DbDataSource, tableRef: DbTableRef)

    /**
     * Maximum length of a column name, in UTF-8 bytes, that the backend stores without truncation.
     * Longer names are rejected by [ru.citeck.ecos.data.sql.service.DbDataServiceImpl] before any
     * DDL is issued, because a silently truncated name breaks every later read and write of the column.
     */
    fun getMaxColumnNameBytes(): Int

    /**
     * True when [exception] means the caller's cached column set no longer matches the database:
     * a table or a column it expects is gone, or a column it believes to have one type now has
     * another. Such a mismatch is normal in a cluster - another instance migrated the schema - and
     * the cure is to drop the cache and let the next call re-read it.
     *
     * The classification lives here because it is inherently backend-specific: error codes, message
     * wording and locale all belong to the database, not to the general layer. A backend that
     * cannot produce such a mismatch at all (an in-memory store) answers false.
     *
     * Implementations must inspect the whole cause chain: the state-carrying exception is often
     * wrapped by the driver or by a pool.
     */
    fun isSchemaMismatchError(exception: Throwable): Boolean

    /**
     * True when this backend can turn [currentColumn] into [targetColumn] in place, without losing
     * the stored values.
     *
     * The general layer uses this to choose a strategy before touching anything; it never sees the
     * expression itself, which is the backend's business. A false answer is not a
     * refusal to migrate - it means the conversion has to go the other way, through a new column
     * and a background transfer.
     *
     * Must agree with [setColumnType]: whenever this returns true, [setColumnType] with the same
     * pair must succeed; whenever it returns false, [setColumnType] must fail with an error naming
     * the pair rather than emitting SQL the database will reject.
     */
    fun isTypeChangeSupported(currentColumn: DbColumnDef, targetColumn: DbColumnDef): Boolean

    /**
     * Renames a column, dropping the indexes that exist only to serve it.
     *
     * Used to move a column aside as a backup. The indexes go because a backup column is never
     * queried and never joined, while every one of its indexes keeps costing on every insert and
     * update of the table. Indexes that span several columns are left alone - they belong to the
     * table, not to this column - and so is the primary key.
     *
     * A column that does not exist is not an error: the caller may be retrying after a partial
     * failure, and the end state is the same.
     */
    fun renameColumn(dataSource: DbDataSource, tableRef: DbTableRef, name: String, newName: String)

    /**
     * Whether the table holds more than [limit] rows.
     *
     * Deliberately not "how many rows": the answer is needed to pick a migration strategy, and the
     * synchronous phase that asks must not degrade with table size. Implementations must answer in
     * time bounded by [limit], never by the number of rows - a sequential `COUNT(*)` over a large
     * table is exactly what this exists to avoid.
     */
    fun isRowsCountGreaterThan(dataSource: DbDataSource, tableRef: DbTableRef, limit: Long): Boolean

    /**
     * A **cheap** estimate of how many rows the table holds - cheap enough to call from a batch
     * task's `prepare`, which the engine re-invokes on every drain tick. A backend with no estimate
     * available may return an exact count, and one with nothing at all may return -1, which means
     * "unknown" everywhere this value travels.
     */
    fun estimateRowsCount(dataSource: DbDataSource, tableRef: DbTableRef): Long

    /**
     * Builds the single-column index of [column], unless the table already has one serving it.
     *
     * The counterpart of [renameColumn]'s index dropping, and the one way an index deferred by a
     * column migration is ever built: the shadow-column transition adds the replacement column with
     * its index disabled, because building a btree means a full heap scan and it would happen inside
     * a user's mutation. The `column-migration` handler calls this once the column is full.
     *
     * Idempotent by contract, because the handler's `onFinish` can be re-entered after a failure:
     * calling it twice must leave the table with one index, not two.
     *
     * Idempotent is **not** the same as thread-safe, and this makes no concurrency promise: the
     * check and the create are two statements, so two callers racing can still both decide the index
     * is missing. What actually serialises them is the caller's own mutual exclusion - for the only
     * caller there is, the batch engine's per-table distributed lock, which admits one runner per
     * table across the whole cluster. A future caller outside that lock has to bring its own.
     */
    fun createColumnIndexIfMissing(dataSource: DbDataSource, tableRef: DbTableRef, column: DbColumnDef)
}
