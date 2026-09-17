package ru.citeck.ecos.data.sql.inmem

import ru.citeck.ecos.data.sql.datasource.DbDataSource
import ru.citeck.ecos.data.sql.dto.DbColumnConstraint
import ru.citeck.ecos.data.sql.dto.DbColumnDef
import ru.citeck.ecos.data.sql.dto.DbColumnType
import ru.citeck.ecos.data.sql.dto.DbIndexDef
import ru.citeck.ecos.data.sql.dto.DbTableRef
import ru.citeck.ecos.data.sql.dto.fk.DbFkConstraint
import ru.citeck.ecos.data.sql.inmem.datasource.InMemDataSource
import ru.citeck.ecos.data.sql.schema.DbSchemaDao
import ru.citeck.ecos.data.sql.schema.DbSchemaListener
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList

/**
 * In-memory [DbSchemaDao]. Applies structural changes directly to the
 * [ru.citeck.ecos.data.sql.inmem.store.InMemStore]; it does not generate or execute SQL.
 *
 * Every structural change registers a *schema command* on the data source. The command is just a
 * change marker - the migration flow in [ru.citeck.ecos.data.sql.service.DbDataServiceImpl] only
 * checks whether the watched command list is non-empty to detect that a migration happened; the
 * marker text is never parsed, so it is a plain human-readable description, not SQL. During a schema
 * mock ([DbDataSource.withSchemaMock]) the marker is still registered but the structural change is
 * NOT applied, mirroring the PG dao's updateSchema-under-mock.
 *
 * Index/constraint/FK definitions are accepted and recorded as commands but not enforced: filtering
 * in the in-mem backend does not rely on indexes, and the PG dao likewise treats them as pure
 * storage-engine hints.
 *
 * No locking is performed: [InMemStore]/[InMemTable] use plain (non-concurrent) maps and the backend
 * is single-threaded per transaction by design (see [InMemDataSource]).
 */
class InMemSchemaDao : DbSchemaDao {

    private val listeners: MutableMap<String, MutableList<DbSchemaListener>> = ConcurrentHashMap()

    /**
     * Columns [createColumnIndexIfMissing] has already been asked about - see its doc.
     */
    private val indexedColumns: MutableMap<DbTableRef, MutableSet<String>> = ConcurrentHashMap()

    override fun addSchemaListener(schema: String, listener: DbSchemaListener) {
        listeners.computeIfAbsent(schema) { CopyOnWriteArrayList() }.add(listener)
    }

    override fun isTableExists(dataSource: DbDataSource, tableRef: DbTableRef): Boolean {
        return ds(dataSource).getStore().getTable(tableRef) != null
    }

    override fun isSchemaExists(dataSource: DbDataSource, schema: String): Boolean {
        return ds(dataSource).getStore().isSchemaExists(schema)
    }

    override fun getColumns(dataSource: DbDataSource, tableRef: DbTableRef): List<DbColumnDef> {
        return ds(dataSource).getStore().getTable(tableRef)?.getColumns() ?: emptyList()
    }

    override fun createTable(dataSource: DbDataSource, tableRef: DbTableRef, columns: List<DbColumnDef>) {
        val ds = ds(dataSource)
        if (tableRef.schema.isNotBlank() && !ds.getStore().isSchemaExists(tableRef.schema)) {
            ds.registerSchemaCommand("create schema ${tableRef.schema}")
            if (!ds.isSchemaMock()) {
                listeners[tableRef.schema]?.forEach { it.onSchemaCreated() }
            }
        }
        ds.registerSchemaCommand("create table ${tableRef.fullName}")
        if (!ds.isSchemaMock()) {
            val table = ds.getStore().getOrCreateTable(tableRef)
            columns.forEach { table.addColumn(it) }
        }
    }

    override fun addColumns(dataSource: DbDataSource, tableRef: DbTableRef, columns: List<DbColumnDef>) {
        if (columns.isEmpty()) {
            return
        }
        val ds = ds(dataSource)
        columns.forEach { column ->
            ds.registerSchemaCommand("add column ${column.name} to ${tableRef.fullName}")
            if (!ds.isSchemaMock()) {
                ds.getStore().getOrCreateTable(tableRef).addColumn(column)
            }
        }
    }

    override fun setColumnType(
        dataSource: DbDataSource,
        tableRef: DbTableRef,
        name: String,
        multiple: Boolean,
        newType: DbColumnType
    ) {
        val ds = ds(dataSource)
        val table = ds.getStore().getTable(tableRef) ?: return
        val current = table.getColumn(name) ?: return
        if (current.type == newType && current.multiple == multiple) {
            return
        }
        ds.registerSchemaCommand("change type of $name in ${tableRef.fullName} to $newType")
        if (!ds.isSchemaMock()) {
            table.setColumn(
                DbColumnDef.Builder(current)
                    .withType(newType)
                    .withMultiple(multiple)
                    .build()
            )
        }
    }

    override fun createIndexes(dataSource: DbDataSource, tableRef: DbTableRef, indexes: List<DbIndexDef>) {
        if (indexes.isEmpty()) {
            return
        }
        val ds = ds(dataSource)
        indexes.forEach { index ->
            ds.registerSchemaCommand("create index on ${tableRef.fullName} (${index.columns.joinToString(",")})")
        }
    }

    override fun createFkConstraints(dataSource: DbDataSource, tableRef: DbTableRef, constraints: List<DbFkConstraint>) {
        if (constraints.isEmpty()) {
            return
        }
        val ds = ds(dataSource)
        constraints.forEach { constraint ->
            ds.registerSchemaCommand("add constraint ${constraint.name} to ${tableRef.fullName}")
        }
    }

    override fun setColumnConstraints(
        dataSource: DbDataSource,
        tableRef: DbTableRef,
        columnName: String,
        constraints: List<DbColumnConstraint>
    ) {
        val ds = ds(dataSource)
        val table = ds.getStore().getTable(tableRef) ?: return
        val current = table.getColumn(columnName) ?: return
        ds.registerSchemaCommand("set constraints on $columnName in ${tableRef.fullName}")
        if (!ds.isSchemaMock()) {
            table.setColumn(current.withConstraints(constraints))
        }
    }

    override fun resetCache(dataSource: DbDataSource, tableRef: DbTableRef) {
        // no prepared-statement cache to reset for the in-memory backend
    }

    /**
     * The in-memory store has no limit of its own, so it deliberately returns the portable platform
     * limit - the minimum over the supported backends, today PostgreSQL's NAMEDATALEN - 1. Application
     * tests running on this backend must reject the same models that production rejects.
     */
    override fun getMaxColumnNameBytes(): Int {
        return 63
    }

    /**
     * The in-memory store answers structural questions from the same maps the caller's cache was
     * built from, so it cannot produce a stale-cache error to recognise. Always false.
     */
    override fun isSchemaMismatchError(exception: Throwable): Boolean {
        return false
    }

    /**
     * The in-memory backend changes a column's definition without touching the stored values, so
     * every pair is "supported" structurally. This is not a claim that values are converted - they
     * are not, which is why the row-wise transfer is the portable path.
     */
    override fun isTypeChangeSupported(currentColumn: DbColumnDef, targetColumn: DbColumnDef): Boolean {
        return true
    }

    override fun renameColumn(dataSource: DbDataSource, tableRef: DbTableRef, name: String, newName: String) {
        val ds = ds(dataSource)
        val table = ds.getStore().getTable(tableRef) ?: return
        if (table.getColumn(name) == null) {
            return
        }
        ds.registerSchemaCommand("rename column $name in ${tableRef.fullName} to $newName")
        if (!ds.isSchemaMock()) {
            table.renameColumn(name, newName)
            indexedColumns[tableRef]?.remove(name)
        }
    }

    /**
     * Exact, and cheap: the store knows its own size. There are no planner statistics to fall back
     * on and none are needed.
     */
    override fun isRowsCountGreaterThan(dataSource: DbDataSource, tableRef: DbTableRef, limit: Long): Boolean {
        val table = ds(dataSource).getStore().getTable(tableRef) ?: return false
        return table.getRows().size > limit
    }

    /**
     * Exact, for the same reason [isRowsCountGreaterThan] is: the store knows its own size and
     * counting it is O(1). -1 for a table that does not exist - there is nothing to estimate.
     */
    override fun estimateRowsCount(dataSource: DbDataSource, tableRef: DbTableRef): Long {
        val table = ds(dataSource).getStore().getTable(tableRef) ?: return -1
        return table.getRows().size.toLong()
    }

    /**
     * The in-memory backend enforces no index, so this builds nothing - it registers the same
     * change marker [createIndexes] does. The set of columns it has been asked about is kept only
     * so that "if missing" is a real answer rather than a word in the method name: a caller that
     * asks twice has to see one index created, on this backend as much as on PostgreSQL. A column
     * renamed away (a backup, see [renameColumn]) loses its entry, matching the PG backend dropping
     * the indexes that served the old name.
     *
     * Deliberately *only* what this method was asked for: an index that came with [addColumns] or
     * [createTable] is not in the set, because the in-memory store keeps no index state of its own
     * to read it back from. That is harmless for the one caller there is - the column migration
     * builds an index precisely on a column that was added without one.
     */
    override fun createColumnIndexIfMissing(dataSource: DbDataSource, tableRef: DbTableRef, column: DbColumnDef) {
        val ds = ds(dataSource)
        val indexed = indexedColumns.computeIfAbsent(tableRef) { ConcurrentHashMap.newKeySet() }
        if (indexed.contains(column.name)) {
            return
        }
        ds.registerSchemaCommand("create index on ${tableRef.fullName} (${column.name})")
        if (!ds.isSchemaMock()) {
            indexed.add(column.name)
        }
    }

    private fun ds(dataSource: DbDataSource): InMemDataSource {
        return dataSource as? InMemDataSource
            ?: error("InMemSchemaDao requires an InMemDataSource, but got: ${dataSource::class}")
    }
}
