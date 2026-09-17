package ru.citeck.ecos.data.sql.inmem.store

import ru.citeck.ecos.data.sql.dto.DbColumnDef
import ru.citeck.ecos.data.sql.dto.DbTableRef
import ru.citeck.ecos.data.sql.repo.entity.DbEntity
import java.util.concurrent.atomic.AtomicLong

/**
 * In-memory representation of all schemas/tables managed by a single
 * [ru.citeck.ecos.data.sql.inmem.datasource.InMemDataSource].
 *
 * The store is the single source of truth that both [ru.citeck.ecos.data.sql.inmem.InMemSchemaDao]
 * (table/column structure) and [ru.citeck.ecos.data.sql.inmem.InMemEntityRepo] (rows) mutate.
 *
 * **Transactional isolation via an undo-log (not a snapshot copy).** There is exactly one store
 * instance for a data source; transactions mutate it *in place*. Every structural/row mutation first
 * records an inverse operation in the shared [UndoLog]. A transaction begins by taking a
 * [UndoLog.mark]; on rollback it replays the inverses back to that mark ([UndoLog.rollbackTo]),
 * leaving the store byte-identical to its pre-transaction state; on commit it simply forgets those
 * inverses ([UndoLog.commitTo]) — the changes are already applied. This makes a write O(1) instead of
 * O(store size): the old `store.copy()` deep-copied every table on every top-level / `requiresNew`
 * write, so bulk creation (each new EntityRef opens a `requiresNew` id-mapping txn) degraded to
 * O(N^2). The undo-log brings it back to O(N).
 *
 * The undo-log is the *only* way the store is mutated, so isolation is exactly as strong as the old
 * deep-copy: a rolled-back txn replays the precise inverse of each change it made (rows are never
 * mutated in place by callers — every write fully replaces a row via [InMemTable.putRow] — so the
 * captured "previous value" is a stable reference and replaying it restores the prior state exactly).
 */
class InMemStore private constructor(
    private val tables: MutableMap<String, InMemTable>,
    private val undoLog: UndoLog
) {

    constructor() : this(LinkedHashMap(), UndoLog())

    fun getOrCreateTable(tableRef: DbTableRef): InMemTable {
        val existing = tables[tableRef.fullName]
        if (existing != null) {
            return existing
        }
        val table = InMemTable(tableRef, undoLog)
        tables[tableRef.fullName] = table
        undoLog.record { tables.remove(tableRef.fullName) }
        return table
    }

    fun getTable(tableRef: DbTableRef): InMemTable? {
        return tables[tableRef.fullName]
    }

    fun getTables(): List<InMemTable> {
        return tables.values.toList()
    }

    fun dropAllTables() {
        if (tables.isEmpty()) {
            return
        }
        val previous = LinkedHashMap(tables)
        tables.clear()
        undoLog.record {
            tables.clear()
            tables.putAll(previous)
        }
    }

    fun isSchemaExists(schema: String): Boolean {
        return tables.values.any { it.tableRef.schema == schema }
    }

    fun mark(): Int = undoLog.mark()

    fun rollbackTo(mark: Int) = undoLog.rollbackTo(mark)

    fun commitTo(mark: Int) = undoLog.commitTo(mark)
}

/**
 * A stack of inverse operations. Each store mutation pushes the action that undoes it. A transaction
 * snapshots its starting depth with [mark]; [rollbackTo] runs (LIFO) and discards every inverse pushed
 * after that depth; [commitTo] discards them *without running* (the change is kept). Entries pushed by
 * a fully-completed nested scope are always a contiguous suffix of the log, so both operations only
 * ever touch that scope's own entries.
 */
class UndoLog {

    private val entries = ArrayList<() -> Unit>()

    fun mark(): Int = entries.size

    fun record(inverse: () -> Unit) {
        entries.add(inverse)
    }

    fun rollbackTo(mark: Int) {
        while (entries.size > mark) {
            entries.removeAt(entries.size - 1).invoke()
        }
    }

    fun commitTo(mark: Int) {
        if (mark == 0) {
            entries.clear()
        } else {
            while (entries.size > mark) {
                entries.removeAt(entries.size - 1)
            }
        }
    }
}

/**
 * A single in-memory table: ordered column definitions plus rows keyed by the long [id].
 *
 * Rows are stored as plain [Map] of column name to JVM value (already converted to the column's
 * "common" type via [ru.citeck.ecos.data.sql.type.DbTypesConverter]). An ext-id index mirrors the
 * unique index on [ru.citeck.ecos.data.sql.repo.entity.DbEntity.EXT_ID] in the PG backend.
 *
 * Every mutator records an inverse in the shared [UndoLog] *before* applying its change, so the owning
 * [InMemStore] can roll a transaction back without deep-copying the table.
 */
class InMemTable internal constructor(
    val tableRef: DbTableRef,
    private val undoLog: UndoLog,
    /**
     * The id sequence. Shared by reference (it is NOT value-copied / snapshotted) so that, like a
     * PostgreSQL `SEQUENCE`, id allocation is **non-transactional**: a rolled-back transaction still
     * consumes its ids and they are never reissued. This is essential once writes can be rolled back by
     * the platform transaction manager — reissuing an id already taken by a committed `requiresNew`
     * child would corrupt the long-id <-> EntityRef mappings (a record's type ref-id would resolve to
     * an unrelated ref). [nextId] is therefore deliberately NOT recorded in the undo-log.
     */
    private val idCounter: AtomicLong = AtomicLong(0)
) {

    private val columns = LinkedHashMap<String, DbColumnDef>()
    private val rows = LinkedHashMap<Long, MutableMap<String, Any?>>()
    private val extIdToId = HashMap<String, Long>()

    fun getColumns(): List<DbColumnDef> {
        return columns.values.toList()
    }

    fun getColumn(name: String): DbColumnDef? {
        return columns[name]
    }

    fun hasColumn(name: String): Boolean {
        return columns.containsKey(name)
    }

    fun addColumn(column: DbColumnDef) {
        setColumn(column)
    }

    fun setColumn(column: DbColumnDef) {
        val previous = columns.put(column.name, column)
        undoLog.record {
            if (previous == null) {
                columns.remove(column.name)
            } else {
                columns[column.name] = previous
            }
        }
    }

    /**
     * Renames a column and every row's entry for it.
     *
     * Rows are mutated in place here rather than replaced through [putRow], because the key is what
     * changes and [putRow] needs an ext id the schema layer does not have. Every mutation records
     * its inverse in the undo log, so rollback isolation is preserved - the rule the other writers
     * follow by replacing rows is honoured here by recording, not by copying.
     *
     * Rejects a collision with an existing column rather than silently overwriting it. PostgreSQL's
     * `RENAME COLUMN` raises in the same situation; agreeing here matters because a caller (the
     * column migration) generates backup names and must be able to trust that a collision surfaces
     * as an error on both backends rather than quietly clobbering data on one of them.
     */
    fun renameColumn(oldName: String, newName: String) {
        val column = columns[oldName] ?: return
        require(!columns.containsKey(newName)) {
            "Column '$newName' already exists in table '${tableRef.fullName}', cannot rename '$oldName' to it"
        }
        val previousColumns = LinkedHashMap(columns)
        columns.remove(oldName)
        columns[newName] = DbColumnDef.Builder(column).withName(newName).build()
        undoLog.record {
            columns.clear()
            columns.putAll(previousColumns)
        }
        for (row in rows.values) {
            if (!row.containsKey(oldName)) {
                continue
            }
            val value = row.remove(oldName)
            row[newName] = value
            undoLog.record {
                row.remove(newName)
                row[oldName] = value
            }
        }
    }

    fun nextId(): Long {
        return idCounter.incrementAndGet()
    }

    /**
     * The live row collection (not a copy), for read-only iteration on the hot find/query path.
     * Callers MUST treat the rows as read-only: mutating a returned row in place would bypass the
     * undo-log and silently break transaction rollback isolation (writers always replace a row via
     * [putRow] rather than mutating it).
     */
    fun getRows(): Collection<MutableMap<String, Any?>> {
        return rows.values
    }

    /**
     * Remove every row matching [predicate]. Works for tables without an `id` column (e.g.
     * `ed_associations`, `ed_read_perms`) because it removes by the internal row key rather than by
     * [ru.citeck.ecos.data.sql.repo.entity.DbEntity.ID].
     */
    fun removeRowsBy(predicate: (Map<String, Any?>) -> Boolean) {
        val keysToRemove = rows.entries.filter { predicate(it.value) }.map { it.key }
        keysToRemove.forEach { removeRow(it) }
    }

    fun getRowById(id: Long): MutableMap<String, Any?>? {
        return rows[id]
    }

    fun getRowByExtId(extId: String): MutableMap<String, Any?>? {
        val id = extIdToId[extId] ?: return null
        return rows[id]
    }

    /**
     * The row key behind an ext-id. Needed to rewrite a row found by ext-id: the key is the internal
     * row id, which is not part of the row itself in tables without an id column.
     */
    fun getIdByExtId(extId: String): Long? {
        return extIdToId[extId]
    }

    fun putRow(id: Long, extId: String, row: MutableMap<String, Any?>) {
        // Mirror the forward op exactly so the inverse is its precise undo:
        //   rows[id] = row;  if (extId != "") extIdToId[extId] = id
        val previousRow = rows.put(id, row)
        val hadExtIdMapping = extId.isNotEmpty() && extIdToId.containsKey(extId)
        val previousExtIdMapping = if (hadExtIdMapping) extIdToId[extId] else null
        if (extId.isNotEmpty()) {
            extIdToId[extId] = id
        }
        undoLog.record {
            if (previousRow == null) {
                rows.remove(id)
            } else {
                rows[id] = previousRow
            }
            if (extId.isNotEmpty()) {
                if (hadExtIdMapping) {
                    extIdToId[extId] = previousExtIdMapping!!
                } else {
                    extIdToId.remove(extId)
                }
            }
        }
    }

    fun removeRow(id: Long) {
        // Mirror removeRow's forward op: drop rows[id] and, if the row carried an ext-id whose index
        // entry points back to this id, drop that index entry too.
        val row = rows.remove(id) ?: return
        val extId = row[EXT_ID_KEY] as? String
        val removedExtIdMapping = if (extId != null && extIdToId[extId] == id) {
            extIdToId.remove(extId)
        } else {
            null
        }
        undoLog.record {
            rows[id] = row
            if (removedExtIdMapping != null) {
                extIdToId[extId!!] = removedExtIdMapping
            }
        }
    }

    /**
     * Removes all rows but keeps the column structure, mirroring SQL TRUNCATE.
     */
    fun truncate() {
        if (rows.isEmpty() && extIdToId.isEmpty()) {
            return
        }
        val previousRows = LinkedHashMap(rows)
        val previousExtIdToId = HashMap(extIdToId)
        rows.clear()
        extIdToId.clear()
        undoLog.record {
            rows.clear()
            rows.putAll(previousRows)
            extIdToId.clear()
            extIdToId.putAll(previousExtIdToId)
        }
    }

    companion object {
        private const val EXT_ID_KEY = DbEntity.EXT_ID
    }
}
