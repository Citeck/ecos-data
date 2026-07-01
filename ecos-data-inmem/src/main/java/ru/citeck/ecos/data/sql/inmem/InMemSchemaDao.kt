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

    private fun ds(dataSource: DbDataSource): InMemDataSource {
        return dataSource as? InMemDataSource
            ?: error("InMemSchemaDao requires an InMemDataSource, but got: ${dataSource::class}")
    }
}
