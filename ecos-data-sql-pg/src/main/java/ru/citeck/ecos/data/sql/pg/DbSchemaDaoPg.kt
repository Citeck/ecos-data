package ru.citeck.ecos.data.sql.pg

import mu.KotlinLogging
import ru.citeck.ecos.data.sql.datasource.DbDataSource
import ru.citeck.ecos.data.sql.dto.*
import ru.citeck.ecos.data.sql.dto.fk.DbFkConstraint
import ru.citeck.ecos.data.sql.dto.fk.FkCascadeActionOptions
import ru.citeck.ecos.data.sql.schema.DbSchemaDao
import ru.citeck.ecos.data.sql.schema.DbSchemaListener
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList

open class DbSchemaDaoPg internal constructor() : DbSchemaDao {

    companion object {
        val log = KotlinLogging.logger {}

        const val COLUMN_TYPE_NAME = "TYPE_NAME"
        const val COLUMN_COLUMN_NAME = "COLUMN_NAME"

        private val INDEXED_COLUMN_TYPES = setOf(
            DbColumnType.DATETIME,
            DbColumnType.DATE,
            DbColumnType.INT,
            DbColumnType.LONG,
            DbColumnType.DOUBLE,
            DbColumnType.TEXT,
            DbColumnType.BIGSERIAL,
            DbColumnType.BOOLEAN,
            DbColumnType.UUID
        )
        private val CONVERTABLE_TO_TEXT_TYPES = setOf(
            DbColumnType.JSON,
            DbColumnType.BOOLEAN,
            DbColumnType.DOUBLE,
            DbColumnType.INT,
            DbColumnType.LONG,
            DbColumnType.UUID
        )
    }

    private val listeners: MutableMap<String, MutableList<DbSchemaListener>> = ConcurrentHashMap()

    override fun addSchemaListener(schema: String, listener: DbSchemaListener) {
        listeners.computeIfAbsent(schema) { CopyOnWriteArrayList() }.add(listener)
    }

    override fun getColumns(dataSource: DbDataSource, tableRef: DbTableRef): List<DbColumnDef> {

        return dataSource.withMetaData { metaData ->
            val schema = tableRef.schema.replace("_", "\\_").ifEmpty { "%" }
            val table = tableRef.table.replace("_", "\\_")
            metaData.getColumns(null, schema, table, "%")
                .use {
                    val columns = arrayListOf<DbColumnDef>()
                    while (it.next()) {
                        try {
                            val typeName = it.getString(COLUMN_TYPE_NAME)
                            val (type, multiple) = getColumnType(typeName)
                            columns.add(
                                DbColumnDef.create {
                                    withName(it.getString(COLUMN_COLUMN_NAME))
                                    withType(type)
                                    withMultiple(multiple)
                                }
                            )
                        } catch (e: Exception) {
                            log.warn { "Column error: '${it.getString(COLUMN_COLUMN_NAME)}' ${e.message}" }
                        }
                    }
                    columns
                }
        }
    }

    override fun createTable(dataSource: DbDataSource, tableRef: DbTableRef, columns: List<DbColumnDef>) {
        synchronized(tableRef) {
            createTableInSync(dataSource, tableRef, columns)
        }
    }

    private fun createTableInSync(dataSource: DbDataSource, tableRef: DbTableRef, columns: List<DbColumnDef>) {

        if (tableRef.schema.isNotBlank()) {
            dataSource.query(
                "SELECT EXISTS(SELECT 1 FROM pg_namespace WHERE nspname = '${tableRef.schema}')",
                emptyList()
            ) { rs ->
                if (!rs.next() || !rs.getBoolean(1)) {
                    dataSource.updateSchema("CREATE SCHEMA IF NOT EXISTS \"${tableRef.schema}\"")
                    listeners[tableRef.schema]?.forEach { it.onSchemaCreated() }
                }
            }
        }

        val queryBuilder = StringBuilder()

        queryBuilder.append("CREATE TABLE ${tableRef.fullName} (")
        columns.forEach {
            queryBuilder.append("\"").append(it.name).append("\" ")
                .append(getColumnSqlType(it.type, it.multiple))
                .append(getColumnSqlConstraintsWithSpaceIfNotEmpty(it.constraints))
                .append(",")
        }
        queryBuilder.setLength(queryBuilder.length - 1)
        queryBuilder.append(")")

        dataSource.updateSchema(queryBuilder.toString())

        columns.forEach {
            addColumnIndex(dataSource, tableRef, it)
        }
    }

    override fun addColumns(dataSource: DbDataSource, tableRef: DbTableRef, columns: List<DbColumnDef>) {
        synchronized(tableRef) {
            addColumnsInSync(dataSource, tableRef, columns)
        }
    }

    private fun addColumnsInSync(dataSource: DbDataSource, tableRef: DbTableRef, columns: List<DbColumnDef>) {

        if (columns.isEmpty()) {
            return
        }

        columns.forEach { column ->
            val query = "ALTER TABLE ${tableRef.fullName} " +
                "ADD COLUMN \"${column.name}\" " +
                getColumnSqlType(column.type, column.multiple) +
                getColumnSqlConstraintsWithSpaceIfNotEmpty(column.constraints)
            dataSource.updateSchema(query)
            addColumnIndex(dataSource, tableRef, column)
        }
    }

    private fun addColumnIndex(dataSource: DbDataSource, tableRef: DbTableRef, column: DbColumnDef) {

        if (!column.index.enabled || !INDEXED_COLUMN_TYPES.contains(column.type)) {
            return
        }
        val columnToIndex = if (!column.multiple && column.type == DbColumnType.TEXT) {
            "LOWER(\"${column.name}\")"
        } else {
            "\"${column.name}\""
        }
        val query = if (column.multiple) {
            "CREATE INDEX ON ${tableRef.fullName} USING GIN ($columnToIndex);"
        } else {
            "CREATE INDEX ON ${tableRef.fullName} ($columnToIndex);"
        }
        dataSource.updateSchema(query)
    }

    override fun setColumnType(
        dataSource: DbDataSource,
        tableRef: DbTableRef,
        name: String,
        multiple: Boolean,
        newType: DbColumnType
    ) {
        synchronized(tableRef) {
            setColumnTypeInSync(dataSource, tableRef, name, multiple, newType)
        }
    }

    private fun setColumnTypeInSync(
        dataSource: DbDataSource,
        tableRef: DbTableRef,
        name: String,
        multiple: Boolean,
        newType: DbColumnType
    ) {

        dataSource.withMetaData { metaData ->

            metaData.getColumns(null, tableRef.schema.ifEmpty { "%" }, tableRef.table, name)
                .use { rs ->
                    if (!rs.next()) {
                        error("Column doesn't found in table '$tableRef' with name '$name'")
                    }
                    val typeName = rs.getString(COLUMN_TYPE_NAME)
                    val (currentType, currentMultiple) = getColumnType(typeName)
                    if (currentType == newType && currentMultiple == multiple) {
                        // everything ok
                    } else {
                        if (multiple && !currentMultiple && newType != DbColumnType.JSON) {
                            // automatic conversion of array to single value column is not available
                            // it is not a huge problem because it affects only on fuzzy searching
                            dataSource.updateSchema(
                                "ALTER TABLE ${tableRef.fullName} " +
                                    "ALTER \"$name\" " +
                                    "TYPE ${getColumnSqlType(currentType, true)} " +
                                    "USING array[\"$name\"];"
                            )
                        }
                        val isMultipleColumn = multiple || currentMultiple
                        if (currentType != newType) {
                            var conversion = if (currentType == DbColumnType.TEXT && newType == DbColumnType.JSON) {
                                "jsonb"
                            } else if (newType == DbColumnType.TEXT && CONVERTABLE_TO_TEXT_TYPES.contains(currentType)) {
                                "text"
                            } else if (currentType == DbColumnType.DATE && newType == DbColumnType.DATETIME) {
                                "timestamp AT TIME ZONE 'UTC'"
                            } else {
                                error(
                                    "Conversion of column '$name' from type $currentType " +
                                        "to $newType is not supported. " +
                                        "Multiple flag: $currentMultiple -> $multiple"
                                )
                            }
                            if (isMultipleColumn) {
                                conversion = "$conversion[]"
                            }
                            dataSource.updateSchema(
                                "ALTER TABLE ${tableRef.fullName} " +
                                    "ALTER \"$name\" " +
                                    "TYPE ${getColumnSqlType(newType, isMultipleColumn)} " +
                                    "USING \"$name\"::$conversion;"
                            )
                        }
                    }
                }
        }
    }

    override fun createFkConstraints(
        dataSource: DbDataSource,
        tableRef: DbTableRef,
        constraints: List<DbFkConstraint>
    ) {

        constraints.forEach { constraint ->

            if (constraint.name.isBlank()) {
                error("Constraint name is missing: $constraint")
            }

            val query = StringBuilder()
            query.append("ALTER TABLE ")
                .append(tableRef.fullName)
                .append(" ADD CONSTRAINT \"")
                .append(constraint.name)
                .append("\" FOREIGN KEY (\"")
                .append(constraint.baseColumnName)
                .append("\") REFERENCES ")
                .append(constraint.referencedTable.fullName)
                .append(" (\"")
                .append(constraint.referencedColumn)
                .append("\")")

            if (constraint.onDelete != FkCascadeActionOptions.NO_ACTION) {
                val action = constraint.onDelete.name.replace("_", " ")
                query.append(" ON DELETE ").append(action)
            }
            if (constraint.onUpdate != FkCascadeActionOptions.NO_ACTION) {
                val action = constraint.onUpdate.name.replace("_", " ")
                query.append(" ON UPDATE ").append(action)
            }
            query.append(";")

            dataSource.updateSchema(query.toString())
        }
    }

    override fun setColumnConstraints(
        dataSource: DbDataSource,
        tableRef: DbTableRef,
        columnName: String,
        constraints: List<DbColumnConstraint>
    ) {

        for (constraint in constraints) {

            val query = StringBuilder()
            query.append("ALTER TABLE ")
                .append(tableRef.fullName)
                .append(" ALTER COLUMN \"")
                .append(columnName)
                .append("\" SET ")
                .append(getColumnSqlConstraint(constraint))

            dataSource.updateSchema(query.toString())
        }
    }

    override fun createIndexes(dataSource: DbDataSource, tableRef: DbTableRef, indexes: List<DbIndexDef>) {

        indexes.forEach { index ->

            val query = StringBuilder()
            query.append("CREATE ")
            if (index.unique) {
                query.append("UNIQUE ")
            }
            query.append("INDEX ")
            if (index.name.isNotBlank()) {
                query.append("\"")
                    .append(index.name)
                    .append("\" ")
            }
            query.append("ON ${tableRef.fullName} (")
            query.append(
                index.columns.joinToString(",") {
                    if (!index.caseInsensitive) {
                        "\"$it\""
                    } else {
                        "LOWER(\"$it\")"
                    }
                }
            )
            query.append(")")

            dataSource.updateSchema(query.toString())
        }
    }

    override fun resetCache(dataSource: DbDataSource, tableRef: DbTableRef) {
        dataSource.updateSchema("DEALLOCATE ALL")
    }

    private fun getColumnType(fullTypeName: String): Pair<DbColumnType, Boolean> {

        val (typeName, multiple) = if (fullTypeName[0] == '_') {
            fullTypeName.substring(1) to true
        } else {
            fullTypeName to false
        }

        return when (typeName) {
            "bigserial" -> DbColumnType.BIGSERIAL
            "int4" -> DbColumnType.INT
            "float8" -> DbColumnType.DOUBLE
            "bool" -> DbColumnType.BOOLEAN
            "date" -> DbColumnType.DATE
            "timestamp" -> DbColumnType.DATETIME
            "timestamptz" -> DbColumnType.DATETIME
            "int8" -> DbColumnType.LONG
            "jsonb" -> DbColumnType.JSON
            "varchar" -> DbColumnType.TEXT
            "bytea" -> DbColumnType.BINARY
            "uuid" -> DbColumnType.UUID
            else -> error("Unknown type: $typeName")
        } to multiple
    }

    private fun getColumnSqlConstraintsWithSpaceIfNotEmpty(constraints: List<DbColumnConstraint>): String {
        if (constraints.isEmpty()) {
            return ""
        }
        return " " + constraints.joinToString(" ") {
            getColumnSqlConstraint(it)
        }
    }

    private fun getColumnSqlConstraint(constraint: DbColumnConstraint): String {
        return when (constraint) {
            DbColumnConstraint.NOT_NULL -> "NOT NULL"
            DbColumnConstraint.PRIMARY_KEY -> "PRIMARY KEY"
            DbColumnConstraint.UNIQUE -> "UNIQUE"
        }
    }

    private fun getColumnSqlType(type: DbColumnType, multiple: Boolean): String {

        val baseType = when (type) {
            DbColumnType.BIGSERIAL -> "BIGSERIAL"
            DbColumnType.INT -> "INT"
            DbColumnType.DOUBLE -> "DOUBLE PRECISION"
            DbColumnType.BOOLEAN -> "BOOLEAN"
            DbColumnType.DATE -> "DATE"
            DbColumnType.DATETIME -> "TIMESTAMPTZ"
            DbColumnType.LONG -> "BIGINT"
            DbColumnType.JSON -> "JSONB"
            DbColumnType.TEXT -> "VARCHAR"
            DbColumnType.BINARY -> "BYTEA"
            DbColumnType.UUID -> "UUID"
        }
        return if (multiple && type != DbColumnType.JSON) {
            "$baseType[]"
        } else {
            baseType
        }
    }
}
