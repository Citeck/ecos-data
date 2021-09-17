package ru.citeck.ecos.data.sql.pg

import mu.KotlinLogging
import ru.citeck.ecos.data.sql.datasource.DbDataSource
import ru.citeck.ecos.data.sql.dto.*
import ru.citeck.ecos.data.sql.dto.fk.DbFkConstraint
import ru.citeck.ecos.data.sql.dto.fk.FkCascadeActionOptions
import ru.citeck.ecos.data.sql.schema.DbSchemaDao
import ru.citeck.ecos.data.sql.utils.use

class DbSchemaDaoPg(
    private val dataSource: DbDataSource,
    private val tableRef: DbTableRef
) : DbSchemaDao {

    companion object {
        val log = KotlinLogging.logger {}

        const val COLUMN_TYPE_NAME = "TYPE_NAME"
        const val COLUMN_COLUMN_NAME = "COLUMN_NAME"
    }

    override fun getColumns(): List<DbColumnDef> {
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
                                DbColumnDef(
                                    it.getString(COLUMN_COLUMN_NAME),
                                    type,
                                    multiple,
                                    emptyList()
                                )
                            )
                        } catch (e: Exception) {
                            log.warn { "Column error: '${it.getString(COLUMN_COLUMN_NAME)}' ${e.message}" }
                        }
                    }
                    columns
                }
        }
    }

    @Synchronized
    override fun createTable(columns: List<DbColumnDef>) {

        if (tableRef.schema.isNotBlank()) {
            dataSource.query(
                "SELECT EXISTS(SELECT 1 FROM pg_namespace WHERE nspname = '${tableRef.schema}')", emptyList()
            ) {
                if (!it.next() || !it.getBoolean(1)) {
                    dataSource.updateSchema("CREATE SCHEMA IF NOT EXISTS \"${tableRef.schema}\"")
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
    }

    @Synchronized
    override fun addColumns(columns: List<DbColumnDef>) {

        if (columns.isEmpty()) {
            return
        }
        columns.forEach { column ->
            val query = "ALTER TABLE ${tableRef.fullName} " +
                "ADD COLUMN \"${column.name}\" " +
                getColumnSqlType(column.type, column.multiple) +
                getColumnSqlConstraintsWithSpaceIfNotEmpty(column.constraints)
            dataSource.updateSchema(query)
        }
    }

    @Synchronized
    override fun setColumnType(name: String, multiple: Boolean, newType: DbColumnType) {

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
                            } else if (currentType == DbColumnType.JSON && newType == DbColumnType.TEXT) {
                                "text"
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

    override fun createFkConstraints(constraints: List<DbFkConstraint>) {

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

    override fun createIndexes(indexes: List<DbIndexDef>) {

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
            query.append(index.columns.joinToString(",") { '"' + it + '"' })
            query.append(")")

            dataSource.updateSchema(query.toString())
        }
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
            when (it) {
                DbColumnConstraint.NOT_NULL -> "NOT NULL"
                DbColumnConstraint.PRIMARY_KEY -> "PRIMARY KEY"
                DbColumnConstraint.UNIQUE -> "UNIQUE"
            }
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
