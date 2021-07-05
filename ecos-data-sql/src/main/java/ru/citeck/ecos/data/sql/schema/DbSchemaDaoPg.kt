package ru.citeck.ecos.data.sql.schema

import mu.KotlinLogging
import ru.citeck.ecos.data.sql.datasource.DbDataSource
import ru.citeck.ecos.data.sql.dto.DbColumnConstraint
import ru.citeck.ecos.data.sql.dto.DbColumnDef
import ru.citeck.ecos.data.sql.dto.DbColumnType
import ru.citeck.ecos.data.sql.dto.DbTableRef
import ru.citeck.ecos.data.sql.repo.entity.DbEntity

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
        return getColumns("%")
    }

    private fun getColumns(namePattern: String): List<DbColumnDef> {
        return dataSource.withMetaData { metaData ->
            metaData.getColumns(null, tableRef.schema.ifEmpty { "%" }, tableRef.table, namePattern)
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

        onColumnsCreated(columns, true)
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
        onColumnsCreated(columns, false)
    }

    @Synchronized
    override fun setColumnType(name: String, multiple: Boolean, newType: DbColumnType) {

        dataSource.withMetaData { metaData ->

            metaData.getColumns(null, tableRef.schema.ifEmpty { "%" }, tableRef.table, name)
                .use { rs ->
                    val typeName = rs.getString(COLUMN_TYPE_NAME)
                    val (currentType, currentMultiple) = getColumnType(typeName)
                    if (currentType == newType && currentMultiple == multiple) {
                        // everything ok
                    } else {
                        error(
                            "Conversion $currentType to $newType is not supported. " +
                                "Multiple flag: $currentMultiple -> $multiple"
                        )
                    }
                }
        }
    }

    private fun onColumnsCreated(columns: List<DbColumnDef>, tableJustCreated: Boolean) {

        val hasTenant = if (columns.any { it.name == DbEntity.TENANT }) {
            true
        } else if (!tableJustCreated) {
            getColumns(DbEntity.TENANT).isNotEmpty()
        } else {
            false
        }

        val tenantIndexPrefix = if (hasTenant) {
            "\"${DbEntity.TENANT}\","
        } else {
            ""
        }
        if (columns.any { it.name == DbEntity.EXT_ID }) {
            dataSource.updateSchema(
                "CREATE UNIQUE INDEX ON ${tableRef.fullName} ($tenantIndexPrefix\"${DbEntity.EXT_ID}\")"
            )
        }
        listOf(DbEntity.DELETED, DbEntity.MODIFIED, DbEntity.CREATED).forEach { indexedColumn ->
            if (columns.any { it.name == indexedColumn }) {
                dataSource.updateSchema(
                    "CREATE INDEX ON ${tableRef.fullName} ($tenantIndexPrefix\"$indexedColumn\")"
                )
            }
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
            "timestamp" -> DbColumnType.DATETIME
            "int8" -> DbColumnType.LONG
            "jsonb" -> DbColumnType.JSON
            "varchar" -> DbColumnType.TEXT
            "bytea" -> DbColumnType.BINARY
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
            DbColumnType.DATETIME -> "TIMESTAMP WITHOUT TIME ZONE"
            DbColumnType.LONG -> "BIGINT"
            DbColumnType.JSON -> "JSONB"
            DbColumnType.TEXT -> "VARCHAR"
            DbColumnType.BINARY -> "BYTEA"
        }
        return if (multiple) {
            "$baseType[]"
        } else {
            baseType
        }
    }
}
