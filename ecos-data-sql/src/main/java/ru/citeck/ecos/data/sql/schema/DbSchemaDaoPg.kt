package ru.citeck.ecos.data.sql.schema

import mu.KotlinLogging
import ru.citeck.ecos.data.sql.datasource.DbDataSource
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
        return dataSource.withMetaData { metaData ->
            metaData.getColumns(null, tableRef.schema.ifEmpty { "%" }, tableRef.table, "%")
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
            queryBuilder.append("\"${it.name}\" ${getColumnSqlType(it.type, it.multiple)},")
        }
        queryBuilder.setLength(queryBuilder.length - 1)
        queryBuilder.append(")")

        dataSource.updateSchema(queryBuilder.toString())
        val tenantIndexPrefix = if (columns.any { it.name == DbEntity.TENANT }) {
            "\"${DbEntity.TENANT}\","
        } else {
            ""
        }
        if (columns.any { it.name == DbEntity.EXT_ID }) {
            dataSource.updateSchema(
                "CREATE UNIQUE INDEX ON ${tableRef.fullName} ($tenantIndexPrefix\"${DbEntity.EXT_ID}\")"
            )
        }
        if (columns.any { it.name == DbEntity.DELETED }) {
            dataSource.updateSchema(
                "CREATE INDEX ON ${tableRef.fullName} ($tenantIndexPrefix\"${DbEntity.DELETED}\")"
            )
        }
    }

    @Synchronized
    override fun addColumns(columns: List<DbColumnDef>) {

        if (columns.isEmpty()) {
            return
        }
        columns.forEach { column ->
            val query = "ALTER TABLE ${tableRef.fullName} " +
                "ADD COLUMN \"${column.name}\" ${getColumnSqlType(column.type, column.multiple)}"
            dataSource.updateSchema(query)
        }
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
                        error("Conversion $currentType to $newType is not supported. " +
                            "Multiple flag: $currentMultiple -> $multiple")
                    }
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
