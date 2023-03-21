package ru.citeck.ecos.data.sql.schema

import ru.citeck.ecos.data.sql.datasource.DbDataSource
import ru.citeck.ecos.data.sql.dto.*
import ru.citeck.ecos.data.sql.dto.fk.DbFkConstraint

interface DbSchemaDao {

    fun getColumns(dataSource: DbDataSource, tableRef: DbTableRef): List<DbColumnDef>

    fun createTable(dataSource: DbDataSource, tableRef: DbTableRef, columns: List<DbColumnDef>)

    fun addColumns(dataSource: DbDataSource, tableRef: DbTableRef, columns: List<DbColumnDef>)

    fun setColumnType(dataSource: DbDataSource, tableRef: DbTableRef, name: String, multiple: Boolean, newType: DbColumnType)

    fun createIndexes(dataSource: DbDataSource, tableRef: DbTableRef, indexes: List<DbIndexDef>)

    fun createFkConstraints(dataSource: DbDataSource, tableRef: DbTableRef, constraints: List<DbFkConstraint>)

    fun setColumnConstraints(dataSource: DbDataSource, tableRef: DbTableRef, columnName: String, constraints: List<DbColumnConstraint>)

    fun resetCache(dataSource: DbDataSource, tableRef: DbTableRef)
}
