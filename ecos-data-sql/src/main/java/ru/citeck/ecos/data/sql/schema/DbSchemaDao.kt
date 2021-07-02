package ru.citeck.ecos.data.sql.schema

import ru.citeck.ecos.data.sql.dto.DbColumnDef
import ru.citeck.ecos.data.sql.dto.DbColumnType

interface DbSchemaDao {

    fun getColumns(): List<DbColumnDef>

    fun createTable(columns: List<DbColumnDef>)

    fun addColumns(columns: List<DbColumnDef>)

    fun setColumnType(name: String, multiple: Boolean, newType: DbColumnType)
}
