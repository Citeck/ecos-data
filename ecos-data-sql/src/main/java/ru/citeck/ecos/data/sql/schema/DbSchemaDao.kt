package ru.citeck.ecos.data.sql.schema

import ru.citeck.ecos.data.sql.dto.DbColumnConstraint
import ru.citeck.ecos.data.sql.dto.DbColumnDef
import ru.citeck.ecos.data.sql.dto.DbColumnType
import ru.citeck.ecos.data.sql.dto.DbIndexDef
import ru.citeck.ecos.data.sql.dto.fk.DbFkConstraint

interface DbSchemaDao {

    fun getColumns(): List<DbColumnDef>

    fun createTable(columns: List<DbColumnDef>)

    fun addColumns(columns: List<DbColumnDef>)

    fun setColumnType(name: String, multiple: Boolean, newType: DbColumnType)

    fun createIndexes(indexes: List<DbIndexDef>)

    fun createFkConstraints(constraints: List<DbFkConstraint>)

    fun setColumnConstraints(columnName: String, constraints: List<DbColumnConstraint>)
}
