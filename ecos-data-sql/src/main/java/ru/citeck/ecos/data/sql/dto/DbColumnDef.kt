package ru.citeck.ecos.data.sql.dto

data class DbColumnDef(
    val name: String,
    val type: DbColumnType,
    val multiple: Boolean,
    val constraints: List<DbColumnConstraint>
)
