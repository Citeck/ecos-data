package ru.citeck.ecos.data.sql.dto

data class DbTableRef(
    val schema: String,
    val table: String
) {
    val fullName = if (schema.isEmpty()) {
        "\"$table\""
    } else {
        "\"$schema\".\"$table\""
    }
}
