package ru.citeck.ecos.data.sql.dto

data class DbTableRef(
    val schema: String,
    val table: String
) {
    companion object {
        val EMPTY = DbTableRef("", "")
    }

    val fullName = if (schema.isEmpty()) {
        "\"$table\""
    } else {
        "\"$schema\".\"$table\""
    }

    fun withTable(table: String): DbTableRef {
        return DbTableRef(schema, table)
    }
}
