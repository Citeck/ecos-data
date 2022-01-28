package ru.citeck.ecos.data.sql.dto

data class DbColumnIndexDef(
    val enabled: Boolean
) {
    companion object {
        val EMPTY = DbColumnIndexDef(false)
    }
}
