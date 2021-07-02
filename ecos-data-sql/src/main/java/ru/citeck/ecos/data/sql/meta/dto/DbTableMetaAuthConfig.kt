package ru.citeck.ecos.data.sql.meta.dto

data class DbTableMetaAuthConfig(
    val enabled: Boolean
) {
    companion object {
        val DEFAULT = DbTableMetaAuthConfig(false)
    }
}
