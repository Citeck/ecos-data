package ru.citeck.ecos.data.sql.meta.dto

data class DbTableMetaConfig(
    val auth: DbTableMetaAuthConfig
) {
    companion object {
        val DEFAULT = DbTableMetaConfig(
            DbTableMetaAuthConfig.DEFAULT
        )
    }
}
