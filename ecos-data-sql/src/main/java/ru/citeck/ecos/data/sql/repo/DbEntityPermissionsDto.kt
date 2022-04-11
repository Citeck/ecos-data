package ru.citeck.ecos.data.sql.repo

data class DbEntityPermissionsDto(

    val id: String,
    /**
     * Authorities which is allowed to read entity
     */
    val readAllowed: Set<Long>,
    /**
     * Authorities which is not allowed to read entity
     */
    val readDenied: Set<Long>
)
