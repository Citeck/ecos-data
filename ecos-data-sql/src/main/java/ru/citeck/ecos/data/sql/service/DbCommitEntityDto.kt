package ru.citeck.ecos.data.sql.service

data class DbCommitEntityDto(

    val id: String,
    /**
     * Authorities which is allowed to read entity
     */
    val readAllowed: Set<String>,
    /**
     * Authorities which is not allowed to read entity
     */
    val readDenied: Set<String>
)
