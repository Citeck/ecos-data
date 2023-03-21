package ru.citeck.ecos.data.sql.perms

data class DbEntityPermsDto(

    val entityRefId: Long,
    /**
     * Authorities which is allowed to read entity
     */
    val readAllowed: Set<String>,
    /**
     * Authorities which is not allowed to read entity
     */
    val readDenied: Set<String>
)
