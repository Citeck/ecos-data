package ru.citeck.ecos.data.sql.repo.find

data class DbFindRes<T>(
    val entities: List<T>,
    val totalCount: Long
)
