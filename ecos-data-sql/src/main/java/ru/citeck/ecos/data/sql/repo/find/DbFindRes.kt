package ru.citeck.ecos.data.sql.repo.find

data class DbFindRes<T>(
    val entities: List<T>,
    val totalCount: Long
) {
    constructor() : this(emptyList(), 0)

    fun <O> mapEntities(mapping: (T) -> O): DbFindRes<O> {
        val resEntities = entities.map { mapping.invoke(it) }
        return DbFindRes(resEntities, totalCount)
    }
}
