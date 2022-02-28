package ru.citeck.ecos.data.sql.repo.find

data class DbFindRes<T>(
    val entities: List<T>,
    val totalCount: Long
) {

    companion object {

        val EMPTY = DbFindRes<Any>()

        @JvmStatic
        fun <T> empty(): DbFindRes<T> {
            @Suppress("UNCHECKED_CAST")
            return EMPTY as DbFindRes<T>
        }
    }

    constructor() : this(emptyList(), 0)

    fun <O> mapEntities(mapping: (T) -> O): DbFindRes<O> {
        val resEntities = entities.map { mapping.invoke(it) }
        return DbFindRes(resEntities, totalCount)
    }
}
