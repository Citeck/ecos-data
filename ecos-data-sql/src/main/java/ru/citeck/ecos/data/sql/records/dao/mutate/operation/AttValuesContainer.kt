package ru.citeck.ecos.data.sql.records.dao.mutate.operation

interface AttValuesContainer<T : Any> {

    fun addAll(values: Collection<T>)

    fun removeAll(values: Collection<T>)
}
