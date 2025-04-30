package ru.citeck.ecos.data.sql.records.dao.mutate.operation

interface AttValuesContainer<T : Any> {

    fun addAll(values: Collection<*>)

    fun removeAll(values: Collection<*>)
}
