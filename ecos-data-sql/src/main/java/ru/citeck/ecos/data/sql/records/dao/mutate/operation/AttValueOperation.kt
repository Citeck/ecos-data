package ru.citeck.ecos.data.sql.records.dao.mutate.operation

interface AttValueOperation {
    fun invoke(value: Any?): Any?
    fun getAttName(): String
}
