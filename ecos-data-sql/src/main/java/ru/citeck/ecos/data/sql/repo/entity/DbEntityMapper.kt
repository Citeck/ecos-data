package ru.citeck.ecos.data.sql.repo.entity

interface DbEntityMapper<T> {

    fun getEntityColumns(): List<DbEntityColumn>

    fun convertToEntity(data: Map<String, Any?>): T

    fun convertToMap(entity: T): Map<String, Any?>
}
