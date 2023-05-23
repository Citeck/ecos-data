package ru.citeck.ecos.data.sql.repo.entity

import ru.citeck.ecos.data.sql.dto.DbIndexDef

interface DbEntityMapper<T> {

    fun getEntityIndexes(): List<DbIndexDef>

    fun getEntityColumns(): List<DbEntityColumn>

    fun convertToEntity(data: Map<String, Any?>): T

    fun convertToEntity(data: Map<String, Any?>, schemaVersion: Int): T

    fun convertToMap(entity: T): Map<String, Any?>

    fun getExtId(entity: T): String
}
