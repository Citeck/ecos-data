package ru.citeck.ecos.data.sql.meta.schema

import ru.citeck.ecos.commons.data.DataValue

interface DbSchemaMetaService {

    companion object {
        const val ROOT_SCOPE = "ROOT"
    }

    fun getScoped(scope: String): DbSchemaMetaService

    fun getValue(key: List<String>): DataValue

    fun getValue(key: String): DataValue

    fun <T : Any> getValue(key: List<String>, orElse: T): T

    fun <T : Any> getValue(key: String, orElse: T): T

    fun setValue(key: String, value: Any?)

    fun setValue(key: List<String>, value: Any?)

    fun resetColumnsCache()
}
