package ru.citeck.ecos.data.sql.datasource

import java.sql.DatabaseMetaData
import java.sql.ResultSet

interface DbDataSource {

    fun updateSchema(query: String)

    fun <T> query(query: String, params: List<Any?>, action: (ResultSet) -> T): T

    /**
     * Return updated rows count or id of last inserted column if query contains 'RETURNING id'
     */
    fun update(query: String, params: List<Any?>): List<Long>

    fun <T> withSchemaMock(action: () -> T): T

    fun watchSchemaCommands(action: () -> Unit): List<String>

    fun <T> withMetaData(action: (DatabaseMetaData) -> T): T

    fun <T> withTransaction(readOnly: Boolean, action: () -> T): T
}
