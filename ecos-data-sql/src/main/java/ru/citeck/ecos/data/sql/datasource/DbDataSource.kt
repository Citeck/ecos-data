package ru.citeck.ecos.data.sql.datasource

import java.sql.DatabaseMetaData
import java.sql.ResultSet

interface DbDataSource {

    fun updateSchema(query: String)

    fun <T> query(query: String, params: List<Any?>, action: (ResultSet) -> T): T

    /**
     * Return updated rows count, or the values of the returned column for a query with a
     * 'RETURNING' clause.
     */
    fun update(query: String, params: List<Any?>): List<Long>

    /**
     * [update] for a statement whose `RETURNING` clause names more than one column: [update] reads
     * the first and nothing else.
     *
     * Separate from [query] so that a write is observed as a write - the query type reaches the
     * metrics and the slow-query log, and a statement that inserts rows recorded there as a SELECT
     * is a lie an operator would have to work out for themselves.
     */
    fun <T> updateReturning(query: String, params: List<Any?>, action: (ResultSet) -> T): T

    fun <T> withSchemaMock(action: () -> T): T

    fun watchSchemaCommands(action: () -> Unit): List<String>

    fun <T> withMetaData(action: (DatabaseMetaData) -> T): T

    fun <T> withTransaction(readOnly: Boolean, action: () -> T): T

    fun <T> withTransaction(readOnly: Boolean, requiresNew: Boolean, action: () -> T): T
}
