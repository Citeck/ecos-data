package ru.citeck.ecos.data.sql.datasource

import mu.KotlinLogging
import ru.citeck.ecos.data.sql.utils.use
import ru.citeck.ecos.webapp.api.datasource.JdbcDataSource
import java.sql.*

class DbDataSourceImpl(private val dataSource: JdbcDataSource) : DbDataSource {

    companion object {
        private val log = KotlinLogging.logger {}
    }

    private val currentThreadTxn = ThreadLocal<TxnState>()
    private val thSchemaCommands = ThreadLocal<MutableList<String>>()
    private val thSchemaMock = ThreadLocal.withInitial { false }

    private val javaDataSource = dataSource.getJavaDataSource()

    override fun updateSchema(query: String) {
        withConnection { connection ->
            thSchemaCommands.get()?.add(query)
            if (!thSchemaMock.get()) {
                log.info { "Schema update: $query" }
                connection.createStatement().executeUpdate(query)
            }
        }
    }

    override fun <T> withMetaData(action: (DatabaseMetaData) -> T): T {
        return withConnection { connection ->
            action.invoke(connection.metaData)
        }
    }

    override fun <T> query(query: String, params: List<Any?>, action: (ResultSet) -> T): T {
        return withConnection { connection ->
            val queryStart = System.currentTimeMillis()
            val result = try {
                connection.prepareStatement(query).use { statement ->
                    setParams(connection, statement, params)
                    statement.executeQuery().use { action.invoke(it) }
                }
            } finally {
                logQuery(System.currentTimeMillis() - queryStart, query)
            }
            result
        }
    }

    override fun update(query: String, params: List<Any?>): Long {
        return withConnection { connection ->
            val queryStart = System.currentTimeMillis()
            val result = try {
                connection.prepareStatement(query).use { statement ->
                    setParams(connection, statement, params)
                    if (query.contains("RETURNING id")) {
                        statement.executeQuery().use { rs ->
                            rs.next()
                            rs.getLong(1)
                        }
                    } else {
                        statement.executeUpdate().toLong()
                    }
                }
            } finally {
                logQuery(System.currentTimeMillis() - queryStart, query)
            }
            result
        }
    }

    private fun logQuery(time: Long, query: String) {
        if (time < 1000) {
            log.debug { "[SQL][$time] $query" }
        } else if (time < 5000) {
            log.info { "[SQL][$time] $query" }
        } else if (time < 10_000) {
            log.warn { "[SQL][$time] $query" }
        }
    }

    private fun setParams(connection: Connection, statement: PreparedStatement, params: List<Any?>) {
        params.forEachIndexed { idx, value ->
            if (value is Array<*> && value.isArrayOf<Timestamp>()) {
                val array = connection.createArrayOf("timestamp", value)
                statement.setObject(idx + 1, array)
            } else {
                statement.setObject(idx + 1, value)
            }
        }
    }

    override fun <T> withSchemaMock(action: () -> T): T {
        if (thSchemaMock.get()) {
            return action.invoke()
        }
        thSchemaMock.set(true)
        try {
            return action.invoke()
        } finally {
            thSchemaMock.set(false)
        }
    }

    override fun watchSchemaCommands(action: () -> Unit): List<String> {
        val commandsBefore = thSchemaCommands.get()
        val commandsList = mutableListOf<String>()
        thSchemaCommands.set(commandsList)
        try {
            action.invoke()
        } finally {
            if (commandsBefore == null) {
                thSchemaCommands.remove()
            } else {
                thSchemaCommands.set(commandsBefore)
                commandsBefore.addAll(commandsList)
            }
        }
        return commandsList
    }

    private fun <T> withConnection(action: (Connection) -> T): T {
        val currentConn = currentThreadTxn.get() ?: error("Transaction is not active")
        return action.invoke(currentConn.connection)
    }

    override fun <T> withTransaction(readOnly: Boolean, action: () -> T): T {
        val currentTxn = currentThreadTxn.get()
        if (currentTxn != null && !currentTxn.connection.isClosed) {
            if (currentTxn.readOnly && !readOnly) {
                error("Write transaction can't be started from readOnly context")
            }
            return if (currentTxn.readOnly == readOnly) {
                action.invoke()
            } else {
                currentThreadTxn.set(TxnState(readOnly, currentTxn.connection))
                try {
                    action.invoke()
                } finally {
                    currentThreadTxn.set(currentTxn)
                }
            }
        } else {
            return javaDataSource.connection.use { connection ->
                val autoCommitBefore = connection.autoCommit
                val readOnlyBefore = connection.isReadOnly
                if (!dataSource.isManaged()) {
                    if (autoCommitBefore) {
                        connection.autoCommit = false
                    }
                    if (readOnlyBefore != readOnly) {
                        connection.isReadOnly = readOnly
                    }
                }
                try {
                    currentThreadTxn.set(TxnState(readOnly, connection))
                    val actionRes = action.invoke()
                    if (!dataSource.isManaged()) {
                        connection.commit()
                    }
                    actionRes
                } catch (originalEx: Throwable) {
                    if (!dataSource.isManaged()) {
                        try {
                            connection.rollback()
                        } catch (e: Throwable) {
                            originalEx.addSuppressed(e)
                        }
                    }
                    throw originalEx
                } finally {
                    if (!dataSource.isManaged()) {
                        if (autoCommitBefore) {
                            connection.autoCommit = true
                        }
                        if (readOnlyBefore != readOnly) {
                            connection.isReadOnly = readOnlyBefore
                        }
                    }
                    currentThreadTxn.remove()
                }
            }
        }
    }

    class TxnState(
        val readOnly: Boolean,
        val connection: Connection
    )
}
