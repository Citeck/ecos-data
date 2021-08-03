package ru.citeck.ecos.data.sql.datasource

import mu.KotlinLogging
import ru.citeck.ecos.data.sql.utils.use
import java.lang.Exception
import java.sql.*
import javax.sql.DataSource

class DbDataSourceImpl(private val dataSource: DataSource) : DbDataSource {

    companion object {
        private val log = KotlinLogging.logger {}
    }

    private val currentThreadTxn = ThreadLocal<TxnState>()
    private val txnCommands = ThreadLocal<MutableList<String>>()

    override fun updateSchema(query: String) {
        withConnection { connection ->
            log.info { "Schema update: $query" }
            txnCommands.get()?.add(query)
            connection.createStatement().executeUpdate(query)
        }
    }

    override fun <T> withMetaData(action: (DatabaseMetaData) -> T): T {
        return withConnection { connection ->
            action.invoke(connection.metaData)
        }
    }

    override fun <T> query(query: String, params: List<Any?>, action: (ResultSet) -> T): T {
        return withConnection { connection ->
            log.debug { "Query: $query" }
            txnCommands.get()?.add(query)
            connection.prepareStatement(query).use { statement ->
                setParams(connection, statement, params)
                statement.executeQuery().use { action.invoke(it) }
            }
        }
    }

    override fun update(query: String, params: List<Any?>): Int {
        return withConnection { connection ->
            log.debug { "Update: $query" }
            txnCommands.get()?.add(query)
            connection.prepareStatement(query).use { statement ->
                setParams(connection, statement, params)
                statement.executeUpdate()
            }
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

    override fun watchCommands(action: () -> Unit): List<String> {
        val commandsList = mutableListOf<String>()
        txnCommands.set(commandsList)
        try {
            action.invoke()
        } finally {
            txnCommands.remove()
        }
        return commandsList
    }

    private fun <T> withConnection(action: (Connection) -> T): T {
        val currentConn = currentThreadTxn.get() ?: error("Transaction is not active")
        return action.invoke(currentConn.connection)
    }

    override fun <T> withTransaction(readOnly: Boolean, action: () -> T): T {
        val currentTxn = currentThreadTxn.get()
        if (currentTxn != null) {
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
            return dataSource.connection.use { connection ->
                val autoCommitBefore = connection.autoCommit
                val readOnlyBefore = connection.isReadOnly
                if (autoCommitBefore) {
                    connection.autoCommit = false
                }
                if (readOnlyBefore != readOnly) {
                    connection.isReadOnly = readOnly
                }
                try {
                    currentThreadTxn.set(TxnState(readOnly, connection))
                    val actionRes = action.invoke()
                    connection.commit()
                    actionRes
                } catch (e: Exception) {
                    connection.rollback()
                    throw e
                } finally {
                    if (autoCommitBefore) {
                        connection.autoCommit = true
                    }
                    if (readOnlyBefore != readOnly) {
                        connection.isReadOnly = readOnlyBefore
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
