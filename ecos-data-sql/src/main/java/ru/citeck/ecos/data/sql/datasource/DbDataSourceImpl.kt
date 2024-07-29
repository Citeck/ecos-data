package ru.citeck.ecos.data.sql.datasource

import io.github.oshai.kotlinlogging.KotlinLogging
import ru.citeck.ecos.commons.task.SystemScheduler
import ru.citeck.ecos.commons.task.schedule.Schedules
import ru.citeck.ecos.micrometer.EcosMicrometerContext
import ru.citeck.ecos.txn.lib.TxnContext
import ru.citeck.ecos.txn.lib.transaction.Transaction
import ru.citeck.ecos.webapp.api.datasource.JdbcDataSource
import java.sql.*
import java.time.Duration
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

class DbDataSourceImpl(
    private val dataSource: JdbcDataSource,
    private val micrometerContext: EcosMicrometerContext = EcosMicrometerContext.NOOP
) : DbDataSource {

    companion object {
        private val log = KotlinLogging.logger {}

        private val activeQueries = Collections.newSetFromMap(ConcurrentHashMap<ActiveQueryData, Boolean>())

        init {
            SystemScheduler.schedule(
                "ecos-dbds-active-query-checker",
                Schedules.fixedDelay(Duration.ofSeconds(10))
            ) {
                val currentTime = System.currentTimeMillis()
                for (queryData in activeQueries) {
                    queryData.reportInProgressIfRequired(currentTime)
                }
            }
        }
    }

    private val currentThreadTxn = ThreadLocal<TxnState>()
    private val thSchemaCommands = ThreadLocal<MutableList<String>>()
    private val thSchemaMock = ThreadLocal.withInitial { false }

    private val javaDataSource = dataSource.getJavaDataSource()

    override fun updateSchema(query: String) {
        withConnection { connection ->
            thSchemaCommands.get()?.add(query)
            if (!thSchemaMock.get()) {
                execSqlQuery(query, DbDsQueryType.SCHEMA_UPDATE, emptyList()) { query ->
                    connection.createStatement().executeUpdate(query)
                }
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
            val result = execSqlQuery(query, DbDsQueryType.SELECT, params) { query ->
                connection.prepareStatement(query).use { statement ->
                    setParams(connection, statement, params)
                    statement.executeQuery().use { action.invoke(it) }
                }
            }
            result
        }
    }

    override fun update(query: String, params: List<Any?>): List<Long> {
        return withConnection { connection ->
            execSqlQuery(query, DbDsQueryType.UPDATE, params) { query ->
                connection.prepareStatement(query).use { statement ->
                    setParams(connection, statement, params)
                    if (query.contains("RETURNING id")) {
                        val ids = arrayListOf<Long>()
                        statement.executeQuery().use { rs ->
                            while (rs.next()) {
                                ids.add(rs.getLong(1))
                            }
                        }
                        ids
                    } else {
                        listOf(statement.executeUpdate().toLong())
                    }
                }
            }
        }
    }

    private inline fun <T> execSqlQuery(
        query: String,
        type: DbDsQueryType,
        params: List<Any?>,
        crossinline impl: (String) -> T
    ): T {

        val transaction = TxnContext.getTxnOrNull()
        val activeQueryData = ActiveQueryData(transaction, Thread.currentThread(), query)

        activeQueries.add(activeQueryData)

        try {
            val res = micrometerContext.createObs(
                DbDsQueryObsCtx(
                    dataSource,
                    query,
                    type,
                    params
                )
            ).observe {
                impl.invoke(query)
            }
            activeQueryData.status = QueryStatus.COMPLETED
            return res
        } catch (e: Throwable) {
            activeQueryData.status = QueryStatus.ERROR
            if (e is InterruptedException) {
                Thread.currentThread().interrupt()
            }
            throw e
        } finally {
            activeQueries.remove(activeQueryData)
            val time = System.currentTimeMillis() - activeQueryData.startTime
            if (activeQueryData.status == QueryStatus.ERROR) {
                log.error { activeQueryData.getMessage(time) }
            } else if (type == DbDsQueryType.SCHEMA_UPDATE) {
                if (time < 30_000) {
                    log.info { activeQueryData.getMessage(time) }
                } else {
                    log.warn { activeQueryData.getMessage(time) }
                }
            } else {
                if (time < 10_000) {
                    log.debug { activeQueryData.getMessage(time) }
                } else if (time < 30_000) {
                    log.info { activeQueryData.getMessage(time) }
                } else {
                    log.warn { activeQueryData.getMessage(time) }
                }
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
        return withTransaction(readOnly, false, action)
    }

    override fun <T> withTransaction(readOnly: Boolean, requiresNew: Boolean, action: () -> T): T {
        val currentTxn = currentThreadTxn.get()
        val isCurrentTxnExists = currentTxn != null && !currentTxn.connection.isClosed
        if (isCurrentTxnExists && !requiresNew) {
            if (currentTxn.readOnly && !readOnly) {
                error("Write transaction can't be started from readOnly context")
            }
            return if (currentTxn.readOnly == readOnly) {
                doAndLogSlowProcessing("action with existing same ro connection") { action.invoke() }
            } else {
                currentThreadTxn.set(TxnState(readOnly, currentTxn.connection))
                try {
                    doAndLogSlowProcessing("action with existing different ro connection") { action.invoke() }
                } finally {
                    currentThreadTxn.set(currentTxn)
                }
            }
        } else {
            return doAndLogSlowProcessing("get connection") { javaDataSource.connection }.use { connection ->
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
                    val actionRes = doAndLogSlowProcessing("action with new connection") { action.invoke() }
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
                    if (requiresNew && isCurrentTxnExists) {
                        currentThreadTxn.set(currentTxn)
                    } else {
                        currentThreadTxn.remove()
                    }
                }
            }
        }
    }

    private inline fun <T> doAndLogSlowProcessing(description: String, crossinline action: () -> T): T {
        val started = System.currentTimeMillis()
        val result = action.invoke()
        val elapsedTime = System.currentTimeMillis() - started

        if (elapsedTime < 100) {
            return result
        }

        when (elapsedTime) {
            in 100..500 -> log.trace { "$description elapsed time: $elapsedTime ms" }
            in 500..2000 -> log.info { "$description elapsed time: $elapsedTime ms" }
            in 2000..10000 -> log.warn { "$description elapsed time: $elapsedTime ms" }
            else -> log.error { "$description elapsed time: $elapsedTime ms" }
        }

        return result
    }

    class TxnState(
        val readOnly: Boolean,
        val connection: Connection
    )

    private class ActiveQueryData(
        val transaction: Transaction?,
        val thread: Thread,
        val query: String
    ) {
        companion object {
            const val IN_PROGRESS_REPORT_TIME = 60_000
            val MAX_IN_PROGRESS_REPORT_DELAY = Duration.ofMinutes(10).toMillis()
        }

        val startTime = System.currentTimeMillis()
        var nextInProgressReportTime = AtomicLong(startTime + IN_PROGRESS_REPORT_TIME)
        var inProgressReportCounter = 0
        var status = QueryStatus.IN_PROGRESS

        fun reportInProgressIfRequired(currentTime: Long) {
            if (status != QueryStatus.IN_PROGRESS || nextInProgressReportTime.get() > currentTime) {
                return
            }
            log.warn { getMessage(currentTime - startTime) }
            inProgressReportCounter++
            var nextReportDelay = (inProgressReportCounter + 1L) * IN_PROGRESS_REPORT_TIME
            if (nextReportDelay > MAX_IN_PROGRESS_REPORT_DELAY) {
                nextReportDelay = MAX_IN_PROGRESS_REPORT_DELAY
            }
            nextInProgressReportTime.set(System.currentTimeMillis() + nextReportDelay)
        }

        fun getMessage(time: Long): String {
            return "[SQL][${thread.name}][${transaction?.getId()?.toString() ?: "no-txn"}][$status][$time] $query"
        }

        override fun equals(other: Any?): Boolean {
            return this === other
        }

        override fun hashCode(): Int {
            return System.identityHashCode(this)
        }
    }

    private enum class QueryStatus {
        IN_PROGRESS,
        COMPLETED,
        ERROR
    }
}
