package ru.citeck.ecos.data.sql.remote

import io.github.oshai.kotlinlogging.KotlinLogging
import ru.citeck.ecos.commons.data.DataValue
import ru.citeck.ecos.commons.json.Json
import ru.citeck.ecos.commons.utils.ReflectUtils
import ru.citeck.ecos.data.sql.context.DbDataSourceContext
import ru.citeck.ecos.data.sql.remote.action.DbRemoteActionExecutor
import ru.citeck.ecos.records3.RecordsService
import ru.citeck.ecos.webapp.api.EcosWebAppApi
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean

class DbRecordsRemoteActionsServiceImpl : DbRecordsRemoteActionsService {

    companion object {
        private val log = KotlinLogging.logger {}
    }

    private val executors = ConcurrentHashMap<String, ExecutorData>()
    private val initialized = AtomicBoolean()

    private lateinit var webAppApi: EcosWebAppApi
    private lateinit var recordsService: RecordsService
    private lateinit var dbDataSourceContext: DbDataSourceContext

    @Synchronized
    fun init(
        dbDataSourceContext: DbDataSourceContext,
        webAppApi: EcosWebAppApi,
        recordsService: RecordsService
    ) {
        this.webAppApi = webAppApi
        this.recordsService = recordsService
        this.dbDataSourceContext = dbDataSourceContext
        executors.values.forEach { it.executor.init(dbDataSourceContext, webAppApi, recordsService) }
        initialized.set(true)
    }

    override fun execute(actionType: String, config: Any): DataValue {
        val executor = executors[actionType] ?: error("Executor doesn't found: $actionType")
        val params = Json.mapper.convertNotNull(config, executor.paramsType)
        return DataValue.createAsIs(executor.executor.execute(params))
    }

    @Synchronized
    fun register(executor: DbRemoteActionExecutor<*>) {
        val paramsType = ReflectUtils.getGenericArg(executor::class.java, DbRemoteActionExecutor::class.java)
        if (paramsType == null) {
            log.warn { "Executor without arg type: $executor - ${executor.getType()}" }
            return
        }
        @Suppress("UNCHECKED_CAST")
        executors[executor.getType()] = ExecutorData(
            paramsType as Class<Any>,
            executor as DbRemoteActionExecutor<Any>
        )
        if (initialized.get()) {
            executor.init(dbDataSourceContext, webAppApi, recordsService)
        }
    }

    private class ExecutorData(
        val paramsType: Class<Any>,
        val executor: DbRemoteActionExecutor<Any>
    )
}
