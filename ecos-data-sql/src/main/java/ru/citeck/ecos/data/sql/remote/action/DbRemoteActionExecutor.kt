package ru.citeck.ecos.data.sql.remote.action

import ru.citeck.ecos.data.sql.context.DbDataSourceContext
import ru.citeck.ecos.records3.RecordsService
import ru.citeck.ecos.webapp.api.EcosWebAppApi

interface DbRemoteActionExecutor<in T : Any> {

    fun init(
        dbDataSourceContext: DbDataSourceContext,
        webAppApi: EcosWebAppApi,
        recordsService: RecordsService
    )

    fun execute(action: T): Any?

    fun getType(): String
}
