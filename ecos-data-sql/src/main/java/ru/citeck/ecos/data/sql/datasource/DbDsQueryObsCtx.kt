package ru.citeck.ecos.data.sql.datasource

import ru.citeck.ecos.micrometer.obs.EcosObsContext
import ru.citeck.ecos.webapp.api.datasource.JdbcDataSource

class DbDsQueryObsCtx(
    val dataSource: JdbcDataSource,
    val query: String,
    val type: DbDsQueryType,
    val params: List<Any?>
) : EcosObsContext(NAME) {
    companion object {
        const val NAME = "ecos.data.dbds.query"
    }
}
