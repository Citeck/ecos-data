package ru.citeck.ecos.data.sql.records.dao

import ru.citeck.ecos.data.sql.content.EcosContentService
import ru.citeck.ecos.data.sql.dto.DbTableRef
import ru.citeck.ecos.data.sql.ecostype.DbEcosTypeService
import ru.citeck.ecos.data.sql.records.DbRecordsDao
import ru.citeck.ecos.data.sql.records.DbRecordsDaoConfig
import ru.citeck.ecos.data.sql.records.dao.content.DbRecContentHandler
import ru.citeck.ecos.data.sql.records.dao.events.DbRecEventsHandler
import ru.citeck.ecos.data.sql.records.dao.mutate.RecMutAssocHandler
import ru.citeck.ecos.data.sql.records.dao.mutate.RecMutConverter
import ru.citeck.ecos.data.sql.records.dao.mutate.operation.RecMutAttOperationsHandler
import ru.citeck.ecos.data.sql.records.listener.DbRecordsListener
import ru.citeck.ecos.data.sql.records.refs.DbRecordRefService
import ru.citeck.ecos.records3.RecordsService
import ru.citeck.ecos.webapp.api.authority.EcosAuthoritiesApi
import ru.citeck.ecos.webapp.api.content.EcosContentApi

class DbRecordsDaoCtx(
    val appName: String,
    val sourceId: String,
    val tableRef: DbTableRef,
    val config: DbRecordsDaoConfig,
    val contentService: EcosContentService?,
    val recordRefService: DbRecordRefService,
    val ecosTypeService: DbEcosTypeService,
    val recordsService: RecordsService,
    val contentApi: EcosContentApi?,
    val authoritiesApi: EcosAuthoritiesApi?,
    val listeners: List<DbRecordsListener>,
    val recordsDao: DbRecordsDao
) {
    val recContentHandler: DbRecContentHandler by lazy { DbRecContentHandler(this) }
    val mutConverter = RecMutConverter()
    val mutAssocHandler: RecMutAssocHandler by lazy { RecMutAssocHandler(this) }
    val mutAttOperationHandler: RecMutAttOperationsHandler by lazy { RecMutAttOperationsHandler() }
    val recEventsHandler: DbRecEventsHandler by lazy { DbRecEventsHandler(this) }
}
