package ru.citeck.ecos.data.sql.records.dao

import ru.citeck.ecos.data.sql.content.DbContentService
import ru.citeck.ecos.data.sql.dto.DbTableRef
import ru.citeck.ecos.data.sql.ecostype.DbEcosModelService
import ru.citeck.ecos.data.sql.records.DbRecordsDao
import ru.citeck.ecos.data.sql.records.DbRecordsDaoConfig
import ru.citeck.ecos.data.sql.records.DbRecordsDeleteDao
import ru.citeck.ecos.data.sql.records.assocs.DbAssocsService
import ru.citeck.ecos.data.sql.records.dao.atts.DbRecord
import ru.citeck.ecos.data.sql.records.dao.content.DbRecContentHandler
import ru.citeck.ecos.data.sql.records.dao.events.DbRecEventsHandler
import ru.citeck.ecos.data.sql.records.dao.mutate.RecMutAssocHandler
import ru.citeck.ecos.data.sql.records.dao.mutate.RecMutConverter
import ru.citeck.ecos.data.sql.records.dao.mutate.operation.RecMutAttOperationsHandler
import ru.citeck.ecos.data.sql.records.listener.DbRecordsListener
import ru.citeck.ecos.data.sql.records.refs.DbGlobalRefCalculator
import ru.citeck.ecos.data.sql.records.refs.DbRecordRefService
import ru.citeck.ecos.data.sql.records.utils.DbAttValueUtils
import ru.citeck.ecos.data.sql.repo.entity.DbEntity
import ru.citeck.ecos.data.sql.service.DbDataService
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.delegation.service.DelegationService
import ru.citeck.ecos.records3.RecordsService
import ru.citeck.ecos.records3.record.atts.value.AttValuesConverter
import ru.citeck.ecos.webapp.api.authority.EcosAuthoritiesApi
import ru.citeck.ecos.webapp.api.content.EcosContentApi
import ru.citeck.ecos.webapp.api.entity.EntityRef
import ru.citeck.ecos.webapp.api.web.client.EcosWebClientApi

class DbRecordsDaoCtx(
    val appName: String,
    val sourceId: String,
    val tableRef: DbTableRef,
    val config: DbRecordsDaoConfig,
    val dataService: DbDataService<DbEntity>,
    val contentService: DbContentService?,
    val recordRefService: DbRecordRefService,
    val ecosTypeService: DbEcosModelService,
    val recordsService: RecordsService,
    val contentApi: EcosContentApi?,
    val authoritiesApi: EcosAuthoritiesApi?,
    val listeners: List<DbRecordsListener>,
    val recordsDao: DbRecordsDao,
    val attValuesConverter: AttValuesConverter,
    val webApiClient: EcosWebClientApi?,
    val delegationService: DelegationService,
    val assocsService: DbAssocsService,
    val globalRefCalculator: DbGlobalRefCalculator
) {
    val recContentHandler: DbRecContentHandler by lazy { DbRecContentHandler(this) }
    val mutConverter = RecMutConverter()
    val mutAssocHandler: RecMutAssocHandler by lazy { RecMutAssocHandler(this) }
    val mutAttOperationHandler: RecMutAttOperationsHandler by lazy { RecMutAttOperationsHandler() }
    val recEventsHandler: DbRecEventsHandler by lazy { DbRecEventsHandler(this) }
    val deleteDao: DbRecordsDeleteDao by lazy { DbRecordsDeleteDao(this) }

    fun getLocalRef(extId: String): EntityRef {
        return EntityRef.create(appName, sourceId, extId)
    }

    fun getGlobalRef(extId: String): EntityRef {
        return globalRefCalculator.getGlobalRef(appName, sourceId, extId)
    }

    fun getEntityMeta(entity: DbEntity): DbEntityMeta {

        val aspectsIds = DbAttValueUtils.collectLongValues(entity.attributes[DbRecord.ATT_ASPECTS])
        val aspectsRefs = recordRefService.getEntityRefsByIds(aspectsIds).toMutableSet()
        val typeId = entity.type.ifBlank {
            config.typeRef.getLocalId()
        }
        val typeInfo = ecosTypeService.getTypeInfoNotNull(typeId)
        typeInfo.aspects.forEach {
            aspectsRefs.add(it.ref)
        }
        val allAttributes = mutableMapOf<String, AttributeDef>()
        val nonSystemAtts = mutableMapOf<String, AttributeDef>()

        val aspectsInfo = ecosTypeService.getAspectsInfo(aspectsRefs)

        aspectsInfo.forEach { aspectInfo ->
            aspectInfo.attributes.forEach {
                allAttributes[it.id] = it
                nonSystemAtts[it.id] = it
            }
            aspectInfo.systemAttributes.forEach {
                allAttributes[it.id] = it
            }
        }
        typeInfo.model.attributes.forEach {
            allAttributes[it.id] = it
            nonSystemAtts[it.id] = it
        }
        typeInfo.model.systemAttributes.forEach {
            allAttributes[it.id] = it
        }

        val localRef = getLocalRef(entity.extId)
        val globalRef = getGlobalRef(entity.extId)

        val isDraft = entity.attributes[DbRecord.COLUMN_IS_DRAFT.name] == true

        return DbEntityMeta(
            localRef,
            globalRef,
            isDraft,
            typeInfo,
            aspectsInfo,
            nonSystemAtts,
            allAttributes
        )
    }
}
