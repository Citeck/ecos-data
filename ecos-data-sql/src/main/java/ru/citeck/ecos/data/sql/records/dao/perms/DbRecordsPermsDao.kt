package ru.citeck.ecos.data.sql.records.dao.perms

import io.github.oshai.kotlinlogging.KotlinLogging
import ru.citeck.ecos.context.lib.auth.AuthContext
import ru.citeck.ecos.context.lib.auth.data.SimpleAuthData
import ru.citeck.ecos.data.sql.perms.DbEntityPermsDto
import ru.citeck.ecos.data.sql.perms.DbEntityPermsService
import ru.citeck.ecos.data.sql.records.DbRecordsUtils
import ru.citeck.ecos.data.sql.records.dao.DbRecordsDaoCtx
import ru.citeck.ecos.data.sql.records.dao.atts.DbRecord
import ru.citeck.ecos.data.sql.records.perms.DbRecordAllowedAllPerms
import ru.citeck.ecos.data.sql.records.perms.DbRecordDeniedAllPerms
import ru.citeck.ecos.data.sql.records.perms.DbRecordPermsContext
import ru.citeck.ecos.data.sql.repo.entity.DbEntity
import ru.citeck.ecos.model.lib.utils.ModelUtils
import ru.citeck.ecos.records2.RecordConstants
import ru.citeck.ecos.records2.predicate.model.Predicates
import ru.citeck.ecos.records3.record.atts.schema.ScalarType
import ru.citeck.ecos.records3.record.request.RequestContext
import ru.citeck.ecos.txn.lib.TxnContext
import kotlin.system.measureTimeMillis

class DbRecordsPermsDao(
    private val daoCtx: DbRecordsDaoCtx
) {

    companion object {
        private val log = KotlinLogging.logger {}
    }

    private val serviceFactory = daoCtx.recordsServiceFactory
    private val permsComponent = daoCtx.permsComponent

    private val dataService = daoCtx.dataService
    private val entityPermsService: DbEntityPermsService = dataService.getTableContext().getPermsService()

    fun getRecordPerms(record: Any): DbRecordPermsContext {
        val auth = AuthContext.getCurrentRunAsAuth()
        val currentUser = auth.getUser()
        val currentAuthorities = DbRecordsUtils.getCurrentAuthorities(auth)
        return getRecordPerms(record, currentUser, currentAuthorities)
    }

    fun getRecordPerms(record: Any, user: String, authorities: Set<String>): DbRecordPermsContext {
        // Optimization to enable caching
        return RequestContext.doWithCtx(
            serviceFactory,
            {
                it.withReadOnly(true)
            },
            {
                TxnContext.doInTxn(true) {
                    AuthContext.runAsSystem {
                        val recordToGetPerms = if (record is String) {
                            daoCtx.attsDao.findDbEntityByExtId(record)?.let { DbRecord(daoCtx, it) }
                        } else {
                            record
                        }
                        val perms = if (authorities.contains(ModelUtils.WORKSPACE_SYSTEM_ROLE)) {
                            val workspaceId = daoCtx.recordsService.getAtt(
                                recordToGetPerms,
                                "${RecordConstants.ATT_WORKSPACE}${ScalarType.LOCAL_ID_SCHEMA}"
                            ).asText()
                            if (daoCtx.workspaceService.isSystemOrWsSystemAuth(
                                    SimpleAuthData(user, authorities.toList()),
                                    workspaceId
                                )
                            ) {
                                DbRecordAllowedAllPerms
                            } else {
                                DbRecordDeniedAllPerms
                            }
                        } else if (recordToGetPerms != null) {
                            permsComponent.getRecordPerms(user, authorities, recordToGetPerms)
                        } else {
                            DbRecordAllowedAllPerms
                        }
                        DbRecordPermsContext(perms)
                    }
                }
            }
        )
    }

    fun updatePermissions(records: List<String>) {
        val time = measureTimeMillis {
            TxnContext.doInTxn {
                val perms = AuthContext.runAsSystem { getEntitiesPerms(records) }
                entityPermsService.setReadPerms(perms)
            }
        }

        log.trace { "Update permissions for <$records> in $time ms" }
    }

    fun getEntitiesPerms(recordIds: Collection<String>): List<DbEntityPermsDto> {
        val recordIdsList: List<String> = if (recordIds is List<String>) {
            recordIds
        } else {
            ArrayList(recordIds)
        }
        val refIds = getEntityRefIds(recordIdsList)
        val result = arrayListOf<DbEntityPermsDto>()
        for ((idx, refId) in refIds.withIndex()) {
            if (refId != -1L) {
                result.add(
                    DbEntityPermsDto(
                        refId,
                        getRecordPerms(recordIdsList[idx]).getAuthoritiesWithReadPermission()
                    )
                )
            }
        }
        return result
    }

    private fun getEntityRefIds(recordIds: List<String>): List<Long> {
        val entitiesByExtId = dataService.findAll(
            Predicates.inVals(DbEntity.EXT_ID, recordIds)
        ).associateBy { it.extId }
        return recordIds.map {
            entitiesByExtId[it]?.refId ?: -1
        }
    }
}
