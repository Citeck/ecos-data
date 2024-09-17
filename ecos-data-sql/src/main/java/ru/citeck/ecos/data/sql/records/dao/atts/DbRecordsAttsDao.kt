package ru.citeck.ecos.data.sql.records.dao.atts

import io.github.oshai.kotlinlogging.KotlinLogging
import ru.citeck.ecos.data.sql.records.dao.DbRecordsDaoCtx
import ru.citeck.ecos.data.sql.records.dao.query.DbFindQueryContext
import ru.citeck.ecos.data.sql.repo.entity.DbEntity
import ru.citeck.ecos.data.sql.service.expression.token.ExpressionToken
import ru.citeck.ecos.model.lib.type.dto.QueryPermsPolicy
import ru.citeck.ecos.records3.record.atts.schema.resolver.AttContext
import ru.citeck.ecos.records3.record.atts.value.AttValue
import ru.citeck.ecos.records3.record.atts.value.impl.EmptyAttValue
import ru.citeck.ecos.txn.lib.TxnContext

class DbRecordsAttsDao(
    private val daoCtx: DbRecordsDaoCtx
) {

    companion object {
        private val log = KotlinLogging.logger {}
    }

    private val config = daoCtx.config
    private val dataService = daoCtx.dataService

    fun getRecordsAtts(recordIds: List<String>): List<AttValue> {

        log.trace { "[Get record atts] Called for records $recordIds" }

        val queryCtx = DbFindQueryContext(
            daoCtx,
            config.typeRef.getLocalId(),
            false,
            null
        )

        AttContext.getCurrent()?.getSchemaAtt()?.inner?.forEach {
            queryCtx.registerSelectAtt(it.name, false)
        }

        log.trace { "[Get record atts] Context prepared for records $recordIds" }

        return TxnContext.doInTxn(readOnly = true) {
            recordIds.map { id ->
                if (id.isEmpty()) {
                    DbEmptyRecord(daoCtx)
                } else {

                    log.trace { "[Get record atts] Process $id inside transaction" }

                    val record = dataService.doWithPermsPolicy(QueryPermsPolicy.PUBLIC) {
                        dataService.findByExtId(id, queryCtx.expressionsCtx.getExpressions())
                    }?.let {
                        log.trace { "[Get record atts] Entity was found for id $id" }
                        DbRecord(daoCtx, queryCtx.expressionsCtx.mapEntityAtts(it), queryCtx)
                    }
                    log.trace { "[Get record atts] DbRecord was created for id $id" }

                    if (record == null || !record.isCurrentUserHasReadPerms()) {
                        log.trace { "[Get record atts] User doesn't have permissions for $id or record doesn't exists" }
                        EmptyAttValue.INSTANCE
                    } else {
                        log.trace { "[Get record atts] Record $id available and will be returned from getRecordAtts" }
                        record
                    }
                }
            }
        }
    }

    fun findDbEntityByExtId(extId: String, expressions: Map<String, ExpressionToken> = emptyMap()): DbEntity? {

        val entity = dataService.doWithPermsPolicy(QueryPermsPolicy.PUBLIC) {
            dataService.findByExtId(extId, expressions)
        } ?: return null

        if (DbRecord(daoCtx, entity).isCurrentUserHasReadPerms()) {
            return entity
        }
        return null
    }
}
