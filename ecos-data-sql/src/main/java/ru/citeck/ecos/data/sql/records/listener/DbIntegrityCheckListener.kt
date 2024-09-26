package ru.citeck.ecos.data.sql.records.listener

import ru.citeck.ecos.context.lib.auth.AuthContext
import ru.citeck.ecos.data.sql.records.dao.DbRecordsDaoCtx
import ru.citeck.ecos.data.sql.records.dao.DbRecordsDaoCtxAware
import ru.citeck.ecos.model.lib.aspect.dto.AspectInfo
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.type.dto.TypeInfo
import ru.citeck.ecos.txn.lib.TxnContext
import ru.citeck.ecos.txn.lib.action.TxnActionType
import ru.citeck.ecos.webapp.api.entity.EntityRef
import java.util.concurrent.atomic.AtomicBoolean

// todo: add checking of parent ref recursion based on ed_associations
class DbIntegrityCheckListener : DbRecordsListenerAdapter(), DbRecordsDaoCtxAware {

    private lateinit var ctx: DbRecordsDaoCtx

    override fun onCreated(event: DbRecordCreatedEvent) {
        val txn = TxnContext.getTxn()
        val data = txn.getData(
            TxnDataKey(event.localRef)
        ) {
            RecordData(event.typeDef, event.aspects, created = true, isDraft = event.isDraft)
        }
        txn.addAction(TxnActionType.BEFORE_COMMIT, 0f) {
            checkIntegrity(event.localRef, event.globalRef, data)
        }
    }

    override fun onChanged(event: DbRecordChangedEvent) {

        val txn = TxnContext.getTxn()
        val isNewRecData = AtomicBoolean(false)
        val data = txn.getData(
            TxnDataKey(event.localRef)
        ) {
            isNewRecData.set(true)
            RecordData(event.typeDef, event.aspects, isDraft = event.isDraft)
        }
        data.typeInfo = event.typeDef
        data.aspectsInfo = event.aspects

        val keys = LinkedHashSet<String>(maxOf(event.before.size, event.after.size))
        keys.addAll(event.before.keys)
        keys.addAll(event.after.keys)
        keys.forEach {
            if (!isEqualValues(event.before[it], event.after[it])) {
                data.changedAtts.add(it)
            }
        }
        for (assoc in event.assocs) {
            data.changedAtts.add(assoc.assocId)
        }

        if (isNewRecData.get()) {
            txn.addAction(TxnActionType.BEFORE_COMMIT, 0f) {
                checkIntegrity(event.localRef, event.globalRef, data)
            }
        }
    }

    override fun onDeleted(event: DbRecordDeletedEvent) {
        val txn = TxnContext.getTxn()
        val data = txn.getData<RecordData>(TxnDataKey(event.localRef))
        if (data != null) {
            data.deleted = true
        }
    }

    private fun checkIntegrity(localRef: EntityRef, globalRef: EntityRef, data: RecordData) {

        if (data.deleted || data.isDraft) {
            return
        }

        val attsById = LinkedHashMap<String, AttributeDef>()
        fun registerAtts(atts: Collection<AttributeDef>) {
            atts.forEach {
                attsById[it.id] = it
            }
        }
        data.aspectsInfo.forEach { aspect ->
            registerAtts(aspect.attributes)
            registerAtts(aspect.systemAttributes)
        }
        registerAtts(data.typeInfo.model.attributes)
        registerAtts(data.typeInfo.model.systemAttributes)

        val changedAtts = if (data.created) {
            attsById.keys
        } else {
            data.changedAtts
        }
        val attsForMandatoryCheck = mutableSetOf<String>()
        for (attributeId in changedAtts) {
            val attributeDef = attsById[attributeId] ?: continue
            if (attributeDef.mandatory) {
                attsForMandatoryCheck.add(attributeDef.id)
            }
        }
        if (attsForMandatoryCheck.isEmpty()) {
            return
        }
        val recordAtts = AuthContext.runAsSystem {
            ctx.recordsService.getAtts(localRef, attsForMandatoryCheck.associateWith { "$it?raw" })
        }.getAtts()

        val emptyMandatoryAtts = attsForMandatoryCheck.filter {
            val value = recordAtts[it]
            value.isNull() || (value.isArray() || value.isObject() || value.isTextual()) && value.isEmpty()
        }
        if (emptyMandatoryAtts.isNotEmpty()) {
            error(
                "Mandatory attributes are empty: ${emptyMandatoryAtts.joinToString(", ")} " +
                    "for record $globalRef"
            )
        }
    }

    private fun isEqualValues(v0: Any?, v1: Any?): Boolean {
        if (v0 == v1) {
            return true
        }
        if (v0 == null || v1 == null) {
            return false
        }
        return false
    }

    private data class TxnDataKey(
        val ref: EntityRef
    )

    private class RecordData(
        var typeInfo: TypeInfo,
        var aspectsInfo: List<AspectInfo>,
        val changedAtts: MutableSet<String> = LinkedHashSet(),
        var created: Boolean = false,
        var isDraft: Boolean = false,
        var deleted: Boolean = false
    )

    override fun setRecordsDaoCtx(recordsDaoCtx: DbRecordsDaoCtx) {
        this.ctx = recordsDaoCtx
    }
}
