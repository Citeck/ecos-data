package ru.citeck.ecos.data.sql.records.listener

import ru.citeck.ecos.commons.data.ObjectData
import ru.citeck.ecos.commons.exception.I18nRuntimeException
import ru.citeck.ecos.commons.utils.StringUtils
import ru.citeck.ecos.context.lib.auth.AuthContext
import ru.citeck.ecos.data.sql.records.dao.DbRecordsDaoCtx
import ru.citeck.ecos.data.sql.records.dao.DbRecordsDaoCtxAware
import ru.citeck.ecos.model.lib.aspect.dto.AspectInfo
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType
import ru.citeck.ecos.model.lib.type.dto.TypeInfo
import ru.citeck.ecos.model.lib.type.dto.WorkspaceScope
import ru.citeck.ecos.records2.RecordConstants
import ru.citeck.ecos.records2.predicate.PredicateService
import ru.citeck.ecos.records2.predicate.model.Predicates
import ru.citeck.ecos.records3.record.atts.schema.ScalarType
import ru.citeck.ecos.records3.record.dao.query.dto.query.RecordsQuery
import ru.citeck.ecos.txn.lib.TxnContext
import ru.citeck.ecos.txn.lib.action.TxnActionType
import ru.citeck.ecos.webapp.api.entity.EntityRef
import java.util.concurrent.atomic.AtomicBoolean

// todo: add checking of parent ref recursion based on ed_associations
class DbIntegrityCheckListener : DbRecordsListenerAdapter(), DbRecordsDaoCtxAware {

    companion object {
        private const val CONFIG_PARAM_UNIQUE = "unique"
    }

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
        val attsForUniqueCheck = mutableSetOf<String>()
        for (attributeId in changedAtts) {
            val attributeDef = attsById[attributeId] ?: continue
            if (attributeDef.mandatory) {
                attsForMandatoryCheck.add(attributeDef.id)
            }
            if (attributeDef.type == AttributeType.TEXT && !attributeDef.multiple &&
                attributeDef.config[CONFIG_PARAM_UNIQUE].asBoolean()
            ) {
                attsForUniqueCheck.add(attributeDef.id)
            }
            if (attributeDef.type == AttributeType.OPTIONS) {
                val options = ctx.computedAttsComponent?.getAttOptions(localRef, attributeDef.config)
                    ?.mapTo(HashSet()) { it.value } ?: continue
                val currentValue = ctx.recordsService.getAtt(
                    localRef,
                    attributeDef.id + "[]" + ScalarType.STR_SCHEMA
                ).asStrList()
                if (currentValue.isNotEmpty()) {
                    val invalidValues = currentValue.filter { !options.contains(it) }
                    if (invalidValues.isNotEmpty()) {
                        val invalidValuesMsg = if (invalidValues.size == 1) {
                            "Invalid value '${invalidValues[0]}'"
                        } else {
                            "Invalid values [${invalidValues.joinToString(", ") { "\"$it\"" }}]"
                        }
                        error(
                            "$invalidValuesMsg of options attribute: ${attributeDef.id} " +
                                "for record '$globalRef'. " +
                                "Allowed values: ${options.joinToString(", ")}"
                        )
                    }
                }
            }
        }
        checkMandatoryAtts(localRef, globalRef, attsForMandatoryCheck)
        checkUniqueAtts(data, localRef, globalRef, attsForUniqueCheck)
    }

    private fun checkMandatoryAtts(localRef: EntityRef, globalRef: EntityRef, attsToCheck: Set<String>) {
        if (attsToCheck.isEmpty()) {
            return
        }
        val recordAtts = AuthContext.runAsSystem {
            ctx.recordsService.getAtts(localRef, attsToCheck.associateWith { it + ScalarType.RAW_SCHEMA })
        }.getAtts()

        val emptyMandatoryAtts = attsToCheck.filter {
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

    private fun checkUniqueAtts(
        data: RecordData,
        localRef: EntityRef,
        globalRef: EntityRef,
        attsToCheck: Set<String>
    ) {
        if (attsToCheck.isEmpty()) {
            return
        }

        val valueByAtt = ctx.recordsService.getAtts(localRef, attsToCheck).getAtts()
        val notNullValueByAtt = ObjectData.create()
        valueByAtt.forEach { key, value ->
            if (value.isNotNull() && value.isNotEmpty()) {
                notNullValueByAtt[key] = value
            }
        }

        val predicates = notNullValueByAtt.map { key, value ->
            Predicates.eq(key, value)
        }
        if (predicates.isEmpty()) {
            return
        }

        val queryBuilder = RecordsQuery.create()
            .withSourceId(localRef.getSourceId())
            .withLanguage(PredicateService.LANGUAGE_PREDICATE)
            .withQuery(
                Predicates.and(
                    Predicates.or(predicates),
                    Predicates.notEq(RecordConstants.ATT_ID, localRef.getLocalId())
                )
            )
            .withMaxItems(1)

        if (data.typeInfo.workspaceScope == WorkspaceScope.PRIVATE) {
            val workspace = ctx.recordsService.getAtt(localRef, "${RecordConstants.ATT_WORKSPACE}?localId").asText()
            if (StringUtils.isNotBlank(workspace)) {
                queryBuilder.withWorkspaces(listOf(workspace))
            }
        }

        val query = queryBuilder.build()
        val foundRecord = ctx.recordsService.queryOne(query, attsToCheck)

        if (foundRecord != null) {
            val recordAtts = foundRecord.getAtts()
            val notUniqueAtts = ObjectData.create()
            recordAtts.forEach { key, value ->
                if (value.isNotNull() && notNullValueByAtt[key] == value) {
                    notUniqueAtts[key] = value
                }
            }

            throw I18nRuntimeException(
                "ecos-data.has-non-unique-attributes",
                mapOf(
                    "recordRef" to globalRef,
                    "attributes" to notUniqueAtts
                )
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
