package ru.citeck.ecos.data.sql.records.dao.mutate

import ru.citeck.ecos.data.sql.records.dao.DbRecordsDaoCtx
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType
import ru.citeck.ecos.model.lib.attributes.dto.computed.ComputedAttDef

class MutationComputeContext(
    val daoCtx: DbRecordsDaoCtx
) {
    val preCalculatedComputedAtts: MutableMap<String, Any?> = HashMap()

    fun calculateAtt(record: Any, attId: String, attributeType: AttributeType, attComputedDef: ComputedAttDef): Any? {
        if (preCalculatedComputedAtts.containsKey(attId)) {
            return preCalculatedComputedAtts[attId]
        }
        daoCtx.computedAttsComponent ?: return null
        val computeRes = daoCtx.computedAttsComponent.computeAtt(
            record,
            attId,
            attributeType,
            attComputedDef
        )
        if (computeRes.stateful) {
            preCalculatedComputedAtts[attId] = computeRes.value
        }
        return computeRes.value
    }
}
