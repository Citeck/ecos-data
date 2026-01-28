package ru.citeck.ecos.data.sql.records.dao.mutate

import ru.citeck.ecos.commons.data.ObjectData
import ru.citeck.ecos.data.sql.records.dao.DbRecordsDaoCtx
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType
import ru.citeck.ecos.model.lib.attributes.dto.computed.ComputedAttDef
import ru.citeck.ecos.model.lib.attributes.dto.computed.ComputedAttStoringType
import ru.citeck.ecos.model.lib.attributes.dto.computed.ComputedAttType
import ru.citeck.ecos.model.lib.type.dto.TypeInfo
import ru.citeck.ecos.records2.RecordConstants
import ru.citeck.ecos.webapp.api.entity.EntityRef

class MutationComputeContext(
    val daoCtx: DbRecordsDaoCtx,
    val counterAttsToUpdate: Set<String>
) {
    companion object {
        private const val COUNTER_CONFIG_TEMPLATE_KEY = "numTemplateRef"
    }

    val preCalculatedComputedAtts: MutableMap<String, Any?> = HashMap()

    fun createDocNumAttDef(templateRef: EntityRef): AttributeDef {
        val config = ObjectData.create()
        config[COUNTER_CONFIG_TEMPLATE_KEY] = templateRef
        return AttributeDef.create()
            .withId(RecordConstants.ATT_DOC_NUM)
            .withType(AttributeType.NUMBER)
            .withComputed(
                ComputedAttDef.create()
                    .withType(ComputedAttType.COUNTER)
                    .withConfig(config)
                    .withStoringType(ComputedAttStoringType.ON_CREATE)
                    .build()
            ).build()
    }

    fun updateCountersIfRequired(record: Any, atts: ObjectData, typeInfo: TypeInfo) {

        daoCtx.computedAttsComponent ?: return
        if (counterAttsToUpdate.isEmpty()) return

        val attDefById = typeInfo.model.attributes.associateBy { it.id }
        for (counterAtt in counterAttsToUpdate) {
            val attDef = if (counterAtt == RecordConstants.ATT_DOC_NUM) {
                if (typeInfo.numTemplateRef.isEmpty()) continue
                createDocNumAttDef(typeInfo.numTemplateRef)
            } else {
                attDefById[counterAtt] ?: continue
            }
            if (attDef.computed.type != ComputedAttType.COUNTER) {
                continue
            }
            atts[counterAtt] = calculateAtt(
                record = record,
                attId = counterAtt,
                attributeType = attDef.type,
                attComputedDef = attDef.computed
            )
        }
    }

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
