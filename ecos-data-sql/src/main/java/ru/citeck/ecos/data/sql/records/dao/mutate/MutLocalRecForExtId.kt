package ru.citeck.ecos.data.sql.records.dao.mutate

import ru.citeck.ecos.commons.data.DataValue
import ru.citeck.ecos.commons.data.ObjectData
import ru.citeck.ecos.data.sql.records.DbRecordsUtils
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType
import ru.citeck.ecos.model.lib.attributes.dto.computed.ComputedAttDef
import ru.citeck.ecos.model.lib.attributes.dto.computed.ComputedAttStoringType
import ru.citeck.ecos.model.lib.attributes.dto.computed.ComputedAttType
import ru.citeck.ecos.model.lib.type.dto.TypeInfo
import ru.citeck.ecos.records2.RecordConstants
import ru.citeck.ecos.records3.record.atts.value.AttValue
import ru.citeck.ecos.webapp.api.entity.EntityRef

class MutLocalRecForExtId(
    private val attributes: ObjectData,
    private val typeInfo: TypeInfo,
    private val mutComputeCtx: MutationComputeContext
) : AttValue {

    companion object {
        private const val COUNTER_CONFIG_TEMPLATE_KEY = "numTemplateRef"
        private val ATT_ID_DEF = AttributeDef.create().withId("id").build()
    }

    private val attDefById = typeInfo.model.getAllAttributes().associateBy { it.id }

    override fun getAtt(name: String): Any? {
        val attDef = when (name) {
            "id" -> ATT_ID_DEF
            RecordConstants.ATT_DOC_NUM -> {
                if (attributes.has(RecordConstants.ATT_DOC_NUM)) {
                    AttributeDef.create()
                        .withId(RecordConstants.ATT_DOC_NUM)
                        .withType(AttributeType.NUMBER)
                        .build()
                } else {
                    if (typeInfo.numTemplateRef.isEmpty()) {
                        return null
                    }
                    val config = ObjectData.create()
                    config[COUNTER_CONFIG_TEMPLATE_KEY] = typeInfo.numTemplateRef
                    AttributeDef.create()
                        .withId(RecordConstants.ATT_DOC_NUM)
                        .withComputed(
                            ComputedAttDef.create()
                                .withType(ComputedAttType.COUNTER)
                                .withConfig(config)
                                .withStoringType(ComputedAttStoringType.ON_CREATE)
                                .build()
                        ).build()
                }
            }
            else -> attDefById[name] ?: error("Attribute is not found: '$name'")
        }
        if (attDef.computed.type != ComputedAttType.NONE) {
            if (attDef.computed.type == ComputedAttType.COUNTER && attributes.has(name)) {
                val counterValue = attributes[name]
                if (counterValue.isValueNode() && counterValue.isNotNull()) {
                    return counterValue
                }
            }
            return mutComputeCtx.calculateAtt(
                this,
                name,
                attDef.type,
                attDef.computed
            )
        }
        if (!attributes.has(name)) {
            error("Attribute '$name' required for localIdTemplate is not present. Type: '${typeInfo.id}'")
        }
        val rawValue = attributes[name]
        if (DbRecordsUtils.isAssocLikeAttribute(attDef)) {
            return convertToAssocValue(rawValue)
        }
        return rawValue
    }

    private fun convertToAssocValue(value: DataValue): Any? {
        if (value.isNull()) {
            return null
        }
        if (value.isArray()) {
            return value.map { convertToAssocValue(it) }
        }
        if (value.isTextual()) {
            return EntityRef.valueOf(value.asText())
        }
        return null
    }
}
