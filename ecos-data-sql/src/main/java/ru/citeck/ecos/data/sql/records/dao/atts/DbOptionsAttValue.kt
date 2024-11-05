package ru.citeck.ecos.data.sql.records.dao.atts

import ru.citeck.ecos.commons.data.DataValue
import ru.citeck.ecos.commons.data.MLText
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.records3.record.atts.value.AttValue
import ru.citeck.ecos.webapp.api.entity.EntityRef

class DbOptionsAttValue(
    private val record: DbRecord,
    private val attDef: AttributeDef,
    private val value: String
) : AttValue {

    private val label: MLText by lazy {
        record.ctx.computedAttsComponent?.getAttOptions(record, attDef.config)?.find {
            it.value == value
        }?.label ?: MLText(value)
    }

    override fun getDisplayName(): Any {
        return label
    }

    override fun asText(): String {
        return value
    }

    override fun getAs(type: String): Any? {
        return when (type) {
            "mltext" -> label
            "ref" -> EntityRef.valueOf(value)
            else -> null
        }
    }

    override fun asJson(): Any {
        return DataValue.createObj()
            .set("value", value)
            .set("label", label)
    }

    override fun has(name: String): Boolean {
        return name == "value" || name == "label"
    }

    override fun getAtt(name: String): Any? {
        return when (name) {
            "value" -> value
            "label" -> label
            else -> null
        }
    }
}
