package ru.citeck.ecos.data.sql.records.dao.atts.stage

import ru.citeck.ecos.commons.data.MLText
import ru.citeck.ecos.model.lib.procstages.dto.ProcStageDef
import ru.citeck.ecos.records3.record.atts.value.AttValue
import ru.citeck.ecos.records3.record.atts.value.impl.AttValueDelegate

data class DbStageValue(val def: ProcStageDef, val value: AttValue) : AttValueDelegate(value) {

    override fun getId(): Any {
        return asText()
    }

    override fun getDisplayName(): Any {
        return if (MLText.isEmpty(def.name)) {
            "Stage '${def.id}'"
        } else {
            def.name
        }
    }

    override fun asText(): String {
        return def.id
    }
}
