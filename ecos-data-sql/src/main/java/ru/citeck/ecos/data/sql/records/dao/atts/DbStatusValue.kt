package ru.citeck.ecos.data.sql.records.dao.atts

import ru.citeck.ecos.model.lib.status.dto.StatusDef
import ru.citeck.ecos.records3.record.atts.value.AttValue
import ru.citeck.ecos.records3.record.atts.value.impl.AttValueDelegate

data class DbStatusValue(val def: StatusDef, val value: AttValue) : AttValueDelegate(value) {

    override fun getDisplayName(): Any {
        return def.name
    }

    override fun asText(): String {
        return def.id
    }
}
