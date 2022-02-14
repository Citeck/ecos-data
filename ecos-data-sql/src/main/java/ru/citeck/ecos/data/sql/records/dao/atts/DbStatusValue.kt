package ru.citeck.ecos.data.sql.records.dao.atts

import ru.citeck.ecos.model.lib.status.dto.StatusDef
import ru.citeck.ecos.records3.record.atts.value.AttValue

data class DbStatusValue(private val def: StatusDef) : AttValue {

    override fun getDisplayName(): Any {
        return def.name
    }

    override fun asText(): String {
        return def.id
    }
}
