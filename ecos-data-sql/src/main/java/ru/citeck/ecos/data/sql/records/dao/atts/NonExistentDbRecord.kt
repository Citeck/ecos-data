package ru.citeck.ecos.data.sql.records.dao.atts

import ru.citeck.ecos.records2.RecordConstants
import ru.citeck.ecos.records3.record.atts.value.AttValue

class NonExistentDbRecord(
    private val id: String
) : AttValue {

    override fun getAtt(name: String): Any? {
        return when (name) {
            RecordConstants.ATT_NOT_EXISTS -> true
            else -> null
        }
    }

    override fun getDisplayName(): Any {
        return id
    }

    override fun asText(): String? = null
}
