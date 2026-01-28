package ru.citeck.ecos.data.sql.records.dao.atts

import ru.citeck.ecos.records2.RecordConstants
import ru.citeck.ecos.records3.record.atts.value.AttValue
import ru.citeck.ecos.webapp.api.entity.EntityRef

class NonExistentDbRecord(
    private val id: String,
    private val movedToRef: EntityRef? = null
) : AttValue {

    companion object {
        const val ATT_MOVED_TO_REF = "_movedToRef"
    }

    override fun getAtt(name: String): Any? {
        return when (name) {
            RecordConstants.ATT_NOT_EXISTS -> true
            ATT_MOVED_TO_REF -> movedToRef
            else -> null
        }
    }

    override fun getDisplayName(): Any {
        return id
    }

    override fun asText(): String? = null
}
