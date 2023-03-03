package ru.citeck.ecos.data.sql.records.dao.atts

import ru.citeck.ecos.model.lib.utils.ModelUtils
import ru.citeck.ecos.records3.record.atts.value.AttValue
import ru.citeck.ecos.webapp.api.entity.EntityRef

class DbAspectsValue(private val aspects: Set<String>) : AttValue {

    override fun getAtt(name: String): Any? {
        return when (name) {
            "list" -> getAspectRefs()
            else -> null
        }
    }

    override fun has(name: String): Boolean {
        return aspects.contains(name)
    }

    fun getAspectRefs(): List<EntityRef> {
        return aspects.map { ModelUtils.getAspectRef(it) }
    }
}
