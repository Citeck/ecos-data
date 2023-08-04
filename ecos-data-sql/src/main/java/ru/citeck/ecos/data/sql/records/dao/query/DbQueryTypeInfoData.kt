package ru.citeck.ecos.data.sql.records.dao.query

import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.type.dto.QueryPermsPolicy
import ru.citeck.ecos.model.lib.type.dto.TypeInfo
import ru.citeck.ecos.webapp.api.entity.EntityRef

interface DbQueryTypeInfoData {

    fun getAttributesById(): Map<String, AttributeDef>

    fun getAttribute(attId: String): AttributeDef?

    fun getQueryPermsPolicy(): QueryPermsPolicy

    fun getTypeAspects(): Set<EntityRef>

    fun getTypeInfo(): TypeInfo?
}
