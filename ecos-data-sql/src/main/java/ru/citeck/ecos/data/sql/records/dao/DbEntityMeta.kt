package ru.citeck.ecos.data.sql.records.dao

import ru.citeck.ecos.model.lib.aspect.dto.AspectInfo
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.type.dto.TypeInfo
import ru.citeck.ecos.webapp.api.entity.EntityRef

data class DbEntityMeta(
    val localRef: EntityRef,
    val globalRef: EntityRef,
    val isDraft: Boolean,
    val typeInfo: TypeInfo,
    val aspectsInfo: List<AspectInfo>,
    val systemAtts: Map<String, AttributeDef>,
    val nonSystemAtts: Map<String, AttributeDef>,
    val allAttributes: Map<String, AttributeDef>
)
