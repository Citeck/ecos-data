package ru.citeck.ecos.data.sql.ecostype

import ru.citeck.ecos.commons.data.MLText
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.status.dto.StatusDef
import ru.citeck.ecos.records2.RecordRef

data class DbEcosTypeInfo(
    val id: String,
    val name: MLText,
    val dispNameTemplate: MLText,
    val numTemplateRef: RecordRef,
    val attributes: List<AttributeDef>,
    val statuses: List<StatusDef>
)
