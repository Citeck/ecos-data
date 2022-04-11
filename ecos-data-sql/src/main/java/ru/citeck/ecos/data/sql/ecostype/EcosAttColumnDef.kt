package ru.citeck.ecos.data.sql.ecostype

import ru.citeck.ecos.data.sql.dto.DbColumnDef
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef

data class EcosAttColumnDef(
    val column: DbColumnDef,
    val attribute: AttributeDef,
    val systemAtt: Boolean
)
