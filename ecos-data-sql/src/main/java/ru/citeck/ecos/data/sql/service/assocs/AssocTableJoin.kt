package ru.citeck.ecos.data.sql.service.assocs

import ru.citeck.ecos.data.sql.context.DbTableContext
import ru.citeck.ecos.records2.predicate.model.Predicate

class AssocTableJoin(
    val attribute: String,
    val srcColumn: String,
    val tableContext: DbTableContext,
    val predicate: Predicate,
    val assocJoins: List<AssocJoin>,
    val assocTableJoins: List<AssocTableJoin>
)
