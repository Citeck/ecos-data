package ru.citeck.ecos.data.sql.service.assocs

import ru.citeck.ecos.data.sql.context.DbTableContext
import ru.citeck.ecos.records2.predicate.model.Predicate

class AssocJoinWithPredicate(
    val attribute: String,
    val srcColumn: String,
    // used when multipleAssoc == true
    val srcAttributeId: Long,
    val multipleAssoc: Boolean,
    val tableContext: DbTableContext,
    val predicate: Predicate,
    val assocTableJoins: List<AssocTableJoin>,
    val assocJoinsWithPredicate: List<AssocJoinWithPredicate>
)
