package ru.citeck.ecos.data.sql.service

import ru.citeck.ecos.data.sql.context.DbTableContext
import ru.citeck.ecos.records2.predicate.model.Predicate

class RawTableJoin(
    val table: DbTableContext,
    val on: Predicate
)
