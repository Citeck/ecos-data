package ru.citeck.ecos.data.sql.repo

import ru.citeck.ecos.data.sql.context.DbTableContext
import ru.citeck.ecos.data.sql.repo.find.DbFindPage
import ru.citeck.ecos.data.sql.repo.find.DbFindRes
import ru.citeck.ecos.data.sql.repo.find.DbFindSort
import ru.citeck.ecos.data.sql.service.aggregation.AggregateFunc
import ru.citeck.ecos.data.sql.service.assocs.AssocJoin
import ru.citeck.ecos.records2.predicate.model.Predicate

interface DbEntityRepo {

    fun find(
        context: DbTableContext,
        predicate: Predicate,
        sort: List<DbFindSort>,
        page: DbFindPage,
        withDeleted: Boolean,
        groupBy: List<String>,
        selectFunctions: List<AggregateFunc>,
        assocJoins: Map<String, AssocJoin>
    ): DbFindRes<Map<String, Any?>>

    fun getCount(
        context: DbTableContext,
        predicate: Predicate,
        groupBy: List<String>,
        assocJoins: Map<String, AssocJoin>
    ): Long

    fun save(context: DbTableContext, entities: List<Map<String, Any?>>): List<Map<String, Any?>>

    fun delete(context: DbTableContext, entity: Map<String, Any?>)

    fun forceDelete(context: DbTableContext, entities: List<Long>)

    fun forceDelete(context: DbTableContext, predicate: Predicate)
}
