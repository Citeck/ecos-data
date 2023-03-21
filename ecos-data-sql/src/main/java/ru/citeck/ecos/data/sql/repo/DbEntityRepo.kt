package ru.citeck.ecos.data.sql.repo

import ru.citeck.ecos.data.sql.context.DbTableContext
import ru.citeck.ecos.data.sql.repo.find.DbFindPage
import ru.citeck.ecos.data.sql.repo.find.DbFindRes
import ru.citeck.ecos.data.sql.repo.find.DbFindSort
import ru.citeck.ecos.data.sql.service.aggregation.AggregateFunc
import ru.citeck.ecos.records2.predicate.model.Predicate

interface DbEntityRepo {

    fun findByColumn(
        context: DbTableContext,
        column: String,
        values: Collection<Any>,
        withDeleted: Boolean,
        limit: Int
    ): List<Map<String, Any?>>

    fun find(
        context: DbTableContext,
        predicate: Predicate,
        sort: List<DbFindSort>,
        page: DbFindPage,
        withDeleted: Boolean,
        groupBy: List<String>,
        selectFunctions: List<AggregateFunc>
    ): DbFindRes<Map<String, Any?>>

    fun getCount(context: DbTableContext, predicate: Predicate): Long

    fun save(context: DbTableContext, entities: List<Map<String, Any?>>): List<Map<String, Any?>>

    fun delete(context: DbTableContext, entity: Map<String, Any?>)

    /**
     * @argument entities - not empty list of entities to delete
     */
    fun forceDelete(context: DbTableContext, entities: List<Long>)

    fun forceDelete(context: DbTableContext, predicate: Predicate)
}
