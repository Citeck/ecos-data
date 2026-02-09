package ru.citeck.ecos.data.sql.repo

import ru.citeck.ecos.data.sql.context.DbTableContext
import ru.citeck.ecos.data.sql.repo.find.DbFindPage
import ru.citeck.ecos.data.sql.repo.find.DbFindQuery
import ru.citeck.ecos.data.sql.repo.find.DbFindRes
import ru.citeck.ecos.records2.predicate.model.Predicate

interface DbEntityRepo {

    fun find(
        context: DbTableContext,
        query: DbFindQuery,
        page: DbFindPage,
        withTotalCount: Boolean
    ): DbFindRes<Map<String, Any?>>

    fun save(context: DbTableContext, entities: List<Map<String, Any?>>): List<Map<String, Any?>>

    fun insertIfNoConflictByExtId(context: DbTableContext, entity: Map<String, Any?>): Long?

    fun delete(context: DbTableContext, entity: Map<String, Any?>)

    fun delete(context: DbTableContext, predicate: Predicate)

    fun delete(context: DbTableContext, entities: List<Long>)
}
