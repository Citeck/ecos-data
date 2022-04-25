package ru.citeck.ecos.data.sql.repo

import ru.citeck.ecos.data.sql.dto.DbColumnDef
import ru.citeck.ecos.data.sql.dto.DbTableRef
import ru.citeck.ecos.data.sql.repo.find.DbFindPage
import ru.citeck.ecos.data.sql.repo.find.DbFindRes
import ru.citeck.ecos.data.sql.repo.find.DbFindSort
import ru.citeck.ecos.data.sql.service.aggregation.AggregateFunc
import ru.citeck.ecos.records2.predicate.model.Predicate

interface DbEntityRepo<T : Any> {

    fun findById(ids: Set<Long>): List<T>

    fun findById(id: Long, withDeleted: Boolean): T?

    fun findByExtId(id: String, withDeleted: Boolean): T?

    fun find(predicate: Predicate, sort: List<DbFindSort>, page: DbFindPage, withDeleted: Boolean): DbFindRes<T>

    fun find(
        predicate: Predicate,
        sort: List<DbFindSort>,
        page: DbFindPage,
        withDeleted: Boolean,
        groupBy: List<String>,
        selectFunctions: List<AggregateFunc>
    ): DbFindRes<T>

    fun getCount(predicate: Predicate): Long

    fun save(entity: T): T

    fun setReadPerms(permissions: List<DbEntityPermissionsDto>)

    fun delete(entity: T)

    fun forceDelete(entity: T)

    fun forceDelete(entities: List<T>)

    fun setColumns(columns: List<DbColumnDef>)

    fun getTableRef(): DbTableRef
}
