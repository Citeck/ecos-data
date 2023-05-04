package ru.citeck.ecos.data.sql.service

import ru.citeck.ecos.data.sql.context.DbTableContext
import ru.citeck.ecos.data.sql.dto.DbColumnDef
import ru.citeck.ecos.data.sql.dto.DbTableRef
import ru.citeck.ecos.data.sql.meta.table.dto.DbTableMetaDto
import ru.citeck.ecos.data.sql.repo.find.DbFindPage
import ru.citeck.ecos.data.sql.repo.find.DbFindRes
import ru.citeck.ecos.data.sql.repo.find.DbFindSort
import ru.citeck.ecos.data.sql.service.aggregation.AggregateFunc
import ru.citeck.ecos.data.sql.service.assocs.AssocJoin
import ru.citeck.ecos.model.lib.type.dto.QueryPermsPolicy
import ru.citeck.ecos.records2.predicate.model.Predicate

interface DbDataService<T : Any> {

    companion object {
        const val NEW_TABLE_SCHEMA_VERSION = 2
    }

    fun <T> doWithPermsPolicy(permsPolicy: QueryPermsPolicy?, action: () -> T): T

    fun getSchemaVersion(): Int

    fun setSchemaVersion(version: Int)

    fun findById(id: Long): T?

    fun findByIds(ids: Set<Long>): List<T>

    fun findByExtId(id: String): T?

    fun findAll(): List<T>

    fun findAll(predicate: Predicate): List<T>

    fun findAll(predicate: Predicate, withDeleted: Boolean): List<T>

    fun findAll(predicate: Predicate, sort: List<DbFindSort>): List<T>

    fun find(predicate: Predicate, sort: List<DbFindSort>, page: DbFindPage): DbFindRes<T>

    fun find(predicate: Predicate, sort: List<DbFindSort>, page: DbFindPage, withDeleted: Boolean): DbFindRes<T>

    fun find(
        predicate: Predicate,
        sort: List<DbFindSort>,
        page: DbFindPage,
        withDeleted: Boolean,
        groupBy: List<String>,
        selectFunctions: List<AggregateFunc>,
        assocJoins: Map<String, AssocJoin>,
        withTotalCount: Boolean
    ): DbFindRes<T>

    fun getCount(predicate: Predicate): Long

    fun save(entity: T): T

    fun save(entities: Collection<T>): List<T>

    fun save(entities: Collection<T>, columns: List<DbColumnDef>): List<T>

    fun save(entity: T, columns: List<DbColumnDef>): T

    fun delete(entity: T)

    fun delete(predicate: Predicate)

    fun forceDelete(predicate: Predicate)

    fun forceDelete(entityId: Long)

    fun forceDelete(entity: T)

    fun forceDelete(entities: List<T>)

    fun getTableRef(): DbTableRef

    fun isTableExists(): Boolean

    fun getTableMeta(): DbTableMetaDto

    fun resetColumnsCache()

    fun runMigrations(
        mock: Boolean,
        diff: Boolean
    ): List<String>

    fun runMigrations(
        expectedColumns: List<DbColumnDef>,
        mock: Boolean,
        diff: Boolean
    ): List<String>

    fun getTableContext(): DbTableContext
}
