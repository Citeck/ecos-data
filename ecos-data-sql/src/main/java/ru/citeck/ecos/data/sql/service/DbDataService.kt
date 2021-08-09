package ru.citeck.ecos.data.sql.service

import ru.citeck.ecos.data.sql.dto.DbColumnDef
import ru.citeck.ecos.data.sql.meta.dto.DbTableMetaDto
import ru.citeck.ecos.data.sql.repo.find.DbFindPage
import ru.citeck.ecos.data.sql.repo.find.DbFindRes
import ru.citeck.ecos.data.sql.repo.find.DbFindSort
import ru.citeck.ecos.records2.predicate.model.Predicate

interface DbDataService<T : Any> {

    fun findById(id: String): T?

    fun findAll(): List<T>

    fun findAll(predicate: Predicate): List<T>

    fun findAll(predicate: Predicate, withDeleted: Boolean): List<T>

    fun findAll(predicate: Predicate, sort: List<DbFindSort>): List<T>

    fun find(predicate: Predicate, sort: List<DbFindSort>, page: DbFindPage): DbFindRes<T>

    fun getCount(predicate: Predicate): Long

    fun save(entity: T, columns: List<DbColumnDef>): T

    fun commit(entitiesId: List<String>)

    fun rollback(entitiesId: List<String>)

    fun delete(id: String)

    fun forceDelete(entity: T)

    fun getTableMeta(): DbTableMetaDto

    fun resetColumnsCache()

    fun runMigrations(
        expectedColumns: List<DbColumnDef>,
        mock: Boolean,
        diff: Boolean,
        onlyOwn: Boolean
    ): List<String>
}
