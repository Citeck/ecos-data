package ru.citeck.ecos.data.sql.repo

import ru.citeck.ecos.data.sql.dto.DbColumnDef
import ru.citeck.ecos.data.sql.repo.find.DbFindPage
import ru.citeck.ecos.data.sql.repo.find.DbFindRes
import ru.citeck.ecos.data.sql.repo.find.DbFindSort
import ru.citeck.ecos.records2.predicate.model.Predicate

interface DbEntityRepo<T : Any> {

    fun findById(id: String): T?

    fun findById(id: String, withDeleted: Boolean): T?

    fun findAll(): List<T>

    fun findAll(predicate: Predicate): List<T>

    fun findAll(predicate: Predicate, withDeleted: Boolean): List<T>

    fun findAll(predicate: Predicate, sort: List<DbFindSort>): List<T>

    fun find(predicate: Predicate, sort: List<DbFindSort>, page: DbFindPage): DbFindRes<T>

    fun getCount(predicate: Predicate): Long

    fun save(entity: T): T

    fun delete(extId: String)

    fun forceDelete(entity: T)

    fun setColumns(columns: List<DbColumnDef>)
}
