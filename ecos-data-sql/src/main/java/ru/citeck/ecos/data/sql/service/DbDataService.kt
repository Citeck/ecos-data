package ru.citeck.ecos.data.sql.service

import ru.citeck.ecos.commons.data.ObjectData
import ru.citeck.ecos.data.sql.dto.DbColumnDef
import ru.citeck.ecos.data.sql.dto.DbTableRef
import ru.citeck.ecos.data.sql.meta.dto.DbTableMetaDto
import ru.citeck.ecos.data.sql.repo.find.DbFindPage
import ru.citeck.ecos.data.sql.repo.find.DbFindRes
import ru.citeck.ecos.data.sql.repo.find.DbFindSort
import ru.citeck.ecos.data.sql.service.migration.DbMigration
import ru.citeck.ecos.records2.predicate.model.Predicate

interface DbDataService<T : Any> {

    fun findById(id: Long): T?

    fun findById(id: Set<Long>): List<T>

    fun findByExtId(id: String): T?

    fun findAll(): List<T>

    fun findAll(predicate: Predicate): List<T>

    fun findAll(predicate: Predicate, withDeleted: Boolean): List<T>

    fun findAll(predicate: Predicate, sort: List<DbFindSort>): List<T>

    fun find(predicate: Predicate, sort: List<DbFindSort>, page: DbFindPage): DbFindRes<T>

    fun find(predicate: Predicate, sort: List<DbFindSort>, page: DbFindPage, withDeleted: Boolean): DbFindRes<T>

    fun getCount(predicate: Predicate): Long

    fun save(entity: T): T

    fun save(entity: T, columns: List<DbColumnDef>): T

    fun commit(entities: List<DbCommitEntityDto>)

    fun rollback(entitiesId: List<String>)

    fun delete(entity: T)

    fun forceDelete(entity: T)

    fun forceDelete(entities: List<T>)

    fun getTableRef(): DbTableRef

    fun isTableExists(): Boolean

    fun getTableMeta(): DbTableMetaDto

    fun resetColumnsCache()

    fun runMigrations(
        mock: Boolean,
        diff: Boolean,
        onlyOwn: Boolean
    ): List<String>

    fun runMigrations(
        expectedColumns: List<DbColumnDef>,
        mock: Boolean,
        diff: Boolean,
        onlyOwn: Boolean
    ): List<String>

    fun runMigrationByType(
        type: String,
        mock: Boolean,
        config: ObjectData
    )

    fun registerMigration(migration: DbMigration<T, *>)
}
