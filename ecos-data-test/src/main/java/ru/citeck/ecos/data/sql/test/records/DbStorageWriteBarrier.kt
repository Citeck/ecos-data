package ru.citeck.ecos.data.sql.test.records

import ru.citeck.ecos.data.sql.context.DbTableContext
import ru.citeck.ecos.data.sql.dto.DbTableRef
import ru.citeck.ecos.data.sql.repo.DbEntityRepo
import ru.citeck.ecos.data.sql.repo.DbInsertOrGetRes
import ru.citeck.ecos.data.sql.repo.find.DbFindPage
import ru.citeck.ecos.data.sql.repo.find.DbFindQuery
import ru.citeck.ecos.data.sql.repo.find.DbFindRes
import ru.citeck.ecos.data.sql.schema.DbSchemaDao
import ru.citeck.ecos.data.sql.service.DbDataServiceFactory
import ru.citeck.ecos.data.sql.type.DbTypesConverter
import ru.citeck.ecos.records2.predicate.model.Predicate

/**
 * A [DbDataServiceFactory] that lets a test stop the storage **inside** a transaction the production
 * code owns, immediately before it writes rows into a table - and do something else while it is
 * stopped there.
 *
 * What this exists for is the only kind of defect a single-threaded test cannot reach at all: a
 * check and the write it authorises that are not one step. "Nothing is stored here, so store this"
 * is right only while nothing else can store something in between, and whether anything can is
 * decided by the statements that run between the two - which is a fact about the production code
 * and cannot be asserted from outside it. The barrier puts the test exactly there: it runs on the
 * same thread, between the two statements, and what it does from there (typically a
 * `TxnContext.doInNewTxn { ... }`, a genuinely separate transaction on its own connection that
 * commits before the caller resumes) is what a concurrent user would have done.
 *
 * **Both backends, which is why the hook is here and not on the SQL.** The in-memory backend's
 * repo never goes through [ru.citeck.ecos.data.sql.datasource.DbDataSource.update], so a barrier
 * matching SQL text would silently be PostgreSQL-only and the in-mem run would be a false green.
 * [DbEntityRepo] is the one seam both backends share.
 *
 * **Reentrancy is the test's to handle**: the action runs storage of its own, which reaches this
 * same barrier. A test arms it for one write and disarms it there - see the callers.
 */
class DbStorageWriteBarrier(
    private val impl: DbDataServiceFactory,
    private val barrier: () -> ((DbTableRef, List<Map<String, Any?>>) -> Unit)?
) : DbDataServiceFactory {

    override fun registerConverters(typesConverter: DbTypesConverter) {
        impl.registerConverters(typesConverter)
    }

    override fun createSchemaDao(): DbSchemaDao {
        return impl.createSchemaDao()
    }

    override fun createEntityRepo(): DbEntityRepo {
        return BarrieredEntityRepo(impl.createEntityRepo())
    }

    private inner class BarrieredEntityRepo(private val repo: DbEntityRepo) : DbEntityRepo {

        override fun find(
            context: DbTableContext,
            query: DbFindQuery,
            page: DbFindPage,
            withTotalCount: Boolean
        ): DbFindRes<Map<String, Any?>> {
            return repo.find(context, query, page, withTotalCount)
        }

        override fun save(context: DbTableContext, entities: List<Map<String, Any?>>): List<Map<String, Any?>> {
            barrier.invoke()?.invoke(context.getTableRef(), entities)
            return repo.save(context, entities)
        }

        override fun insertIfNoConflict(
            context: DbTableContext,
            entities: List<Map<String, Any?>>,
            conflictColumns: List<String>,
            returningColumn: String
        ): List<Long> {
            barrier.invoke()?.invoke(context.getTableRef(), entities)
            return repo.insertIfNoConflict(context, entities, conflictColumns, returningColumn)
        }

        override fun insertOrGetByExtId(
            context: DbTableContext,
            entity: Map<String, Any?>,
            extraLongColumns: List<String>
        ): DbInsertOrGetRes {
            barrier.invoke()?.invoke(context.getTableRef(), listOf(entity))
            return repo.insertOrGetByExtId(context, entity, extraLongColumns)
        }

        override fun updateByExtIdIfMatches(
            context: DbTableContext,
            extId: String,
            expected: Map<String, Any?>,
            newValues: Map<String, Any?>
        ): Boolean {
            return repo.updateByExtIdIfMatches(context, extId, expected, newValues)
        }

        override fun updateByIdIfMatches(
            context: DbTableContext,
            id: Long,
            expected: Map<String, Any?>,
            newValues: Map<String, Any?>
        ): Boolean {
            return repo.updateByIdIfMatches(context, id, expected, newValues)
        }

        override fun delete(context: DbTableContext, entity: Map<String, Any?>) {
            repo.delete(context, entity)
        }

        override fun delete(context: DbTableContext, predicate: Predicate) {
            repo.delete(context, predicate)
        }

        override fun delete(context: DbTableContext, entities: List<Long>) {
            repo.delete(context, entities)
        }
    }
}
