package ru.citeck.ecos.data.sql.service

import ru.citeck.ecos.data.sql.context.DbTableContext
import ru.citeck.ecos.data.sql.dto.DbColumnDef
import ru.citeck.ecos.data.sql.dto.DbTableRef
import ru.citeck.ecos.data.sql.meta.table.dto.DbTableMetaDto
import ru.citeck.ecos.data.sql.repo.entity.DbEntityMapper
import ru.citeck.ecos.data.sql.repo.find.DbFindPage
import ru.citeck.ecos.data.sql.repo.find.DbFindQuery
import ru.citeck.ecos.data.sql.repo.find.DbFindRes
import ru.citeck.ecos.data.sql.repo.find.DbFindSort
import ru.citeck.ecos.data.sql.service.assocs.AssocJoinWithPredicate
import ru.citeck.ecos.data.sql.service.assocs.AssocTableJoin
import ru.citeck.ecos.data.sql.service.expression.token.ExpressionToken
import ru.citeck.ecos.model.lib.type.dto.QueryPermsPolicy
import ru.citeck.ecos.records2.predicate.model.Predicate

interface DbDataService<T : Any> {

    companion object {
        const val NEW_TABLE_SCHEMA_VERSION = 7
    }

    fun <T> doWithPermsPolicy(permsPolicy: QueryPermsPolicy?, action: () -> T): T

    fun getSchemaVersion(): Int

    fun setSchemaVersion(version: Int)

    fun findById(id: Long): T?

    fun findByIds(ids: Set<Long>): List<T>

    fun findByExtId(id: String): T?

    fun findByExtId(id: String, expressions: Map<String, ExpressionToken>): T?

    fun isExistsByExtId(id: String): Boolean

    fun findAll(): List<T>

    fun findAll(predicate: Predicate): List<T>

    fun findAll(predicate: Predicate, sort: List<DbFindSort>): List<T>

    fun find(predicate: Predicate, sort: List<DbFindSort>, page: DbFindPage): DbFindRes<T>

    fun find(
        predicate: Predicate,
        sort: List<DbFindSort>,
        page: DbFindPage,
        groupBy: List<String>,
        assocTableJoins: List<AssocTableJoin>,
        assocJoinWithPredicates: List<AssocJoinWithPredicate>,
        withTotalCount: Boolean
    ): DbFindRes<T>

    fun find(query: DbFindQuery, page: DbFindPage): DbFindRes<T>

    fun find(query: DbFindQuery, page: DbFindPage, withTotalCount: Boolean): DbFindRes<T>

    fun findRaw(
        predicate: Predicate,
        sort: List<DbFindSort>,
        page: DbFindPage,
        groupBy: List<String>,
        assocTableJoins: List<AssocTableJoin>,
        assocJoinWithPredicates: List<AssocJoinWithPredicate>,
        withTotalCount: Boolean
    ): DbFindRes<Map<String, Any?>>

    fun findRaw(query: DbFindQuery, page: DbFindPage, withTotalCount: Boolean): DbFindRes<Map<String, Any?>>

    fun getCount(query: DbFindQuery): Long

    fun getCount(predicate: Predicate): Long

    fun save(entity: T): T

    fun saveAtomicallyOrGetExistingByExtId(entity: T): Long

    /**
     * Atomic compare-and-set on a single row, for state transitions that [save] can't express
     * because it is read-modify-write: assign [newValues] to the row with [extId], but only while
     * every column listed in [expected] still holds the given value.
     *
     * Returns true when the row was updated, false when the row doesn't exist or an expected column
     * no longer matches. A non-match is a normal outcome, not an error.
     *
     * Both maps are keyed by database column name, not by entity field name, and their values are
     * converted to the column type, so a [java.time.Instant] may be passed for a datetime column.
     * Only the listed columns and the update version change; the audit columns are not maintained
     * here. See [ru.citeck.ecos.data.sql.repo.DbEntityRepo.updateByExtIdIfMatches] for the full
     * contract.
     *
     * Unlike [saveAtomicallyOrGetExistingByExtId], this **joins the caller's transaction** instead
     * of running in one of its own. The compare and the write are still indivisible with respect to
     * other transactions, but a true is durable only when the enclosing transaction commits: until
     * then the row stays locked, and an outer rollback undoes the transition. That is deliberate -
     * it lets a caller commit a state transition together with the work that transition authorises,
     * so a crash in between can't leave the two disagreeing. A caller that must see the transition
     * survive on its own has to open its own transaction around this call.
     */
    fun updateByExtIdIfMatches(extId: String, expected: Map<String, Any?>, newValues: Map<String, Any?>): Boolean

    fun save(entities: Collection<T>): List<T>

    fun save(entities: Collection<T>, columns: List<DbColumnDef>): List<T>

    fun save(entity: T, columns: List<DbColumnDef>): T

    fun delete(entity: T)

    fun delete(predicate: Predicate)

    fun delete(entityId: Long)

    fun delete(entities: List<T>)

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

    fun getEntityMapper(): DbEntityMapper<T>
}
