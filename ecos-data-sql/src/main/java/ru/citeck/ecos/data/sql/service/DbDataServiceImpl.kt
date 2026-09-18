package ru.citeck.ecos.data.sql.service

import io.github.oshai.kotlinlogging.KotlinLogging
import ru.citeck.ecos.commons.data.DataValue
import ru.citeck.ecos.commons.data.ObjectData
import ru.citeck.ecos.commons.exception.I18nRuntimeException
import ru.citeck.ecos.commons.json.Json
import ru.citeck.ecos.context.lib.auth.AuthContext
import ru.citeck.ecos.context.lib.auth.AuthUser
import ru.citeck.ecos.data.sql.columnmeta.DbAttTypeColumns
import ru.citeck.ecos.data.sql.columnmeta.DbBackupColumnNames
import ru.citeck.ecos.data.sql.columnmeta.DbColumnConversions
import ru.citeck.ecos.data.sql.columnmeta.DbColumnMetaDto
import ru.citeck.ecos.data.sql.columnmeta.DbColumnSemanticType
import ru.citeck.ecos.data.sql.columnmeta.DbConversionClass
import ru.citeck.ecos.data.sql.columnmeta.DbExpectedAttTypes
import ru.citeck.ecos.data.sql.content.DbContentService
import ru.citeck.ecos.data.sql.context.DbSchemaContext
import ru.citeck.ecos.data.sql.context.DbTableContext
import ru.citeck.ecos.data.sql.datasource.DbDataSource
import ru.citeck.ecos.data.sql.dto.*
import ru.citeck.ecos.data.sql.dto.fk.DbFkConstraint
import ru.citeck.ecos.data.sql.meta.schema.DbSchemaMetaService
import ru.citeck.ecos.data.sql.meta.table.DbTableMetaEntity
import ru.citeck.ecos.data.sql.meta.table.dto.DbTableChangeSet
import ru.citeck.ecos.data.sql.meta.table.dto.DbTableMetaConfig
import ru.citeck.ecos.data.sql.meta.table.dto.DbTableMetaDto
import ru.citeck.ecos.data.sql.migration.column.DbShadowColumnTransition
import ru.citeck.ecos.data.sql.perms.DbEntityPermsService
import ru.citeck.ecos.data.sql.props.DbEcosDataProps
import ru.citeck.ecos.data.sql.records.assocs.DbAssocsService
import ru.citeck.ecos.data.sql.records.refs.DbRecordRefService
import ru.citeck.ecos.data.sql.records.utils.DbReadToleranceLog
import ru.citeck.ecos.data.sql.records.workspace.DbWorkspaceService
import ru.citeck.ecos.data.sql.repo.DbEntityRepo
import ru.citeck.ecos.data.sql.repo.DbInsertOrGetRes
import ru.citeck.ecos.data.sql.repo.entity.DbEntity
import ru.citeck.ecos.data.sql.repo.entity.DbEntityMapper
import ru.citeck.ecos.data.sql.repo.entity.DbEntityMapperImpl
import ru.citeck.ecos.data.sql.repo.find.DbFindPage
import ru.citeck.ecos.data.sql.repo.find.DbFindQuery
import ru.citeck.ecos.data.sql.repo.find.DbFindRes
import ru.citeck.ecos.data.sql.repo.find.DbFindSort
import ru.citeck.ecos.data.sql.schema.DbSchemaDao
import ru.citeck.ecos.data.sql.service.assocs.AssocJoinWithPredicate
import ru.citeck.ecos.data.sql.service.assocs.AssocTableJoin
import ru.citeck.ecos.data.sql.service.expression.token.ExpressionToken
import ru.citeck.ecos.data.sql.type.DbTypeConversionException
import ru.citeck.ecos.data.sql.type.DbTypesConverter
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType
import ru.citeck.ecos.model.lib.type.dto.QueryPermsPolicy
import ru.citeck.ecos.records2.predicate.PredicateUtils
import ru.citeck.ecos.records2.predicate.model.*
import ru.citeck.ecos.txn.lib.TxnContext
import ru.citeck.ecos.webapp.api.authority.EcosAuthoritiesApi
import ru.citeck.ecos.webapp.api.lock.exception.AcquireTimeoutException
import java.time.Instant
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference
import kotlin.reflect.KClass

class DbDataServiceImpl<T : Any> : DbDataService<T> {

    companion object {
        private val log = KotlinLogging.logger {}

        /**
         * The distributed lock key for a table's schema migration. A function, callable from
         * Kotlin as `DbDataServiceImpl.schemaMigrationLockKey(...)` with no further qualification,
         * instead of an inline string built twice: tests that need to observe whether this table's
         * lock was taken again - because "no migration happened" has no more direct signal to
         * assert on - compute the exact same key through this function instead of duplicating the
         * format and risking silent drift from the one actually used below.
         */
        fun schemaMigrationLockKey(tableRef: DbTableRef): String {
            return "ecos-data-schema-migration-${tableRef.schema}-${tableRef.table}"
        }
    }

    private val typesConverter: DbTypesConverter
    private val entityMapper: DbEntityMapper<T>

    private val tableMetaService: DbDataService<DbTableMetaEntity>?
    private val entityPermsService: DbEntityPermsService?

    private val entityRepo: DbEntityRepo
    private val schemaDao: DbSchemaDao
    private val dataSource: DbDataSource
    private val schemaMeta: DbSchemaMetaService
    private val schemaCtx: DbSchemaContext

    private val config: DbDataServiceConfig

    private val tableRef: DbTableRef
    private val schemaCacheUpdateRequired = AtomicBoolean()

    /**
     * The one field a reader of this instance's cached schema state needs to consult - see
     * [setColumns] for why it used to be two fields, and why that was unsafe off the process-local
     * lock this branch removed. [DbTableContextImpl] carries its columns nullably internally
     * (`columnsOrNull`) precisely so this single field can answer both "what are the columns" and
     * "have they even been read yet", the way the old, separate `columns: List<DbColumnDef>?`
     * field used to.
     */
    @Volatile
    private var tableCtx: DbTableContextImpl

    private val metaSchemaVersionKey: List<String>

    private val permsPolicy: ThreadLocal<QueryPermsPolicy>

    private var schemaVersion: Int = -1
    private var schemaVersionNextUpdateMs = 0L
    private val useLastSchemaVersion: Boolean

    /**
     * True while this thread is already inside this table's migration.
     *
     * [EcosLockApi] locks are not reentrant, and the migration of a domain table writes to
     * `ed_column_meta`, whose own data service migrates under its own key. The flag is per data
     * service instance, so a nested migration of a *different* table still takes its own lock -
     * only a re-entry into the same table's migration skips it.
     */
    private val migrationInProgress = ThreadLocal.withInitial { false }

    /**
     * The (column name, semantic type) pairs this instance has already carried through a successful
     * migration, or null if it never has. Used to notice a model change that leaves no trace in the
     * physical column set - several attribute types share one physical type, so `ASSOC -> ENTITY_REF`
     * is invisible to a column definition but not to this set.
     *
     * **A set, not the incoming map's equality.** That map is built per record (base type columns
     * plus the aspects on that record), so it differs from one save to the next on a table with more
     * than one type even when nothing about the schema changed; comparing by equality would migrate
     * - lock, re-read, no-op - on every alternation. Containment fixes that: a reconciled pair is
     * never un-reconciled by a different record failing to mention it. The set only grows, bounded
     * by the table's columns x the types that share it.
     *
     * **Cleared, not merged, on a rollback of the transaction that grew it.** [seedColumnMeta]
     * writes registry rows in the same transaction and produces no schema commands, so those writes
     * are not covered by [runMigrationsInLock]'s own rollback hook. If the surrounding mutation then
     * fails, the rows vanish and this set must not go on claiming they are there.
     *
     * **An [AtomicReference] and not a `@Volatile var`**, because the rollback hook (`.set(null)`)
     * and the merge after a migration (`.updateAndGet`) interleave across threads on one table: a
     * plain read-then-write could read a pre-rollback value and overwrite another thread's `null`
     * with entries that no longer exist in `ed_column_meta` - forever, since only that thread's hook
     * was ever going to retract them. The compare-and-swap loop re-runs the merge against the fresh
     * value instead. The reverse order discards this thread's entries, which is the safe direction:
     * one extra lock later, never a wrongly-skipped migration.
     *
     * The reset [runMigrationsInTxn] performs before re-reading the schema deliberately does **not**
     * clear this field; only [resetColumnsCache] does. A merge may therefore publish one migration's
     * map alone and forget another type's earlier reconciliation - again the safe direction.
     *
     * Only assigned when [TxnContext.getTxnOrNull] is non-null: [TxnContext.doAfterRollback] is a
     * silent no-op outside a transaction, so a `save()` called outside one would publish a
     * reconciliation nothing could retract. Leaving the field null there costs one extra lock.
     */
    private val reconciledAttTypeEntries = AtomicReference<Set<Pair<String, DbColumnSemanticType>>?>(null)

    constructor(
        entityType: Class<T>,
        config: DbDataServiceConfig,
        schemaContext: DbSchemaContext,
        useLastSchemaVersion: Boolean = false
    ) {
        this.useLastSchemaVersion = useLastSchemaVersion
        this.config = config
        this.dataSource = schemaContext.dataSourceCtx.dataSource
        this.tableRef = DbTableRef(schemaContext.schema, config.table)
        this.typesConverter = schemaContext.dataSourceCtx.converter

        entityMapper = DbEntityMapperImpl(entityType.kotlin, typesConverter)

        tableMetaService = if (config.storeTableMeta) {
            schemaContext.tableMetaService
        } else {
            null
        }
        entityPermsService = schemaContext.entityPermsService

        schemaDao = schemaContext.dataSourceCtx.schemaDao
        entityRepo = schemaContext.dataSourceCtx.entityRepo

        tableCtx = DbTableContextImpl(
            config.table,
            schemaCtx = schemaContext,
        )
        schemaMeta = schemaContext.schemaMetaService
        this.schemaCtx = schemaContext

        metaSchemaVersionKey = listOf("table", tableRef.schema, tableRef.table, "schema-version")

        if (config.defaultQueryPermsPolicy == QueryPermsPolicy.DEFAULT) {
            error("defaultQueryPermsPolicy can't be DEFAULT. TableRef: $tableRef")
        }
        permsPolicy = ThreadLocal.withInitial { config.defaultQueryPermsPolicy }
    }

    override fun <T> doWithPermsPolicy(permsPolicy: QueryPermsPolicy?, action: () -> T): T {
        val prevPolicy = this.permsPolicy.get()
        if (permsPolicy == null || permsPolicy == QueryPermsPolicy.DEFAULT) {
            this.permsPolicy.set(config.defaultQueryPermsPolicy)
        } else {
            this.permsPolicy.set(permsPolicy)
        }
        try {
            return action.invoke()
        } finally {
            if (prevPolicy == null) {
                this.permsPolicy.remove()
            } else {
                this.permsPolicy.set(prevPolicy)
            }
        }
    }

    override fun getSchemaVersion(): Int {
        if (useLastSchemaVersion) {
            return DbDataService.NEW_TABLE_SCHEMA_VERSION
        }
        if (System.currentTimeMillis() < schemaVersionNextUpdateMs && schemaVersion > -1) {
            return schemaVersion
        }
        if (!isTableExists()) {
            schemaVersion = DbDataService.NEW_TABLE_SCHEMA_VERSION
            return schemaVersion
        }
        schemaVersion = schemaMeta.getValue(metaSchemaVersionKey, 0)
        schemaVersionNextUpdateMs = System.currentTimeMillis() + 60_000
        return schemaVersion
    }

    override fun setSchemaVersion(version: Int) {
        if (useLastSchemaVersion) {
            return
        }
        schemaVersion = version
        schemaMeta.setValue(metaSchemaVersionKey, version)
        schemaCacheUpdateRequired.set(true)
    }

    override fun findByIds(ids: Set<Long>): List<T> {
        if (ids.isEmpty()) {
            return emptyList()
        }
        return execReadOnlyQuery {
            findByColumn(getTableContext(), DbEntity.ID, ids, ids.size)
        }.map {
            convertToEntity(it)
        }
    }

    private fun convertToEntity(data: Map<String, Any?>): T {
        return entityMapper.convertToEntity(data, getSchemaVersion())
    }

    private fun findByColumn(
        context: DbTableContext,
        column: String,
        values: Collection<Any>,
        limit: Int,
        expressions: Map<String, ExpressionToken> = emptyMap()
    ): List<Map<String, Any?>> {

        val query = DbFindQuery.create()
            .withPredicate(ValuePredicate(column, ValuePredicate.Type.IN, values))
            .withExpressions(expressions)
            .build()

        return findInRepo(context, query, DbFindPage(0, limit), false).entities
    }

    override fun findById(id: Long): T? {
        return findByAnyId(id)
    }

    override fun findByExtId(id: String): T? {
        return findByAnyId(id)
    }

    override fun findByExtId(id: String, expressions: Map<String, ExpressionToken>): T? {
        return findByAnyId(id, expressions = expressions)
    }

    override fun isExistsByExtId(id: String): Boolean {
        val query = DbFindQuery.create {
            withPredicate(Predicates.eq(DbEntity.EXT_ID, id))
        }
        val res = findRaw(query, DbFindPage.FIRST, false)
        return res.entities.isNotEmpty()
    }

    private fun findByAnyId(
        id: Any,
        expressions: Map<String, ExpressionToken> = emptyMap()
    ): T? {
        return findMapByAnyId(id, expressions)?.let { convertToEntity(it) }
    }

    private fun findMapByAnyId(
        id: Any,
        expressions: Map<String, ExpressionToken> = emptyMap()
    ): Map<String, Any?>? {
        getTableContext()
        return execReadOnlyQuery {
            findMapByAnyIdInEntityRepo(id, expressions)
        }
    }

    private fun findMapByAnyIdInEntityRepo(
        id: Any,
        expressions: Map<String, ExpressionToken> = emptyMap()
    ): Map<String, Any?>? {
        val idColumn = when (id) {
            is String -> DbEntity.EXT_ID
            is Long -> DbEntity.ID
            else -> error("Incorrect id type: ${id::class}")
        }
        return findByColumn(
            getTableContext(),
            idColumn,
            listOf(id),
            1,
            expressions
        ).firstOrNull()
    }

    override fun findAll(): List<T> {
        return findAll(Predicates.alwaysTrue())
    }

    override fun findAll(predicate: Predicate): List<T> {

        val srcQuery = DbFindQuery.create()
            .withPredicate(predicate)
            .build()
        return execReadOnlyQueryWithPredicate(srcQuery, emptyList()) { tableCtx, processedQuery ->
            findInRepo(
                tableCtx,
                processedQuery,
                DbFindPage.ALL,
                false
            ).entities.map {
                convertToEntity(it)
            }
        }
    }

    override fun findAll(predicate: Predicate, sort: List<DbFindSort>): List<T> {
        val srcQuery = DbFindQuery.create()
            .withPredicate(predicate)
            .withSortBy(sort)
            .build()

        return execReadOnlyQueryWithPredicate(srcQuery, emptyList()) { tableCtx, processedQuery ->
            findInRepo(
                tableCtx,
                processedQuery,
                DbFindPage.ALL,
                false
            ).entities.map {
                convertToEntity(it)
            }
        }
    }

    override fun find(predicate: Predicate, sort: List<DbFindSort>, page: DbFindPage): DbFindRes<T> {
        return find(
            DbFindQuery.create {
                withPredicate(predicate)
                withSortBy(sort)
            },
            page
        )
    }

    override fun find(
        predicate: Predicate,
        sort: List<DbFindSort>,
        page: DbFindPage,
        groupBy: List<String>,
        assocTableJoins: List<AssocTableJoin>,
        assocJoinWithPredicates: List<AssocJoinWithPredicate>,
        withTotalCount: Boolean
    ): DbFindRes<T> {
        return find(
            DbFindQuery.create {
                withPredicate(predicate)
                withSortBy(sort)

                withGroupBy(groupBy)
                withAssocTableJoins(assocTableJoins)
                withAssocJoinWithPredicates(assocJoinWithPredicates)
            },
            page,
            withTotalCount
        )
    }

    override fun find(query: DbFindQuery, page: DbFindPage): DbFindRes<T> {
        return find(query, page, false)
    }

    override fun find(
        query: DbFindQuery,
        page: DbFindPage,
        withTotalCount: Boolean
    ): DbFindRes<T> {
        return findRaw(query, page, withTotalCount)
            .mapEntities { convertToEntity(it) }
    }

    override fun findRaw(
        predicate: Predicate,
        sort: List<DbFindSort>,
        page: DbFindPage,
        groupBy: List<String>,
        assocTableJoins: List<AssocTableJoin>,
        assocJoinWithPredicates: List<AssocJoinWithPredicate>,
        withTotalCount: Boolean
    ): DbFindRes<Map<String, Any?>> {
        return findRaw(
            DbFindQuery.create {
                withPredicate(predicate)
                withSortBy(sort)

                withGroupBy(groupBy)
                withAssocTableJoins(assocTableJoins)
                withAssocJoinWithPredicates(assocJoinWithPredicates)
            },
            page,
            withTotalCount
        )
    }

    override fun findRaw(
        query: DbFindQuery,
        page: DbFindPage,
        withTotalCount: Boolean
    ): DbFindRes<Map<String, Any?>> {
        return execReadOnlyQueryWithPredicate(
            query,
            DbFindRes.empty()
        ) { tableCtx, processedQuery ->
            findInRepo(tableCtx, processedQuery, page, withTotalCount)
        }
    }

    private fun findInRepo(
        context: DbTableContext,
        query: DbFindQuery,
        page: DbFindPage,
        withTotalCount: Boolean
    ): DbFindRes<Map<String, Any?>> {
        return entityRepo.find(context, query, page, withTotalCount)
    }

    override fun getCount(query: DbFindQuery): Long {
        return execReadOnlyQueryWithPredicate(query, 0) { tableCtx, preparedQuery ->
            entityRepo.find(
                tableCtx,
                preparedQuery,
                DbFindPage.ZERO,
                true
            ).totalCount
        }
    }

    override fun getCount(predicate: Predicate): Long {
        return getCount(
            DbFindQuery.create()
                .withPredicate(predicate)
                .build()
        )
    }

    override fun save(entity: T): T {
        return save(entity, emptyList())
    }

    override fun saveIfNoConflict(
        entities: Collection<T>,
        conflictColumns: List<String>,
        returningColumn: String
    ): List<Long> {
        if (entities.isEmpty()) {
            return emptyList()
        }
        return dataSource.withTransaction(false) {
            runMigrationsInTxn(emptyList(), DbExpectedAttTypes.EMPTY, mock = false, diff = true)
            entityRepo.insertIfNoConflict(
                getTableContext(),
                entities.map { entityMapper.convertToMap(it) },
                conflictColumns,
                returningColumn
            )
        }
    }

    override fun saveAtomicallyOrGetExistingByExtId(
        entity: T,
        extraLongColumns: List<String>
    ): DbInsertOrGetRes {

        val entityMap = HashMap(entityMapper.convertToMap(entity))
        entityMap.remove(DbEntity.ID)

        if ((entityMap[DbEntity.EXT_ID] as? String).isNullOrBlank()) {
            error("Invalid entity without ${DbEntity.EXT_ID}")
        }

        // A transaction of its own, committed before this returns: an id is a promise to every other
        // transaction, including ones that have already written it into a row of theirs, so the
        // caller's rollback must not take it back. It is also what keeps the row lock `ON CONFLICT
        // DO UPDATE` takes as short as the statement - held to the end of a user's mutation instead,
        // it would put this table into the same lock cycles `ed_associations` is in.
        return TxnContext.doInNewTxn {
            dataSource.withTransaction(false, true) {
                entityRepo.insertOrGetByExtId(getTableContext(), entityMap, extraLongColumns)
            }
        }
    }

    override fun updateByExtIdIfMatches(
        extId: String,
        expected: Map<String, Any?>,
        newValues: Map<String, Any?>
    ): Boolean {
        if (extId.isEmpty() || !isTableExists()) {
            return false
        }
        return dataSource.withTransaction(false) {
            entityRepo.updateByExtIdIfMatches(getTableContext(), extId, expected, newValues)
        }
    }

    override fun updateByIdIfMatches(
        id: Long,
        expected: Map<String, Any?>,
        newValues: Map<String, Any?>
    ): Boolean {
        if (id <= 0 || !isTableExists()) {
            return false
        }
        return dataSource.withTransaction(false) {
            entityRepo.updateByIdIfMatches(getTableContext(), id, expected, newValues)
        }
    }

    override fun save(entity: T, columns: List<DbColumnDef>): T {
        return save(listOf(entity), columns, DbExpectedAttTypes.EMPTY)[0]
    }

    override fun save(entity: T, columns: List<DbColumnDef>, attTypes: DbExpectedAttTypes): T {
        return save(listOf(entity), columns, attTypes)[0]
    }

    override fun save(entities: Collection<T>): List<T> {
        return save(entities, emptyList())
    }

    override fun save(entities: Collection<T>, columns: List<DbColumnDef>): List<T> {
        return save(entities, columns, DbExpectedAttTypes.EMPTY)
    }

    override fun save(
        entities: Collection<T>,
        columns: List<DbColumnDef>,
        attTypes: DbExpectedAttTypes
    ): List<T> {

        return dataSource.withTransaction(false) {

            runMigrationsInTxn(columns, attTypes, mock = false, diff = true)
            val tableCtx = getTableContext()

            val entitiesToSave = entities.map { entity ->
                entityMapper.convertToMap(entity)
            }

            entityRepo.save(tableCtx, entitiesToSave).map { convertToEntity(it) }
        }
    }

    override fun delete(entity: T) {
        if (!isTableExists()) {
            return
        }
        dataSource.withTransaction(false) {
            entityRepo.delete(getTableContext(), entityMapper.convertToMap(entity))
        }
    }

    override fun delete(predicate: Predicate) {
        // Normalised first: PredicateUtils.isAlwaysTrue only recognises a bare VoidPredicate, so
        // and(alwaysTrue()), not(alwaysFalse()) and an empty AndPredicate - all of which delete
        // every row of the table just as surely - would otherwise walk straight past the guard.
        // optimize() folds exactly those shapes down to the VoidPredicate the check does recognise.
        val normalized = PredicateUtils.optimize(predicate)
        require(!PredicateUtils.isAlwaysTrue(normalized)) {
            "Deleting all rows of table '$tableRef' by an always-true predicate is not supported. " +
                "If you really mean to remove specific rows, use delete(entityId) / delete(entities: List<Long>) instead."
        }
        if (!isTableExists()) {
            return
        }
        if (PredicateUtils.isAlwaysFalse(normalized)) {
            return
        }
        dataSource.withTransaction(false) {
            entityRepo.delete(getTableContext(), predicate)
        }
    }

    override fun delete(entityId: Long) {
        if (!isTableExists()) {
            return
        }
        dataSource.withTransaction(false) {
            entityRepo.delete(getTableContext(), listOf(entityId))
        }
    }

    override fun delete(entities: List<T>) {
        if (entities.isEmpty() || !isTableExists()) {
            return
        }
        dataSource.withTransaction(false) {
            entityRepo.delete(
                getTableContext(),
                entities.map {
                    entityMapper.convertToMap(it)[DbEntity.ID] as Long
                }
            )
        }
    }

    override fun getTableContext(): DbTableContext {
        if (tableCtx.columnsOrNull == null) {
            val columns = dataSource.withTransaction(true) {
                schemaDao.getColumns(dataSource, tableRef)
            }
            setColumns(columns)
        }
        if (schemaCacheUpdateRequired.compareAndSet(true, false)) {
            dataSource.withTransaction(true) {
                schemaDao.resetCache(dataSource, tableRef)
            }
        }
        return tableCtx
    }

    override fun getEntityMapper(): DbEntityMapper<T> {
        return entityMapper
    }

    override fun getTableMeta(): DbTableMetaDto {
        val id = tableRef.table
        val metaEntity = tableMetaService?.findByExtId(id) ?: return DbTableMetaDto.create().withId(id).build()
        return DbTableMetaDto.create()
            .withId(id)
            .withChangelog(DataValue.create(metaEntity.changelog).asList(DbTableChangeSet::class.java))
            .withConfig(Json.mapper.read(metaEntity.config, DbTableMetaConfig::class.java))
            .build()
    }

    override fun isTableExists(): Boolean {
        return getTableContext().getColumns().isNotEmpty()
    }

    override fun resetColumnsCache() {
        invalidateColumnCacheOnly()
        // the "forget everything" reset: schema-mismatch recovery and the runMigrations() wrapper
        // both need to drop what this instance thought it knew about the model too, not just the
        // physical columns - see reconciledAttTypeEntries for why runMigrationsInTxn's own in-lock
        // reset deliberately does not call this and calls invalidateColumnCacheOnly() instead
        reconciledAttTypeEntries.set(null)
    }

    /**
     * The narrower half of [resetColumnsCache]: drops only the cached column list, leaving
     * [reconciledAttTypeEntries] alone. Used from inside the migration lock, where the reset exists
     * to pick up another instance's concurrent schema change - not to forget what this instance
     * itself has already reconciled about the model.
     */
    private fun invalidateColumnCacheOnly() {
        setColumns(null)
        schemaCacheUpdateRequired.set(true)
    }

    override fun runMigrations(
        mock: Boolean,
        diff: Boolean
    ): List<String> {
        return runMigrations(emptyList(), mock, diff)
    }

    override fun runMigrations(
        expectedColumns: List<DbColumnDef>,
        mock: Boolean,
        diff: Boolean
    ): List<String> {
        return runMigrations(expectedColumns, DbExpectedAttTypes.EMPTY, mock, diff)
    }

    override fun runMigrations(
        expectedColumns: List<DbColumnDef>,
        attTypes: DbExpectedAttTypes,
        mock: Boolean,
        diff: Boolean
    ): List<String> {
        return dataSource.withTransaction(mock) {
            val result = runMigrationsInTxn(expectedColumns, attTypes, mock, diff)
            resetColumnsCache()
            TxnContext.doAfterRollback(0f, false) {
                resetColumnsCache()
            }
            result
        }
    }

    override fun getTableRef(): DbTableRef {
        return tableRef
    }

    /**
     * Limitation this lock does not close, worth stating plainly rather than leaving the next
     * reader to assume more protection than exists: [EcosLockApi.doInSync] releases the lock when
     * its lambda returns, but the DDL issued inside [runMigrationsInLock] is part of the caller's
     * *mutation* transaction, which commits well after that lambda returns. So a second instance
     * can take the lock the moment the first releases it, read the still-pre-commit schema under
     * `READ COMMITTED`, and issue its own `ALTER` - which blocks on the first transaction's locks
     * and then runs once it commits, doing the conversion twice. This is not fixed here: the real
     * fix is to run the synchronous phase in a short transaction of its own, which is a change to
     * the mutation path rather than to this method.
     */
    private fun runMigrationsInTxn(
        expectedColumns: List<DbColumnDef>,
        attTypes: DbExpectedAttTypes,
        mock: Boolean,
        diff: Boolean
    ): List<String> {

        if (mock) {
            return runMigrationsInLock(expectedColumns, attTypes, mock = true, diff)
        }
        if (!diff) {
            error("Full migration without mock doesn't supported")
        }
        if (migrationInProgress.get()) {
            return runMigrationsInLock(expectedColumns, attTypes, mock = false, diff = true)
        }
        if (!isSchemaChangeRequired(expectedColumns, attTypes)) {
            // the overwhelmingly common case: nothing to migrate, so no lock, no round trip
            return emptyList()
        }
        val lockKey = schemaMigrationLockKey(tableRef)
        val lockTimeout = schemaCtx.dataSourceCtx.props.columns.schemaMigrationLockTimeout
        migrationInProgress.set(true)
        try {
            return schemaCtx.webAppApi.getAppLockApi().doInSync(lockKey, lockTimeout) {
                // only the columns, not the reconciled set: this reset exists so another instance's
                // concurrent schema change is picked up, not to forget this instance's own
                // reconciliation - see reconciledAttTypeEntries for the full reasoning, including
                // why a plain read-then-write here would be unsafe across threads
                invalidateColumnCacheOnly()
                getTableContext()
                val result = runMigrationsInLock(expectedColumns, attTypes, mock = false, diff = true)
                // only when a transaction exists to protect it: TxnContext.doAfterRollback below is
                // a silent no-op with no enclosing transaction (a library caller of save() outside
                // TxnContext), while the datasource still rolls the connection back on failure - so
                // publishing an optimistic reconciliation nothing could retract would strand it for
                // the process lifetime, exactly like the failure this same guard prevents inside a
                // transaction. The records mutation path always has one, so this costs it nothing.
                if (TxnContext.getTxnOrNull() != null) {
                    // reconciled: fold this migration's semantic map into whatever the field holds
                    // right now - never a copy captured earlier, which another thread's rollback
                    // hook could not have retracted from - and never attTypes.attTypes itself by
                    // reference, which the caller (MutationContext) can still extend after this
                    // returns
                    reconciledAttTypeEntries.updateAndGet { current ->
                        current.orEmpty() + attTypes.attTypes.map { (name, type) -> name to type }
                    }
                    // ... but only for as long as the transaction that earned it survives:
                    // seedColumnMeta's registry rows are written in this same transaction and
                    // produce no schema commands, so a later rollback (a failed validation, an
                    // assoc, a listener) has to take this reconciliation down with it, or every
                    // later save would wrongly believe the registry already reflects rows that no
                    // longer exist
                    TxnContext.doAfterRollback(0f, false) {
                        reconciledAttTypeEntries.set(null)
                    }
                }
                result
            }
        } catch (e: AcquireTimeoutException) {
            throw IllegalStateException(
                "Schema migration lock can't be acquired for table ${tableRef.fullName} within " +
                    "$lockTimeout - another instance is very likely migrating this table right now.",
                e
            )
        } finally {
            migrationInProgress.remove()
        }
    }

    /**
     * Whether anything the migration cares about has to change, judged without touching the database.
     *
     * A **physical** change - a column missing, of the wrong type, or an array where the model wants
     * a scalar - is read off the cached column set. The narrowing is checked separately, because
     * [isColumnSchemaUpdateRequired] ignores it on purpose (there is no physical work in it) while
     * the conversion map calls it lossy and migrates it; without this the fast path would answer
     * "nothing to do" and that migration would never be entered.
     *
     * A **semantic** one is invisible in the column set: several attribute types share one physical
     * type, so `ASSOC -> ENTITY_REF` changes nothing a column definition can show. That is what
     * [reconciledAttTypeEntries] is for - an incoming pair the set does not contain means the model
     * moved, even if no DDL follows. Containment and not equality; see that field for why.
     *
     * Deliberately cheap and allowed to be wrong only in the optimistic direction: a stale cache can
     * say "yes" when the answer is "no", at the cost of one lock and a no-op migration. Saying "no"
     * wrongly is impossible - a cache that has not seen a column cannot claim it exists.
     *
     * What still answers "yes" on every save is a **frozen** column - two types sharing a table and
     * disagreeing about it, where nothing may change until the model does. The disagreement can be
     * about multiplicity alone, since the conflict shape is `(attType, multiple)`. A cost, not a
     * correctness problem.
     */
    private fun isSchemaChangeRequired(
        expectedColumns: List<DbColumnDef>,
        attTypes: DbExpectedAttTypes
    ): Boolean {
        val currentColumns = tableCtx.columnsOrNull ?: return true
        if (currentColumns.isEmpty()) {
            return true
        }
        val reconciled = reconciledAttTypeEntries.get() ?: return true
        for ((name, type) in attTypes.attTypes) {
            if ((name to type) !in reconciled) {
                return true
            }
        }
        val currentByName = currentColumns.associateBy { it.name }
        for (column in entityMapper.getEntityColumns().map { it.columnDef }) {
            val current = currentByName[column.name] ?: return true
            if (isColumnSchemaUpdateRequired(current, column) || isMultiplicityNarrowed(current, column)) {
                return true
            }
        }
        for (column in expectedColumns) {
            val current = currentByName[column.name] ?: return true
            if (isColumnSchemaUpdateRequired(current, column) || isMultiplicityNarrowed(current, column)) {
                return true
            }
        }
        return false
    }

    private fun runMigrationsInLock(
        expectedColumns: List<DbColumnDef>,
        attTypes: DbExpectedAttTypes,
        mock: Boolean,
        diff: Boolean
    ): List<String> {

        getTableContext()

        val expectedWithEntityColumns = ArrayList(entityMapper.getEntityColumns().map { it.columnDef })
        expectedWithEntityColumns.addAll(expectedColumns)

        validateColumnNames(expectedWithEntityColumns)

        val startTime = Instant.now()
        val changedColumns = mutableListOf<DbColumnDef>()
        val migration = {
            dataSource.watchSchemaCommands {
                changedColumns.addAll(ensureColumnsExistImpl(expectedWithEntityColumns, attTypes, mock, diff))
            }
        }
        if (mock) {
            return dataSource.withSchemaMock { migration.invoke() }
        }

        val commands = try {
            migration.invoke()
        } catch (e: Throwable) {
            resetColumnsCache()
            if (e::class == InterruptedException::class) {
                Thread.currentThread().interrupt()
                throw e
            } else {
                getTableContext()
                migration.invoke()
            }
        }
        val durationMs = System.currentTimeMillis() - startTime.toEpochMilli()

        if (commands.isNotEmpty()) {
            resetColumnsCache()
            TxnContext.doAfterRollback(0f, false) {
                resetColumnsCache()
            }
        }

        if (commands.isNotEmpty() && tableMetaService != null) {

            val currentMeta = tableMetaService.findByExtId(tableRef.table)

            val tableMetaNotNull = if (currentMeta == null) {
                val newMeta = DbTableMetaEntity()
                newMeta.extId = tableRef.table
                val config = DbTableMetaConfig(config)
                newMeta.config = Json.mapper.toString(config) ?: "{}"
                newMeta
            } else {
                currentMeta
            }

            val changeLog: MutableList<DbTableChangeSet> =
                DataValue.create(tableMetaNotNull.changelog).asList(DbTableChangeSet::class.java)

            val params = ObjectData.create()
            params["columns"] = changedColumns

            changeLog.add(
                DbTableChangeSet(
                    startTime,
                    durationMs,
                    "ensure-columns-exist",
                    params,
                    commands
                )
            )

            tableMetaNotNull.changelog = Json.mapper.toString(changeLog) ?: "[]"
            tableMetaService.save(tableMetaNotNull, emptyList())
        }

        return commands
    }

    /**
     * Fails fast on a column name the backend would silently truncate. Runs before any DDL, so the
     * table is not created (or altered) with a mangled name that the read path could never find.
     * The check is deliberately not inside [ensureColumnsExistImpl]: the migration is retried once
     * on failure, which would swallow the first exception.
     */
    private fun validateColumnNames(columns: List<DbColumnDef>) {
        val limit = schemaDao.getMaxColumnNameBytes()
        val tooLong = columns.map { it.name to it.name.toByteArray(Charsets.UTF_8).size }
            .filter { (_, lengthInBytes) -> lengthInBytes > limit }
        if (tooLong.isEmpty()) {
            return
        }
        // every offender goes to the log, so one fix round covers them all; the exception names the first
        for ((name, lengthInBytes) in tooLong) {
            // attribute-derived names are ASCII (see DbEcosModelService.VALID_COLUMN_NAME),
            // so the name the database would silently use is a plain prefix of the requested one
            val truncatedName = if (name.length == lengthInBytes) name.substring(0, limit) else null
            val truncatedExists = truncatedName != null && tableCtx.columnsOrNull?.any { it.name == truncatedName } == true
            log.error {
                "Column name is too long and would be silently truncated by the database. " +
                    "Table: ${tableRef.fullName} column: '$name' " +
                    "length: $lengthInBytes bytes, limit: $limit bytes. " +
                    "Truncated column '${truncatedName ?: "?"}' " +
                    (if (truncatedExists) "already exists" else "doesn't exist") + " in the table."
            }
        }
        val (name, lengthInBytes) = tooLong.first()
        throw I18nRuntimeException(
            messageKey = "ecos-data.column-name-too-long",
            messageArgs = mapOf(
                "column" to name,
                "length" to lengthInBytes,
                "limit" to limit,
                "table" to tableRef.fullName
            )
        )
    }

    private fun ensureColumnsExistImpl(
        expectedColumns: List<DbColumnDef>,
        attTypes: DbExpectedAttTypes,
        mock: Boolean,
        diff: Boolean
    ): List<DbColumnDef> {

        val currentColumns = if (diff) {
            tableCtx.columnsOrNull ?: error("Current columns is null")
        } else {
            emptyList()
        }
        val expectedIndexes = entityMapper.getEntityIndexes()

        if (currentColumns.isEmpty()) {
            schemaDao.createTable(dataSource, tableRef, expectedColumns)
            if (!mock) {
                setSchemaVersion(DbDataService.NEW_TABLE_SCHEMA_VERSION)
                writeColumnMeta(expectedColumns, attTypes)
            }
            addIndexesAndConstraintsForNewColumns(expectedColumns, expectedIndexes, config.fkConstraints)
            return expectedColumns
        }
        val currentColumnsByName = currentColumns.associateBy { it.name }

        // before anything is altered: record what the columns are right now, for every column the
        // registry has not seen yet. On an upgraded installation this is the only moment at which
        // the pre-existing state can still be observed. Done before the diff below reads the
        // registry, so that a column the registry has only just met is described there too.
        if (!mock) {
            seedColumnMeta(currentColumns, attTypes)
        }

        // Read unconditionally, mock runs included: this is what makes a semantic-only change
        // visible (see the diff below): a mock run may not *write* the registry, but it has to
        // read it, because a preview has to reach the same decision a real run would.
        // Skipped only where seedColumnMeta skips too: a table with no type model has no attribute
        // columns for the registry to describe, and asking it about one would also make
        // ed_column_meta's own migration query the very table it is migrating.
        val columnMetaByName = if (attTypes.attTypes.isEmpty()) {
            emptyMap()
        } else {
            schemaCtx.columnMetaService.getByTable(tableRef.table).associateBy { it.columnName }
        }

        val columnsWithChangedType = expectedColumns.filter { expectedColumn ->
            val currentColumn = currentColumnsByName[expectedColumn.name]
            if (currentColumn == null) {
                false
            } else {
                isColumnSchemaUpdateRequired(currentColumn, expectedColumn) ||
                    isSemanticTypeChanged(expectedColumn, attTypes, columnMetaByName)
            }
        }

        val convertedColumns = ArrayList<DbColumnDef>()
        for (expectedColumn in columnsWithChangedType) {
            val currentColumn = currentColumnsByName.getValue(expectedColumn.name)

            val conflictingTypes = attTypes.conflicts[expectedColumn.name]
            if (conflictingTypes != null) {
                // Same dedup-key reasoning as the isTypeChangeSupported branch below: a mock/preview
                // run (runMigrations(mock = true)) must not consume the errorOnce key, or the real
                // mutation that follows would find it already spent and log nothing at all. The
                // freeze itself (the `continue`) is not conditional on mock - a preview must report
                // the same migration decision a real run would make.
                if (!mock) {
                    DbReadToleranceLog.errorOnce(
                        log,
                        "column-type-conflict:${tableRef.fullName}:${expectedColumn.name}"
                    ) {
                        val overBroadNote = if (attTypes.groupingMayBeOverBroad) {
                            " (this table's source id could not be resolved, so this grouping may " +
                                "be over-broad and this may not be a real conflict)"
                        } else {
                            ""
                        }
                        "Types sharing one table declare attribute '${expectedColumn.name}' with " +
                            "different types, so the column is left as it is and will not be " +
                            "migrated. Fix the model to make them agree. " +
                            "Table: ${tableRef.fullName} conflicting types: $conflictingTypes$overBroadNote"
                    }
                }
                continue
            }

            if (isRegistryOnlyChange(currentColumn, expectedColumn, attTypes, columnMetaByName)) {
                // The same bytes, with the same meaning and the same search
                // behaviour. `setColumnType` has nothing to emit for this pair, so the two branches
                // below - both of which exist to keep a *table rewrite* out of a user's mutation -
                // have nothing to protect anyone from, and the row count is irrelevant. Deferring
                // one of these would move a full column into a permanent backup and leave the
                // attribute reading empty until a transfer copied the values back onto themselves.
                convertedColumns.add(expectedColumn)
                continue
            }

            if (!mock && tryRestoreColumn(expectedColumn, attTypes)) {
                // The attribute is going back to a type one of its backups already holds.
                // The restore has already renamed the two columns, rewritten the two registry rows
                // and queued the transfer that fills in the rest, so this column is done - and it
                // deliberately does not join `convertedColumns`, whose registry write would undo
                // half of that.
                continue
            }

            if (!schemaDao.isTypeChangeSupported(currentColumn, expectedColumn)) {
                // Not converting in place is the whole point: failing here would abort the
                // mutation of every record of this table, not just of this attribute. The column
                // moves aside instead and the values are transferred in the
                // background - the old column is kept, so nothing is lost either way.
                //
                // Only log on a real run: a mock run (runMigrations(mock = true), the preview/dry-run
                // path) must not consume the dedup key, or the real mutation that follows would find
                // it already spent and log nothing at all.
                if (!mock) {
                    DbReadToleranceLog.errorOnce(
                        log,
                        "column-type-change-not-supported:${tableRef.fullName}:${expectedColumn.name}"
                    ) {
                        "Column type change is not supported by the backend. " +
                            "Table: ${tableRef.fullName} column: '${expectedColumn.name}' " +
                            "current: ${currentColumn.type}/${currentColumn.multiple} " +
                            "expected: ${expectedColumn.type}/${expectedColumn.multiple}"
                    }
                }
                moveColumnAside(currentColumn, expectedColumn, attTypes, columnMetaByName, mock)
                continue
            }

            val inPlaceMaxRows = schemaCtx.dataSourceCtx.props.columns.inPlaceAlterMaxRows
            if (schemaDao.isRowsCountGreaterThan(dataSource, tableRef, inPlaceMaxRows)) {
                // an in-place ALTER here would rewrite the whole table under an exclusive lock,
                // inside a user's mutation. The column moves aside instead - a rename and an add
                // are O(1) metadata operations - and the background migration takes it from there.
                if (!mock) {
                    DbReadToleranceLog.errorOnce(
                        log,
                        "column-type-change-deferred:${tableRef.fullName}:${expectedColumn.name}"
                    ) {
                        "Column type change was deferred because the table holds more than " +
                            "$inPlaceMaxRows rows and an in-place conversion would lock it. " +
                            "Table: ${tableRef.fullName} column: '${expectedColumn.name}' " +
                            "current: ${currentColumn.type}/${currentColumn.multiple} " +
                            "expected: ${expectedColumn.type}/${expectedColumn.multiple}"
                    }
                }
                moveColumnAside(currentColumn, expectedColumn, attTypes, columnMetaByName, mock)
                continue
            }

            if (!isInPlaceConversionSafe(currentColumn, expectedColumn, attTypes, columnMetaByName)) {
                // The backend is willing, and the table is small enough, but the conversion map is
                // not: a backend that can cast TEXT -> JSON must not be allowed to do it in place,
                // because that pair is classified lossy and the cast throws on the
                // first invalid value - aborting the whole mutation of the user who happened to
                // touch this table first.
                moveColumnAside(currentColumn, expectedColumn, attTypes, columnMetaByName, mock)
                continue
            }
            schemaDao.setColumnType(dataSource, tableRef, expectedColumn.name, expectedColumn.multiple, expectedColumn.type)
            convertedColumns.add(expectedColumn)
        }
        if (!mock && convertedColumns.isNotEmpty()) {
            writeColumnMeta(convertedColumns, attTypes)
        }

        val missedColumns = expectedColumns.filter { !currentColumnsByName.containsKey(it.name) }

        // fix for legacy tables where REF_ID is not filled
        val fixedMissedColumns = missedColumns.map { col ->
            if (col.name == DbEntity.REF_ID) {
                col.withConstraints(col.constraints.filter { it != DbColumnConstraint.NOT_NULL })
            } else {
                col
            }
        }

        schemaDao.addColumns(dataSource, tableRef, fixedMissedColumns)
        addIndexesAndConstraintsForNewColumns(fixedMissedColumns, expectedIndexes, config.fkConstraints)
        if (!mock) {
            writeColumnMeta(fixedMissedColumns, attTypes)
        }

        val changedColumns = ArrayList(convertedColumns)
        changedColumns.addAll(fixedMissedColumns)

        return changedColumns
    }

    /**
     * Whether the model changed what a column is supposed to hold, in a way the physical column
     * definition does not show. The diff is on `(AttributeType, multiple)`, and the registry records
     * both halves, which is what makes the comparison possible at all.
     *
     * **Type.** 15 attribute types map onto 8 column types, so `ASSOC -> ENTITY_REF` or
     * `TEXT -> MLTEXT` leave the definition untouched and move the values somewhere else entirely.
     *
     * **Multiplicity.** [isColumnSchemaUpdateRequired] reports a widening but deliberately ignores a
     * narrowing, because there is no physical work in it - the right answer to the question *it*
     * asks and the wrong one here, since `multiple -> single` is lossy and is one of the changes
     * this machinery exists to stop silently ignoring. Left out, a narrowed array never enters the
     * change set and the column stays an array for ever while the model says scalar.
     *
     * Reporting it here is enough: the ladder that follows refuses it as free
     * ([isRegistryOnlyChange]) and refuses it in place ([isInPlaceConversionSafe]), so it lands on
     * [moveColumnAside]. A JSON column is the one case where flipping the flag really is free, and
     * it stays free.
     */
    private fun isSemanticTypeChanged(
        expectedColumn: DbColumnDef,
        attTypes: DbExpectedAttTypes,
        columnMetaByName: Map<String, DbColumnMetaDto>
    ): Boolean {
        val recorded = columnMetaByName[expectedColumn.name] ?: return false
        val expectedSemantic = attTypes.attTypes[expectedColumn.name] ?: return false
        // a backup is nobody's attribute any more; it must never be diffed against the
        // model, even if a row for it somehow shares this column name
        if (recorded.backup) {
            return false
        }
        return recorded.attType != expectedSemantic || recorded.multiple != expectedColumn.multiple
    }

    /**
     * Whether the model change is a correction to the registry and nothing else - no `ALTER`, no
     * transfer, no backup.
     *
     * `TEXT <-> OPTIONS` and any move between assoc-like types are the same bytes, holding the same
     * meaning, searched the same way. `DbSchemaDao.setColumnType` emits nothing
     * for such a pair on any backend, which is precisely why the decision has to be taken **before**
     * [DbEcosDataProps.ColumnsProps.inPlaceAlterMaxRows] is consulted: that threshold exists to keep
     * a table rewrite out of a user's mutation, and where there is no rewrite it must not apply.
     * Letting it apply would rename a column full of values into a permanent backup, leave the
     * attribute empty for every record until a full-table transfer had copied the values back onto
     * themselves, and - between assoc-like types - drag the links into the association backup for a
     * change that moves no bytes at all.
     *
     * The conversion map has the last word here, not [isColumnSchemaUpdateRequired] and not
     * [DbColumnConversions.isSameStoredForm] alone. Both of those answer only half the question:
     * the first ignores an array narrowed to a scalar (a deliberate, long-standing decision about
     * *physical* work), and the second compares types and says nothing about multiplicity. Their
     * conjunction would therefore call `TEXT[] -> OPTIONS` free, while `multiple -> single` is
     * lossy - the first element is taken and the rest stays in the backup - and the two halves
     * combine by the more cautious of them. Asking
     * [DbColumnConversions.classify] for [DbConversionClass.SAFE] keeps this branch tied to the map
     * that owns that rule, so the two cannot drift apart later; a bare
     * `currentColumn.multiple != expectedColumn.multiple` guard would have to be kept in step with
     * the map by hand.
     */
    private fun isRegistryOnlyChange(
        currentColumn: DbColumnDef,
        expectedColumn: DbColumnDef,
        attTypes: DbExpectedAttTypes,
        columnMetaByName: Map<String, DbColumnMetaDto>
    ): Boolean {
        if (isColumnSchemaUpdateRequired(currentColumn, expectedColumn)) {
            return false
        }
        val sourceType = columnMetaByName[currentColumn.name]?.attType ?: return false
        val targetType = attTypes.attTypes[expectedColumn.name] as? DbColumnSemanticType.Model ?: return false
        if (!DbColumnConversions.isSameStoredForm(sourceType, targetType.attType)) {
            return false
        }
        return DbColumnConversions.classify(
            sourceType,
            currentColumn.multiple,
            targetType.attType,
            expectedColumn.multiple
        ) == DbConversionClass.SAFE
    }

    /**
     * Whether the backend may convert this column where it stands.
     *
     * Two conditions: the conversion has to be [DbConversionClass.SAFE], **and** it has to be
     * something an `ALTER ... TYPE ... USING` actually performs.
     *
     * The first is not what the backend's `isTypeChangeSupported` answers - that one says "can I
     * express this", and `TEXT -> JSON` is expressible and throws on the first value that is not
     * valid JSON, inside the mutation of whichever user touched the table first.
     *
     * The second matters because the diff is semantic: a change that leaves the physical definition
     * untouched gives `setColumnType` nothing to do, so "in place" would mean nothing happening to
     * the values while the registry claims the new type. The case where that is the whole truth is
     * taken by [isRegistryOnlyChange] first; what is left is `TEXT -> MLTEXT`, safe but needing the
     * values rewritten, so it belongs on the shadow-column path.
     *
     * A third condition sits in front of both: an MLTEXT target is never converted in place **unless
     * the source is already MLTEXT**, because `NUMBER -> MLTEXT` (and BOOLEAN, DATE, DATETIME) is
     * SAFE *and* a physical change, and would otherwise take an `ALTER ... USING col::text` that
     * writes a bare string where the model now expects a serialized MLText.
     *
     * A column the registry does not describe on both sides is not a model-level migration at all -
     * a system column, or a table with no type model - and the conversion map has nothing to say
     * about it. Those convert in place as they always did; [moveColumnAside] declines them for the
     * same reason, so refusing here would leave them permanently unconverted.
     */
    private fun isInPlaceConversionSafe(
        currentColumn: DbColumnDef,
        expectedColumn: DbColumnDef,
        attTypes: DbExpectedAttTypes,
        columnMetaByName: Map<String, DbColumnMetaDto>
    ): Boolean {
        val sourceType = columnMetaByName[currentColumn.name]?.attType ?: return true
        val targetType = attTypes.attTypes[expectedColumn.name] as? DbColumnSemanticType.Model ?: return true
        val sourceAttType = (sourceType as? DbColumnSemanticType.Model)?.attType
        if (targetType.attType == AttributeType.MLTEXT && sourceAttType != AttributeType.MLTEXT) {
            // An MLTEXT column holds a serialized MLText - `{"en":"100"}` - and no backend expression
            // produces one from a value that is not already one. `TEXT/OPTIONS -> MLTEXT` is
            // already refused here, by the physical-change condition below; the scalar sources are the same
            // case wearing a disguise, because `SAFE_TO_STRING -> G_STR` is class SAFE and `G_STR`
            // includes MLTEXT, so `NUMBER -> MLTEXT` (and BOOLEAN, DATE, DATETIME) *does* change the
            // physical type and would take `ALTER ... USING col::text`, leaving the bare string
            // `100` in a column the registry then records as MLTEXT.
            //
            // That is not a cosmetic difference: `DbRecordsQueryDao` rewrites an `EQ` on an MLTEXT
            // attribute into `CONTAINS '"value"'`, the quoted form, so a bare `100` never matches
            // `%"100"%` and the attribute's own equality search stops finding the row - for good, on
            // every table below `inPlaceAlterMaxRows`, with nothing queued to repair it. The
            // row-by-row path writes the MLText form and gets it right, so those targets go there.
            //
            // An MLTEXT **source** is deliberately not one of them. `MLTEXT -> MLTEXT[]` is the
            // ordinary "make this field multi-valued" change, and the backend's
            // `ALTER ... USING array[col]` performs it in O(values) without touching a value: there
            // is no bare string to rewrite, because the column already holds the serialized form.
            // Sending it to the shadow column would copy a whole column aside to achieve what one
            // cheap ALTER already did.
            return false
        }
        val conversionClass = DbColumnConversions.classify(
            sourceType,
            currentColumn.multiple,
            targetType.attType,
            expectedColumn.multiple
        )
        if (conversionClass != DbConversionClass.SAFE) {
            return false
        }
        // A purely semantic change that gets this far is not one of the free ones - those are taken
        // by [isRegistryOnlyChange] before the branches above - so there is genuinely nothing for
        // `setColumnType` to do, and the values the model now wants have to be written by someone.
        return isColumnSchemaUpdateRequired(currentColumn, expectedColumn)
    }

    /**
     * Asked for one column of the diff: is there a backup of this attribute that
     * already holds exactly the `(AttributeType, multiple)` pair the model is asking for? If there
     * is, that backup comes back into place instead of the column being converted or replaced.
     *
     * **Both sides of where this sits in the ladder matter.**
     *
     * It is *before* [moveColumnAside] and before the in-place `setColumnType`, because a matching
     * backup wins over both. Asked only on the shadow path it would be skipped by exactly the round
     * trips an administrator hits first: the forward leg is lossy, which is what leaves a backup,
     * and the reverse leg is very often a pure cast (`DATE -> DATETIME`, `NUMBER -> TEXT`,
     * `X -> X[]`) - so below [DbEcosDataProps.ColumnsProps.inPlaceAlterMaxRows] the column would be
     * cast where it stands and the attribute would answer from the lossy leg while its value sat in
     * a backup of this very table.
     *
     * It is *after* [isRegistryOnlyChange], because a relabel leaves the live column holding the
     * attribute's current values. A restore there would put a stale backup of the target type in
     * their place, and between assoc-like types it would do worse: raising the parked links is
     * additive, so links the user has moved on from would come back alongside the live ones. Such a
     * pair is reachable - `OPTIONS -> NUMBER -> TEXT -> OPTIONS` - so the ordering is load-bearing.
     *
     * Never asked on a mock run: a preview may not rewrite the registry.
     *
     * @return true when the restore happened and this column needs nothing further.
     */
    private fun tryRestoreColumn(
        expectedColumn: DbColumnDef,
        attTypes: DbExpectedAttTypes
    ): Boolean {
        val targetType = (attTypes.attTypes[expectedColumn.name] as? DbColumnSemanticType.Model)?.attType
            ?: return false
        return DbShadowColumnTransition(
            // getTableContext() rather than the captured tableCtx, for the same reason
            // [moveColumnAside] does it: an earlier column of this very loop may already have moved
            // aside and invalidated the cached list, and the aside name is derived from the names
            // already taken
            getTableContext(),
            ::invalidateColumnCacheOnly
        ).tryRestore(
            expectedColumn,
            targetType,
            attTypes.childAtts.contains(expectedColumn.name),
            AuthContext.getCurrentUser().ifBlank { AuthUser.SYSTEM }
        )
    }

    /**
     * Hands a column the backend cannot convert in place - or one on a table too
     * large to convert inside a user's mutation, or one whose conversion is not safe enough to do
     * in place - to the shadow-column path, which keeps the old values in a backup and fills the
     * new column in the background.
     *
     * A column the registry does not describe, or one with no attribute type to migrate towards, is
     * not moved: on a real run [seedColumnMeta] has just written a row for every model-driven
     * column, so the only way to get here without one is a column that is not an attribute at all -
     * and there is nothing to migrate it towards. On a mock run the seeding has not happened, so a
     * table the registry has never seen previews as "nothing to do" rather than as a transition.
     */
    private fun moveColumnAside(
        currentColumn: DbColumnDef,
        expectedColumn: DbColumnDef,
        attTypes: DbExpectedAttTypes,
        columnMetaByName: Map<String, DbColumnMetaDto>,
        mock: Boolean
    ) {
        val sourceType = columnMetaByName[currentColumn.name]?.attType ?: return
        val targetType = (attTypes.attTypes[expectedColumn.name] as? DbColumnSemanticType.Model)?.attType ?: return
        DbShadowColumnTransition(
            // getTableContext() rather than the captured tableCtx: an earlier column of this very
            // loop may already have moved aside and invalidated the cached list, and the backup
            // name is derived from the names already taken
            getTableContext(),
            // the transition's DDL bypasses this data service entirely, so its own cached column
            // list has to be dropped by hand - see the transition for why the schema-level reset
            // is not enough
            ::invalidateColumnCacheOnly
        ).moveAside(
            currentColumn,
            expectedColumn,
            sourceType,
            targetType,
            attTypes.childAtts.contains(expectedColumn.name),
            AuthContext.getCurrentUser().ifBlank { AuthUser.SYSTEM },
            mock
        )
    }

    /**
     * Fills in registry rows for model-driven columns that do not have one yet, describing them as
     * they physically are.
     *
     * A column whose physical shape matches what its model type would produce is recorded with that
     * model type. A column whose shape contradicts the model is recorded as an unknown source type
     * carrying only its physical shape - there is no [ru.citeck.ecos.model.lib.attributes.dto.AttributeType]
     * to record, because nothing ever wrote one down, and guessing would be worse than admitting it
     *. The second case is what a failed conversion leaves behind, so an upgrade to this
     * version is also what makes an already broken table describable.
     *
     * Only columns the model knows about are touched. System columns are not attributes, and a
     * registry row for a column whose attribute has since been deleted is left alone.
     */
    private fun seedColumnMeta(
        currentColumns: List<DbColumnDef>,
        attTypes: DbExpectedAttTypes
    ) {
        if (attTypes.attTypes.isEmpty()) {
            // no type model behind this table - every system table, including ed_column_meta itself,
            // whose own migration must not recurse into the registry
            return
        }
        val known = schemaCtx.columnMetaService.getByTable(tableRef.table).map { it.columnName }.toSet()
        val currentByName = currentColumns.associateBy { it.name }

        val toSeed = ArrayList<DbColumnMetaDto>()
        for ((columnName, semanticType) in attTypes.attTypes) {
            if (known.contains(columnName)) {
                continue
            }
            val currentColumn = currentByName[columnName] ?: continue
            val expectedPhysicalType = (semanticType as? DbColumnSemanticType.Model)?.let {
                DbAttTypeColumns.getColumnType(it.attType)
            }
            val matchesModel = expectedPhysicalType == currentColumn.type
            toSeed.add(
                newColumnMeta(
                    columnName = columnName,
                    attId = columnName,
                    attType = if (matchesModel) {
                        semanticType
                    } else {
                        DbColumnSemanticType.Raw(currentColumn.type)
                    },
                    multiple = currentColumn.multiple
                )
            )
        }
        schemaCtx.columnMetaService.saveAll(toSeed)
    }

    /**
     * Records the given columns with their model semantic type, replacing any existing row.
     *
     * Called only for columns that were just created or just converted, i.e. for columns whose
     * physical content now really is what the model says. A semantic change with no physical effect
     * deliberately does not come through here: the registry has to keep describing the old content
     * until something actually moves it.
     */
    private fun writeColumnMeta(
        columns: List<DbColumnDef>,
        attTypes: DbExpectedAttTypes
    ) {
        if (attTypes.attTypes.isEmpty() || columns.isEmpty()) {
            return
        }
        val existingByName = schemaCtx.columnMetaService.getByTable(tableRef.table).associateBy { it.columnName }
        val toWrite = ArrayList<DbColumnMetaDto>()
        for (column in columns) {
            val semanticType = attTypes.attTypes[column.name] ?: continue
            val existing = existingByName[column.name]
            toWrite.add(
                newColumnMeta(
                    columnName = column.name,
                    attId = column.name,
                    attType = semanticType,
                    multiple = column.multiple,
                    id = existing?.id ?: DbColumnMetaDto.NEW_REC_ID,
                    created = existing?.created
                )
            )
        }
        schemaCtx.columnMetaService.saveAll(toWrite)
    }

    private fun newColumnMeta(
        columnName: String,
        attId: String,
        attType: DbColumnSemanticType,
        multiple: Boolean,
        id: Long = DbColumnMetaDto.NEW_REC_ID,
        created: Instant? = null
    ): DbColumnMetaDto {
        return DbColumnMetaDto(
            id = id,
            table = tableRef.table,
            columnName = columnName,
            attId = attId,
            attType = attType,
            multiple = multiple,
            backup = false,
            created = created ?: Instant.now(),
            creator = AuthContext.getCurrentUser().ifBlank { AuthUser.SYSTEM }
        )
    }

    private fun addIndexesAndConstraintsForNewColumns(
        newColumns: List<DbColumnDef>,
        indexes: List<DbIndexDef>,
        fkConstraints: List<DbFkConstraint>
    ) {

        if (newColumns.isEmpty()) {
            return
        }
        getTableContext()

        val currentColumnsNames = schemaDao.getColumns(dataSource, tableRef).map { it.name }.toSet()
        val newColumnsNames = newColumns.map { it.name }.toSet()

        val newIndexes = indexes.filter { index ->
            val columns = index.columns
            columns.any { newColumnsNames.contains(it) } &&
                columns.all { currentColumnsNames.contains(it) }
        }
        if (newIndexes.isNotEmpty()) {
            schemaDao.createIndexes(dataSource, tableRef, newIndexes)
        }
        val newConstraints = fkConstraints.filter { constraint ->
            newColumnsNames.contains(constraint.baseColumnName)
        }
        if (newConstraints.isNotEmpty()) {
            schemaDao.createFkConstraints(dataSource, tableRef, newConstraints)
        }
    }

    /**
     * Whether the column is an array where the model now asks for a single value.
     *
     * **Kept apart from [isColumnSchemaUpdateRequired] because of the column the registry does not
     * describe.** [isInPlaceConversionSafe] answers `true` early for a column with no registry row
     * on either side - a system column, or a table with no type model - and such a column goes
     * straight to `setColumnType`; a predicate that folded a narrowing into
     * [isColumnSchemaUpdateRequired] would hand it one, which it has no expression for. For a
     * registry-*described* column the folding would be harmless, because [isInPlaceConversionSafe]
     * gates on `classify(...) == SAFE` first and a narrowing is class C, so the argument that the
     * two functions answer different questions is true but is not what keeps them apart.
     *
     * The different questions are worth stating anyway, since they are why the answers differ:
     * [isColumnSchemaUpdateRequired] answers "does the physical column definition have to be
     * altered", and for a narrowing the long-standing - and still correct - answer is no. This one
     * answers "does the *model* disagree with the column", which for a narrowing is yes: the first
     * element is kept and the rest stays in the backup.
     *
     * JSON is excluded for the same reason it is excluded there: a JSON column is physically scalar
     * whatever the model's multiplicity says, so its flag flipping is a no-op, not a narrowing.
     */
    private fun isMultiplicityNarrowed(currentColumn: DbColumnDef, expectedColumn: DbColumnDef): Boolean {
        return currentColumn.multiple &&
            !expectedColumn.multiple &&
            expectedColumn.type != DbColumnType.JSON
    }

    private fun isColumnSchemaUpdateRequired(currentColumn: DbColumnDef, expectedColumn: DbColumnDef): Boolean {

        if (currentColumn.type != expectedColumn.type) {
            return true
        }

        return currentColumn.multiple != expectedColumn.multiple &&
            !currentColumn.multiple &&
            expectedColumn.type != DbColumnType.JSON
    }

    private fun prepareQuery(query: DbFindQuery): DbFindQuery {
        return query.copy()
            .withPredicate(
                preparePredicate(
                    query.predicate,
                    query.assocTableJoins,
                    query.assocJoinsWithPredicate,
                    query.expressions
                )
            ).build()
    }

    private fun preparePredicate(
        predicate: Predicate,
        assocTableJoins: List<AssocTableJoin>,
        assocJoinWithPredicates: List<AssocJoinWithPredicate>,
        expressions: Map<String, ExpressionToken>
    ): Predicate {

        if (PredicateUtils.isAlwaysTrue(predicate) || PredicateUtils.isAlwaysFalse(predicate)) {
            return predicate
        }

        val assocAttToColumnMap = HashMap<String, String>()
        assocTableJoins.forEach { assocAttToColumnMap[it.attribute] = it.srcColumn }
        assocJoinWithPredicates.forEach { assocAttToColumnMap[it.attribute] = it.srcColumn }

        val tableCtx = getTableContext()

        val columnsPred = PredicateUtils.mapAttributePredicates(
            predicate,
            { pred ->
                var column = tableCtx.getColumnByName(pred.getAttribute())
                if (column == null) {
                    val columnName = assocAttToColumnMap[pred.getAttribute()]
                    if (columnName != null) {
                        column = tableCtx.getColumnByName(columnName)
                    }
                }
                if (column == null) {
                    if (expressions.containsKey(pred.getAttribute())) {
                        pred
                    } else {
                        getPredicateForMissingColumn(pred)
                    }
                } else if (pred is ValuePredicate && pred.getType() == ValuePredicate.Type.IN) {
                    val value = pred.getValue()
                    if (!value.isArray() || value.isEmpty()) {
                        Predicates.alwaysFalse()
                    } else {
                        pred
                    }
                } else {
                    pred
                }
            },
            onlyAnd = false,
            optimize = true,
            filterEmptyComposite = false
        ) ?: Predicates.alwaysTrue()

        return PredicateUtils.optimize(columnsPred)
    }

    private fun getPredicateForMissingColumn(predicate: AttributePredicate): Predicate {
        if (predicate is EmptyPredicate) {
            return Predicates.alwaysTrue()
        }
        if (predicate is ValuePredicate &&
            predicate.getType() == ValuePredicate.Type.EQ &&
            predicate.getValue().isNull()
        ) {
            return Predicates.alwaysTrue()
        }
        return Predicates.alwaysFalse()
    }

    private fun setColumns(columns: List<DbColumnDef>?) {
        if (this.tableCtx.columnsOrNull == columns) {
            return
        }
        // a single volatile write publishes both facts (which columns, and whether they have been
        // read at all) together - see the tableCtx field KDoc for why splitting this into two
        // fields let a reader observe one updated and the other stale
        this.tableCtx = this.tableCtx.withColumns(columns)
    }

    private fun <T> execReadOnlyQueryWithPredicate(
        query: DbFindQuery,
        defaultRes: T,
        action: (DbTableContext, DbFindQuery) -> T
    ): T {
        val tableCtx = getTableContext()
        val preparedQuery = prepareQuery(query)
        if (PredicateUtils.isAlwaysFalse(preparedQuery.predicate)) {
            return defaultRes
        }
        return execReadOnlyQuery {
            action(tableCtx, preparedQuery)
        }
    }

    private fun <T> execReadOnlyQuery(action: () -> T): T {
        try {
            return dataSource.withTransaction(true, action)
        } catch (e: InterruptedException) {
            Thread.currentThread().interrupt()
            throw e
        } catch (e: Throwable) {
            // the cached column set disagrees with the database - most often because another
            // instance migrated the schema. Drop it so that the next call re-reads, and let the
            // caller see the original error: this attempt is lost, the next one is not.
            if (isStaleSchemaError(e)) {
                resetColumnsCache()
            }
            throw e
        }
    }

    /**
     * The two shapes a stale column cache takes on the read path. Both have to be here, and the
     * second one is the reason this is a function rather than one `if`.
     *
     * A **renamed or dropped** column makes PostgreSQL refuse the statement, and the SQL state says
     * so - that is [DbSchemaDao.isSchemaMismatchError], and it was the only case handled before.
     *
     * A column whose **type** changed under this instance does not fail in the database at all. The
     * statement succeeds, and the mismatch appears while mapping the row: the value arrives as a
     * `Double` where this instance's cached column list says `String`, and
     * [ru.citeck.ecos.data.sql.type.DbTypesConverter] has no such conversion. That is not a SQL
     * exception, so the reset never ran and the cache stayed wrong **for ever** - every later read
     * of that column through this service failed identically, with nothing in the system able to
     * repair it. Measured, not imagined: five consecutive reads after a neighbour DAO migrated the
     * column, all five failing, and a successful read through the migrating DAO in between changing
     * nothing.
     *
     * Reachable in two ordinary ways, and the background schema reconciliation makes the second one
     * routine rather than exotic: another records DAO over the same table in this JVM, and - the
     * important one - the same table on another instance of the cluster, where the local
     * reconciliation tick finds `ed_column_meta` already consistent and therefore never migrates,
     * never resets, and never notices.
     */
    private fun isStaleSchemaError(e: Throwable): Boolean {
        if (schemaDao.isSchemaMismatchError(e)) {
            return true
        }
        var current: Throwable? = e
        var depth = 0
        while (current != null && depth++ < 20) {
            if (current is DbTypeConversionException) {
                return true
            }
            current = current.cause
        }
        return false
    }

    private inner class DbTableContextImpl(
        private val table: String,
        /**
         * Null until the first real read - the same "not read yet" signal the old, separate
         * `DbDataServiceImpl.columns` field used to carry, moved in here so there is only one
         * volatile field to publish both facts together (see [tableCtx]).
         */
        val columnsOrNull: List<DbColumnDef>? = null,
        private val schemaCtx: DbSchemaContext
    ) : DbTableContext {

        private val tableRef = DbTableRef(schemaCtx.schema, table)
        private val addressableColumns = if (config.includeBackupColumns) {
            columnsOrNull.orEmpty()
        } else {
            columnsOrNull.orEmpty().filter { !DbBackupColumnNames.isBackupName(it.name) }
        }
        private val columnsByName = addressableColumns.associateBy { it.name }
        private val hasIdColumn = columnsByName.containsKey(DbEntity.ID)

        override fun getWorkspaceService(): DbWorkspaceService {
            return schemaCtx.workspaceService
        }

        override fun getRecordRefsService(): DbRecordRefService {
            return schemaCtx.recordRefService
        }

        override fun getAssocsService(): DbAssocsService {
            return schemaCtx.assocsService
        }

        override fun getContentService(): DbContentService {
            return schemaCtx.contentService
        }

        override fun getPermsService(): DbEntityPermsService {
            return schemaCtx.entityPermsService
        }

        override fun getAuthoritiesApi(): EcosAuthoritiesApi {
            return schemaCtx.authoritiesApi
        }

        override fun getTableRef(): DbTableRef {
            return tableRef
        }

        override fun getColumns(): List<DbColumnDef> {
            return addressableColumns
        }

        override fun getAllPhysicalColumns(): List<DbColumnDef> {
            return columnsOrNull ?: emptyList()
        }

        override fun getEntityValueTypeForColumn(name: String?): KClass<*> {
            name ?: return Any::class
            val entityColumn = entityMapper.getEntityColumnByColumnName(name)
            if (entityColumn != null) {
                return entityColumn.fieldType
            }
            val columnType = columnsByName[name]?.type ?: return Any::class
            return when (columnType) {
                DbColumnType.BIGSERIAL -> Long::class
                DbColumnType.TEXT -> String::class
                DbColumnType.DOUBLE -> Double::class
                DbColumnType.INT -> Int::class
                DbColumnType.LONG -> Long::class
                DbColumnType.BOOLEAN -> Boolean::class
                DbColumnType.DATETIME -> Instant::class
                DbColumnType.DATE -> Instant::class
                DbColumnType.JSON -> DataValue::class
                DbColumnType.BINARY -> ByteArray::class
                DbColumnType.UUID -> String::class
            }
        }

        override fun getColumnByName(name: String?): DbColumnDef? {
            name ?: return null
            return columnsByName[name]
        }

        override fun hasColumn(name: String?): Boolean {
            name ?: return false
            return columnsByName.containsKey(name)
        }

        override fun hasIdColumn(): Boolean {
            return hasIdColumn
        }

        override fun getDataSource(): DbDataSource {
            return schemaCtx.dataSourceCtx.dataSource
        }

        override fun getTypesConverter(): DbTypesConverter {
            return schemaCtx.dataSourceCtx.converter
        }

        override fun getQueryPermsPolicy(): QueryPermsPolicy {
            return permsPolicy.get()
        }

        override fun getAuthoritiesIdsMap(authorities: Collection<String>): Map<String, Long> {
            return schemaCtx.authorityService.getIdsByExtIds(authorities)
        }

        override fun isSameSchema(other: DbTableContext): Boolean {
            if (other !is DbDataServiceImpl<*>.DbTableContextImpl) {
                return false
            }
            return schemaCtx.dataSourceCtx === other.schemaCtx.dataSourceCtx &&
                tableRef.schema == other.tableRef.schema
        }

        fun withColumns(columns: List<DbColumnDef>?): DbTableContextImpl {
            return DbTableContextImpl(
                table,
                columns,
                schemaCtx
            )
        }

        override fun getSchemaCtx(): DbSchemaContext {
            return schemaCtx
        }
    }
}
