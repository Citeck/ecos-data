package ru.citeck.ecos.data.sql.service

import io.github.oshai.kotlinlogging.KotlinLogging
import ru.citeck.ecos.commons.data.DataValue
import ru.citeck.ecos.commons.data.ObjectData
import ru.citeck.ecos.commons.json.Json
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
import ru.citeck.ecos.data.sql.perms.DbEntityPermsService
import ru.citeck.ecos.data.sql.records.assocs.DbAssocsService
import ru.citeck.ecos.data.sql.records.refs.DbRecordRefService
import ru.citeck.ecos.data.sql.repo.DbEntityRepo
import ru.citeck.ecos.data.sql.repo.entity.DbEntity
import ru.citeck.ecos.data.sql.repo.entity.DbEntityMapper
import ru.citeck.ecos.data.sql.repo.entity.DbEntityMapperImpl
import ru.citeck.ecos.data.sql.repo.entity.auth.DbAuthorityEntity
import ru.citeck.ecos.data.sql.repo.find.DbFindPage
import ru.citeck.ecos.data.sql.repo.find.DbFindQuery
import ru.citeck.ecos.data.sql.repo.find.DbFindRes
import ru.citeck.ecos.data.sql.repo.find.DbFindSort
import ru.citeck.ecos.data.sql.schema.DbSchemaDao
import ru.citeck.ecos.data.sql.service.assocs.AssocJoinWithPredicate
import ru.citeck.ecos.data.sql.service.assocs.AssocTableJoin
import ru.citeck.ecos.data.sql.service.expression.token.ExpressionToken
import ru.citeck.ecos.data.sql.type.DbTypesConverter
import ru.citeck.ecos.model.lib.type.dto.QueryPermsPolicy
import ru.citeck.ecos.records2.predicate.PredicateUtils
import ru.citeck.ecos.records2.predicate.model.Predicate
import ru.citeck.ecos.records2.predicate.model.Predicates
import ru.citeck.ecos.records2.predicate.model.ValuePredicate
import ru.citeck.ecos.txn.lib.TxnContext
import ru.citeck.ecos.webapp.api.authority.EcosAuthoritiesApi
import java.sql.SQLException
import java.time.Instant
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.reflect.KClass

class DbDataServiceImpl<T : Any> : DbDataService<T> {

    companion object {
        private val log = KotlinLogging.logger {}
    }

    private val typesConverter: DbTypesConverter
    private val entityMapper: DbEntityMapper<T>

    private val tableMetaService: DbDataService<DbTableMetaEntity>?
    private val entityPermsService: DbEntityPermsService?

    private val entityRepo: DbEntityRepo
    private val schemaDao: DbSchemaDao
    private val dataSource: DbDataSource
    private val schemaMeta: DbSchemaMetaService

    private val config: DbDataServiceConfig

    private val tableRef: DbTableRef
    private val maxItemsToAllowSchemaMigration: Long
    private val schemaCacheUpdateRequired = AtomicBoolean()

    private var columns: List<DbColumnDef>? = null
    private var tableCtx: DbTableContextImpl

    private val hasDeletedFlag: Boolean

    private val metaSchemaVersionKey: List<String>

    private val permsPolicy: ThreadLocal<QueryPermsPolicy>

    private var schemaVersion: Int = -1
    private var schemaVersionNextUpdateMs = 0L
    private val useLastSchemaVersion: Boolean

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
        this.maxItemsToAllowSchemaMigration = config.maxItemsToAllowSchemaMigration
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

        hasDeletedFlag = entityMapper.getEntityColumns().any { it.columnDef.name == DbEntity.DELETED }

        tableCtx = DbTableContextImpl(
            config.table,
            schemaCtx = schemaContext,
        )
        schemaMeta = schemaContext.schemaMetaService

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
            findByColumn(getTableContext(), DbEntity.ID, ids, false, ids.size)
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
        withDeleted: Boolean,
        limit: Int,
        expressions: Map<String, ExpressionToken> = emptyMap()
    ): List<Map<String, Any?>> {

        val query = DbFindQuery.create()
            .withPredicate(ValuePredicate(column, ValuePredicate.Type.IN, values))
            .withDeleted(withDeleted)
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
        withDeleted: Boolean = false,
        expressions: Map<String, ExpressionToken> = emptyMap()
    ): T? {
        return findMapByAnyId(id, withDeleted, expressions)?.let { convertToEntity(it) }
    }

    private fun findMapByAnyId(
        id: Any,
        withDeleted: Boolean = false,
        expressions: Map<String, ExpressionToken> = emptyMap()
    ): Map<String, Any?>? {
        getTableContext()
        return execReadOnlyQuery {
            findMapByAnyIdInEntityRepo(id, withDeleted, expressions)
        }
    }

    private fun findMapByAnyIdInEntityRepo(
        id: Any,
        withDeleted: Boolean = false,
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
            withDeleted,
            1,
            expressions
        ).firstOrNull()
    }

    override fun findAll(): List<T> {
        return findAll(Predicates.alwaysTrue())
    }

    override fun findAll(predicate: Predicate): List<T> {
        return findAll(predicate, false)
    }

    override fun findAll(predicate: Predicate, withDeleted: Boolean): List<T> {

        val srcQuery = DbFindQuery.create()
            .withPredicate(predicate)
            .withDeleted(withDeleted)
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
        withDeleted: Boolean
    ): DbFindRes<T> {
        return find(
            DbFindQuery.create {
                withPredicate(predicate)
                withSortBy(sort)
                withDeleted(withDeleted)
            },
            page
        )
    }

    override fun find(
        predicate: Predicate,
        sort: List<DbFindSort>,
        page: DbFindPage,
        withDeleted: Boolean,
        groupBy: List<String>,
        assocTableJoins: List<AssocTableJoin>,
        assocJoinWithPredicates: List<AssocJoinWithPredicate>,
        withTotalCount: Boolean
    ): DbFindRes<T> {
        return find(
            DbFindQuery.create {
                withPredicate(predicate)
                withSortBy(sort)
                withDeleted(withDeleted)
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
        withDeleted: Boolean,
        groupBy: List<String>,
        assocTableJoins: List<AssocTableJoin>,
        assocJoinWithPredicates: List<AssocJoinWithPredicate>,
        withTotalCount: Boolean
    ): DbFindRes<Map<String, Any?>> {
        return findRaw(
            DbFindQuery.create {
                withPredicate(predicate)
                withSortBy(sort)
                withDeleted(withDeleted)
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

    override fun save(entity: T, columns: List<DbColumnDef>): T {
        return save(listOf(entity), columns)[0]
    }

    override fun save(entities: Collection<T>): List<T> {
        return save(entities, emptyList())
    }

    override fun save(entities: Collection<T>, columns: List<DbColumnDef>): List<T> {

        return dataSource.withTransaction(false) {

            runMigrationsInTxn(columns, mock = false, diff = true)
            val tableCtx = getTableContext()

            val entitiesToSave = entities.map { entity ->

                val entityMap = entityMapper.convertToMap(entity)

                // entities with 'deleted' flag field doesn't really delete from table.
                // We set deleted = true for it instead. When new record will be created
                // with same id we should remove old record with deleted flag.
                if (hasDeletedFlag && entityMap[DbEntity.ID] == DbEntity.NEW_REC_ID) {
                    val extId = entityMap[DbEntity.EXT_ID] as? String ?: ""
                    if (extId.isNotBlank()) {
                        val existingEntityMap = findMapByAnyId(extId, true)
                        if (existingEntityMap != null) {
                            // check that entity was deleted before
                            if (existingEntityMap[DbEntity.DELETED] == true) {
                                entityRepo.forceDelete(tableCtx, listOf(existingEntityMap[DbEntity.ID] as Long))
                            } else {
                                throw IllegalStateException("New entity with same extId, but with new dbId. ExtId: $extId")
                            }
                        }
                    }
                }
                entityMap
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
        if (!isTableExists()) {
            return
        }
        if (PredicateUtils.isAlwaysFalse(predicate)) {
            return
        }
        dataSource.withTransaction(false) {
            entityRepo.delete(getTableContext(), predicate)
        }
    }

    override fun forceDelete(predicate: Predicate) {
        if (!isTableExists()) {
            return
        }
        if (PredicateUtils.isAlwaysFalse(predicate)) {
            return
        }
        dataSource.withTransaction(false) {
            entityRepo.forceDelete(getTableContext(), predicate)
        }
    }

    override fun getTableContext(): DbTableContext {
        var columns = this.columns
        if (columns == null) {
            columns = dataSource.withTransaction(true) {
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

    override fun forceDelete(entityId: Long) {
        if (!isTableExists()) {
            return
        }
        dataSource.withTransaction(false) {
            entityRepo.forceDelete(getTableContext(), listOf(entityId))
        }
    }

    override fun forceDelete(entity: T) {
        if (!isTableExists()) {
            return
        }
        dataSource.withTransaction(false) {
            entityRepo.forceDelete(getTableContext(), listOf(entityMapper.convertToMap(entity)[DbEntity.ID] as Long))
        }
    }

    override fun forceDelete(entities: List<T>) {
        if (entities.isEmpty() || !isTableExists()) {
            return
        }
        dataSource.withTransaction(false) {
            entityRepo.forceDelete(
                getTableContext(),
                entities.map {
                    entityMapper.convertToMap(it)[DbEntity.ID] as Long
                }
            )
        }
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
        return dataSource.withTransaction(mock) {
            val result = runMigrationsInTxn(expectedColumns, mock, diff)
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

    @Synchronized
    private fun runMigrationsInTxn(
        expectedColumns: List<DbColumnDef>,
        mock: Boolean,
        diff: Boolean
    ): List<String> {

        getTableContext()

        val expectedWithEntityColumns = ArrayList(entityMapper.getEntityColumns().map { it.columnDef })
        expectedWithEntityColumns.addAll(expectedColumns)

        val startTime = Instant.now()
        val changedColumns = mutableListOf<DbColumnDef>()
        val migration = {
            dataSource.watchSchemaCommands {
                changedColumns.addAll(ensureColumnsExistImpl(expectedWithEntityColumns, mock, diff))
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

    private fun ensureColumnsExistImpl(
        expectedColumns: List<DbColumnDef>,
        mock: Boolean,
        diff: Boolean
    ): List<DbColumnDef> {

        val currentColumns = if (diff) {
            this.columns ?: error("Current columns is null")
        } else {
            emptyList()
        }
        val expectedIndexes = entityMapper.getEntityIndexes()

        if (currentColumns.isEmpty()) {
            schemaDao.createTable(dataSource, tableRef, expectedColumns)
            if (!mock) {
                setSchemaVersion(DbDataService.NEW_TABLE_SCHEMA_VERSION)
            }
            addIndexesAndConstraintsForNewColumns(expectedColumns, expectedIndexes, config.fkConstraints)
            return expectedColumns
        }
        val currentColumnsByName = currentColumns.associateBy { it.name }

        val columnsWithChangedType = expectedColumns.filter { expectedColumn ->
            val currentColumn = currentColumnsByName[expectedColumn.name]
            if (currentColumn != null) {
                isColumnSchemaUpdateRequired(currentColumn, expectedColumn)
            } else {
                false
            }
        }
        if (columnsWithChangedType.isNotEmpty()) {
            if (!mock) {
                val currentCount = getCount(Predicates.alwaysTrue())
                if (currentCount > maxItemsToAllowSchemaMigration) {
                    val baseMsg = "Schema migration can't be performed because table has too much items: $currentCount."
                    val newColumnsMsg = columnsWithChangedType.joinToString { it.toString() }
                    val oldColumnsMsg = columnsWithChangedType.joinToString { currentColumnsByName[it.name].toString() }
                    log.error { "$baseMsg\n New columns: $newColumnsMsg\n Old columns: $oldColumnsMsg" }
                    error(baseMsg)
                }
            }
            columnsWithChangedType.forEach {
                schemaDao.setColumnType(dataSource, tableRef, it.name, it.multiple, it.type)
            }
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

        val changedColumns = ArrayList(columnsWithChangedType)
        changedColumns.addAll(fixedMissedColumns)

        return changedColumns
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
                        Predicates.alwaysFalse()
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

    private fun setColumns(columns: List<DbColumnDef>?) {
        if (this.columns == columns) {
            return
        }
        this.columns = columns

        val notNullColumns = columns ?: emptyList()
        this.tableCtx = this.tableCtx.withColumns(notNullColumns)
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
        } catch (e: SQLException) {
            if (e.message?.contains("(column|relation) .+ does not exist".toRegex()) == true) {
                resetColumnsCache()
            }
            throw e
        }
    }

    private inner class DbTableContextImpl(
        private val table: String,
        private val columns: List<DbColumnDef> = emptyList(),
        private val schemaCtx: DbSchemaContext
    ) : DbTableContext {

        private val tableRef = DbTableRef(schemaCtx.schema, table)
        private val columnsByName = columns.associateBy { it.name }
        private val hasIdColumn = columnsByName.containsKey(DbEntity.ID)
        private val hasDeleteFlag = columnsByName.containsKey(DbEntity.DELETED)

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
            return columns
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

        override fun hasDeleteFlag(): Boolean {
            return hasDeleteFlag
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
            if (authorities.isEmpty()) {
                return emptyMap()
            }
            val authorityEntities = schemaCtx.authorityDataService.findAll(
                Predicates.`in`(DbAuthorityEntity.EXT_ID, authorities)
            )
            return authorityEntities.associate { it.extId to it.id }
        }

        override fun isSameSchema(other: DbTableContext): Boolean {
            if (other !is DbDataServiceImpl<*>.DbTableContextImpl) {
                return false
            }
            return schemaCtx.dataSourceCtx === other.schemaCtx.dataSourceCtx &&
                tableRef.schema == other.tableRef.schema
        }

        fun withColumns(columns: List<DbColumnDef>): DbTableContextImpl {
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
