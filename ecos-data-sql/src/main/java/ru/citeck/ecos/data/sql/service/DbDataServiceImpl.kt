package ru.citeck.ecos.data.sql.service

import mu.KotlinLogging
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
import ru.citeck.ecos.data.sql.records.DbRecordsUtils
import ru.citeck.ecos.data.sql.records.assocs.DbAssocsService
import ru.citeck.ecos.data.sql.records.refs.DbRecordRefService
import ru.citeck.ecos.data.sql.repo.DbEntityRepo
import ru.citeck.ecos.data.sql.repo.entity.DbEntity
import ru.citeck.ecos.data.sql.repo.entity.DbEntityMapper
import ru.citeck.ecos.data.sql.repo.entity.DbEntityMapperImpl
import ru.citeck.ecos.data.sql.repo.entity.auth.DbAuthorityEntity
import ru.citeck.ecos.data.sql.repo.find.DbFindPage
import ru.citeck.ecos.data.sql.repo.find.DbFindRes
import ru.citeck.ecos.data.sql.repo.find.DbFindSort
import ru.citeck.ecos.data.sql.schema.DbSchemaDao
import ru.citeck.ecos.data.sql.service.aggregation.AggregateFunc
import ru.citeck.ecos.data.sql.type.DbTypesConverter
import ru.citeck.ecos.model.lib.type.dto.QueryPermsPolicy
import ru.citeck.ecos.records2.predicate.PredicateUtils
import ru.citeck.ecos.records2.predicate.model.Predicate
import ru.citeck.ecos.records2.predicate.model.Predicates
import ru.citeck.ecos.records2.predicate.model.ValuePredicate
import ru.citeck.ecos.txn.lib.TxnContext
import java.lang.IllegalStateException
import java.sql.SQLException
import java.time.Instant
import java.util.*
import java.util.concurrent.atomic.AtomicBoolean

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

    constructor(
        entityType: Class<T>,
        config: DbDataServiceConfig,
        schemaContext: DbSchemaContext
    ) {

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
        if (!config.storeTableMeta || !isTableExists()) {
            return DbDataService.NEW_TABLE_SCHEMA_VERSION
        }
        return schemaMeta.getValue(metaSchemaVersionKey, 0)
    }

    override fun setSchemaVersion(version: Int) {
        if (!config.storeTableMeta) {
            return
        }
        schemaMeta.setValue(metaSchemaVersionKey, version)
    }

    override fun findByIds(ids: Set<Long>): List<T> {
        if (ids.isEmpty()) {
            return emptyList()
        }
        return execReadOnlyQuery {
            entityRepo.findByColumn(getTableContext(), "id", ids, false, ids.size)
        }.map {
            entityMapper.convertToEntity(it)
        }
    }

    override fun findById(id: Long): T? {
        return findByAnyId(id)
    }

    override fun findByExtId(id: String): T? {
        return findByAnyId(id)
    }

    private fun findByAnyId(id: Any, withDeleted: Boolean = false): T? {
        return findMapByAnyId(id, withDeleted)?.let { entityMapper.convertToEntity(it) }
    }

    private fun findMapByAnyId(id: Any, withDeleted: Boolean = false): Map<String, Any?>? {
        getTableContext()
        return execReadOnlyQuery {
            findMapByAnyIdInEntityRepo(id, withDeleted)
        }
    }

    private fun findMapByAnyIdInEntityRepo(id: Any, withDeleted: Boolean = false): Map<String, Any?>? {
        val idColumn = when (id) {
            is String -> DbEntity.EXT_ID
            is Long -> DbEntity.ID
            else -> error("Incorrect id type: ${id::class}")
        }
        return entityRepo.findByColumn(
            getTableContext(),
            idColumn,
            listOf(id),
            withDeleted,
            1
        ).firstOrNull()
    }

    override fun findAll(): List<T> {
        return findAll(Predicates.alwaysTrue())
    }

    override fun findAll(predicate: Predicate): List<T> {
        return findAll(predicate, false)
    }

    override fun findAll(predicate: Predicate, withDeleted: Boolean): List<T> {
        return execReadOnlyQueryWithPredicate(predicate, emptyList()) { tableCtx, pred ->
            entityRepo.find(
                tableCtx,
                pred,
                emptyList(),
                DbFindPage.ALL,
                withDeleted,
                emptyList(),
                emptyList()
            ).entities.map {
                entityMapper.convertToEntity(it)
            }
        }
    }

    override fun findAll(predicate: Predicate, sort: List<DbFindSort>): List<T> {
        return execReadOnlyQueryWithPredicate(predicate, emptyList()) { tableCtx, pred ->
            entityRepo.find(
                tableCtx,
                pred,
                sort,
                DbFindPage.ALL,
                false,
                emptyList(),
                emptyList()
            ).entities.map {
                entityMapper.convertToEntity(it)
            }
        }
    }

    override fun find(predicate: Predicate, sort: List<DbFindSort>, page: DbFindPage): DbFindRes<T> {
        return find(predicate, sort, page, false)
    }

    override fun find(
        predicate: Predicate,
        sort: List<DbFindSort>,
        page: DbFindPage,
        withDeleted: Boolean
    ): DbFindRes<T> {
        return execReadOnlyQueryWithPredicate(predicate, DbFindRes.empty()) { tableCtx, pred ->
            entityRepo.find(
                tableCtx,
                pred,
                sort,
                page,
                withDeleted,
                emptyList(),
                emptyList()
            ).mapEntities {
                entityMapper.convertToEntity(it)
            }
        }
    }

    override fun find(
        predicate: Predicate,
        sort: List<DbFindSort>,
        page: DbFindPage,
        withDeleted: Boolean,
        groupBy: List<String>,
        selectFunctions: List<AggregateFunc>
    ): DbFindRes<T> {
        return execReadOnlyQueryWithPredicate(predicate, DbFindRes.empty()) { tableCtx, pred ->
            entityRepo.find(tableCtx, pred, sort, page, withDeleted, groupBy, selectFunctions).mapEntities {
                entityMapper.convertToEntity(it)
            }
        }
    }

    override fun getCount(predicate: Predicate): Long {
        return execReadOnlyQueryWithPredicate(predicate, 0) { tableCtx, pred ->
            entityRepo.getCount(tableCtx, pred, emptyList())
        }
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

        val columnsBefore = this.columns
        try {
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
                                // check that it's not a txn table and entity in was deleted before
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

                entityRepo.save(tableCtx, entitiesToSave).map { entityMapper.convertToEntity(it) }
            }
        } catch (e: Exception) {
            setColumns(columnsBefore)
            throw e
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

        val commands = migration.invoke()
        val durationMs = System.currentTimeMillis() - startTime.toEpochMilli()

        if (!mock && commands.isNotEmpty()) {
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
            setSchemaVersion(DbDataService.NEW_TABLE_SCHEMA_VERSION)

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
                val currentCount = entityRepo.getCount(getTableContext(), Predicates.alwaysTrue(), emptyList())
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
        val tableCtx = getTableContext()

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

    private fun preparePredicate(predicate: Predicate): Predicate {

        if (PredicateUtils.isAlwaysTrue(predicate) || PredicateUtils.isAlwaysFalse(predicate)) {
            return predicate
        }

        val tableCtx = getTableContext()

        val columnsPred = PredicateUtils.mapAttributePredicates(
            predicate,
            { pred ->
                val column = tableCtx.getColumnByName(pred.getAttribute())
                if (column == null) {
                    Predicates.alwaysFalse()
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
            optimize = true
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
        predicate: Predicate,
        defaultRes: T,
        action: (DbTableContext, Predicate) -> T
    ): T {
        val tableCtx = getTableContext()
        val preparedPred = preparePredicate(predicate)
        if (PredicateUtils.isAlwaysFalse(preparedPred)) {
            return defaultRes
        }
        return execReadOnlyQuery {
            action(tableCtx, preparedPred)
        }
    }

    private fun <T> execReadOnlyQuery(action: () -> T): T {
        try {
            return dataSource.withTransaction(true, action)
        } catch (rootEx: SQLException) {
            if (rootEx.message?.contains("(column|relation) .+ does not exist".toRegex()) == true) {
                resetColumnsCache()
                try {
                    return dataSource.withTransaction(true, action)
                } catch (e: Exception) {
                    e.addSuppressed(rootEx)
                    throw e
                }
            }
            throw rootEx
        }
    }

    private inner class DbTableContextImpl(
        private val table: String,
        private val columns: List<DbColumnDef> = emptyList(),
        val schemaCtx: DbSchemaContext
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

        override fun getTableRef(): DbTableRef {
            return tableRef
        }

        override fun getColumns(): List<DbColumnDef> {
            return columns
        }

        override fun getColumnByName(name: String): DbColumnDef? {
            return columnsByName[name]
        }

        override fun hasColumn(name: String): Boolean {
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

        override fun getCurrentUserAuthorityIds(): Set<Long> {
            val authorityEntities = schemaCtx.authorityDataService.findAll(
                Predicates.`in`(DbAuthorityEntity.EXT_ID, DbRecordsUtils.getCurrentAuthorities())
            )
            return authorityEntities.map { it.id }.toSet()
        }

        fun withColumns(columns: List<DbColumnDef>): DbTableContextImpl {
            return DbTableContextImpl(
                table,
                columns,
                schemaCtx
            )
        }
    }
}
