package ru.citeck.ecos.data.sql.service

import mu.KotlinLogging
import ru.citeck.ecos.commons.data.DataValue
import ru.citeck.ecos.commons.data.ObjectData
import ru.citeck.ecos.commons.json.Json
import ru.citeck.ecos.context.lib.auth.AuthContext
import ru.citeck.ecos.data.sql.datasource.DbDataSource
import ru.citeck.ecos.data.sql.dto.*
import ru.citeck.ecos.data.sql.dto.fk.DbFkConstraint
import ru.citeck.ecos.data.sql.dto.fk.FkCascadeActionOptions
import ru.citeck.ecos.data.sql.meta.DbTableMetaEntity
import ru.citeck.ecos.data.sql.meta.dto.DbTableChangeSet
import ru.citeck.ecos.data.sql.meta.dto.DbTableMetaConfig
import ru.citeck.ecos.data.sql.meta.dto.DbTableMetaDto
import ru.citeck.ecos.data.sql.repo.DbEntityPermissionsDto
import ru.citeck.ecos.data.sql.repo.DbEntityRepo
import ru.citeck.ecos.data.sql.repo.DbEntityRepoConfig
import ru.citeck.ecos.data.sql.repo.entity.DbEntity
import ru.citeck.ecos.data.sql.repo.entity.DbEntityMapper
import ru.citeck.ecos.data.sql.repo.entity.DbEntityMapperImpl
import ru.citeck.ecos.data.sql.repo.entity.auth.DbAuthorityEntity
import ru.citeck.ecos.data.sql.repo.entity.auth.DbPermsEntity
import ru.citeck.ecos.data.sql.repo.find.DbFindPage
import ru.citeck.ecos.data.sql.repo.find.DbFindRes
import ru.citeck.ecos.data.sql.repo.find.DbFindSort
import ru.citeck.ecos.data.sql.schema.DbSchemaDao
import ru.citeck.ecos.data.sql.service.migration.DbMigration
import ru.citeck.ecos.data.sql.service.migration.DbMigrationService
import ru.citeck.ecos.data.sql.txn.ExtTxnContext
import ru.citeck.ecos.data.sql.type.DbTypesConverter
import ru.citeck.ecos.records2.predicate.model.Predicate
import ru.citeck.ecos.records2.predicate.model.Predicates
import ru.citeck.ecos.records2.predicate.model.VoidPredicate
import java.time.Instant
import java.util.*
import kotlin.collections.ArrayList
import kotlin.collections.HashMap
import kotlin.collections.HashSet
import kotlin.collections.LinkedHashMap

class DbDataServiceImpl<T : Any> : DbDataService<T> {

    companion object {
        private const val COLUMN_EXT_TXN_ID = "__ext_txn_id"

        private const val META_TABLE_NAME = "ecos_data_table_meta"
        private const val AUTHORITIES_TABLE_NAME = "ecos_authorities"

        private val log = KotlinLogging.logger {}
    }

    private val typesConverter: DbTypesConverter
    private val entityMapper: DbEntityMapper<T>

    private val tableMetaService: DbDataService<DbTableMetaEntity>?
    private val txnDataService: DbDataService<T>?
    private val authorityDataService: DbDataService<DbAuthorityEntity>?
    private val permsDataService: DbDataService<DbPermsEntity>?

    private val entityRepo: DbEntityRepo<T>
    private val schemaDao: DbSchemaDao
    private val dataSource: DbDataSource

    private val migrations: DbMigrationService<T>

    private val config: DbDataServiceConfig

    private val tableRef: DbTableRef
    private val authEnabled: Boolean
    private val maxItemsToAllowSchemaMigration: Long

    private var columns: List<DbColumnDef>? = null

    private val hasDeletedFlag: Boolean

    constructor(
        entityType: Class<T>,
        config: DbDataServiceConfig,
        dataSource: DbDataSource,
        dataServiceFactory: DbDataServiceFactory
    ) : this(entityType, config, dataSource, dataServiceFactory, null)

    private constructor(
        entityType: Class<T>,
        config: DbDataServiceConfig,
        dataSource: DbDataSource,
        dataServiceFactory: DbDataServiceFactory,
        parent: DbDataServiceImpl<*>?
    ) {
        this.config = config
        this.dataSource = dataSource
        this.tableRef = config.tableRef
        this.authEnabled = config.authEnabled
        this.maxItemsToAllowSchemaMigration = config.maxItemsToAllowSchemaMigration

        typesConverter = parent?.typesConverter ?: DbTypesConverter()
        if (parent == null) {
            dataServiceFactory.registerConverters(typesConverter)
        }

        entityMapper = DbEntityMapperImpl(entityType.kotlin, typesConverter)

        val tableRef = config.tableRef

        val permsTableRef = tableRef.withTable(tableRef.table + "__perms")
        val txnTableRef = tableRef.withTable(tableRef.table + "__ext_txn")
        val authoritiesTableRef = tableRef.withTable(AUTHORITIES_TABLE_NAME)

        tableMetaService = if (config.storeTableMeta && parent == null) {
            DbDataServiceImpl(
                DbTableMetaEntity::class.java,
                DbDataServiceConfig.create()
                    .withTableRef(tableRef.withTable(META_TABLE_NAME))
                    .build(),
                dataSource,
                dataServiceFactory,
                this
            )
        } else {
            null
        }

        txnDataService = if (config.transactional && parent == null) {
            DbDataServiceImpl(
                entityType,
                DbDataServiceConfig.create()
                    .withTableRef(txnTableRef)
                    .build(),
                dataSource,
                dataServiceFactory,
                this
            )
        } else {
            null
        }

        authorityDataService = if (authEnabled && parent == null) {
            DbDataServiceImpl(
                DbAuthorityEntity::class.java,
                DbDataServiceConfig.create()
                    .withTableRef(authoritiesTableRef)
                    .build(),
                dataSource,
                dataServiceFactory,
                this
            )
        } else {
            null
        }

        permsDataService = if (authEnabled && parent == null) {
            DbDataServiceImpl(
                DbPermsEntity::class.java,
                DbDataServiceConfig.create()
                    .withTableRef(permsTableRef)
                    .withFkConstraints(
                        listOf(
                            DbFkConstraint.create {
                                withName("fk_" + permsTableRef.table + "_authority_id")
                                withBaseColumnName(DbPermsEntity.AUTHORITY_ID)
                                withReferencedTable(authoritiesTableRef)
                                withReferencedColumn(DbAuthorityEntity.ID)
                                withOnDelete(FkCascadeActionOptions.CASCADE)
                            },
                            DbFkConstraint.create {
                                withName("fk_" + permsTableRef.table + "_record_id")
                                withBaseColumnName(DbPermsEntity.RECORD_ID)
                                withReferencedTable(tableRef)
                                withReferencedColumn(DbEntity.ID)
                                withOnDelete(FkCascadeActionOptions.CASCADE)
                            }
                        )
                    )
                    .build(),
                dataSource,
                dataServiceFactory,
                this
            )
        } else {
            null
        }

        schemaDao = dataServiceFactory.createSchemaDao(tableRef, dataSource)
        entityRepo = dataServiceFactory.createEntityRepo(
            config.tableRef,
            dataSource,
            entityMapper,
            typesConverter,
            DbEntityRepoConfig(
                config.authEnabled,
                permsTable = permsTableRef,
                authoritiesTable = authoritiesTableRef
            )
        )

        hasDeletedFlag = entityMapper.getEntityColumns().any { it.columnDef.name == DbEntity.DELETED }

        migrations = DbMigrationService(this, schemaDao, entityRepo, dataSource)
    }

    private fun isAuthTableRequiredAndDoesntExists(): Boolean {
        return authorityDataService != null && !AuthContext.isRunAsSystem() && !authorityDataService.isTableExists()
    }

    override fun findById(id: Set<Long>): List<T> {
        if (txnDataService == null) {
            initColumns()
            return dataSource.withTransaction(true) {
                entityRepo.findById(id)
            }
        }
        return id.mapNotNull { findById(it) }
    }

    override fun findById(id: Long): T? {
        return findByAnyId(id)
    }

    override fun findByExtId(id: String): T? {
        return findByAnyId(id)
    }

    private fun findByAnyId(id: Any): T? {
        initColumns()
        if (isAuthTableRequiredAndDoesntExists()) {
            return null
        }
        return dataSource.withTransaction(true) {
            val txnId = ExtTxnContext.getExtTxnId()
            if (txnDataService == null || txnId == null) {
                findByAnyIdInEntityRepo(id)
            } else {
                val txnRes = findTxnEntityById(txnId, id)
                if (txnRes != null) {
                    val map = entityMapper.convertToMap(txnRes)
                    if (map[DbEntity.DELETED] == true) {
                        null
                    } else {
                        txnRes
                    }
                } else {
                    findByAnyIdInEntityRepo(id)
                }
            }
        }
    }

    private fun findByAnyIdInEntityRepo(id: Any): T? {
        return when (id) {
            is String -> entityRepo.findByExtId(id)
            is Long -> entityRepo.findById(id)
            else -> error("Incorrect id type: ${id::class}")
        }
    }

    override fun findAll(): List<T> {
        initColumns()
        if (isAuthTableRequiredAndDoesntExists()) {
            return emptyList()
        }
        return dataSource.withTransaction(true) {
            entityRepo.findAll()
        }
    }

    override fun findAll(predicate: Predicate): List<T> {
        return findAll(predicate, false)
    }

    override fun findAll(predicate: Predicate, withDeleted: Boolean): List<T> {
        initColumns()
        if (isAuthTableRequiredAndDoesntExists()) {
            return emptyList()
        }
        return dataSource.withTransaction(true) {
            entityRepo.findAll(predicate, withDeleted)
        }
    }

    override fun findAll(predicate: Predicate, sort: List<DbFindSort>): List<T> {
        initColumns()
        if (isAuthTableRequiredAndDoesntExists()) {
            return emptyList()
        }
        return dataSource.withTransaction(true) {
            entityRepo.findAll(predicate, sort)
        }
    }

    override fun find(predicate: Predicate, sort: List<DbFindSort>, page: DbFindPage): DbFindRes<T> {
        initColumns()
        if (isAuthTableRequiredAndDoesntExists()) {
            return DbFindRes()
        }
        return dataSource.withTransaction(true) {
            entityRepo.find(predicate, sort, page)
        }
    }

    override fun getCount(predicate: Predicate): Long {
        initColumns()
        if (isAuthTableRequiredAndDoesntExists()) {
            return 0
        }
        return dataSource.withTransaction(true) {
            entityRepo.getCount(predicate)
        }
    }

    override fun save(entity: T): T {
        return save(entity, emptyList())
    }

    override fun save(entity: T, columns: List<DbColumnDef>): T {

        val txnId = ExtTxnContext.getExtTxnId()
        val columnsBefore = this.columns
        try {
            return dataSource.withTransaction(false) {

                runMigrationsInTxn(columns, mock = false, diff = true, onlyOwn = false)

                val newEntity = HashMap(entityMapper.convertToMap(entity))

                // entities with 'deleted' flag field doesn't really delete from table.
                // We set deleted = true for it instead. When new record will be created
                // with same id we should remove old record with deleted flag.
                if (hasDeletedFlag && newEntity[DbEntity.ID] == DbEntity.NEW_REC_ID) {
                    val extId = newEntity[DbEntity.EXT_ID] as? String ?: ""
                    if (extId.isNotBlank()) {
                        val existingEntity = entityRepo.findByExtId(extId, true)
                        if (existingEntity != null) {
                            val entityMap = entityMapper.convertToMap(existingEntity)
                            // check that it's not a txn table and entity in was deleted before
                            if (!entityMap.containsKey(COLUMN_EXT_TXN_ID) && entityMap[DbEntity.DELETED] == true) {
                                entityRepo.forceDelete(existingEntity)
                            }
                        }
                    }
                }

                if (txnDataService == null || txnId == null) {
                    entityRepo.save(entity)
                } else {
                    val extId = newEntity[DbEntity.EXT_ID] as String?
                    if (extId.isNullOrBlank()) {
                        // create new record
                        newEntity[COLUMN_EXT_TXN_ID] = txnId
                        txnDataService.save(entityMapper.convertToEntity(newEntity), getTxnColumns(columns))
                    } else {
                        if (!newEntity.containsKey(COLUMN_EXT_TXN_ID)) {
                            // modify existing record, but it is not in txn table yet
                            newEntity[COLUMN_EXT_TXN_ID] = txnId
                            newEntity.remove(DbEntity.ID)
                            txnDataService.save(entityMapper.convertToEntity(newEntity), getTxnColumns(columns))
                        } else {
                            // modify existing record in txn table
                            txnDataService.save(entity, getTxnColumns(columns))
                        }
                    }
                }
            }
        } catch (e: Exception) {
            this.columns = columnsBefore
            entityRepo.setColumns(columnsBefore ?: emptyList())
            throw e
        }
    }

    override fun delete(entity: T) {

        val columns = initColumns()

        dataSource.withTransaction(false) {

            val txnDataService = this.txnDataService
            val txnId = ExtTxnContext.getExtTxnId()
            if (txnDataService == null || txnId == null) {
                entityRepo.delete(entity)
            } else {
                val entityMap = LinkedHashMap(entityMapper.convertToMap(entity))
                if (entityMap.containsKey(COLUMN_EXT_TXN_ID)) {
                    txnDataService.delete(entity)
                } else {
                    entityMap[COLUMN_EXT_TXN_ID] = txnId
                    entityMap[DbEntity.DELETED] = true
                    entityMap.remove(DbEntity.ID)
                    txnDataService.save(entityMapper.convertToEntity(entityMap), getTxnColumns(columns))
                }
            }
        }
    }

    override fun forceDelete(entity: T) {
        initColumns()
        dataSource.withTransaction(false) {
            entityRepo.forceDelete(entity)
        }
    }

    override fun forceDelete(entities: List<T>) {
        if (entities.isEmpty()) {
            return
        }
        initColumns()
        dataSource.withTransaction(false) {
            entityRepo.forceDelete(entities)
        }
    }

    override fun commit(entities: List<DbCommitEntityDto>) {
        dataSource.withTransaction(false) {
            AuthContext.runAsSystem {
                completeExtTxn(entities.map { it.id }, true)
                if (config.authEnabled) {
                    val allAuthorities = mutableSetOf<String>()
                    entities.forEach {
                        allAuthorities.addAll(it.readAllowed)
                        allAuthorities.addAll(it.readDenied)
                    }
                    val authoritiesId = ensureAuthoritiesExists(allAuthorities)
                    entityRepo.setReadPerms(
                        entities.map { entity ->
                            DbEntityPermissionsDto(
                                entity.id,
                                entity.readAllowed.mapTo(HashSet()) { authoritiesId[it]!! },
                                entity.readDenied.mapTo(HashSet()) { authoritiesId[it]!! }
                            )
                        }
                    )
                }
            }
        }
    }

    private fun ensureAuthoritiesExists(authorities: Set<String>): Map<String, Long> {

        if (authorities.isEmpty() || authorityDataService == null) {
            return emptyMap()
        }

        val authorityEntities = authorityDataService.findAll(Predicates.`in`(DbAuthorityEntity.EXT_ID, authorities))
        val authoritiesId = mutableMapOf<String, Long>()

        for (authEntity in authorityEntities) {
            authoritiesId[authEntity.extId] = authEntity.id
        }

        for (authority in authorities) {
            if (!authoritiesId.containsKey(authority)) {
                val authEntity = DbAuthorityEntity()
                authEntity.extId = authority
                authoritiesId[authority] = authorityDataService.save(authEntity, emptyList()).id
            }
        }

        return authoritiesId
    }

    override fun rollback(entitiesId: List<String>) {
        dataSource.withTransaction(false) {
            AuthContext.runAsSystem {
                completeExtTxn(entitiesId, false)
            }
        }
    }

    private fun completeExtTxn(entitiesId: List<String>, success: Boolean) {

        val txnId = ExtTxnContext.getExtTxnId() ?: return

        val txnDataService = this.txnDataService ?: return
        entitiesId.forEach { id ->
            val txnEntity = findTxnEntityById(txnId, id)
            if (txnEntity != null) {
                if (success) {

                    val entityMap = HashMap(entityMapper.convertToMap(txnEntity))
                    entityMap.remove(DbEntity.ID)
                    entityMap.remove(COLUMN_EXT_TXN_ID)

                    val entityFromRepo = entityRepo.findByExtId(entityMap[DbEntity.EXT_ID] as String)
                    if (entityFromRepo != null) {
                        val entityMapFromRepo = entityMapper.convertToMap(entityFromRepo)
                        entityMap[DbEntity.ID] = entityMapFromRepo[DbEntity.ID]
                        entityMap[DbEntity.UPD_VERSION] = entityMapFromRepo[DbEntity.UPD_VERSION]
                    }
                    if (entityFromRepo != null || entityMap[DbEntity.DELETED] != true) {
                        entityRepo.save(entityMapper.convertToEntity(entityMap))
                    }
                }
                txnDataService.forceDelete(txnEntity)
            }
        }
    }

    private fun findTxnEntityById(txnId: UUID, id: Any): T? {
        if (txnDataService == null) {
            return null
        }
        val fieldName = when (id) {
            is String -> DbEntity.EXT_ID
            is Long -> DbEntity.ID
            else -> error("Incorrect id value type: ${id::class}")
        }
        val txnData = txnDataService.findAll(
            Predicates.and(
                Predicates.eq(fieldName, id),
                Predicates.eq(COLUMN_EXT_TXN_ID, txnId)
            ),
            true
        )
        if (txnData.size > 1) {
            error(
                "Found more than one transaction entity. " +
                    "Something went wrong. TxnId: '$txnId' IdField: '$fieldName' IdValue: '$id'"
            )
        }
        return if (txnData.size == 1) {
            txnData[0]
        } else {
            null
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
        return initColumns().isNotEmpty()
    }

    override fun resetColumnsCache() {
        columns = null
        entityRepo.setColumns(emptyList())
        tableMetaService?.resetColumnsCache()
        txnDataService?.resetColumnsCache()
        authorityDataService?.resetColumnsCache()
        permsDataService?.resetColumnsCache()
        tableMetaService?.resetColumnsCache()
    }

    override fun runMigrationByType(type: String, mock: Boolean, config: ObjectData) {
        dataSource.withTransaction(mock) {
            migrations.runMigrationByType(type, mock, config)
            txnDataService?.runMigrationByType(type, mock, config)
            resetColumnsCache()
        }
    }

    override fun registerMigration(migration: DbMigration<T, *>) {
        migrations.register(migration)
        txnDataService?.registerMigration(migration)
    }

    override fun runMigrations(
        mock: Boolean,
        diff: Boolean,
        onlyOwn: Boolean
    ): List<String> {
        return runMigrations(emptyList(), mock, diff, onlyOwn)
    }

    override fun runMigrations(
        expectedColumns: List<DbColumnDef>,
        mock: Boolean,
        diff: Boolean,
        onlyOwn: Boolean
    ): List<String> {

        return dataSource.withTransaction(mock) {
            val result = runMigrationsInTxn(expectedColumns, mock, diff, onlyOwn)
            resetColumnsCache()
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
        diff: Boolean,
        onlyOwn: Boolean
    ): List<String> {

        initColumns()

        val expectedWithEntityColumns = ArrayList(entityMapper.getEntityColumns().map { it.columnDef })
        expectedWithEntityColumns.addAll(expectedColumns)

        val startTime = Instant.now()
        val changedColumns = mutableListOf<DbColumnDef>()
        val migration = {
            dataSource.watchSchemaCommands {
                changedColumns.addAll(ensureColumnsExistImpl(expectedWithEntityColumns, mock, diff))
                if (!onlyOwn) {
                    tableMetaService?.runMigrations(mock, diff, true)
                    txnDataService?.runMigrations(getTxnColumns(expectedColumns), mock, diff, true)
                    authorityDataService?.runMigrations(mock, diff, true)
                    permsDataService?.runMigrations(mock, diff, true)
                }
            }
        }
        if (mock) {
            return dataSource.withSchemaMock { migration.invoke() }
        }

        val commands = migration.invoke()
        val durationMs = System.currentTimeMillis() - startTime.toEpochMilli()

        if (!mock && commands.isNotEmpty()) {
            this.columns = null
            val newColumns = initColumns()
            entityRepo.setColumns(newColumns)
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
            params.set("columns", changedColumns)

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

    private fun getTxnColumns(columns: List<DbColumnDef>): List<DbColumnDef> {
        val txnColumns = ArrayList(columns)
        txnColumns.add(
            DbColumnDef(
                COLUMN_EXT_TXN_ID,
                DbColumnType.UUID,
                false,
                emptyList()
            )
        )
        return txnColumns
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
            schemaDao.createTable(expectedColumns)
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
                val currentCount = entityRepo.getCount(VoidPredicate.INSTANCE)
                if (currentCount > maxItemsToAllowSchemaMigration) {
                    val baseMsg = "Schema migration can't be performed because table has too much items: $currentCount."
                    val newColumnsMsg = columnsWithChangedType.joinToString { it.toString() }
                    val oldColumnsMsg = columnsWithChangedType.joinToString { currentColumnsByName[it.name].toString() }
                    log.error { "$baseMsg\n New columns: $newColumnsMsg\n Old columns: $oldColumnsMsg" }
                    error(baseMsg)
                }
            }
            columnsWithChangedType.forEach {
                schemaDao.setColumnType(it.name, it.multiple, it.type)
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

        schemaDao.addColumns(fixedMissedColumns)
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

        val currentColumnsNames = schemaDao.getColumns().map { it.name }.toSet()
        val newColumnsNames = newColumns.map { it.name }.toSet()

        val newIndexes = indexes.filter { index ->
            val columns = index.columns
            columns.any { newColumnsNames.contains(it) } &&
                columns.all { currentColumnsNames.contains(it) }
        }
        if (newIndexes.isNotEmpty()) {
            schemaDao.createIndexes(newIndexes)
        }
        val newConstraints = fkConstraints.filter { constraint ->
            newColumnsNames.contains(constraint.baseColumnName)
        }
        if (newConstraints.isNotEmpty()) {
            schemaDao.createFkConstraints(newConstraints)
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

    private fun initColumns(): List<DbColumnDef> {
        var columns = this.columns
        if (columns == null) {
            columns = dataSource.withTransaction(true) {
                val schemaCols = schemaDao.getColumns()
                entityRepo.setColumns(schemaCols)
                schemaCols
            }
            this.columns = columns
        }
        return columns
    }
}
