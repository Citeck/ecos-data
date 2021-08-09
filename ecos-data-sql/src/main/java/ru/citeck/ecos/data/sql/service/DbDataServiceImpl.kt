package ru.citeck.ecos.data.sql.service

import ru.citeck.ecos.commons.data.DataValue
import ru.citeck.ecos.commons.data.ObjectData
import ru.citeck.ecos.commons.json.Json
import ru.citeck.ecos.data.sql.datasource.DbDataSource
import ru.citeck.ecos.data.sql.dto.DbColumnDef
import ru.citeck.ecos.data.sql.dto.DbColumnType
import ru.citeck.ecos.data.sql.dto.DbTableRef
import ru.citeck.ecos.data.sql.meta.DbTableMetaEntity
import ru.citeck.ecos.data.sql.meta.dto.DbTableChangeSet
import ru.citeck.ecos.data.sql.meta.dto.DbTableMetaAuthConfig
import ru.citeck.ecos.data.sql.meta.dto.DbTableMetaConfig
import ru.citeck.ecos.data.sql.meta.dto.DbTableMetaDto
import ru.citeck.ecos.data.sql.repo.DbEntityRepo
import ru.citeck.ecos.data.sql.repo.entity.DbEntity
import ru.citeck.ecos.data.sql.repo.entity.DbEntityMapperImpl
import ru.citeck.ecos.data.sql.repo.find.DbFindPage
import ru.citeck.ecos.data.sql.repo.find.DbFindRes
import ru.citeck.ecos.data.sql.repo.find.DbFindSort
import ru.citeck.ecos.data.sql.schema.DbSchemaDao
import ru.citeck.ecos.data.sql.txn.ExtTxnContext
import ru.citeck.ecos.data.sql.type.DbTypesConverter
import ru.citeck.ecos.records2.predicate.model.Predicate
import ru.citeck.ecos.records2.predicate.model.Predicates
import java.time.Instant
import java.util.*
import kotlin.collections.ArrayList
import kotlin.collections.HashMap
import kotlin.reflect.KClass

class DbDataServiceImpl<T : Any>(
    private val config: DbDataServiceConfig,
    private val tableRef: DbTableRef,
    private val dataSource: DbDataSource,
    entityType: KClass<T>,
    private val schemaDao: DbSchemaDao,
    private val entityRepo: DbEntityRepo<T>,
    private val tableMetaService: DbDataService<DbTableMetaEntity>?,
    private val txnDataService: DbDataService<T>?
) : DbDataService<T> {

    companion object {
        private const val COLUMN_EXT_TXN_ID = "__ext_txn_id"
    }

    private val typesConverter = DbTypesConverter()
    private val entityMapper = DbEntityMapperImpl(entityType, typesConverter)

    private var columns: List<DbColumnDef>? = null

    override fun findById(id: String): T? {
        return dataSource.withTransaction(true) {
            initColumns()
            val txnId = ExtTxnContext.getExtTxnId()
            if (txnDataService == null || txnId == null) {
                entityRepo.findById(id)
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
                    entityRepo.findById(id)
                }
            }
        }
    }

    override fun findAll(): List<T> {
        return dataSource.withTransaction(true) {
            initColumns()
            entityRepo.findAll()
        }
    }

    override fun findAll(predicate: Predicate): List<T> {
        return findAll(predicate, false)
    }

    override fun findAll(predicate: Predicate, withDeleted: Boolean): List<T> {
        return dataSource.withTransaction(true) {
            initColumns()
            entityRepo.findAll(predicate, withDeleted)
        }
    }

    override fun findAll(predicate: Predicate, sort: List<DbFindSort>): List<T> {
        return dataSource.withTransaction(true) {
            initColumns()
            entityRepo.findAll(predicate, sort)
        }
    }

    override fun find(predicate: Predicate, sort: List<DbFindSort>, page: DbFindPage): DbFindRes<T> {
        return dataSource.withTransaction(true) {
            initColumns()
            entityRepo.find(predicate, sort, page)
        }
    }

    override fun getCount(predicate: Predicate): Long {
        return dataSource.withTransaction(true) {
            initColumns()
            entityRepo.getCount(predicate)
        }
    }

    override fun save(entity: T, columns: List<DbColumnDef>): T {
        val txnId = ExtTxnContext.getExtTxnId()
        val columnsBefore = this.columns
        try {
            return dataSource.withTransaction(false) {
                runMigrationsInTxn(columns, mock = false, diff = true, onlyOwn = false)

                if (txnDataService == null || txnId == null) {
                    entityRepo.save(entity)
                } else {
                    val newEntity = HashMap(entityMapper.convertToMap(entity))
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

    override fun delete(id: String) {

        val columns = initColumns()

        dataSource.withTransaction(false) {

            val txnDataService = this.txnDataService
            val txnId = ExtTxnContext.getExtTxnId()
            if (txnDataService == null || txnId == null) {
                entityRepo.delete(id)
            } else {
                var txnEntity = findTxnEntityById(txnId, id)
                if (txnEntity != null) {
                    txnDataService.delete(id)
                } else {
                    txnEntity = entityRepo.findById(id)
                    if (txnEntity != null) {
                        val entityMap = HashMap(entityMapper.convertToMap(txnEntity))
                        entityMap[COLUMN_EXT_TXN_ID] = txnId
                        entityMap[DbEntity.DELETED] = true
                        entityMap.remove(DbEntity.ID)
                        txnDataService.save(entityMapper.convertToEntity(entityMap), getTxnColumns(columns))
                    }
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

    override fun commit(entitiesId: List<String>) {
        completeExtTxn(entitiesId, true)
    }

    override fun rollback(entitiesId: List<String>) {
        completeExtTxn(entitiesId, false)
    }

    private fun completeExtTxn(entitiesId: List<String>, success: Boolean) {

        val txnId = ExtTxnContext.getExtTxnId()
            ?: error("External transaction ID is null. Entities: $entitiesId")

        val txnDataService = this.txnDataService ?: return
        dataSource.withTransaction(false) {
            entitiesId.forEach { id ->
                val txnEntity = findTxnEntityById(txnId, id)
                if (txnEntity != null) {
                    if (success) {

                        val entityMap = HashMap(entityMapper.convertToMap(txnEntity))
                        entityMap.remove(DbEntity.ID)
                        entityMap.remove(COLUMN_EXT_TXN_ID)

                        val entityFromRepo = entityRepo.findById(entityMap[DbEntity.EXT_ID] as String)
                        if (entityFromRepo != null) {
                            val entityMapFromRepo = entityMapper.convertToMap(entityFromRepo)
                            entityMap[DbEntity.ID] = entityMapFromRepo[DbEntity.ID]
                        }
                        if (entityFromRepo != null || entityMap[DbEntity.DELETED] != true) {
                            entityRepo.save(entityMapper.convertToEntity(entityMap))
                        }
                    }
                    txnDataService.forceDelete(txnEntity)
                }
            }
        }
    }

    private fun findTxnEntityById(txnId: UUID, id: String): T? {
        if (txnDataService == null) {
            return null
        }
        val txnData = txnDataService.findAll(
            Predicates.and(
                Predicates.eq(DbEntity.EXT_ID, id),
                Predicates.eq(COLUMN_EXT_TXN_ID, txnId)
            ),
            true
        )
        if (txnData.size > 1) {
            error(
                "Found more than one transaction entity. " +
                    "Something went wrong. TxnId: '$txnId' EntityId: '$id'"
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
        val metaEntity = tableMetaService?.findById(id) ?: return DbTableMetaDto.create().withId(id).build()
        return DbTableMetaDto.create()
            .withId(id)
            .withChangelog(DataValue.create(metaEntity.changelog).asList(DbTableChangeSet::class.java))
            .withConfig(Json.mapper.read(metaEntity.config, DbTableMetaConfig::class.java))
            .build()
    }

    override fun resetColumnsCache() {
        columns = null
        tableMetaService?.resetColumnsCache()
    }

    @Synchronized
    override fun runMigrations(
        expectedColumns: List<DbColumnDef>,
        mock: Boolean,
        diff: Boolean,
        onlyOwn: Boolean
    ): List<String> {

        return dataSource.withTransaction(mock) {
            runMigrationsInTxn(expectedColumns, mock, diff, onlyOwn)
        }
    }

    private fun runMigrationsInTxn(
        expectedColumns: List<DbColumnDef>,
        mock: Boolean,
        diff: Boolean,
        onlyOwn: Boolean
    ): List<String> {

        initColumns()

        val fullColumns = ArrayList(entityMapper.getEntityColumns().map { it.columnDef })
        fullColumns.addAll(expectedColumns)

        val startTime = Instant.now()
        val changedColumns = mutableListOf<DbColumnDef>()
        val migration = {
            dataSource.watchCommands {
                changedColumns.addAll(ensureColumnsExistImpl(fullColumns, diff))
                if (!onlyOwn) {
                    tableMetaService?.runMigrations(emptyList(), true, diff, true)
                    txnDataService?.runMigrations(getTxnColumns(expectedColumns), mock, diff, true)
                }
            }
        }
        if (mock) {
            return dataSource.withSchemaMock { migration.invoke() }
        }

        val commands = migration.invoke()
        val durationMs = System.currentTimeMillis() - startTime.toEpochMilli()

        if (!mock && commands.isNotEmpty()) {
            this.columns = fullColumns
            entityRepo.setColumns(fullColumns)
        }

        if (commands.isNotEmpty() && tableMetaService != null) {

            val currentMeta = tableMetaService.findById(tableRef.table)

            val tableMetaNotNull = if (currentMeta == null) {
                val newMeta = DbTableMetaEntity()
                newMeta.extId = tableRef.table
                val config = DbTableMetaConfig(DbTableMetaAuthConfig(this.config.authEnabled))
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

    private fun ensureColumnsExistImpl(expectedColumns: List<DbColumnDef>, diff: Boolean): List<DbColumnDef> {

        val currentColumns = if (diff) {
            this.columns ?: error("Current columns is null")
        } else {
            emptyList()
        }
        if (currentColumns.isEmpty()) {
            schemaDao.createTable(expectedColumns)
            return expectedColumns
        }
        val currentColumnsByName = currentColumns.associateBy { it.name }

        val columnsWithChangedType = expectedColumns.filter { expectedColumn ->
            val currentColumn = currentColumnsByName[expectedColumn.name]
            if (currentColumn != null) {
                currentColumn.type != expectedColumn.type ||
                    (currentColumn.multiple != expectedColumn.multiple && !currentColumn.multiple)
            } else {
                false
            }
        }
        columnsWithChangedType.forEach {
            schemaDao.setColumnType(it.name, it.multiple, it.type)
        }
        val missingColumns = expectedColumns.filter { !currentColumnsByName.containsKey(it.name) }
        schemaDao.addColumns(missingColumns)

        val changedColumns = ArrayList(columnsWithChangedType)
        changedColumns.addAll(missingColumns)

        return changedColumns
    }

    private fun initColumns(): List<DbColumnDef> {
        var columns = this.columns
        if (columns == null) {
            columns = schemaDao.getColumns()
            entityRepo.setColumns(columns)
            this.columns = columns
        }
        return columns
    }
}
