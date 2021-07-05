package ru.citeck.ecos.data.sql.service

import ru.citeck.ecos.commons.data.DataValue
import ru.citeck.ecos.commons.data.ObjectData
import ru.citeck.ecos.commons.json.Json
import ru.citeck.ecos.data.sql.datasource.DbDataSource
import ru.citeck.ecos.data.sql.dto.DbColumnDef
import ru.citeck.ecos.data.sql.dto.DbTableRef
import ru.citeck.ecos.data.sql.meta.DbTableMetaEntity
import ru.citeck.ecos.data.sql.meta.dto.DbTableChangeSet
import ru.citeck.ecos.data.sql.meta.dto.DbTableMetaAuthConfig
import ru.citeck.ecos.data.sql.meta.dto.DbTableMetaConfig
import ru.citeck.ecos.data.sql.repo.DbContextManager
import ru.citeck.ecos.data.sql.repo.DbEntityRepoPg
import ru.citeck.ecos.data.sql.repo.entity.DbEntityMapperImpl
import ru.citeck.ecos.data.sql.repo.find.DbFindPage
import ru.citeck.ecos.data.sql.repo.find.DbFindRes
import ru.citeck.ecos.data.sql.repo.find.DbFindSort
import ru.citeck.ecos.data.sql.schema.DbSchemaDaoPg
import ru.citeck.ecos.data.sql.type.DbTypesConverter
import ru.citeck.ecos.records2.predicate.model.Predicate
import java.time.Instant
import kotlin.reflect.KClass

class DbDataService<T : Any>(
    private val config: DbDataServiceConfig,
    private val tableRef: DbTableRef,
    private val dataSource: DbDataSource,
    entityType: KClass<T>,
    dbContextManager: DbContextManager,
    storeTableMeta: Boolean
) {

    companion object {
        private const val META_TABLE_NAME = "ecos_data_table_meta"
    }

    private val typesConverter = DbTypesConverter()
    private val schemaDao = DbSchemaDaoPg(dataSource, tableRef)
    private val entityMapper = DbEntityMapperImpl(entityType, typesConverter)
    private val entityRepo = DbEntityRepoPg(entityMapper, dbContextManager, dataSource, tableRef, typesConverter)

    private var columns: List<DbColumnDef>? = null

    private val tableMetaService: DbDataService<DbTableMetaEntity>? = if (storeTableMeta) {
        DbDataService(
            DbDataServiceConfig(false),
            DbTableRef(tableRef.schema, META_TABLE_NAME),
            dataSource,
            DbTableMetaEntity::class,
            dbContextManager,
            false
        )
    } else {
        null
    }

    fun findById(id: String): T? {
        return dataSource.withTransaction(true) {
            initColumns()
            entityRepo.findById(id)
        }
    }

    fun findAll(): List<T> {
        return dataSource.withTransaction(true) {
            initColumns()
            entityRepo.findAll()
        }
    }

    fun findAll(predicate: Predicate): List<T> {
        return dataSource.withTransaction(true) {
            initColumns()
            entityRepo.findAll(predicate)
        }
    }

    fun findAll(predicate: Predicate, sort: List<DbFindSort>): List<T> {
        return dataSource.withTransaction(true) {
            initColumns()
            entityRepo.findAll(predicate, sort)
        }
    }

    fun find(predicate: Predicate, sort: List<DbFindSort>, page: DbFindPage): DbFindRes<T> {
        return dataSource.withTransaction(true) {
            initColumns()
            entityRepo.find(predicate, sort, page)
        }
    }

    fun getCount(predicate: Predicate): Long {
        return dataSource.withTransaction(true) {
            initColumns()
            entityRepo.getCount(predicate)
        }
    }

    fun save(entity: T, columns: List<DbColumnDef>): T {
        val columnsBefore = this.columns
        try {
            return dataSource.withTransaction(false) {
                initColumns()
                val fullColumns = ArrayList(entityMapper.getEntityColumns().map { it.columnDef })
                fullColumns.addAll(columns)
                ensureColumnsExist(fullColumns)
                entityRepo.save(entity)
            }
        } catch (e: Exception) {
            this.columns = columnsBefore
            entityRepo.setColumns(columnsBefore ?: emptyList())
            throw e
        }
    }

    fun delete(extId: String) {
        initColumns()
        dataSource.withTransaction(false) {
            entityRepo.delete(extId)
        }
    }

    @Synchronized
    fun ensureColumnsExist(expectedColumns: List<DbColumnDef>) {

        val startTime = Instant.now()
        val changedColumns = mutableListOf<DbColumnDef>()
        val commands = dataSource.watchCommands {
            changedColumns.addAll(ensureColumnsExistImpl(expectedColumns))
        }
        val durationMs = System.currentTimeMillis() - startTime.toEpochMilli()

        if (commands.isNotEmpty()) {
            this.columns = expectedColumns
            entityRepo.setColumns(expectedColumns)
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
    }

    private fun ensureColumnsExistImpl(expectedColumns: List<DbColumnDef>): List<DbColumnDef> {

        val currentColumns = this.columns ?: error("Current columns is null")
        if (currentColumns.isEmpty()) {
            schemaDao.createTable(expectedColumns)
            return expectedColumns
        }
        val currentColumnsByName = currentColumns.associateBy { it.name }

        val columnsWithChangedType = expectedColumns.filter {
            val currentColumn = currentColumnsByName[it.name]
            if (currentColumn != null) {
                currentColumn.type != it.type || currentColumn.multiple != it.multiple
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

    private fun initColumns() {
        if (columns == null) {
            val columns = schemaDao.getColumns()
            entityRepo.setColumns(columns)
            this.columns = columns
        }
    }

    fun getMetaService(): DbDataService<DbTableMetaEntity>? {
        return tableMetaService
    }
}
