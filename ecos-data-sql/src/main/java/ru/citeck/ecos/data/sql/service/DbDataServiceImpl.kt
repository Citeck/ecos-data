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
import ru.citeck.ecos.data.sql.repo.DbEntityRepo
import ru.citeck.ecos.data.sql.repo.entity.DbEntityMapperImpl
import ru.citeck.ecos.data.sql.repo.find.DbFindPage
import ru.citeck.ecos.data.sql.repo.find.DbFindRes
import ru.citeck.ecos.data.sql.repo.find.DbFindSort
import ru.citeck.ecos.data.sql.schema.DbSchemaDao
import ru.citeck.ecos.data.sql.type.DbTypesConverter
import ru.citeck.ecos.records2.predicate.model.Predicate
import java.time.Instant
import kotlin.reflect.KClass

class DbDataServiceImpl<T : Any>(
    private val config: DbDataServiceConfig,
    private val tableRef: DbTableRef,
    private val dataSource: DbDataSource,
    entityType: KClass<T>,
    private val schemaDao: DbSchemaDao,
    private val entityRepo: DbEntityRepo<T>,
    private val tableMetaService: DbDataService<DbTableMetaEntity>?
) : DbDataService<T> {

    private val typesConverter = DbTypesConverter()
    private val entityMapper = DbEntityMapperImpl(entityType, typesConverter)

    private var columns: List<DbColumnDef>? = null

    override fun findById(id: String): T? {
        return dataSource.withTransaction(true) {
            initColumns()
            entityRepo.findById(id)
        }
    }

    override fun findAll(): List<T> {
        return dataSource.withTransaction(true) {
            initColumns()
            entityRepo.findAll()
        }
    }

    override fun findAll(predicate: Predicate): List<T> {
        return dataSource.withTransaction(true) {
            initColumns()
            entityRepo.findAll(predicate)
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
        val columnsBefore = this.columns
        try {
            return dataSource.withTransaction(false) {
                runMigrationsInTxn(columns, mock = false, diff = true)
                entityRepo.save(entity)
            }
        } catch (e: Exception) {
            this.columns = columnsBefore
            entityRepo.setColumns(columnsBefore ?: emptyList())
            throw e
        }
    }

    override fun delete(extId: String) {
        initColumns()
        dataSource.withTransaction(false) {
            entityRepo.delete(extId)
        }
    }

    override fun resetColumnsCache() {
        this.columns = null
    }

    @Synchronized
    override fun runMigrations(
        expectedColumns: List<DbColumnDef>,
        mock: Boolean,
        diff: Boolean
    ): List<String> {

        return dataSource.withTransaction(mock) {
            runMigrationsInTxn(expectedColumns, mock, diff)
        }
    }

    private fun runMigrationsInTxn(
        expectedColumns: List<DbColumnDef>,
        mock: Boolean,
        diff: Boolean
    ): List<String> {

        initColumns()

        val fullColumns = ArrayList(entityMapper.getEntityColumns().map { it.columnDef })
        fullColumns.addAll(expectedColumns)

        val startTime = Instant.now()
        val changedColumns = mutableListOf<DbColumnDef>()
        val commands = if (mock) {
            dataSource.withSchemaMock {
                dataSource.watchCommands {
                    changedColumns.addAll(ensureColumnsExistImpl(fullColumns, diff))
                    tableMetaService?.runMigrations(emptyList(), true, diff)
                }
            }
        } else {
            dataSource.watchCommands {
                changedColumns.addAll(ensureColumnsExistImpl(fullColumns, diff))
            }
        }
        if (mock) {
            return commands
        }

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

    private fun initColumns() {
        if (columns == null) {
            val columns = schemaDao.getColumns()
            entityRepo.setColumns(columns)
            this.columns = columns
        }
    }
}
