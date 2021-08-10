package ru.citeck.ecos.data.sql.pg

import mu.KotlinLogging
import ru.citeck.ecos.data.sql.datasource.DbDataSource
import ru.citeck.ecos.data.sql.dto.DbColumnDef
import ru.citeck.ecos.data.sql.dto.DbColumnType
import ru.citeck.ecos.data.sql.dto.DbTableRef
import ru.citeck.ecos.data.sql.repo.DbContextManager
import ru.citeck.ecos.data.sql.repo.DbEntityRepo
import ru.citeck.ecos.data.sql.repo.entity.DbEntity
import ru.citeck.ecos.data.sql.repo.entity.DbEntityMapper
import ru.citeck.ecos.data.sql.repo.find.DbFindPage
import ru.citeck.ecos.data.sql.repo.find.DbFindRes
import ru.citeck.ecos.data.sql.repo.find.DbFindSort
import ru.citeck.ecos.data.sql.type.DbTypeUtils
import ru.citeck.ecos.data.sql.type.DbTypesConverter
import ru.citeck.ecos.records2.predicate.model.*
import java.sql.ResultSet
import java.sql.Timestamp
import java.time.Instant
import java.util.*
import kotlin.collections.ArrayList
import kotlin.reflect.KClass

class DbEntityRepoPg<T : Any>(
    private val mapper: DbEntityMapper<T>,
    private val ctxManager: DbContextManager,
    private val dataSource: DbDataSource,
    private val tableRef: DbTableRef,
    private val typesConverter: DbTypesConverter
) : DbEntityRepo<T> {

    companion object {
        private val log = KotlinLogging.logger {}
    }

    private var columns: List<DbColumnDef> = ArrayList()
    private var schemaCacheUpdateRequired = false

    private val hasDeletedFlag = mapper.getEntityColumns().any { it.columnDef.name == DbEntity.DELETED }

    override fun setColumns(columns: List<DbColumnDef>) {
        val newColumns = ArrayList(columns)
        if (this.columns == newColumns) {
            return
        }
        this.columns = newColumns
        schemaCacheUpdateRequired = true
    }

    override fun findById(id: String): T? {
        return findById(id, false)
    }

    override fun findById(id: String, withDeleted: Boolean): T? {
        return findOneByColumnAsMap(DbEntity.EXT_ID, id, columns, withDeleted)?.let {
            mapper.convertToEntity(it)
        }
    }

    private fun findByExtIdAsMap(
        id: String,
        columns: List<DbColumnDef>,
        withDeleted: Boolean = false
    ): Map<String, Any?>? {

        return findOneByColumnAsMap(DbEntity.EXT_ID, id, columns, withDeleted)
    }

    private fun findOneByColumnAsMap(
        column: String,
        value: Any,
        columns: List<DbColumnDef>,
        withDeleted: Boolean
    ): Map<String, Any?>? {

        if (columns.isEmpty()) {
            return null
        }

        updateSchemaCache()

        val select = createSelect(columns, withDeleted)
        select.append(" AND \"$column\"=?")

        return dataSource.query(select.toString(), listOf(value)) { resultSet ->
            if (resultSet.next()) {
                convertRowToMap(resultSet, columns)
            } else {
                null
            }
        }
    }

    private fun convertRowToMap(row: ResultSet, columns: List<DbColumnDef>): Map<String, Any?> {
        val result = LinkedHashMap<String, Any?>()
        columns.forEach { column ->
            val value = row.getObject(column.name)
            result[column.name] = if (value != null) {
                var expectedType = column.type.type
                if (column.multiple) {
                    expectedType = DbTypeUtils.getArrayType(expectedType)
                }
                typesConverter.convert(value, expectedType)
            } else {
                null
            }
        }
        return result
    }

    override fun forceDelete(entity: T) {
        if (columns.isEmpty()) {
            return
        }
        forceDelete(mapper.convertToMap(entity))
    }

    override fun delete(extId: String) {

        if (columns.isEmpty()) {
            return
        }

        val entity = findByExtIdAsMap(extId, columns) ?: return
        if (hasDeletedFlag) {
            val mutableEntity = LinkedHashMap(entity)
            mutableEntity[DbEntity.DELETED] = true
            saveImpl(mutableEntity)
        } else {
            forceDelete(entity)
        }
    }

    private fun forceDelete(entity: Map<String, Any?>) {
        updateSchemaCache()
        dataSource.update(
            "DELETE FROM ${tableRef.fullName} WHERE \"${DbEntity.ID}\"=${entity[DbEntity.ID]}",
            emptyList()
        )
    }

    override fun save(entity: T): T {
        if (columns.isEmpty()) {
            error("Columns is empty")
        }
        val entityMap = LinkedHashMap(mapper.convertToMap(entity))
        return mapper.convertToEntity(saveAndGet(entityMap))
    }

    private fun saveAndGet(entity: Map<String, Any?>): Map<String, Any?> {
        val extId = saveImpl(entity)
        return findByExtIdAsMap(extId, columns) ?: error("Entity with extId $extId was inserted but can't be found.")
    }

    private fun saveImpl(entity: Map<String, Any?>): String {

        updateSchemaCache()

        val nowInstant = Instant.now()
        val entityMap = LinkedHashMap(entity)

        val id = entityMap[DbEntity.ID] as? Long ?: error("ID is a mandatory parameter!")
        var extId = entityMap[DbEntity.EXT_ID] as? String ?: ""
        val deleted = entityMap[DbEntity.DELETED] as? Boolean ?: false

        if (deleted && extId.isBlank()) {
            return ""
        } else if (extId.isBlank() && !deleted) {
            extId = UUID.randomUUID().toString()
            entityMap[DbEntity.EXT_ID] = extId
        }

        val attsToSave = LinkedHashMap(entityMap)
        attsToSave.remove(DbEntity.ID)
        attsToSave[DbEntity.MODIFIED] = nowInstant
        attsToSave[DbEntity.MODIFIER] = ctxManager.getCurrentUser()

        if (id == DbEntity.NEW_REC_ID) {
            insertImpl(attsToSave, nowInstant)
        } else {
            updateImpl(id, attsToSave)
        }

        return extId
    }

    private fun insertImpl(entity: Map<String, Any?>, nowInstant: Instant) {

        val attsToInsert = LinkedHashMap(entity)
        if (!hasDeletedFlag) {
            attsToInsert[DbEntity.DELETED] = false
        }
        attsToInsert[DbEntity.UPD_VERSION] = 0L
        attsToInsert[DbEntity.CREATED] = nowInstant
        attsToInsert[DbEntity.CREATOR] = ctxManager.getCurrentUser()

        val valuesForDb = prepareValuesForDb(attsToInsert)
        val columnNames = valuesForDb.joinToString(",") { "\"${it.name}\"" }
        val columnPlaceholders = valuesForDb.joinToString(",") { it.placeholder }

        val query = "INSERT INTO ${tableRef.fullName} ($columnNames) VALUES ($columnPlaceholders)"

        dataSource.update(query, valuesForDb.map { it.value })
    }

    private fun updateImpl(id: Long, attributes: Map<String, Any?>) {

        val version: Long = attributes[DbEntity.UPD_VERSION] as? Long
            ?: error("Missing attribute: ${DbEntity.UPD_VERSION}")

        var newVersion = version + 1
        if (newVersion >= Int.MAX_VALUE) {
            newVersion = 0L
        }

        val attsToUpdate = LinkedHashMap(attributes)
        attsToUpdate[DbEntity.UPD_VERSION] = newVersion

        val valuesForDb = prepareValuesForDb(attsToUpdate)
        val setPlaceholders = valuesForDb.joinToString(",") { "\"${it.name}\"=${it.placeholder}" }

        val query = "UPDATE ${tableRef.fullName} SET $setPlaceholders " +
            "WHERE \"${DbEntity.ID}\"='$id' " +
            "AND \"${DbEntity.UPD_VERSION}\"=$version"

        if (dataSource.update(query, valuesForDb.map { it.value }) != 1) {
            error("Concurrent modification of record with id: $id")
        }
    }

    override fun getCount(predicate: Predicate): Long {

        if (columns.isEmpty()) {
            return 0
        }

        val params = mutableListOf<Any?>()
        val sqlPredicate = StringBuilder()

        toSqlPredicate(predicate, sqlPredicate, params, columns.associateBy { it.name })

        return getCountImpl(sqlPredicate.toString(), params)
    }

    private fun getCountImpl(sqlPredicate: String, params: List<Any?>): Long {
        updateSchemaCache()
        val selectQuery = createSelect("COUNT(*)")
        if (sqlPredicate.isNotBlank()) {
            selectQuery.append(" AND ").append(sqlPredicate)
        }
        return dataSource.query(selectQuery.toString(), params) { resultSet ->
            if (resultSet.next()) {
                resultSet.getLong(1)
            } else {
                0
            }
        }
    }

    override fun findAll(): List<T> {
        return find(VoidPredicate.INSTANCE, emptyList(), DbFindPage.ALL).entities
    }

    override fun findAll(predicate: Predicate): List<T> {
        return findAll(predicate, false)
    }

    override fun findAll(predicate: Predicate, withDeleted: Boolean): List<T> {
        return find(predicate, emptyList(), DbFindPage.ALL, withDeleted).entities
    }

    override fun findAll(predicate: Predicate, sort: List<DbFindSort>): List<T> {
        return find(predicate, sort, DbFindPage.ALL).entities
    }

    override fun find(
        predicate: Predicate,
        sort: List<DbFindSort>,
        page: DbFindPage
    ): DbFindRes<T> {

        return find(predicate, sort, page, false)
    }

    private fun find(
        predicate: Predicate,
        sort: List<DbFindSort>,
        page: DbFindPage,
        withDeleted: Boolean
    ): DbFindRes<T> {

        if (columns.isEmpty()) {
            return DbFindRes(emptyList(), 0)
        }
        updateSchemaCache()

        val queryBuilder = createSelect(columns, withDeleted)
        val columnsByName = columns.associateBy { it.name }

        val params = mutableListOf<Any?>()
        var sqlPredicate = ""

        if (predicate !is VoidPredicate) {
            val sqlPredicateBuilder = StringBuilder()
            toSqlPredicate(predicate, sqlPredicateBuilder, params, columnsByName)
            sqlPredicate = sqlPredicateBuilder.toString()
            if (sqlPredicate.isNotBlank()) {
                queryBuilder.append(" AND ")
                queryBuilder.append(sqlPredicate)
            }
        }

        if (sort.isNotEmpty()) {
            queryBuilder.append(" ORDER BY ")
            val orderBy = sort.joinToString {
                if (!columnsByName.containsKey(it.column)) {
                    error("Column is not allowed: ${it.column}")
                }
                "\"" + it.column + "\" " + if (it.ascending) { "ASC" } else { "DESC" }
            }
            queryBuilder.append(orderBy)
        }

        if (page.maxItems >= 0) {
            queryBuilder.append(" LIMIT ").append(page.maxItems)
        }
        if (page.skipCount > 0) {
            queryBuilder.append(" OFFSET ").append(page.skipCount)
        }

        val resultEntities = dataSource.query(queryBuilder.toString(), params) { resultSet ->
            val resultList = mutableListOf<T>()
            while (resultSet.next()) {
                resultList.add(mapper.convertToEntity(convertRowToMap(resultSet, columns)))
            }
            resultList
        }

        val totalCount = if (page.maxItems == -1) {
            resultEntities.size.toLong() + page.skipCount
        } else {
            getCountImpl(sqlPredicate, params)
        }
        return DbFindRes(resultEntities, totalCount)
    }

    private fun createSelect(selectColumns: List<DbColumnDef>, withDeleted: Boolean = false): StringBuilder {
        return createSelect(selectColumns.joinToString { "\"${it.name}\"" }, withDeleted)
    }

    private fun createSelect(selectColumns: String, withDeleted: Boolean = false): StringBuilder {
        val sb = StringBuilder()
        sb.append("SELECT $selectColumns FROM ${tableRef.fullName} WHERE ")
        if (!hasDeletedFlag || withDeleted) {
            sb.append("true")
        } else {
            sb.append("\"").append(DbEntity.DELETED).append("\"!=true")
        }
        return sb
    }

    private fun prepareValuesForDb(entity: Map<String, Any?>): List<ValueForDb> {
        return columns.mapNotNull { column ->
            if (!entity.containsKey(column.name)) {
                null
            } else {
                val placeholder = if (column.type == DbColumnType.JSON) {
                    "?::jsonb"
                } else {
                    "?"
                }
                var value = entity[column.name]
                value = if (value == null) {
                    value
                } else {
                    typesConverter.convert(
                        value,
                        getParamTypeForColumn(column.type, column.multiple)
                    )
                }
                ValueForDb(column.name, placeholder, value)
            }
        }
    }

    private fun getParamTypeForColumn(type: DbColumnType, multiple: Boolean): KClass<*> {
        val baseType = when (type) {
            DbColumnType.BIGSERIAL -> Long::class
            DbColumnType.INT -> Int::class
            DbColumnType.DOUBLE -> Double::class
            DbColumnType.BOOLEAN -> Boolean::class
            DbColumnType.DATETIME -> Timestamp::class
            DbColumnType.LONG -> Long::class
            DbColumnType.JSON -> String::class
            DbColumnType.TEXT -> String::class
            DbColumnType.BINARY -> ByteArray::class
            DbColumnType.UUID -> UUID::class
        }
        return if (multiple) {
            DbTypeUtils.getArrayType(baseType)
        } else {
            baseType
        }
    }

    private fun toSqlPredicate(
        predicate: Predicate,
        query: StringBuilder,
        queryParams: MutableList<Any?>,
        columnsByName: Map<String, DbColumnDef>
    ): Boolean {

        when (predicate) {

            is ComposedPredicate -> {

                val joinOperator: String = when (predicate) {
                    is AndPredicate -> " AND "
                    is OrPredicate -> " OR "
                    else -> error("Unknown predicate type: " + predicate.javaClass)
                }
                query.append("(")
                var notEmpty = false
                for (innerPred in predicate.getPredicates()) {
                    if (toSqlPredicate(innerPred, query, queryParams, columnsByName)) {
                        query.append(joinOperator)
                        notEmpty = true
                    }
                }
                return if (notEmpty) {
                    query.setLength(query.length - joinOperator.length)
                    query.append(")")
                    true
                } else {
                    query.setLength(query.length - 1)
                    false
                }
            }
            is ValuePredicate -> {

                val columnDef = columnsByName[predicate.getAttribute()]

                // todo: add ability to search for multiple values fields
                if (columnDef == null || columnDef.multiple) {
                    return false
                }

                val type = predicate.getType()
                val attribute: String = predicate.getAttribute()
                val value = predicate.getValue()

                val operator = when (type) {
                    ValuePredicate.Type.IN -> "IN"
                    ValuePredicate.Type.EQ -> "="
                    ValuePredicate.Type.LIKE,
                    ValuePredicate.Type.CONTAINS ->
                        if (value.isNumber()) {
                            "="
                        } else {
                            "LIKE"
                        }
                    ValuePredicate.Type.GT -> ">"
                    ValuePredicate.Type.GE -> ">="
                    ValuePredicate.Type.LT -> "<"
                    ValuePredicate.Type.LE -> "<="
                    else -> {
                        log.error { "Unknown predicate type: $type" }
                        return false
                    }
                }

                when (type) {
                    ValuePredicate.Type.IN -> {
                        if (!value.isArray()) {
                            log.error { "illegal value for IN: $value" }
                            return false
                        }
                        query.append('"').append(attribute).append('"').append(" IN (")
                        var first = true
                        for (v in value) {
                            if (!first) {
                                query.append(",")
                            }
                            query.append("?")
                            queryParams.add(v.asText())
                            first = false
                        }
                        query.append(")")
                    }
                    ValuePredicate.Type.EQ,
                    ValuePredicate.Type.LIKE,
                    ValuePredicate.Type.GT,
                    ValuePredicate.Type.GE,
                    ValuePredicate.Type.LT,
                    ValuePredicate.Type.LE,
                    ValuePredicate.Type.CONTAINS -> {
                        query.append('"').append(attribute).append('"')
                            .append(" ")
                            .append(operator)
                            .append(" ?")
                        if (value.isTextual() && type == ValuePredicate.Type.CONTAINS) {
                            queryParams.add("%" + value.asText() + "%")
                        } else if (value.isTextual() && columnDef.type == DbColumnType.UUID) {
                            queryParams.add(UUID.fromString(value.asText()))
                        } else {
                            queryParams.add(value.asJavaObj())
                        }
                    }
                }
                return true
            }
            is NotPredicate -> {
                query.append("NOT ")
                return if (toSqlPredicate(predicate.getPredicate(), query, queryParams, columnsByName)) {
                    true
                } else {
                    query.setLength(query.length - 4)
                    false
                }
            }
            is VoidPredicate -> {
                return false
            }
            else -> {
                log.error { "Unknown predicate type: ${predicate::class}" }
                return false
            }
        }
    }

    private fun updateSchemaCache() {
        if (schemaCacheUpdateRequired) {
            dataSource.updateSchema("DEALLOCATE ALL")
            schemaCacheUpdateRequired = false
        }
    }

    private data class ValueForDb(
        val name: String,
        val placeholder: String,
        val value: Any?
    )
}
