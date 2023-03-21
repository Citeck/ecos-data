package ru.citeck.ecos.data.sql.pg

import mu.KotlinLogging
import ru.citeck.ecos.context.lib.auth.AuthContext
import ru.citeck.ecos.data.sql.context.DbTableContext
import ru.citeck.ecos.data.sql.dto.DbColumnDef
import ru.citeck.ecos.data.sql.dto.DbColumnType
import ru.citeck.ecos.data.sql.perms.DbPermsEntity
import ru.citeck.ecos.data.sql.repo.DbEntityRepo
import ru.citeck.ecos.data.sql.repo.entity.DbEntity
import ru.citeck.ecos.data.sql.repo.find.DbFindPage
import ru.citeck.ecos.data.sql.repo.find.DbFindRes
import ru.citeck.ecos.data.sql.repo.find.DbFindSort
import ru.citeck.ecos.data.sql.service.DbDataReqContext
import ru.citeck.ecos.data.sql.service.aggregation.AggregateFunc
import ru.citeck.ecos.data.sql.type.DbTypeUtils
import ru.citeck.ecos.data.sql.type.DbTypesConverter
import ru.citeck.ecos.model.lib.type.dto.TypePermsPolicy
import ru.citeck.ecos.records2.RecordConstants
import ru.citeck.ecos.records2.predicate.model.*
import java.sql.Date
import java.sql.ResultSet
import java.sql.Timestamp
import java.time.*
import java.util.*
import kotlin.collections.ArrayList
import kotlin.reflect.KClass

open class DbEntityRepoPg internal constructor() : DbEntityRepo {

    companion object {
        private val log = KotlinLogging.logger {}

        private const val ALWAYS_FALSE_CONDITION = "0=1"
        private const val WHERE_ALWAYS_FALSE = "WHERE $ALWAYS_FALSE_CONDITION"

        private const val RECORD_TABLE_ALIAS = "r"
        private const val PERMS_TABLE_ALIAS = "p"

        private const val IS_TRUE = "IS TRUE"
        private const val IS_FALSE = "IS FALSE"
        private const val IS_NULL = "IS NULL"

        private const val COUNT_COLUMN = "COUNT(*)"
    }

    override fun findByColumn(
        context: DbTableContext,
        column: String,
        values: Collection<Any>,
        withDeleted: Boolean,
        limit: Int
    ): List<Map<String, Any?>> {

        val columns = context.getColumns()

        if (columns.isEmpty() || values.isEmpty()) {
            return emptyList()
        }
        val permsColumn = getPermsColumn(context)
        if (permsColumn.isNotEmpty() && !context.getPermsService().isTableExists()) {
            return emptyList()
        }

        val condition = StringBuilder()
        appendRecordColumnName(condition, column)
        if (values.size == 1) {
            condition.append("=?")
        } else {
            condition.append(" IN (")
            repeat(values.size) { condition.append("?,") }
            condition.setLength(condition.length - 1)
            condition.append(")")
        }

        val query = createSelectQuery(
            context,
            columns,
            permsColumn,
            withDeleted,
            condition.toString(),
            page = DbFindPage(0, limit)
        )
        if (hasAlwaysFalseCondition(query)) {
            return emptyList()
        }

        val typesConverter = context.getTypesConverter()

        return context.getDataSource().query(query, values.toList()) { resultSet ->
            val result = mutableListOf<Map<String, Any?>>()
            while (resultSet.next()) {
                result.add(convertRowToMap(typesConverter, resultSet, columns))
            }
            result
        }
    }

    private fun hasAlwaysFalseCondition(query: String): Boolean {
        return query.contains(WHERE_ALWAYS_FALSE)
    }

    private fun convertRowToMap(
        typesConverter: DbTypesConverter,
        row: ResultSet,
        columns: List<DbColumnDef>,
        groupBy: List<String> = emptyList(),
        selectFunctions: List<AggregateFunc> = emptyList()
    ): Map<String, Any?> {
        val result = LinkedHashMap<String, Any?>()
        if (groupBy.isEmpty()) {
            columns.forEach { column ->
                val value = row.getObject(column.name)
                result[column.name] = if (value != null) {
                    var expectedType = column.type.type
                    if (column.multiple && column.type != DbColumnType.JSON) {
                        expectedType = DbTypeUtils.getArrayType(expectedType)
                    }
                    typesConverter.convert(value, expectedType)
                } else {
                    null
                }
            }
        } else {
            val columnByName = columns.associateBy { it.name }
            groupBy.forEach {
                val value = row.getObject(it)
                result[it] = if (value != null) {
                    val column = columnByName[it]!!
                    typesConverter.convert(value, column.type.type)
                } else {
                    null
                }
            }
            selectFunctions.forEach {
                val value = row.getObject(it.alias)
                result[it.alias] = if (value != null) {
                    val expectedType = when (it.func) {
                        "sum", "count" -> DbColumnType.DOUBLE
                        else -> columnByName[it.field]!!.type
                    }
                    typesConverter.convert(value, expectedType.type)
                } else {
                    null
                }
                it.alias
            }
        }
        return result
    }

    override fun delete(context: DbTableContext, entity: Map<String, Any?>) {
        if (context.hasDeleteFlag()) {
            val mutableEntity = LinkedHashMap(entity)
            mutableEntity[DbEntity.DELETED] = true
            saveImpl(context, listOf(mutableEntity))[0]
        } else {
            forceDelete(context, listOf(entity[DbEntity.ID] as Long))
        }
    }

    override fun forceDelete(context: DbTableContext, predicate: Predicate) {

        val query = StringBuilder("DELETE FROM ")
            .append(context.getTableRef().fullName)
            .append(" \"r\" WHERE ")

        val parameters = arrayListOf<Any?>()
        toSqlCondition(context, query, predicate, parameters)

        context.getDataSource().update(query.toString(), parameters)
    }

    override fun forceDelete(context: DbTableContext, entities: List<Long>) {
        val identifiersStr = entities.joinToString(",")
        context.getDataSource().update(
            "DELETE FROM ${context.getTableRef().fullName} WHERE \"${DbEntity.ID}\" IN ($identifiersStr)",
            emptyList()
        )
    }

    override fun save(context: DbTableContext, entities: List<Map<String, Any?>>): List<Map<String, Any?>> {
        if (context.getColumns().isEmpty()) {
            error("Columns is empty")
        }
        return saveAndGet(context, entities)
    }

    private fun saveAndGet(context: DbTableContext, entities: List<Map<String, Any?>>): List<Map<String, Any?>> {
        val ids = saveImpl(context, entities)
        if (!context.hasIdColumn()) {
            return entities
        }
        return DbDataReqContext.doWithPermsPolicy(TypePermsPolicy.PUBLIC) {
            val entitiesAfterMutation = findByColumn(
                context,
                DbEntity.ID,
                ids.toList(),
                true,
                entities.size
            ).associateBy { it[DbEntity.ID] as Long }

            val missedEntities = ids.filter { !entitiesAfterMutation.containsKey(it) }
            if (missedEntities.isNotEmpty()) {
                error("Entities with ids $missedEntities was inserted or updated but can't be found")
            }
            ids.map {
                entitiesAfterMutation[it] ?: error("Entity with id $it doesn't found after updating")
            }
        }
    }

    private fun isAuthEnabled(context: DbTableContext): Boolean {
        return DbDataReqContext.getPermsPolicy(context.getDefaultPermsPolicy()) != TypePermsPolicy.PUBLIC
    }

    private fun saveImpl(context: DbTableContext, entities: List<Map<String, Any?>>): LongArray {

        val tableRef = context.getTableRef()

        if (isAuthEnabled(context) && AuthContext.getCurrentUser().isEmpty()) {
            error("Current user is empty. Table: $tableRef")
        }
        val hasIdColumn = context.hasIdColumn()

        val nowInstant = Instant.now()

        val entitiesToInsert = mutableListOf<EntityToInsert>()
        val entitiesToUpdate = mutableListOf<EntityToUpdate>()
        val resultIds = LongArray(entities.size) { -1 }

        for ((entityIdx, entity) in entities.withIndex()) {

            val entityMap = LinkedHashMap(entity)
            val entityId: Long = if (hasIdColumn) {
                val id = entityMap[DbEntity.ID] as? Long ?: error("ID is a mandatory parameter!")
                entityMap.remove(DbEntity.ID)
                id
            } else {
                DbEntity.NEW_REC_ID
            }

            var extId = entityMap[DbEntity.EXT_ID] as? String ?: ""
            val deleted = entityMap[DbEntity.DELETED] as? Boolean ?: false

            if (deleted && extId.isBlank()) {
                continue
            } else if (extId.isBlank() && !deleted) {
                extId = UUID.randomUUID().toString()
                entityMap[DbEntity.EXT_ID] = extId
            }

            if (!DbDataReqContext.withoutModifiedMeta.get()) {
                entityMap[DbEntity.MODIFIED] = nowInstant
                entityMap[DbEntity.MODIFIER] = AuthContext.getCurrentUser()
            }

            if (entityId == DbEntity.NEW_REC_ID) {
                entitiesToInsert.add(EntityToInsert(entityIdx, entityMap))
            } else {
                resultIds[entityIdx] = entityId
                entitiesToUpdate.add(EntityToUpdate(entityId, entityMap))
            }
        }

        if (entitiesToUpdate.isNotEmpty()) {
            updateImpl(context, entitiesToUpdate)
        }
        if (entitiesToInsert.isNotEmpty()) {
            val insertIds = insertImpl(context, entitiesToInsert.map { it.data }, nowInstant)
            if (hasIdColumn) {
                entitiesToInsert.forEachIndexed { idx, entity -> resultIds[entity.resIdIdx] = insertIds[idx] }
            }
        }

        return resultIds
    }

    private fun insertImpl(
        context: DbTableContext,
        entities: List<Map<String, Any?>>,
        nowInstant: Instant
    ): List<Long> {

        if (entities.isEmpty()) {
            return emptyList()
        }
        val tableRef = context.getTableRef()
        val typesConverter = context.getTypesConverter()
        val hasDeletedFlag = context.hasDeleteFlag()

        val entitiesToInsert = entities.map { entity ->

            val attsToInsert = LinkedHashMap(entity)
            if (hasDeletedFlag) {
                attsToInsert[DbEntity.DELETED] = false
            }
            attsToInsert[DbEntity.UPD_VERSION] = 0L

            val currentCreator = entity[DbEntity.CREATOR]
            if (currentCreator == null || currentCreator == "") {
                attsToInsert[DbEntity.CREATOR] = AuthContext.getCurrentUser()
                val currentCreated = attsToInsert[DbEntity.CREATED]
                if (currentCreated == null || currentCreated == Instant.EPOCH) {
                    attsToInsert[DbEntity.CREATED] = nowInstant
                }
            }
            attsToInsert
        }

        val columnNames = linkedSetOf<String>()
        for (entity in entitiesToInsert) {
            columnNames.addAll(entity.keys)
        }
        val columns = context.getColumns().filter { columnNames.contains(it.name) }
        val preparedValues = prepareValuesForDb(columns, typesConverter, entitiesToInsert)

        val query = StringBuilder("INSERT INTO ")
            .append(tableRef.fullName)
            .append(" (")

        for (preparedValue in preparedValues) {
            query.append("\"").append(preparedValue.name).append("\"").append(',')
        }
        query.setLength(query.length - 1)
        query.append(") VALUES ")
        for (rowIdx in entities.indices) {
            query.append("(")
            for (preparedValue in preparedValues) {
                query.append(preparedValue.placeholder).append(',')
            }
            query.setLength(query.length - 1)
            query.append("),")
        }
        query.setLength(query.length - 1)
        if (context.hasIdColumn()) {
            query.append(" RETURNING id")
        }
        query.append(";")

        val values = arrayListOf<Any?>()
        for (entityIdx in entitiesToInsert.indices) {
            for (preparedValue in preparedValues) {
                values.add(preparedValue.values[entityIdx])
            }
        }
        return context.getDataSource().update(query.toString(), values)
    }

    private fun updateImpl(context: DbTableContext, entities: List<EntityToUpdate>) {

        val tableRef = context.getTableRef()
        val dataSource = context.getDataSource()
        val typesConverter = context.getTypesConverter()

        for (entity in entities) {

            val attributes = entity.data
            val version: Long = attributes[DbEntity.UPD_VERSION] as? Long
                ?: error("Missing attribute: ${DbEntity.UPD_VERSION}")

            var newVersion = version + 1
            if (newVersion >= Int.MAX_VALUE) {
                newVersion = 0L
            }

            val attsToUpdate = LinkedHashMap(attributes)
            attsToUpdate[DbEntity.UPD_VERSION] = newVersion

            val columns = context.getColumns().filter { attsToUpdate.containsKey(it.name) }

            val valuesForDb = prepareValuesForDb(columns, typesConverter, listOf(attsToUpdate))
            val setPlaceholders = valuesForDb.joinToString(",") { "\"${it.name}\"=${it.placeholder}" }

            val query = "UPDATE ${tableRef.fullName} SET $setPlaceholders " +
                "WHERE \"${DbEntity.ID}\"='${entity.id}' " +
                "AND \"${DbEntity.UPD_VERSION}\"=$version"

            if (dataSource.update(query, valuesForDb.map { it.values[0] }).first() != 1L) {
                error("Concurrent modification of record with id: ${entity.id}")
            }
        }
    }

    override fun getCount(context: DbTableContext, predicate: Predicate): Long {

        if (context.getColumns().isEmpty()) {
            return 0
        }
        val withAuth = isAuthEnabled(context)
        if (withAuth && !context.getPermsService().isTableExists()) {
            return 0
        }

        val params = mutableListOf<Any?>()
        val sqlCondition = toSqlCondition(context, predicate, params)

        return getCountImpl(context, sqlCondition, params)
    }

    private fun getPermsColumn(context: DbTableContext): String {
        return when (DbDataReqContext.getPermsPolicy(context.getDefaultPermsPolicy())) {
            TypePermsPolicy.INHERITED -> RecordConstants.ATT_PARENT
            TypePermsPolicy.OWN -> DbEntity.REF_ID
            TypePermsPolicy.PUBLIC -> ""
        }
    }

    private fun getCountImpl(context: DbTableContext, sqlCondition: String, params: List<Any?>): Long {

        val selectQuery = createSelectQuery(
            context,
            COUNT_COLUMN,
            permsColumn = getPermsColumn(context),
            withDeleted = false,
            sqlCondition
        )
        if (selectQuery.contains(WHERE_ALWAYS_FALSE)) {
            return 0
        }

        return context.getDataSource().query(selectQuery, params) { resultSet ->
            if (resultSet.next()) {
                resultSet.getLong(1)
            } else {
                0
            }
        }
    }

    override fun find(
        context: DbTableContext,
        predicate: Predicate,
        sort: List<DbFindSort>,
        page: DbFindPage,
        withDeleted: Boolean,
        groupBy: List<String>,
        selectFunctions: List<AggregateFunc>
    ): DbFindRes<Map<String, Any?>> {

        val columns = context.getColumns()
        val typesConverter = context.getTypesConverter()

        if (columns.isEmpty()) {
            return DbFindRes(emptyList(), 0)
        }

        val permsColumn = getPermsColumn(context)

        if (permsColumn.isNotEmpty() && !context.getPermsService().isTableExists()) {
            return DbFindRes(emptyList(), 0)
        }

        val columnsByName = columns.associateBy { it.name }

        val queryGroupBy = ArrayList(groupBy)
        if (selectFunctions.isNotEmpty()) {
            val invalidFunctions = selectFunctions.filter { it.field != "*" && !columnsByName.containsKey(it.field) }
            if (invalidFunctions.isNotEmpty()) {
                error("Function can't be evaluated by non-existing column. Invalid functions: $invalidFunctions")
            }
        }
        if (queryGroupBy.isNotEmpty()) {
            val invalidColumns = queryGroupBy.filter { !columnsByName.containsKey(it) }
            if (invalidColumns.isNotEmpty()) {
                error("Grouping by columns $invalidColumns is not allowed")
            }
        }

        val params = mutableListOf<Any?>()
        val sqlCondition = toSqlCondition(context, predicate, params)
        val query = createSelectQuery(
            context,
            columns,
            permsColumn,
            withDeleted,
            sqlCondition,
            sort,
            page,
            queryGroupBy,
            selectFunctions
        )

        val resultEntities = context.getDataSource().query(query, params) { resultSet ->
            val resultList = mutableListOf<Map<String, Any?>>()
            while (resultSet.next()) {
                resultList.add(
                    convertRowToMap(
                        typesConverter,
                        resultSet,
                        columns,
                        queryGroupBy,
                        selectFunctions
                    )
                )
            }
            resultList
        }

        val totalCount = if (page.maxItems == -1) {
            resultEntities.size.toLong() + page.skipCount
        } else {
            getCountImpl(context, sqlCondition, params)
        }
        return DbFindRes(resultEntities, totalCount)
    }

    private fun createSelectQuery(
        context: DbTableContext,
        selectColumns: List<DbColumnDef>,
        permsColumn: String,
        withDeleted: Boolean = false,
        condition: String,
        sort: List<DbFindSort> = emptyList(),
        page: DbFindPage = DbFindPage.ALL,
        groupBy: List<String> = emptyList(),
        selectFunctions: List<AggregateFunc> = emptyList(),
    ): String {

        val selectColumnsStr = StringBuilder()
        if (groupBy.isEmpty()) {
            selectColumns.forEach {
                appendRecordColumnName(selectColumnsStr, it.name)
                selectColumnsStr.append(",")
            }
        } else {
            groupBy.forEach {
                appendRecordColumnName(selectColumnsStr, it)
                selectColumnsStr.append(",")
            }
        }
        selectFunctions.forEach {
            selectColumnsStr.append(it.func).append("(")
            if (it.field != "*") {
                appendRecordColumnName(selectColumnsStr, it.field)
            } else {
                selectColumnsStr.append("*")
            }
            selectColumnsStr.append(") AS \"${it.alias}\",")
        }
        selectColumnsStr.setLength(selectColumnsStr.length - 1)

        return createSelectQuery(
            context,
            selectColumnsStr.toString(),
            permsColumn,
            withDeleted,
            condition,
            sort,
            page,
            groupBy
        )
    }

    private fun createSelectQuery(
        context: DbTableContext,
        selectColumns: String,
        permsColumn: String,
        withDeleted: Boolean,
        condition: String,
        sort: List<DbFindSort> = emptyList(),
        page: DbFindPage = DbFindPage.ALL,
        groupBy: List<String> = emptyList(),
        innerAuthSelect: Boolean = false
    ): String {

        val delCondition = if (context.hasDeleteFlag() && !withDeleted) {
            "\"${DbEntity.DELETED}\"!=true"
        } else {
            ""
        }

        val query = StringBuilder()
        query.append("SELECT $selectColumns FROM ${context.getTableRef().fullName} \"$RECORD_TABLE_ALIAS\"")

        if (permsColumn.isNotEmpty()) {

            if (innerAuthSelect) {
                joinWithAuthorities(context, query, permsColumn)
            }
            query.append(" WHERE ")
            if (innerAuthSelect) {
                val currentAuthCondition = getCurrentUserAuthoritiesCondition(context)
                if (currentAuthCondition == ALWAYS_FALSE_CONDITION) {
                    query.append(ALWAYS_FALSE_CONDITION)
                } else {
                    query.append(joinConditionsByAnd(delCondition, currentAuthCondition, condition))
                }
                query.append(" GROUP BY ")
                appendRecordColumnName(query, "id")
                query.append(" HAVING bool_and(\"")
                    .append(PERMS_TABLE_ALIAS)
                    .append("\".\"")
                    .append(DbPermsEntity.ALLOWED)
                    .append("\")")
                addSortAndPage(query, sort, page)
            } else {
                val innerQuery = createSelectQuery(
                    context,
                    selectColumns = "\"$RECORD_TABLE_ALIAS\".\"id\"",
                    permsColumn = permsColumn,
                    withDeleted = withDeleted,
                    condition = condition,
                    sort,
                    page,
                    innerAuthSelect = true
                )
                if (hasAlwaysFalseCondition(innerQuery)) {
                    query.append(ALWAYS_FALSE_CONDITION)
                } else {
                    appendRecordColumnName(query, "id")
                    query.append(" IN (")
                        .append(innerQuery)
                        .append(")")
                    addSortAndPage(query, sort, DbFindPage.ALL)
                }
            }
            return query.toString()
        }

        val newCondition = joinConditionsByAnd(delCondition, condition)
        if (newCondition.isNotBlank()) {
            query.append(" WHERE ").append(newCondition)
        }
        addGrouping(query, groupBy)
        addSortAndPage(query, sort, page)

        return query.toString()
    }

    private fun addGrouping(query: StringBuilder, groupBy: List<String>) {
        if (groupBy.isEmpty()) {
            return
        }
        query.append(" GROUP BY ")
        groupBy.forEach {
            appendRecordColumnName(query, it)
            query.append(",")
        }
        query.setLength(query.length - 1)
    }

    private fun addSortAndPage(query: StringBuilder, sorting: List<DbFindSort>, page: DbFindPage) {

        if (sorting.isNotEmpty()) {
            query.append(" ORDER BY ")
            for (sort in sorting) {
                appendRecordColumnName(query, sort.column)
                if (sort.ascending) {
                    query.append(" ASC ")
                } else {
                    query.append(" DESC ")
                }
            }
            query.setLength(query.length - 1)
        }

        if (page.maxItems >= 0) {
            query.append(" LIMIT ").append(page.maxItems)
        }
        if (page.skipCount > 0) {
            query.append(" OFFSET ").append(page.skipCount)
        }
    }

    private fun joinConditionsByAnd(vararg conditions: String): String {
        var condition = ""
        for (cond in conditions) {
            if (cond.isNotBlank()) {
                condition = if (condition.isNotBlank()) {
                    "$condition AND $cond"
                } else {
                    cond
                }
            }
        }
        return condition
    }

    private fun prepareValuesForDb(
        columns: List<DbColumnDef>,
        converter: DbTypesConverter,
        entities: List<Map<String, Any?>>
    ): List<ValueForDb> {
        return columns.map { column ->
            val placeholder = if (column.type == DbColumnType.JSON) {
                "?::jsonb"
            } else {
                "?"
            }
            val values = ArrayList<Any?>(entities.size)
            for (entity in entities) {
                var value = entity[column.name]
                value = if (value == null) {
                    null
                } else {
                    val multiple = column.multiple && column.type != DbColumnType.JSON
                    val targetType = getParamTypeForColumn(column.type, multiple)
                    try {
                        converter.convert(value, targetType)
                    } catch (exception: RuntimeException) {
                        throw RuntimeException(
                            "Column data conversion failed. Column: ${column.name} Target type: $targetType " +
                                "entityId: ${entity[DbEntity.ID]} extId: ${entity[DbEntity.EXT_ID]}",
                            exception
                        )
                    }
                }
                values.add(value)
            }
            ValueForDb(column.name, placeholder, values)
        }
    }

    private fun getParamTypeForColumn(type: DbColumnType, multiple: Boolean): KClass<*> {
        val baseType = when (type) {
            DbColumnType.BIGSERIAL -> Long::class
            DbColumnType.INT -> Int::class
            DbColumnType.DOUBLE -> Double::class
            DbColumnType.BOOLEAN -> Boolean::class
            DbColumnType.DATETIME -> Timestamp::class
            DbColumnType.DATE -> Date::class
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

    private fun joinWithAuthorities(context: DbTableContext, query: StringBuilder, permsColumn: String) {

        val tableRef = context.getTableRef()
        val permsTableName = tableRef.withTable(DbPermsEntity.TABLE).fullName

        query.append(" INNER JOIN $permsTableName \"$PERMS_TABLE_ALIAS\" ")
            .append("ON \"$RECORD_TABLE_ALIAS\".\"$permsColumn\"=")
            .append("\"$PERMS_TABLE_ALIAS\".\"${DbPermsEntity.ENTITY_REF_ID}\"")
    }

    private fun getCurrentUserAuthoritiesCondition(context: DbTableContext): String {

        val userAuthorities = context.getCurrentUserAuthorityIds()

        if (userAuthorities.isEmpty()) {
            return ALWAYS_FALSE_CONDITION
        }
        val query = StringBuilder()
        query.append("\"$PERMS_TABLE_ALIAS\".\"${DbPermsEntity.AUTHORITY_ID}\" IN (")
        for (authority in userAuthorities) {
            query.append(authority).append(",")
        }
        query.setLength(query.length - 1)
        query.append(")")
        return query.toString()
    }

    private fun toSqlCondition(
        context: DbTableContext,
        predicate: Predicate,
        queryParams: MutableList<Any?>
    ): String {
        val sb = StringBuilder()
        toSqlCondition(context, sb, predicate, queryParams)
        return sb.toString()
    }

    private fun toSqlCondition(
        context: DbTableContext,
        query: StringBuilder,
        predicate: Predicate,
        queryParams: MutableList<Any?>
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
                    if (toSqlCondition(context, query, innerPred, queryParams)) {
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

                val columnDef = context.getColumnByName(predicate.getAttribute())
                    ?: error("column is not found: ${predicate.getAttribute()}")

                val type = predicate.getType()
                val attribute: String = predicate.getAttribute()
                val value = predicate.getValue()

                if (columnDef.multiple) {
                    if (type != ValuePredicate.Type.EQ && type != ValuePredicate.Type.CONTAINS) {
                        return false
                    }
                    appendRecordColumnName(query, attribute)
                    query.append(" && ARRAY[")
                    if (!value.isArray()) {
                        query.append("?")
                        queryParams.add(value.asJavaObj())
                    } else {
                        for (elem in value) {
                            query.append("?,")
                            queryParams.add(elem.asJavaObj())
                        }
                        query.setLength(query.length - 1)
                    }
                    query.append("]")
                    return true
                }

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
                        appendRecordColumnName(query, attribute)
                        query.append(" IN (")
                        var first = true
                        for (v in value) {
                            if (!first) {
                                query.append(",")
                            }
                            query.append("?")
                            val convertedValue = when (columnDef.type) {
                                DbColumnType.TEXT -> v.asText()
                                DbColumnType.LONG -> v.asLong()
                                DbColumnType.INT -> v.asInt()
                                DbColumnType.BOOLEAN -> v.asBoolean()
                                else -> v.asJavaObj()
                            }
                            queryParams.add(convertedValue)
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
                        if (type == ValuePredicate.Type.EQ && value.isNull()) {
                            appendRecordColumnName(query, attribute)
                            query.append(" ").append(IS_NULL)
                        } else if (columnDef.type == DbColumnType.TEXT) {
                            if (type == ValuePredicate.Type.LIKE || type == ValuePredicate.Type.CONTAINS) {
                                query.append("LOWER(")
                                appendRecordColumnName(query, attribute)
                                query.append(") ")
                                    .append(operator)
                                    .append(" LOWER(?)")
                            } else {
                                appendRecordColumnName(query, attribute)
                                query.append(' ')
                                    .append(operator)
                                    .append(" ?")
                            }
                            if (type == ValuePredicate.Type.CONTAINS) {
                                queryParams.add("%" + value.asText() + "%")
                            } else {
                                queryParams.add(value.asText())
                            }
                        } else if (columnDef.type == DbColumnType.BOOLEAN) {
                            appendRecordColumnName(query, attribute)
                            if (value.asBoolean()) {
                                query.append(IS_TRUE)
                            } else {
                                query.append(IS_FALSE)
                            }
                        } else {

                            appendRecordColumnName(query, attribute)
                            query.append(' ')
                                .append(operator)
                                .append(" ?")

                            if (value.isTextual() && columnDef.type == DbColumnType.UUID) {
                                queryParams.add(UUID.fromString(value.asText()))
                            } else if (columnDef.type == DbColumnType.DATETIME || columnDef.type == DbColumnType.DATE) {
                                val offsetDateTime: OffsetDateTime = if (value.isTextual()) {
                                    val txt = value.asText()
                                    OffsetDateTime.parse(
                                        if (!txt.contains('T')) {
                                            "${txt}T00:00:00Z"
                                        } else {
                                            txt
                                        }
                                    )
                                } else if (value.isNumber()) {
                                    OffsetDateTime.ofInstant(Instant.ofEpochMilli(value.asLong()), ZoneOffset.UTC)
                                } else {
                                    error("Unknown datetime value: '$value'")
                                }
                                if (columnDef.type == DbColumnType.DATE) {
                                    queryParams.add(offsetDateTime.toLocalDate())
                                } else {
                                    queryParams.add(offsetDateTime)
                                }
                            } else {
                                queryParams.add(value.asJavaObj())
                            }
                        }
                    }
                }
                return true
            }

            is NotPredicate -> {
                query.append("NOT ")
                return if (toSqlCondition(context, query, predicate.getPredicate(), queryParams)) {
                    true
                } else {
                    query.setLength(query.length - 4)
                    false
                }
            }

            is EmptyPredicate -> {

                val columnDef = context.getColumnByName(predicate.getAttribute()) ?: return false
                val attribute: String = predicate.getAttribute()
                if (columnDef.multiple) {
                    query.append("array_length(")
                    appendRecordColumnName(query, attribute)
                    query.append(",1) ").append(IS_NULL)
                } else if (columnDef.type == DbColumnType.TEXT) {
                    query.append("(")
                    appendRecordColumnName(query, attribute)
                    query.append(" ").append(IS_NULL).append(" OR ")
                    appendRecordColumnName(query, attribute)
                    query.append("='')")
                } else {
                    appendRecordColumnName(query, attribute)
                    query.append(" ").append(IS_NULL)
                }
                return true
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

    private fun appendRecordColumnName(query: StringBuilder, name: String) {
        query.append("\"$RECORD_TABLE_ALIAS\".\"")
            .append(name)
            .append("\"")
    }

    private data class ValueForDb(
        val name: String,
        val placeholder: String,
        val values: List<Any?>
    )

    private data class EntityToUpdate(
        val id: Long,
        val data: Map<String, Any?>
    )

    private data class EntityToInsert(
        val resIdIdx: Int,
        val data: Map<String, Any?>
    )
}
