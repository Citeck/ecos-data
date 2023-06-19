package ru.citeck.ecos.data.sql.pg

import mu.KotlinLogging
import ru.citeck.ecos.context.lib.auth.AuthContext
import ru.citeck.ecos.data.sql.context.DbTableContext
import ru.citeck.ecos.data.sql.dto.DbColumnDef
import ru.citeck.ecos.data.sql.dto.DbColumnType
import ru.citeck.ecos.data.sql.perms.DbPermsEntity
import ru.citeck.ecos.data.sql.records.assocs.DbAssocEntity
import ru.citeck.ecos.data.sql.records.utils.DbAttValueUtils
import ru.citeck.ecos.data.sql.repo.DbEntityRepo
import ru.citeck.ecos.data.sql.repo.entity.DbEntity
import ru.citeck.ecos.data.sql.repo.find.DbFindPage
import ru.citeck.ecos.data.sql.repo.find.DbFindRes
import ru.citeck.ecos.data.sql.repo.find.DbFindSort
import ru.citeck.ecos.data.sql.service.aggregation.AggregateFunc
import ru.citeck.ecos.data.sql.service.assocs.AssocJoin
import ru.citeck.ecos.data.sql.service.assocs.AssocTableJoin
import ru.citeck.ecos.data.sql.type.DbTypeUtils
import ru.citeck.ecos.data.sql.type.DbTypesConverter
import ru.citeck.ecos.model.lib.type.dto.QueryPermsPolicy
import ru.citeck.ecos.records2.RecordConstants
import ru.citeck.ecos.records2.predicate.model.*
import java.sql.Date
import java.sql.ResultSet
import java.sql.Timestamp
import java.time.*
import java.util.*
import kotlin.collections.ArrayList
import kotlin.collections.LinkedHashMap
import kotlin.reflect.KClass

open class DbEntityRepoPg internal constructor() : DbEntityRepo {

    companion object {
        private val log = KotlinLogging.logger {}

        private const val ALWAYS_FALSE_CONDITION = "0=1"
        private const val WHERE_ALWAYS_FALSE = "WHERE $ALWAYS_FALSE_CONDITION"

        private const val RECORD_TABLE_ALIAS = "r"
        private const val PERMS_TABLE_ALIAS = "p"
        private const val ASSOCS_TABLE_ALIAS_PREFIX = "a"

        private const val IS_TRUE = "IS TRUE"
        private const val IS_FALSE = "IS FALSE"
        private const val IS_NULL = "IS NULL"

        private const val COUNT_COLUMN = "COUNT(*)"

        private const val SEARCH_DISABLED_COLUMN = "__search__disabled__"
    }

    private val disableQueryPermsCheck = ThreadLocal.withInitial { false }

    private inline fun <T> doWithoutQueryPermsCheck(action: () -> T): T {
        if (disableQueryPermsCheck.get()) {
            return action.invoke()
        }
        disableQueryPermsCheck.set(true)
        try {
            return action.invoke()
        } finally {
            disableQueryPermsCheck.set(false)
        }
    }

    private fun findByColumn(
        context: DbTableContext,
        column: String,
        values: Collection<Any>,
        withDeleted: Boolean,
        limit: Int
    ): List<Map<String, Any?>> {
        return find(
            context,
            ValuePredicate(column, ValuePredicate.Type.IN, values),
            emptyList(),
            DbFindPage(0, limit),
            withDeleted,
            emptyList(),
            emptyList(),
            emptyList(),
            emptyList(),
            false
        ).entities
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

    override fun delete(context: DbTableContext, predicate: Predicate) {

        if (!context.hasDeleteFlag()) {
            forceDelete(context, predicate)
        }

        val query = StringBuilder("UPDATE ")
            .append(context.getTableRef().fullName)
            .append(" \"$RECORD_TABLE_ALIAS\" SET \"${DbEntity.DELETED}\"=true WHERE ")

        val parameters = arrayListOf<Any?>()
        toSqlCondition(context, query, RECORD_TABLE_ALIAS, predicate, emptyMap(), emptyMap(), parameters)

        context.getDataSource().update(query.toString(), parameters)
    }

    override fun forceDelete(context: DbTableContext, predicate: Predicate) {

        val query = StringBuilder("DELETE FROM ")
            .append(context.getTableRef().fullName)
            .append(" \"$RECORD_TABLE_ALIAS\" WHERE ")

        val parameters = arrayListOf<Any?>()
        toSqlCondition(context, query, RECORD_TABLE_ALIAS, predicate, emptyMap(), emptyMap(), parameters)

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
        return doWithoutQueryPermsCheck {
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
        return getPermsColumn(context).isNotEmpty()
    }

    private fun saveImpl(context: DbTableContext, entities: List<Map<String, Any?>>): LongArray {

        val tableRef = context.getTableRef()

        if (isAuthEnabled(context) && AuthContext.getCurrentUser().isEmpty()) {
            error("Current user is empty. Table: $tableRef")
        }
        val hasIdColumn = context.hasIdColumn()

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
            val insertIds = insertImpl(context, entitiesToInsert.map { it.data })
            if (hasIdColumn) {
                entitiesToInsert.forEachIndexed { idx, entity -> resultIds[entity.resIdIdx] = insertIds[idx] }
            }
        }

        return resultIds
    }

    private fun insertImpl(
        context: DbTableContext,
        entities: List<Map<String, Any?>>
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

    override fun getCount(
        context: DbTableContext,
        predicate: Predicate,
        groupBy: List<String>,
        assocJoins: List<AssocJoin>,
        assocTableJoins: List<AssocTableJoin>,
    ): Long {

        if (context.getColumns().isEmpty()) {
            return 0
        }
        val permsColumn = getPermsColumn(context)
        if (permsColumn.isNotEmpty() && (
            !context.getPermsService()
                .isTableExists() || !context.hasColumn(permsColumn)
            )
        ) {
            return 0
        }

        val repoAssocJoins = assocJoins.associateBy { it.attribute }
        val repoAssocTableJoins = assocTableJoins.associateBy { it.attribute }

        val params = mutableListOf<Any?>()
        val sqlCondition = toSqlCondition(
            context,
            predicate,
            RECORD_TABLE_ALIAS,
            repoAssocJoins,
            repoAssocTableJoins,
            params
        )

        return getCountImpl(context, RECORD_TABLE_ALIAS, sqlCondition, params, permsColumn, groupBy)
    }

    private fun getPermsColumn(context: DbTableContext): String {
        if (disableQueryPermsCheck.get()) {
            return ""
        }
        return when (val policy = context.getQueryPermsPolicy()) {
            QueryPermsPolicy.PARENT -> RecordConstants.ATT_PARENT
            QueryPermsPolicy.OWN -> DbEntity.REF_ID
            QueryPermsPolicy.PUBLIC -> ""
            QueryPermsPolicy.NONE -> SEARCH_DISABLED_COLUMN
            else -> error("Invalid perms policy: $policy")
        }
    }

    private fun getCountImpl(
        context: DbTableContext,
        table: String,
        sqlCondition: String,
        params: List<Any?>,
        permsColumn: String,
        groupBy: List<String>
    ): Long {

        val selectQuery = createSelectQuery(
            context,
            table,
            COUNT_COLUMN,
            permsColumn,
            withDeleted = false,
            sqlCondition,
            emptyList(),
            DbFindPage.ALL,
            groupBy
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
        selectFunctions: List<AggregateFunc>,
        assocJoins: List<AssocJoin>,
        assocTableJoins: List<AssocTableJoin>,
        withTotalCount: Boolean
    ): DbFindRes<Map<String, Any?>> {

        val columns = context.getColumns()
        val typesConverter = context.getTypesConverter()

        if (columns.isEmpty()) {
            return DbFindRes(emptyList(), 0)
        }

        val permsColumn = getPermsColumn(context)

        if (permsColumn.isNotEmpty() && (
            !context.getPermsService()
                .isTableExists() || !context.hasColumn(permsColumn)
            )
        ) {
            return DbFindRes(emptyList(), 0)
        }

        val columnsByName = columns.associateBy { it.name }

        if (selectFunctions.isNotEmpty()) {
            val invalidFunctions = selectFunctions.filter { it.field != "*" && !columnsByName.containsKey(it.field) }
            if (invalidFunctions.isNotEmpty()) {
                error("Function can't be evaluated by non-existing column. Invalid functions: $invalidFunctions")
            }
        }
        val queryGroupBy = ArrayList(groupBy)
        if (queryGroupBy.isNotEmpty()) {
            val invalidColumns = queryGroupBy.filter { !columnsByName.containsKey(it) }
            if (invalidColumns.isNotEmpty()) {
                error("Grouping by columns $invalidColumns is not allowed")
            }
        }

        val repoAssocJoins = assocJoins.associateBy { it.attribute }
        val repoAssocTableJoins = assocTableJoins.associateBy { it.attribute }

        val params = mutableListOf<Any?>()
        val sqlCondition =
            toSqlCondition(context, predicate, RECORD_TABLE_ALIAS, repoAssocJoins, repoAssocTableJoins, params)
        val query = createSelectQuery(
            context,
            RECORD_TABLE_ALIAS,
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

        val totalCount = if (!withTotalCount || page.maxItems == -1 || page.maxItems > resultEntities.size) {
            resultEntities.size.toLong() + page.skipCount
        } else {
            getCountImpl(context, RECORD_TABLE_ALIAS, sqlCondition, params, permsColumn, groupBy)
        }
        return DbFindRes(resultEntities, totalCount)
    }

    private fun createSelectQuery(
        context: DbTableContext,
        table: String,
        selectColumns: List<DbColumnDef>,
        permsColumn: String,
        withDeleted: Boolean = false,
        condition: String,
        sort: List<DbFindSort> = emptyList(),
        page: DbFindPage = DbFindPage.ALL,
        groupBy: List<String> = emptyList(),
        selectFunctions: List<AggregateFunc> = emptyList()
    ): String {

        val selectColumnsStr = StringBuilder()
        if (groupBy.isEmpty()) {
            selectColumns.forEach {
                appendRecordColumnName(selectColumnsStr, table, it.name)
                selectColumnsStr.append(",")
            }
        } else {
            groupBy.forEach {
                appendRecordColumnName(selectColumnsStr, table, it)
                selectColumnsStr.append(",")
            }
        }
        selectFunctions.forEach {
            selectColumnsStr.append(it.func).append("(")
            if (it.field != "*") {
                appendRecordColumnName(selectColumnsStr, table, it.field)
            } else {
                selectColumnsStr.append("*")
            }
            selectColumnsStr.append(") AS \"${it.alias}\",")
        }
        selectColumnsStr.setLength(selectColumnsStr.length - 1)

        return createSelectQuery(
            context,
            table,
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
        table: String,
        selectColumns: String,
        permsColumn: String,
        withDeleted: Boolean,
        condition: String,
        sort: List<DbFindSort> = emptyList(),
        page: DbFindPage = DbFindPage.ALL,
        groupBy: List<String> = emptyList()
    ): String {

        val delCondition = if (context.hasDeleteFlag() && !withDeleted) {
            "\"$table\".\"${DbEntity.DELETED}\"!=true"
        } else {
            ""
        }

        val permsCondition = getPermsCondition(context, permsColumn)
        val fullCondition = joinConditionsByAnd(delCondition, condition, permsCondition)

        val query = StringBuilder()
        query.append("SELECT ")
            .append(selectColumns)
            .append(" FROM ${context.getTableRef().fullName} \"$table\"")

        if (fullCondition.isNotBlank()) {
            query.append(" WHERE ").append(fullCondition)
        }

        addGrouping(query, table, groupBy)
        addSortAndPage(query, table, sort, page)

        return query.toString()
    }

    private fun addAssocTableCondition(
        query: StringBuilder,
        table: String,
        tableJoin: AssocTableJoin,
        queryParams: MutableList<Any?>
    ) {
        val targetTableName = "$table$RECORD_TABLE_ALIAS"
        val targetTable = tableJoin.tableContext.getTableRef()
        query.append("EXISTS(SELECT 1 FROM ${targetTable.fullName} $targetTableName WHERE ")
            .append("\"$table\".\"${tableJoin.srcColumn}\"=\"${targetTableName}\".\"${DbEntity.REF_ID}\" AND ")

        val assocJoins = tableJoin.assocJoins.associateBy { it.attribute }
        val assocTableJoins = tableJoin.assocTableJoins.associateBy { it.attribute }

        toSqlCondition(
            tableJoin.tableContext,
            query,
            targetTableName,
            tableJoin.predicate,
            assocJoins,
            assocTableJoins,
            queryParams
        )
        query.append(")")
    }

    private fun addAssocCondition(
        context: DbTableContext,
        query: StringBuilder,
        table: String,
        attId: Long,
        values: Collection<Long>
    ) {
        if (values.isEmpty()) {
            query.append(ALWAYS_FALSE_CONDITION)
            return
        }
        val tableRef = context.getTableRef()
        val assocsTableName = tableRef.withTable(DbAssocEntity.TABLE).fullName
        query.append(
            "EXISTS(SELECT 1 FROM $assocsTableName a WHERE " +
                "a.${DbAssocEntity.SOURCE_ID}=\"$table\".${DbEntity.REF_ID} " +
                "AND a.${DbAssocEntity.ATTRIBUTE}=$attId AND a.${DbAssocEntity.TARGET_ID} IN ("
        )
        values.forEach {
            query.append(it).append(",")
        }
        query.setLength(query.length - 1)
        query.append("))")
    }

    private fun addGrouping(query: StringBuilder, table: String, groupBy: List<String>) {
        if (groupBy.isEmpty()) {
            return
        }
        query.append(" GROUP BY ")
        groupBy.forEach {
            appendRecordColumnName(query, table, it)
            query.append(",")
        }
        query.setLength(query.length - 1)
    }

    private fun addSortAndPage(query: StringBuilder, table: String, sorting: List<DbFindSort>, page: DbFindPage) {

        if (sorting.isNotEmpty()) {
            query.append(" ORDER BY ")
            for (sort in sorting) {
                val columnName = if (sort.column == DbEntity.CREATED) {
                    // this replacement improve query performance
                    // works only if ID counter in postgres without cache
                    DbEntity.ID
                } else {
                    sort.column
                }
                appendRecordColumnName(query, table, columnName)
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
                    if (condition == ALWAYS_FALSE_CONDITION) {
                        return ALWAYS_FALSE_CONDITION
                    }
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

    private fun getPermsCondition(context: DbTableContext, permsColumn: String): String {

        if (permsColumn.isBlank()) {
            return ""
        }

        val authorities = context.getCurrentUserAuthorityIds()
        if (authorities.isEmpty()) {
            return ALWAYS_FALSE_CONDITION
        }

        val tableRef = context.getTableRef()
        val permsTableName = tableRef.withTable(DbPermsEntity.TABLE).fullName

        val permsAlias = "\"$PERMS_TABLE_ALIAS\""
        val condition = StringBuilder()
        condition.append("EXISTS(SELECT 1 FROM ")
            .append(permsTableName).append(" ").append(permsAlias)
            .append(" WHERE $permsAlias.\"${DbPermsEntity.ENTITY_REF_ID}\"=\"${RECORD_TABLE_ALIAS}\".$permsColumn AND")
            .append(" $permsAlias.\"${DbPermsEntity.AUTHORITY_ID}\" IN (")

        condition.append("")
        authorities.forEach {
            condition.append(it).append(",")
        }
        condition.setLength(condition.length - 1)
        condition.append("))")

        return condition.toString()
    }

    private fun toSqlCondition(
        context: DbTableContext,
        predicate: Predicate,
        table: String,
        assocJoins: Map<String, AssocJoin>,
        assocTableJoins: Map<String, AssocTableJoin>,
        queryParams: MutableList<Any?>
    ): String {
        val sb = StringBuilder()
        toSqlCondition(context, sb, table, predicate, assocJoins, assocTableJoins, queryParams)
        return sb.toString()
    }

    private fun toSqlCondition(
        context: DbTableContext,
        query: StringBuilder,
        table: String,
        predicate: Predicate,
        assocJoins: Map<String, AssocJoin>,
        assocTableJoins: Map<String, AssocTableJoin>,
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
                    if (toSqlCondition(context, query, table, innerPred, assocJoins, assocTableJoins, queryParams)) {
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

                var columnDef = context.getColumnByName(predicate.getAttribute())
                if (columnDef == null) {
                    val assocJoin = assocJoins[predicate.getAttribute()]
                    if (assocJoin != null) {
                        columnDef = context.getColumnByName(assocJoin.srcColumn)
                    } else {
                        val assocTableJoin = assocTableJoins[predicate.getAttribute()]
                        if (assocTableJoin != null) {
                            columnDef = context.getColumnByName(assocTableJoin.srcColumn)
                        }
                    }
                }
                columnDef ?: error("column is not found: ${predicate.getAttribute()}")

                val type = predicate.getType()
                val attribute: String = predicate.getAttribute()
                val value = predicate.getValue()

                if (columnDef.type == DbColumnType.LONG) {
                    val assocJoin = assocJoins[attribute]
                    if (assocJoin != null) {
                        if (type != ValuePredicate.Type.EQ &&
                            type != ValuePredicate.Type.CONTAINS &&
                            type != ValuePredicate.Type.IN
                        ) {
                            return false
                        }
                        val longs = DbAttValueUtils.anyToSetOfLongs(value)
                        addAssocCondition(context, query, table, assocJoin.attId, longs)
                        return true
                    }
                    val assocTableJoin = assocTableJoins[attribute]
                    if (assocTableJoin != null) {
                        if (type != ValuePredicate.Type.EQ &&
                            type != ValuePredicate.Type.CONTAINS &&
                            type != ValuePredicate.Type.IN
                        ) {
                            return false
                        }
                        addAssocTableCondition(query, table, assocTableJoin, queryParams)
                        return true
                    }
                }
                if (columnDef.multiple) {

                    if (type != ValuePredicate.Type.EQ &&
                        type != ValuePredicate.Type.CONTAINS &&
                        type != ValuePredicate.Type.IN
                    ) {
                        return false
                    }
                    appendRecordColumnName(query, table, attribute)
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
                        appendRecordColumnName(query, table, attribute)
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
                            appendRecordColumnName(query, table, attribute)
                            query.append(" ").append(IS_NULL)
                        } else if (columnDef.type == DbColumnType.TEXT) {
                            var queryParam = value.asText()
                            if (type == ValuePredicate.Type.LIKE || type == ValuePredicate.Type.CONTAINS) {
                                query.append("LOWER(")
                                appendRecordColumnName(query, table, attribute)
                                query.append(") ")
                                    .append(operator)
                                    .append(" ?")
                                queryParam = queryParam.lowercase()
                                if (type == ValuePredicate.Type.CONTAINS) {
                                    queryParam = "%$queryParam%"
                                }
                            } else {
                                appendRecordColumnName(query, table, attribute)
                                query.append(' ')
                                    .append(operator)
                                    .append(" ?")
                            }
                            queryParams.add(queryParam)
                        } else if (columnDef.type == DbColumnType.BOOLEAN) {
                            appendRecordColumnName(query, table, attribute)
                            if (value.asBoolean()) {
                                query.append(IS_TRUE)
                            } else {
                                query.append(IS_FALSE)
                            }
                        } else {

                            appendRecordColumnName(query, table, attribute)
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
                return if (toSqlCondition(context, query, table, predicate.getPredicate(), assocJoins, assocTableJoins, queryParams)) {
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
                    appendRecordColumnName(query, table, attribute)
                    query.append(",1) ").append(IS_NULL)
                } else if (columnDef.type == DbColumnType.TEXT) {
                    query.append("(")
                    appendRecordColumnName(query, table, attribute)
                    query.append(" ").append(IS_NULL).append(" OR ")
                    appendRecordColumnName(query, table, attribute)
                    query.append("='')")
                } else {
                    appendRecordColumnName(query, table, attribute)
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

    private fun appendRecordColumnName(query: StringBuilder, table: String, name: String) {
        query.append("\"$table\".\"")
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
