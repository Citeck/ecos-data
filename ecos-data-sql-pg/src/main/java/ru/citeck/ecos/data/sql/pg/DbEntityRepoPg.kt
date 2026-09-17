package ru.citeck.ecos.data.sql.pg

import io.github.oshai.kotlinlogging.KotlinLogging
import ru.citeck.ecos.context.lib.auth.AuthContext
import ru.citeck.ecos.data.sql.context.DbTableContext
import ru.citeck.ecos.data.sql.dto.DbColumnDef
import ru.citeck.ecos.data.sql.dto.DbColumnType
import ru.citeck.ecos.data.sql.perms.DbPermsEntity
import ru.citeck.ecos.data.sql.records.DbRecordsUtils
import ru.citeck.ecos.data.sql.records.assocs.DbAssocEntity
import ru.citeck.ecos.data.sql.records.dao.atts.DbExpressionAttsContext
import ru.citeck.ecos.data.sql.records.utils.DbAttValueUtils
import ru.citeck.ecos.data.sql.records.utils.DbReadToleranceLog
import ru.citeck.ecos.data.sql.repo.DbConditionalUpdate
import ru.citeck.ecos.data.sql.repo.DbEntityRepo
import ru.citeck.ecos.data.sql.repo.entity.DbEntity
import ru.citeck.ecos.data.sql.repo.find.DbFindPage
import ru.citeck.ecos.data.sql.repo.find.DbFindQuery
import ru.citeck.ecos.data.sql.repo.find.DbFindRes
import ru.citeck.ecos.data.sql.repo.find.DbFindSort
import ru.citeck.ecos.data.sql.service.RawTableJoin
import ru.citeck.ecos.data.sql.service.assocs.AssocJoinWithPredicate
import ru.citeck.ecos.data.sql.service.assocs.AssocTableJoin
import ru.citeck.ecos.data.sql.service.expression.token.*
import ru.citeck.ecos.data.sql.type.DbTypeUtils
import ru.citeck.ecos.data.sql.type.DbTypesConverter
import ru.citeck.ecos.model.lib.type.dto.QueryPermsPolicy
import ru.citeck.ecos.records2.RecordConstants
import ru.citeck.ecos.records2.predicate.PredicateUtils
import ru.citeck.ecos.records2.predicate.model.*
import java.sql.ResultSet
import java.sql.Timestamp
import java.time.Instant
import java.time.LocalDate
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.util.*
import java.util.concurrent.atomic.AtomicInteger
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

        private const val SEARCH_DISABLED_COLUMN = "__search__disabled__"

        private const val VIRTUAL_COLUMN_PREFIX = "v__"

        // PostgreSQL type names as ResultSetMetaData.getColumnTypeName reports them ('_' prefixes
        // the array form) - see getProlepticDateColumns for why these four, and only these four
        private const val PG_TYPE_DATE = "date"
        private const val PG_TYPE_TIMESTAMPTZ = "timestamptz"
        private const val PG_TYPE_DATE_ARRAY = "_date"
        private const val PG_TYPE_TIMESTAMPTZ_ARRAY = "_timestamptz"

        private val PROLEPTIC_DATE_PG_TYPES = setOf(
            PG_TYPE_DATE,
            PG_TYPE_TIMESTAMPTZ,
            PG_TYPE_DATE_ARRAY,
            PG_TYPE_TIMESTAMPTZ_ARRAY
        )

        // 'IS DISTINCT FROM' works as '<>' (not-eq) except it include records with null values
        private val COLUMN_TYPES_FOR_IS_DISTINCT_FROM_OPERATOR = setOf(
            DbColumnType.TEXT,
            DbColumnType.INT,
            DbColumnType.DOUBLE,
            DbColumnType.LONG,
            DbColumnType.DATE
        )
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
        limit: Int
    ): List<Map<String, Any?>> {
        return find(
            context,
            DbFindQuery.create {
                withPredicate(ValuePredicate(column, ValuePredicate.Type.IN, values))
            },
            DbFindPage(0, limit),
            false
        ).entities
    }

    /**
     * The columns of [resultSet] whose value PostgreSQL keeps in the proleptic Gregorian calendar
     * and JDBC's legacy types cannot carry across, keyed by column label.
     *
     * [java.sql.Date] and [java.sql.Timestamp] hold their value as epoch millis and read their
     * fields back through [java.util.Date]'s hybrid calendar - Julian before the 1582-10-15 cutover,
     * Gregorian after it - while PostgreSQL and [java.time] are proleptic Gregorian throughout.
     * Every value older than the cutover is therefore shifted by the Julian/Gregorian difference of
     * its own epoch on one of the two crossings: two days at year 1, ten days in the 1500s,
     * thirty-eight days at 4713 BC. Which crossing is the broken one depends on the driver, so
     * neither legacy type can be trusted and every date column - scalar or array - has to be read as
     * a [java.time] value.
     *
     * Only what the result set really is counts here, not what the column metadata says it should
     * be: a `timestamp` without a time zone is deliberately left off this list, because a bare
     * `timestamp` is written and read in the JVM's own zone and reading it as an [OffsetDateTime]
     * would move it by that zone's offset. The first column of a label wins, matching which one
     * `getObject(label)` would have returned.
     */
    private fun getProlepticDateColumns(resultSet: ResultSet): Map<String, String> {
        val metaData = resultSet.metaData
        val result = LinkedHashMap<String, String>()
        for (idx in 1..metaData.columnCount) {
            val typeName = metaData.getColumnTypeName(idx)
            if (PROLEPTIC_DATE_PG_TYPES.contains(typeName)) {
                result.putIfAbsent(metaData.getColumnLabel(idx), typeName)
            }
        }
        return result
    }

    /**
     * Reads one column, taking a date out of the result set as a [java.time] value whenever
     * [prolepticDateColumns] says the legacy JDBC type would shift it - see
     * [getProlepticDateColumns].
     *
     * The value handed back keeps the type the rest of the read path already expects for that
     * column, so only the crossing changes and nothing downstream has to know: a `timestamptz`
     * still arrives as a [Timestamp], built from an instant rather than from calendar fields.
     */
    private fun readColumnValue(row: ResultSet, name: String, prolepticDateColumns: Map<String, String>): Any? {
        return when (prolepticDateColumns[name]) {
            PG_TYPE_DATE -> row.getObject(name, LocalDate::class.java)
            PG_TYPE_TIMESTAMPTZ -> row.getObject(name, OffsetDateTime::class.java)?.let {
                Timestamp.from(it.toInstant())
            }
            PG_TYPE_DATE_ARRAY -> readArray(row, name, LocalDate::class.java, LocalDate::class.java) { it }
            PG_TYPE_TIMESTAMPTZ_ARRAY -> readArray(
                row,
                name,
                OffsetDateTime::class.java,
                Timestamp::class.java
            ) { Timestamp.from(it.toInstant()) }
            else -> row.getObject(name)
        }
    }

    /**
     * Reads an array column element by element through the array's own result set, which is the only
     * way to ask the driver for a [java.time] value: `java.sql.Array.getArray()` always answers with
     * the legacy [java.sql.Date] / [Timestamp] elements that carry the shift.
     *
     * A null array stays null and a null element stays a null element, exactly as the untyped read
     * left them. [elementType] is the type the elements are converted to afterwards, so the array
     * handed back is the one the rest of the read path already expects for that column.
     */
    private fun <T : Any, R : Any> readArray(
        row: ResultSet,
        name: String,
        readAs: Class<T>,
        elementType: Class<R>,
        convert: (T) -> R
    ): Any? {
        val array = row.getArray(name) ?: return null
        val values = ArrayList<R?>()
        array.resultSet.use { elements ->
            while (elements.next()) {
                values.add(elements.getObject(2, readAs)?.let(convert))
            }
        }
        val result = java.lang.reflect.Array.newInstance(elementType, values.size)
        for (idx in values.indices) {
            java.lang.reflect.Array.set(result, idx, values[idx])
        }
        return result
    }

    private fun convertRowToMap(
        typesConverter: DbTypesConverter,
        row: ResultSet,
        columns: List<DbColumnDef>,
        prolepticDateColumns: Map<String, String>,
        groupBy: List<String> = emptyList(),
        selectExpressions: Set<String>,
        asjAliases: Map<String, String>
    ): Map<String, Any?> {
        val result = LinkedHashMap<String, Any?>()
        if (groupBy.isEmpty()) {
            columns.forEach { column ->
                val value = readColumnValue(row, column.name, prolepticDateColumns)
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
            selectExpressions.forEach {
                result[it] = row.getObject(it)
            }
        } else {
            val columnByName = columns.associateBy { it.name }
            groupBy.forEach {
                if (it != "*" && !selectExpressions.contains(it)) {
                    val alias = asjAliases[it]
                    if (alias != null) {
                        result[it] = row.getObject(alias)
                    } else {
                        val value = readColumnValue(row, it, prolepticDateColumns)
                        result[it] = if (value != null) {
                            val column = columnByName[it]!!
                            typesConverter.convert(value, column.type.type)
                        } else {
                            null
                        }
                    }
                }
            }
            selectExpressions.forEach {
                result[it] = row.getObject(it)
            }
        }
        return result
    }

    override fun delete(context: DbTableContext, entity: Map<String, Any?>) {
        delete(context, listOf(entity[DbEntity.ID] as Long))
    }

    override fun delete(context: DbTableContext, predicate: Predicate) {

        // Normalised first: PredicateUtils.isAlwaysTrue only recognises a bare VoidPredicate, so
        // and(alwaysTrue()), not(alwaysFalse()) and an empty AndPredicate - all of which delete
        // every row of the table just as surely - would otherwise walk straight past the guard.
        // optimize() folds exactly those shapes down to the VoidPredicate the check does recognise.
        require(!PredicateUtils.isAlwaysTrue(PredicateUtils.optimize(predicate))) {
            "Deleting all rows of table '${context.getTableRef()}' by an always-true predicate is not supported. " +
                "If you really mean to remove specific rows, use delete(entityId) / delete(entities: List<Long>) instead."
        }

        val query = StringBuilder("DELETE FROM ")
            .append(context.getTableRef().fullName)
            .append(" \"$RECORD_TABLE_ALIAS\" WHERE ")

        val parameters = arrayListOf<Any?>()
        toSqlCondition(
            context,
            query,
            RECORD_TABLE_ALIAS,
            predicate,
            emptyMap(),
            emptyMap(),
            emptyMap(),
            emptyMap(),
            emptyMap(),
            parameters
        )

        context.getDataSource().update(query.toString(), parameters)
    }

    override fun delete(context: DbTableContext, entities: List<Long>) {
        context.getDataSource().update(
            "DELETE FROM ${context.getTableRef().fullName} WHERE \"${DbEntity.ID}\" = ANY(?)",
            listOf(entities.toTypedArray())
        )
    }

    override fun insertIfNoConflictByExtId(context: DbTableContext, entity: Map<String, Any?>): Long? {

        checkAuth(context)

        val columns = context.getColumns().filter { entity.containsKey(it.name) }
        val typesConverter = context.getTypesConverter()

        val preparedValues = prepareValuesForDb(columns, typesConverter, listOf(entity))

        if (preparedValues.isEmpty()) {
            error(
                "Can't insert entity into ${context.getTableRef().fullName}: " +
                    "no columns matching entity keys. " +
                    "Table is not initialized or required columns are missing. " +
                    "Entity keys: ${entity.keys}, table columns: ${context.getColumns().map { it.name }}"
            )
        }

        val query = StringBuilder("INSERT INTO ")
            .append(context.getTableRef().fullName)
            .append(" (")

        for (preparedValue in preparedValues) {
            query.append("\"").append(preparedValue.name).append("\"").append(',')
        }
        query.setLength(query.length - 1)
        query.append(") VALUES (")
        for (preparedValue in preparedValues) {
            query.append(preparedValue.placeholder).append(',')
        }
        query.setLength(query.length - 1)
        query.append(")")

        if (context.hasIdColumn()) {
            query.append(" ON CONFLICT (${DbEntity.EXT_ID}) DO NOTHING RETURNING id")
        }
        query.append(";")

        val values = arrayListOf<Any?>()
        for (preparedValue in preparedValues) {
            values.add(preparedValue.values[0])
        }

        val ids = context.getDataSource().update(query.toString(), values)
        return ids.firstOrNull()
    }

    override fun updateByExtIdIfMatches(
        context: DbTableContext,
        extId: String,
        expected: Map<String, Any?>,
        newValues: Map<String, Any?>
    ): Boolean {
        return conditionalUpdate(context, DbEntity.EXT_ID, extId, expected, newValues)
    }

    override fun updateByIdIfMatches(
        context: DbTableContext,
        id: Long,
        expected: Map<String, Any?>,
        newValues: Map<String, Any?>
    ): Boolean {
        return conditionalUpdate(context, DbEntity.ID, id, expected, newValues)
    }

    /**
     * Shared implementation of [updateByExtIdIfMatches] and [updateByIdIfMatches]: the two differ
     * only in which column identifies the row, so [idColumn]/[idValue] are the only thing that
     * changes between them.
     */
    private fun conditionalUpdate(
        context: DbTableContext,
        idColumn: String,
        idValue: Any,
        expected: Map<String, Any?>,
        newValues: Map<String, Any?>
    ): Boolean {

        checkAuth(context)

        if (newValues.isEmpty()) {
            error("New values are empty. Table: ${context.getTableRef().fullName}")
        }
        // an empty condition would turn a compare-and-set into an unconditional overwrite by row id
        if (expected.isEmpty()) {
            error("Expected values are empty. Table: ${context.getTableRef().fullName}")
        }
        val setColumns = DbConditionalUpdate.getColumns(context, newValues, "new values")
        val expectedColumns = DbConditionalUpdate.getColumns(context, expected, "expected values")

        val typesConverter = context.getTypesConverter()
        val setValues = prepareValuesForDb(setColumns, typesConverter, listOf(newValues))
        val expectedValues = prepareValuesForDb(expectedColumns, typesConverter, listOf(expected))

        val params = arrayListOf<Any?>()
        val query = StringBuilder("UPDATE ")
            .append(context.getTableRef().fullName)
            .append(" SET ")

        for (value in setValues) {
            query.append("\"").append(value.name).append("\"=").append(value.placeholder).append(',')
            params.add(value.values[0])
        }
        query.setLength(query.length - 1)

        if (context.hasColumn(DbEntity.UPD_VERSION)) {
            // keep the optimistic lock coherent: a reader holding the pre-update row can't save over this change
            val version = "COALESCE(\"${DbEntity.UPD_VERSION}\",0)+1"
            query.append(",\"").append(DbEntity.UPD_VERSION).append("\"=")
                .append("CASE WHEN ").append(version).append(">=").append(Int.MAX_VALUE)
                .append(" THEN 0 ELSE ").append(version).append(" END")
        }

        query.append(" WHERE \"").append(idColumn).append("\"=?")
        params.add(idValue)
        for (value in expectedValues) {
            query.append(" AND \"").append(value.name).append("\"")
            val expectedValue = value.values[0]
            if (expectedValue == null) {
                query.append(" IS NULL")
            } else {
                query.append("=").append(value.placeholder)
                params.add(expectedValue)
            }
        }

        val updatedRows = context.getDataSource().update(query.toString(), params).firstOrNull() ?: 0L
        if (updatedRows > 1) {
            error(
                "Conditional update matched $updatedRows rows instead of one. " +
                    "Table: ${context.getTableRef().fullName} $idColumn: $idValue"
            )
        }
        return updatedRows == 1L
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

    private fun checkAuth(context: DbTableContext) {
        if (isAuthEnabled(context) && AuthContext.getCurrentUser().isEmpty()) {
            error("Current user is empty. Table: ${context.getTableRef()}")
        }
    }

    private fun saveImpl(context: DbTableContext, entities: List<Map<String, Any?>>): LongArray {

        checkAuth(context)

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

        val entitiesToInsert = entities.map { entity ->

            val attsToInsert = LinkedHashMap(entity)
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
            val attsToUpdate = LinkedHashMap(attributes)

            var currentVersion: Long = -1
            if (context.hasColumn(DbEntity.UPD_VERSION)) {
                currentVersion = attributes[DbEntity.UPD_VERSION] as? Long
                    ?: error("Missing attribute: ${DbEntity.UPD_VERSION}")

                var newVersion = currentVersion + 1
                if (newVersion >= Int.MAX_VALUE) {
                    newVersion = 0L
                }
                attsToUpdate[DbEntity.UPD_VERSION] = newVersion
            }

            val columns = context.getColumns().filter { attsToUpdate.containsKey(it.name) }

            val valuesForDb = prepareValuesForDb(columns, typesConverter, listOf(attsToUpdate))
            val setPlaceholders = valuesForDb.joinToString(",") { "\"${it.name}\"=${it.placeholder}" }

            val params = valuesForDb.map { it.values[0] }.toMutableList<Any?>()
            var query = "UPDATE ${tableRef.fullName} SET $setPlaceholders " +
                "WHERE \"${DbEntity.ID}\"=?"
            params.add(entity.id)
            if (currentVersion > -1) {
                query += " AND \"${DbEntity.UPD_VERSION}\"=?"
                params.add(currentVersion)
            }
            if (dataSource.update(query, params).first() != 1L) {
                error("Concurrent modification of record with id: ${entity.id}")
            }
        }
    }

    private fun getCount(context: DbTableContext, query: DbFindQuery): Long {

        if (context.getColumns().isEmpty()) {
            return 0
        }
        val permsColumn = getPermsColumn(context)
        if (permsColumn.isNotEmpty() &&
            (
                !context.getPermsService()
                    .isTableExists() ||
                    !context.hasColumn(permsColumn)
                )
        ) {
            return 0
        }

        val repoAssocJoins = query.assocTableJoins.associateBy { it.attribute }
        val repoAssocTableJoins = query.assocJoinsWithPredicate.associateBy { it.attribute }

        val sqlExpressionsByAlias = getSqlExpressionsByAliases(
            RECORD_TABLE_ALIAS,
            query.expressions,
            query.rawTableJoins
        )

        val params = mutableListOf<Any?>()
        val sqlCondition = toSqlCondition(
            context,
            query.predicate,
            RECORD_TABLE_ALIAS,
            repoAssocJoins,
            repoAssocTableJoins,
            query.rawTableJoins,
            query.expressions,
            sqlExpressionsByAlias,
            params
        )

        return getCountImpl(
            context,
            RECORD_TABLE_ALIAS,
            sqlCondition,
            params,
            permsColumn,
            query.groupBy,
            query.assocSelectJoins,
            query.rawTableJoins,
            query.userAuthorities,
            query.delegatedAuthorities
        )
    }

    private fun getSqlExpressionsByAliases(
        table: String,
        expressions: Map<String, ExpressionToken>,
        rawTableJoins: Map<String, RawTableJoin>
    ): Map<String, String> {

        if (expressions.isEmpty()) {
            return emptyMap()
        }

        val sqlExpressionsByAlias = HashMap<String, String>()
        expressions.forEach { expression ->
            val expressionStr = expression.value.toString { token ->
                if (token is DbExpressionAttsContext.AssocAggregationSelectExpression) {
                    val tableRef = token.tableContext.getTableRef()
                    val assocTable = tableRef.withTable(DbAssocEntity.MAIN_TABLE)
                    val expressionStr = token.expression.toString { toStrToken ->
                        if (toStrToken is ColumnToken) {
                            "\"target\".\"${toStrToken.name}\""
                        } else {
                            toStrToken.toString()
                        }
                    }
                    "(SELECT $expressionStr FROM ${tableRef.fullName} target INNER JOIN ${assocTable.fullName} assoc " +
                        "ON assoc.${DbAssocEntity.SOURCE_ID} = $table.${DbEntity.REF_ID} " +
                        "AND assoc.${DbAssocEntity.ATTRIBUTE} = ${token.attributeId} " +
                        "AND assoc.${DbAssocEntity.TARGET_ID} = target.${DbEntity.REF_ID} GROUP BY assoc.${DbAssocEntity.SOURCE_ID})"
                } else {
                    if (token is ColumnToken) {
                        val dotIdx = token.name.indexOf('.')
                        if (dotIdx > 0) {
                            val joinSrcAtt = token.name.substring(0, dotIdx)
                            if (!rawTableJoins.containsKey(joinSrcAtt)) {
                                val joinTgtAtt = token.name.substring(dotIdx + 1)
                                "\"asj__$joinSrcAtt\".\"$joinTgtAtt\""
                            } else {
                                token.name.split(".").joinToString(".") { "\"$it\"" }
                            }
                        } else {
                            "\"$table\".\"${token.name}\""
                        }
                    } else {
                        token.toString()
                    }
                }
            }
            sqlExpressionsByAlias[expression.key] = expressionStr
        }
        return sqlExpressionsByAlias
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
        groupBy: List<String>,
        assocSelectJoins: Map<String, DbTableContext>,
        rawTableJoins: Map<String, RawTableJoin>,
        userAuthorities: Set<String>,
        delegatedAuthorities: List<Pair<Set<Long>, Set<String>>>
    ): Long {

        val selectQuery = createSelectQuery(
            context,
            table,
            COUNT_COLUMN,
            permsColumn,
            sqlCondition,
            emptyList(),
            DbFindPage.ALL,
            emptyMap(),
            groupBy,
            assocSelectJoins,
            rawTableJoins,
            userAuthorities,
            delegatedAuthorities
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
        query: DbFindQuery,
        page: DbFindPage,
        withTotalCount: Boolean
    ): DbFindRes<Map<String, Any?>> {

        val columns = context.getColumns()

        if (columns.isEmpty()) {
            return DbFindRes(emptyList(), 0)
        }
        if (page.maxItems == 0) {
            return DbFindRes(emptyList(), getCount(context, query))
        }
        val typesConverter = context.getTypesConverter()

        val permsColumn = getPermsColumn(context)

        if (permsColumn.isNotEmpty() &&
            (
                !context.getPermsService()
                    .isTableExists() ||
                    !context.hasColumn(permsColumn)
                )
        ) {
            return DbFindRes(emptyList(), 0)
        }

        val columnsByName = columns.associateBy { it.name }

        fun isColumnValid(name: String): Boolean {
            if (columnsByName.containsKey(name) || query.expressions.containsKey(name)) {
                return true
            }
            val dotIdx = name.indexOf('.')
            if (dotIdx == -1) {
                return false
            }
            val nameBeforeDot = name.substring(0, dotIdx)
            if (query.assocSelectJoins.containsKey(nameBeforeDot)) {
                return true
            }
            if (query.rawTableJoins.containsKey(nameBeforeDot)) {
                return true
            }
            return false
        }

        if (query.expressions.isNotEmpty()) {
            val invalidColumns = LinkedHashSet<String>()
            query.expressions.values.forEach {
                it.visitColumns { token ->
                    if (!isColumnValid(token.name)) {
                        invalidColumns.add(token.name)
                    }
                }
            }
            if (invalidColumns.isNotEmpty()) {
                error("Invalid columns used in expressions: $invalidColumns")
            }
        }
        val queryGroupBy = ArrayList(query.groupBy)
        if (queryGroupBy.isNotEmpty()) {
            val invalidColumns = queryGroupBy.filter { it != "*" && !isColumnValid(it) }
            if (invalidColumns.isNotEmpty()) {
                error("Grouping by columns $invalidColumns is not allowed")
            }
        }

        val repoAssocJoins = query.assocTableJoins.associateBy { it.attribute }
        val repoAssocTableJoins = query.assocJoinsWithPredicate.associateBy { it.attribute }
        val rawTableJoins = query.rawTableJoins

        val sqlExpressionsByAlias = getSqlExpressionsByAliases(
            RECORD_TABLE_ALIAS,
            query.expressions,
            rawTableJoins
        )

        val params = mutableListOf<Any?>()
        val sqlCondition =
            toSqlCondition(
                context,
                query.predicate,
                RECORD_TABLE_ALIAS,
                repoAssocJoins,
                repoAssocTableJoins,
                rawTableJoins,
                query.expressions,
                sqlExpressionsByAlias,
                params
            )

        val selectExpressions = HashSet<String>(sqlExpressionsByAlias.keys)
        if (queryGroupBy.isNotEmpty()) {
            val groupByExpressions = queryGroupBy.mapTo(HashSet()) {
                query.expressions[it] ?: ColumnToken(it)
            }
            query.expressions.forEach {
                if (!queryGroupBy.contains(it.key) &&
                    !isValidExpressionForQuerySelectAttWithGrouping(groupByExpressions, it.value)
                ) {
                    selectExpressions.remove(it.key)
                }
            }
        }

        val asjAliases = HashMap<String, String>()
        val selectQuery = createSelectQuery(
            context,
            RECORD_TABLE_ALIAS,
            columns,
            permsColumn,
            sqlCondition,
            query.sortBy,
            page,
            queryGroupBy,
            query.expressions,
            sqlExpressionsByAlias,
            query.assocSelectJoins,
            query.rawTableJoins,
            asjAliases,
            selectExpressions,
            query.userAuthorities,
            query.delegatedAuthorities
        )

        val resultEntities = context.getDataSource().query(selectQuery, params) { resultSet ->
            val resultList = mutableListOf<Map<String, Any?>>()
            // resolved once for the whole result set: the answer is the same for every row, and
            // reading it per row would put a metadata lookup on every column of every record
            val prolepticDateColumns = getProlepticDateColumns(resultSet)
            while (resultSet.next()) {
                resultList.add(
                    convertRowToMap(
                        typesConverter,
                        resultSet,
                        columns,
                        prolepticDateColumns,
                        queryGroupBy,
                        selectExpressions,
                        asjAliases
                    )
                )
            }
            resultList
        }

        val totalCount = if (!withTotalCount || page.maxItems == -1 || page.maxItems > resultEntities.size) {
            resultEntities.size.toLong() + page.skipCount
        } else {
            getCountImpl(
                context,
                RECORD_TABLE_ALIAS,
                sqlCondition,
                params,
                permsColumn,
                query.groupBy,
                query.assocSelectJoins,
                query.rawTableJoins,
                query.userAuthorities,
                query.delegatedAuthorities
            )
        }
        return DbFindRes(resultEntities, totalCount)
    }

    private fun createSelectQuery(
        context: DbTableContext,
        table: String,
        selectColumns: List<DbColumnDef>,
        permsColumn: String,
        condition: String,
        sort: List<DbFindSort> = emptyList(),
        page: DbFindPage = DbFindPage.ALL,
        groupBy: List<String> = emptyList(),
        expressions: Map<String, ExpressionToken>,
        sqlExpressionsByAlias: Map<String, String>,
        assocSelectJoins: Map<String, DbTableContext>,
        rawTableJoins: Map<String, RawTableJoin>,
        asjAliases: MutableMap<String, String>,
        selectExpressions: Set<String>,
        userAuthorities: Set<String>,
        delegatedAuthorities: List<Pair<Set<Long>, Set<String>>>
    ): String {

        val selectColumnsStr = StringBuilder()

        val asjAliasesCounter = AtomicInteger()
        fun registerAsjColumn(name: String): String {
            val dotIdx = name.indexOf('.')
            if (dotIdx == -1) {
                return name
            }
            return asjAliases.computeIfAbsent(name) {
                val joinSrcAtt = name.substring(0, dotIdx)
                val joinTgtAtt = name.substring(dotIdx + 1)
                val alias = "$VIRTUAL_COLUMN_PREFIX${asjAliasesCounter.getAndIncrement()}"
                selectColumnsStr.append(' ')
                    .append('"')
                    .append("asj__")
                    .append(joinSrcAtt)
                    .append("\".\"")
                    .append(joinTgtAtt)
                    .append('"')
                    .append(" AS ")
                    .append(alias)
                    .append(",")
                alias
            }
        }

        val convertedGroupBy = groupBy.map { registerAsjColumn(it) }
        val convertedSortBy = sort.map {
            if (it.column.contains('.')) {
                DbFindSort(registerAsjColumn(it.column), it.ascending)
            } else {
                it
            }
        }

        if (convertedGroupBy.isEmpty()) {
            selectColumns.forEach {
                appendRecordColumnName(selectColumnsStr, table, it.name)
                selectColumnsStr.append(",")
            }
        } else {
            convertedGroupBy.forEach { groupByIt ->
                if (groupByIt != "*" &&
                    !expressions.containsKey(groupByIt) &&
                    !groupByIt.startsWith(VIRTUAL_COLUMN_PREFIX)
                ) {
                    appendRecordColumnName(selectColumnsStr, table, groupByIt)
                    selectColumnsStr.append(",")
                }
            }
        }

        for (alias in selectExpressions) {
            val expression = sqlExpressionsByAlias[alias] ?: continue
            selectColumnsStr.append(expression).append(" AS ").append(alias).append(",")
        }

        selectColumnsStr.setLength(selectColumnsStr.length - 1)

        return createSelectQuery(
            context,
            table,
            selectColumnsStr.toString(),
            permsColumn,
            condition,
            convertedSortBy,
            page,
            expressions,
            convertedGroupBy,
            assocSelectJoins,
            rawTableJoins,
            userAuthorities,
            delegatedAuthorities
        )
    }

    private fun isValidExpressionForQuerySelectAttWithGrouping(
        groupBy: Set<ExpressionToken>,
        expression: ExpressionToken?
    ): Boolean {
        expression ?: return true
        if (groupBy.contains(expression)) {
            return true
        }
        return if (expression is ColumnToken) {
            groupBy.contains(expression)
        } else if (expression is FunctionToken) {
            if (expression.isAggregationFunc()) {
                true
            } else {
                expression.args.all {
                    isValidExpressionForQuerySelectAttWithGrouping(groupBy, it)
                }
            }
        } else if (expression is GroupToken) {
            expression.tokens.all {
                isValidExpressionForQuerySelectAttWithGrouping(groupBy, it)
            }
        } else if (expression is CaseToken) {
            expression.branches.all {
                isValidExpressionForQuerySelectAttWithGrouping(groupBy, it.condition) &&
                    isValidExpressionForQuerySelectAttWithGrouping(groupBy, it.thenResult)
            } &&
                isValidExpressionForQuerySelectAttWithGrouping(groupBy, expression.orElse)
        } else {
            true
        }
    }

    private fun createSelectQuery(
        context: DbTableContext,
        table: String,
        selectColumns: String,
        permsColumn: String,
        condition: String,
        sort: List<DbFindSort> = emptyList(),
        page: DbFindPage = DbFindPage.ALL,
        expressions: Map<String, ExpressionToken>,
        groupBy: List<String> = emptyList(),
        assocSelectJoins: Map<String, DbTableContext>,
        rawTableJoins: Map<String, RawTableJoin>,
        userAuthorities: Set<String>,
        delegatedAuthorities: List<Pair<Set<Long>, Set<String>>>
    ): String {

        val permsCondition = getPermsCondition(context, permsColumn, userAuthorities, delegatedAuthorities)
        val fullCondition = joinConditionsByAnd(condition, permsCondition)

        val query = StringBuilder()
        query.append("SELECT ")
            .append(selectColumns)
            .append(" FROM ${context.getTableRef().fullName} \"$table\"")

        for ((srcColumn, targetCtx) in assocSelectJoins) {
            val alias = "\"asj__$srcColumn\""
            query.append(" LEFT JOIN ${targetCtx.getTableRef().fullName} $alias ON $alias.${DbEntity.REF_ID} = ")
            appendRecordColumnName(query, table, srcColumn)
        }

        for ((alias, join) in rawTableJoins) {
            if (alias.any { !it.isLetterOrDigit() && it != '_' }) {
                error("Invalid alias: '$alias'")
            }
            val tableName = join.table.getTableRef().fullName
            query.append(" LEFT JOIN $tableName $alias ON ")
            val joinOn = join.on
            if (joinOn !is ValuePredicate || joinOn.getType() != ValuePredicate.Type.EQ) {
                error("Invalid 'join on' condition: $joinOn")
            }
            fun addField(query: StringBuilder, table: String, field: String) {
                if (field.startsWith("$alias.")) {
                    query.append(field)
                } else {
                    appendRecordColumnName(query, table, field)
                }
            }
            addField(query, table, joinOn.getAttribute())
            query.append(" = ")
            addField(query, table, joinOn.getValue().asText())
        }

        if (fullCondition.isNotBlank()) {
            query.append(" WHERE ").append(fullCondition)
        }

        addGrouping(query, table, expressions, groupBy)
        addSortAndPage(query, table, expressions, sort, page)

        return query.toString()
    }

    private fun addAssocTableCondition(
        query: StringBuilder,
        table: String,
        tableJoin: AssocJoinWithPredicate,
        queryParams: MutableList<Any?>
    ) {
        val targetTableName = "$table$RECORD_TABLE_ALIAS"
        val targetTable = tableJoin.tableContext.getTableRef()

        query.append("EXISTS(SELECT 1 FROM ")
        if (tableJoin.multipleAssoc) {
            query.append(targetTable.withTable(DbAssocEntity.MAIN_TABLE).fullName)
                .append(" assoc INNER JOIN ")
                .append(targetTable.fullName).append(" ").append(targetTableName)
                .append(" ON assoc.\"${DbAssocEntity.SOURCE_ID}\"=\"$table\".\"${DbEntity.REF_ID}\" ")
                .append("AND assoc.\"${DbAssocEntity.ATTRIBUTE}\"=${tableJoin.srcAttributeId} ")
                .append("AND assoc.\"${DbAssocEntity.TARGET_ID}\"=\"${targetTableName}\".\"${DbEntity.REF_ID}\"")
                .append(" WHERE ")
        } else {
            query.append("${targetTable.fullName} $targetTableName WHERE ")
                .append("\"$table\".\"${tableJoin.srcColumn}\"=\"${targetTableName}\".\"${DbEntity.REF_ID}\" AND ")
        }
        val assocJoins = tableJoin.assocTableJoins.associateBy { it.attribute }
        val assocTableJoins = tableJoin.assocJoinsWithPredicate.associateBy { it.attribute }

        toSqlCondition(
            tableJoin.tableContext,
            query,
            targetTableName,
            tableJoin.predicate,
            assocJoins,
            assocTableJoins,
            emptyMap(),
            emptyMap(),
            emptyMap(),
            queryParams
        )
        query.append(")")
    }

    private fun addAssocCondition(
        context: DbTableContext,
        query: StringBuilder,
        table: String,
        assocTableJoin: AssocTableJoin,
        values: Collection<Long>
    ) {
        if (values.isEmpty()) {
            query.append(ALWAYS_FALSE_CONDITION)
            return
        }
        val tableRef = context.getTableRef()
        val assocsTableName = tableRef.withTable(DbAssocEntity.MAIN_TABLE).fullName
        val attId = assocTableJoin.attId
        query.append("EXISTS(SELECT 1 FROM $assocsTableName a WHERE ")
        if (assocTableJoin.target) {
            query.append(
                "a.${DbAssocEntity.SOURCE_ID}=\"$table\".${DbEntity.REF_ID} " +
                    "AND a.${DbAssocEntity.ATTRIBUTE}=$attId AND a.${DbAssocEntity.TARGET_ID} IN ("
            )
        } else {
            query.append(
                "a.${DbAssocEntity.TARGET_ID}=\"$table\".${DbEntity.REF_ID} " +
                    "AND a.${DbAssocEntity.ATTRIBUTE}=$attId AND a.${DbAssocEntity.SOURCE_ID} IN ("
            )
        }
        values.forEach {
            query.append(it).append(",")
        }
        query.setLength(query.length - 1)
        query.append("))")
    }

    private fun addGrouping(
        query: StringBuilder,
        table: String,
        expressions: Map<String, ExpressionToken>,
        groupBy: List<String>
    ) {
        if (groupBy.isEmpty() || groupBy.size == 1 && groupBy[0] == "*") {
            return
        }
        query.append(" GROUP BY ")
        groupBy.forEach {
            if (expressions.containsKey(it)) {
                query.append(it)
            } else if (it.startsWith(VIRTUAL_COLUMN_PREFIX)) {
                query.append('"').append(it).append('"')
            } else {
                appendRecordColumnName(query, table, it)
            }
            query.append(",")
        }
        query.setLength(query.length - 1)
    }

    private fun addSortAndPage(
        query: StringBuilder,
        table: String,
        expressions: Map<String, ExpressionToken>,
        sorting: List<DbFindSort>,
        page: DbFindPage
    ) {

        if (sorting.isNotEmpty()) {
            query.append(" ORDER BY ")
            for (sort in sorting) {
                if (expressions.containsKey(sort.column)) {
                    query.append(sort.column)
                } else if (sort.column.startsWith(VIRTUAL_COLUMN_PREFIX)) {
                    query.append("\"").append(sort.column).append("\"")
                } else {
                    appendRecordColumnName(query, table, sort.column)
                }
                if (sort.ascending) {
                    query.append(" ASC")
                } else {
                    query.append(" DESC")
                }
                query.append(",")
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
            DbColumnType.DATE -> LocalDate::class
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

    private fun getPermsCondition(
        context: DbTableContext,
        permsColumn: String,
        userAuthorities: Set<String>,
        delegatedAuthorities: List<Pair<Set<Long>, Set<String>>>
    ): String {

        if (permsColumn.isBlank()) {
            return ""
        }

        val authorities = userAuthorities.ifEmpty { DbRecordsUtils.getCurrentAuthorities() }
        if (authorities.isEmpty()) {
            return ALWAYS_FALSE_CONDITION
        }
        val fullAuthorities = HashSet<String>(authorities)
        for ((_, delegationAuth) in delegatedAuthorities) {
            fullAuthorities.addAll(delegationAuth)
        }
        val authorityIdByName = context.getAuthoritiesIdsMap(fullAuthorities)
        val authoritiesIds = authorities.mapNotNull { authorityIdByName[it] }
        if (authoritiesIds.isEmpty() && delegatedAuthorities.isEmpty()) {
            return ALWAYS_FALSE_CONDITION
        }

        val tableRef = context.getTableRef()
        val permsTableName = tableRef.withTable(DbPermsEntity.TABLE).fullName
        val isCheckPermsByParent = context.getQueryPermsPolicy() == QueryPermsPolicy.PARENT

        val permsAlias = "\"$PERMS_TABLE_ALIAS\""
        val condition = StringBuilder()
        val permsJoinCondition = "$permsAlias.\"${DbPermsEntity.ENTITY_REF_ID}\"=\"${RECORD_TABLE_ALIAS}\".$permsColumn"

        if (isCheckPermsByParent) {
            condition.append("(")
        }

        condition.append("EXISTS(SELECT 1 FROM ")
            .append(permsTableName).append(" ").append(permsAlias)
            .append(" WHERE ")
            .append(permsJoinCondition)
            .append(" AND ($permsAlias.\"${DbPermsEntity.AUTHORITY_ID}\" IN (")

        authoritiesIds.forEach {
            condition.append(it).append(",")
        }
        condition.setLength(condition.length - 1)
        condition.append(")")
        if (context.hasColumn(DbEntity.TYPE)) {
            for ((delegatedTypesIds, delegationAuth) in delegatedAuthorities) {
                val authIds = delegationAuth.mapNotNull { authorityIdByName[it] }
                if (authIds.isEmpty() || delegatedTypesIds.isEmpty()) {
                    continue
                }
                condition.append(" OR (\"$RECORD_TABLE_ALIAS\".\"${DbEntity.TYPE}\" IN (")
                for (typeId in delegatedTypesIds) {
                    condition.append(typeId).append(",")
                }
                condition.setLength(condition.length - 1)
                condition.append(") AND $permsAlias.\"${DbPermsEntity.AUTHORITY_ID}\" IN (")
                for (authId in authIds) {
                    condition.append(authId).append(",")
                }
                condition.setLength(condition.length - 1)
                condition.append("))")
            }
        }
        condition.append("))")
        if (isCheckPermsByParent) {
            condition.append(" OR NOT EXISTS(SELECT 1 FROM ").append(permsTableName).append(" ")
                .append(permsAlias)
                .append(" WHERE ")
                .append(permsJoinCondition)
                .append("))")
        }

        return condition.toString()
    }

    private fun toSqlCondition(
        context: DbTableContext,
        predicate: Predicate,
        table: String,
        assocTableJoins: Map<String, AssocTableJoin>,
        assocTargetJoinsWithPredicate: Map<String, AssocJoinWithPredicate>,
        rawTableJoins: Map<String, RawTableJoin>,
        expressions: Map<String, ExpressionToken>,
        sqlExpressionsByAlias: Map<String, String>,
        queryParams: MutableList<Any?>
    ): String {
        val sb = StringBuilder()
        toSqlCondition(
            context,
            sb,
            table,
            predicate,
            assocTableJoins,
            assocTargetJoinsWithPredicate,
            rawTableJoins,
            expressions,
            sqlExpressionsByAlias,
            queryParams
        )
        return sb.toString()
    }

    private fun toSqlCondition(
        context: DbTableContext,
        query: StringBuilder,
        table: String,
        predicate: Predicate,
        assocTableJoins: Map<String, AssocTableJoin>,
        assocTargetJoinsWithPredicate: Map<String, AssocJoinWithPredicate>,
        rawTableJoins: Map<String, RawTableJoin>,
        expressions: Map<String, ExpressionToken>,
        sqlExpressionsByAlias: Map<String, String>,
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
                    if (toSqlCondition(
                            context,
                            query,
                            table,
                            innerPred,
                            assocTableJoins,
                            assocTargetJoinsWithPredicate,
                            rawTableJoins,
                            expressions,
                            sqlExpressionsByAlias,
                            queryParams
                        )
                    ) {
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

                val attribute: String = predicate.getAttribute()

                var columnDef = context.getColumnByName(attribute)
                if (columnDef == null) {
                    val assocJoin = assocTableJoins[attribute]
                    if (assocJoin != null) {
                        columnDef = context.getColumnByName(assocJoin.srcColumn)
                    } else {
                        val assocTableJoin = assocTargetJoinsWithPredicate[attribute]
                        if (assocTableJoin != null) {
                            columnDef = context.getColumnByName(assocTableJoin.srcColumn)
                        } else {
                            val expression = expressions[attribute]
                            if (expression != null) {
                                val retType = if (expression is FunctionToken) {
                                    FunctionToken.getFunctionReturnType(expression)
                                } else {
                                    DbColumnType.LONG
                                }
                                columnDef = DbColumnDef.create {
                                    withName(attribute)
                                    withType(retType)
                                }
                            } else if (rawTableJoins.isNotEmpty()) {
                                val dotIdx = attribute.indexOf('.')
                                if (dotIdx > 0) {
                                    val firstPart = attribute.substring(0, dotIdx)
                                    val rawJoin = rawTableJoins[firstPart]
                                    if (rawJoin != null) {
                                        columnDef = rawJoin.table.getColumnByName(attribute.substring(dotIdx + 1))
                                    }
                                }
                            }
                        }
                    }
                }

                val type = predicate.getType()
                val value = predicate.getValue()

                // Gated by the presence of the join, not by the physical column type. The model
                // and the schema can legitimately disagree - a failed column conversion, or a
                // migration still running in background. When a join was requested for this
                // attribute, the condition must be built through it whatever the column type is:
                // otherwise the synthetic join alias leaks into the SQL as a column name and the
                // whole query fails with "column <att>-1 does not exist".
                //
                // Both assoc branches are evaluated before the "column is not found" check below:
                // they go through ed_associations and don't need the source column at all, so a
                // missing column is no reason to refuse an association condition.
                val assocJoin = assocTableJoins[attribute]
                if (assocJoin != null) {
                    if (type != ValuePredicate.Type.EQ &&
                        type != ValuePredicate.Type.CONTAINS &&
                        type != ValuePredicate.Type.IN
                    ) {
                        return false
                    }
                    val longs = DbAttValueUtils.anyToSetOfLongs(value)
                    addAssocCondition(context, query, table, assocJoin, longs)
                    return true
                }
                val assocTargetTableJoin = assocTargetJoinsWithPredicate[attribute]
                if (assocTargetTableJoin != null) {
                    if (type != ValuePredicate.Type.EQ &&
                        type != ValuePredicate.Type.CONTAINS &&
                        type != ValuePredicate.Type.IN
                    ) {
                        return false
                    }
                    addAssocTableCondition(query, table, assocTargetTableJoin, queryParams)
                    return true
                }

                if (columnDef == null) {
                    // The schema is generated from the type model by the write path
                    // (ensureColumnsExist), so an attribute added to the model but never yet
                    // saved simply has no column. Reading must not fail because of that: the
                    // condition can't match anything, so it degrades to a false condition.
                    //
                    // "false" is returned as ALWAYS_FALSE_CONDITION rather than by returning
                    // false from this method: returning false drops the condition from the
                    // enclosing AND, which would widen the result set instead of narrowing it.
                    DbReadToleranceLog.warnOnce(
                        log,
                        "predicate-column-not-found-${context.getTableRef()}-$attribute"
                    ) {
                        "Column is not found for attribute '$attribute' in table " +
                            "${context.getTableRef()}. The attribute exists in the type model, " +
                            "but was never materialized in the database schema. The condition " +
                            "will not match any record"
                    }
                    query.append(ALWAYS_FALSE_CONDITION)
                    return true
                }

                val expression = expressions[attribute]
                if (expression != null) {
                    val operator = when (type) {
                        ValuePredicate.Type.EQ -> "="
                        ValuePredicate.Type.LIKE,
                        ValuePredicate.Type.CONTAINS -> {
                            if (value.isNumber()) {
                                "="
                            } else {
                                "LIKE"
                            }
                        }
                        ValuePredicate.Type.GT -> ">"
                        ValuePredicate.Type.GE -> ">="
                        ValuePredicate.Type.LT -> "<"
                        ValuePredicate.Type.LE -> "<="
                        else -> {
                            log.error { "Unknown predicate type: $type for expression" }
                            return false
                        }
                    }
                    var convertedParam = if (value.isNumber()) {
                        if (value.isIntegralNumber()) {
                            value.asLong()
                        } else {
                            value.asDouble()
                        }
                    } else if (value.isBoolean()) {
                        value.asBoolean()
                    } else {
                        value.asText()
                    }
                    val sqlExpression = sqlExpressionsByAlias[attribute]
                        ?: error("SQL expression is not found for $expression")

                    if (convertedParam is String &&
                        (
                            type == ValuePredicate.Type.LIKE ||
                                type == ValuePredicate.Type.CONTAINS
                            )
                    ) {
                        query.append("LOWER(")
                            .append(sqlExpression)
                            .append(") ")
                            .append(operator)
                            .append(" ?")

                        convertedParam = convertedParam.lowercase().replace("\\", "\\\\")
                        if (type == ValuePredicate.Type.CONTAINS) {
                            convertedParam = "%$convertedParam%"
                        }
                    } else {
                        query.append(sqlExpression).append(" ").append(operator).append(" ?")
                    }

                    queryParams.add(convertedParam)
                    return true
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
                                queryParam = queryParam.lowercase().replace("\\", "\\\\")
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
                            query.append(' ')
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
                            } else if (value.isTextual()) {
                                queryParams.add(
                                    when (columnDef.type) {
                                        DbColumnType.DOUBLE -> value.asDouble()
                                        DbColumnType.INT -> value.asLong()
                                        DbColumnType.LONG -> value.asLong()
                                        DbColumnType.BIGSERIAL -> value.asLong()
                                        else -> value.asJavaObj()
                                    }
                                )
                            } else {
                                queryParams.add(value.asJavaObj())
                            }
                        }
                    }
                }
                return true
            }

            is NotPredicate -> {
                val innerPredicate = predicate.getPredicate()

                if (isNeedToReplaceEqOperatorWithDistinctFrom(innerPredicate, context, assocTargetJoinsWithPredicate)) {
                    return if (toSqlCondition(
                            context,
                            query,
                            table,
                            innerPredicate,
                            assocTableJoins,
                            assocTargetJoinsWithPredicate,
                            rawTableJoins,
                            expressions,
                            sqlExpressionsByAlias,
                            queryParams
                        )
                    ) {
                        // remove "= ?" after ValuePredicate processing
                        // todo: this doesn't work for conditions on joined tables
                        query.setLength(query.length - 3)
                        query.append("IS DISTINCT FROM ?")
                        true
                    } else {
                        false
                    }
                } else {
                    query.append("NOT ")
                    return if (toSqlCondition(
                            context,
                            query,
                            table,
                            innerPredicate,
                            assocTableJoins,
                            assocTargetJoinsWithPredicate,
                            rawTableJoins,
                            expressions,
                            sqlExpressionsByAlias,
                            queryParams
                        )
                    ) {
                        true
                    } else {
                        query.setLength(query.length - 4)
                        false
                    }
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

    private fun isNeedToReplaceEqOperatorWithDistinctFrom(
        innerPredicate: Predicate,
        context: DbTableContext,
        assocTargetJoinsWithPredicate: Map<String, AssocJoinWithPredicate>
    ): Boolean {
        if (innerPredicate !is ValuePredicate) {
            return false
        }

        val predicateAtt = innerPredicate.getAttribute()
        val columnDef = context.getColumnByName(predicateAtt)
        val predicateValue = innerPredicate.getValue()

        val isEqualsTypePredicate = innerPredicate.getType() == ValuePredicate.Type.EQ
        val isPredicateFromAssocTable = assocTargetJoinsWithPredicate.containsKey(predicateAtt)

        return isEqualsTypePredicate &&
            !isPredicateFromAssocTable &&
            predicateValue.isNotNull() &&
            isColumnTypeMatchWithDistinctFromOperator(columnDef)
    }

    private fun isColumnTypeMatchWithDistinctFromOperator(columnDef: DbColumnDef?): Boolean {
        return columnDef != null && COLUMN_TYPES_FOR_IS_DISTINCT_FROM_OPERATOR.contains(columnDef.type)
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
