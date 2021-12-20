package ru.citeck.ecos.data.sql.pg

import mu.KotlinLogging
import ru.citeck.ecos.context.lib.auth.AuthContext
import ru.citeck.ecos.data.sql.datasource.DbDataSource
import ru.citeck.ecos.data.sql.dto.DbColumnDef
import ru.citeck.ecos.data.sql.dto.DbColumnType
import ru.citeck.ecos.data.sql.dto.DbTableRef
import ru.citeck.ecos.data.sql.repo.DbEntityPermissionsDto
import ru.citeck.ecos.data.sql.repo.DbEntityRepo
import ru.citeck.ecos.data.sql.repo.DbEntityRepoConfig
import ru.citeck.ecos.data.sql.repo.entity.DbEntity
import ru.citeck.ecos.data.sql.repo.entity.DbEntityMapper
import ru.citeck.ecos.data.sql.repo.entity.auth.DbAuthorityEntity
import ru.citeck.ecos.data.sql.repo.entity.auth.DbPermsEntity
import ru.citeck.ecos.data.sql.repo.find.DbFindPage
import ru.citeck.ecos.data.sql.repo.find.DbFindRes
import ru.citeck.ecos.data.sql.repo.find.DbFindSort
import ru.citeck.ecos.data.sql.txn.ExtTxnContext
import ru.citeck.ecos.data.sql.type.DbTypeUtils
import ru.citeck.ecos.data.sql.type.DbTypesConverter
import ru.citeck.ecos.model.lib.role.constants.RoleConstants
import ru.citeck.ecos.records2.predicate.model.*
import java.sql.Date
import java.sql.ResultSet
import java.sql.Timestamp
import java.time.*
import java.util.*
import kotlin.collections.HashSet
import kotlin.reflect.KClass

class DbEntityRepoPg<T : Any>(
    private val mapper: DbEntityMapper<T>,
    private val dataSource: DbDataSource,
    private val tableRef: DbTableRef,
    private val typesConverter: DbTypesConverter,
    private val config: DbEntityRepoConfig
) : DbEntityRepo<T> {

    companion object {
        private val log = KotlinLogging.logger {}

        private const val ALWAYS_FALSE_CONDITION = "0=1"
        private const val WHERE_ALWAYS_FALSE = "WHERE $ALWAYS_FALSE_CONDITION"

        private const val RECORD_TABLE_ALIAS = "r"
        private const val AUTHORITIES_TABLE_ALIAS = "a"
        private const val PERMS_TABLE_ALIAS = "p"

        private const val COUNT_COLUMN = "COUNT(*)"
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

    override fun findById(ids: Set<Long>): List<T> {
        return findByColumnAsMap(DbEntity.ID, ids, columns, false, ids.size).map {
            mapper.convertToEntity(it)
        }
    }

    override fun findById(id: Long): T? {
        return findById(id, false)
    }

    override fun findById(id: Long, withDeleted: Boolean): T? {
        return findOneByColumnAsMap(DbEntity.ID, id, columns, withDeleted)?.let {
            mapper.convertToEntity(it)
        }
    }

    override fun findByExtId(id: String): T? {
        return findByExtId(id, false)
    }

    override fun findByExtId(id: String, withDeleted: Boolean): T? {
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
        return findByColumnAsMap(column, setOf(value), columns, withDeleted, 1).firstOrNull()
    }

    private fun findByColumnAsMap(
        column: String,
        values: Set<Any>,
        columns: List<DbColumnDef>,
        withDeleted: Boolean,
        limit: Int
    ): List<Map<String, Any?>> {

        if (columns.isEmpty() || values.isEmpty()) {
            return emptyList()
        }

        updateSchemaCacheIfRequired()

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
            columns,
            withAuth = isAuthEnabled(),
            withDeleted,
            condition.toString(),
            page = DbFindPage(0, limit)
        )
        if (hasAlwaysFalseCondition(query)) {
            return emptyList()
        }

        return dataSource.query(query, values.toList()) { resultSet ->
            val result = mutableListOf<Map<String, Any?>>()
            while (resultSet.next()) {
                result.add(convertRowToMap(resultSet, columns))
            }
            result
        }
    }

    private fun hasAlwaysFalseCondition(query: String): Boolean {
        return query.contains(WHERE_ALWAYS_FALSE)
    }

    private fun convertRowToMap(row: ResultSet, columns: List<DbColumnDef>): Map<String, Any?> {
        val result = LinkedHashMap<String, Any?>()
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
        return result
    }

    override fun forceDelete(entity: T) {
        if (columns.isEmpty()) {
            return
        }
        forceDelete(mapper.convertToMap(entity))
    }

    override fun delete(entity: T) {

        if (columns.isEmpty()) {
            return
        }

        val entityMap = mapper.convertToMap(entity)
        if (hasDeletedFlag) {
            val mutableEntity = LinkedHashMap(entityMap)
            mutableEntity[DbEntity.DELETED] = true
            saveImpl(mutableEntity)
        } else {
            forceDelete(entity)
        }
    }

    override fun forceDelete(entities: List<T>) {
        if (entities.isEmpty()) {
            return
        }
        val identifiers = entities.mapNotNull { mapper.convertToMap(it)[DbEntity.ID] as? Long }
        val identifiersStr = identifiers.joinToString(",")
        dataSource.update(
            "DELETE FROM ${tableRef.fullName} WHERE \"${DbEntity.ID}\" IN ($identifiersStr)",
            emptyList()
        )
    }

    private fun forceDelete(entity: Map<String, Any?>) {
        updateSchemaCacheIfRequired()
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

    override fun setReadPerms(permissions: List<DbEntityPermissionsDto>) {

        for (entity in permissions) {

            val recordEntity = findByExtIdAsMap(entity.id, columns, true) ?: continue
            val recordEntityId = recordEntity[DbEntity.ID] as Long

            val permsTableName = config.permsTable.fullName

            val currentPerms: List<DbPermsEntity> = dataSource.query(
                "SELECT " +
                    "\"${DbPermsEntity.RECORD_ID}\"," +
                    "\"${DbPermsEntity.AUTHORITY_ID}\"," +
                    "\"${DbPermsEntity.ALLOWED}\" " +
                    "FROM $permsTableName WHERE \"${DbPermsEntity.RECORD_ID}\"=$recordEntityId",
                emptyList()
            ) { resultSet ->

                val res = mutableListOf<DbPermsEntity>()
                while (resultSet.next()) {
                    val recordId = resultSet.getLong(DbPermsEntity.RECORD_ID)
                    val authorityId = resultSet.getLong(DbPermsEntity.AUTHORITY_ID)
                    val allowed = resultSet.getBoolean(DbPermsEntity.ALLOWED)
                    res.add(DbPermsEntity(recordId, authorityId, allowed))
                }
                res
            }

            val allowedAuth = HashSet(entity.readAllowed)
            val deniedAuth = HashSet(entity.readDenied)

            val permsAuthToDelete = currentPerms.filter {
                if (it.allowed) {
                    !allowedAuth.contains(it.authorityId)
                } else {
                    !deniedAuth.contains(it.authorityId)
                }
            }.map {
                it.authorityId
            }
            if (permsAuthToDelete.isNotEmpty()) {
                dataSource.update(
                    "DELETE FROM $permsTableName " +
                        "WHERE \"${DbPermsEntity.RECORD_ID}\"=$recordEntityId " +
                        "AND \"${DbPermsEntity.AUTHORITY_ID}\" IN (${permsAuthToDelete.joinToString(",")})",
                    emptyList()
                )
            }

            allowedAuth.removeAll(currentPerms.filter { it.allowed }.map { it.authorityId })
            deniedAuth.removeAll(currentPerms.filter { !it.allowed }.map { it.authorityId })

            if (allowedAuth.isEmpty() && deniedAuth.isEmpty()) {
                return
            }

            val query = StringBuilder()
            query.append(
                "INSERT INTO $permsTableName " +
                    "(" +
                    "\"${DbPermsEntity.RECORD_ID}\"," +
                    "\"${DbPermsEntity.AUTHORITY_ID}\"," +
                    "\"${DbPermsEntity.ALLOWED}\"" +
                    ") VALUES "
            )
            val append = { authorities: Set<Long>, allowed: Boolean ->
                authorities.forEach {
                    query.append("(")
                        .append(recordEntityId)
                        .append(",")
                        .append(it)
                        .append(",")
                        .append(allowed)
                        .append("),")
                }
            }
            append(allowedAuth, true)
            append(deniedAuth, false)

            query.setLength(query.length - 1)
            dataSource.update(query.toString(), emptyList())
        }
    }

    private fun saveAndGet(entity: Map<String, Any?>): Map<String, Any?> {
        val id = saveImpl(entity)
        return AuthContext.runAsSystem {
            findOneByColumnAsMap(DbEntity.ID, id, columns, true)
                ?: error("Entity with id $id was inserted or updated but can't be found.")
        }
    }

    private fun isAuthEnabled(): Boolean {
        return config.authEnabled && !AuthContext.isRunAsSystem()
    }

    private fun saveImpl(entity: Map<String, Any?>): Long {

        if (isAuthEnabled() && AuthContext.getCurrentUser().isEmpty()) {
            error("Current user is empty. Table: $tableRef")
        }

        updateSchemaCacheIfRequired()

        val nowInstant = Instant.now()
        val entityMap = LinkedHashMap(entity)

        val id = entityMap[DbEntity.ID] as? Long ?: error("ID is a mandatory parameter!")
        var extId = entityMap[DbEntity.EXT_ID] as? String ?: ""
        val deleted = entityMap[DbEntity.DELETED] as? Boolean ?: false

        if (deleted && extId.isBlank()) {
            return -1
        } else if (extId.isBlank() && !deleted) {
            extId = UUID.randomUUID().toString()
            entityMap[DbEntity.EXT_ID] = extId
        }

        val attsToSave = LinkedHashMap(entityMap)
        attsToSave.remove(DbEntity.ID)
        if (!ExtTxnContext.isWithoutModifiedMeta()) {
            attsToSave[DbEntity.MODIFIED] = nowInstant
            attsToSave[DbEntity.MODIFIER] = AuthContext.getCurrentUser()
        }
        return if (id == DbEntity.NEW_REC_ID) {
            insertImpl(attsToSave, nowInstant)
        } else {
            updateImpl(id, attsToSave)
            id
        }
    }

    private fun insertImpl(entity: Map<String, Any?>, nowInstant: Instant): Long {

        val attsToInsert = LinkedHashMap(entity)
        if (!hasDeletedFlag) {
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

        val valuesForDb = prepareValuesForDb(attsToInsert)
        val columnNames = valuesForDb.joinToString(",") { "\"${it.name}\"" }
        val columnPlaceholders = valuesForDb.joinToString(",") { it.placeholder }

        val query = "INSERT INTO ${tableRef.fullName} ($columnNames) VALUES ($columnPlaceholders) RETURNING id;"

        return dataSource.update(query, valuesForDb.map { it.value })
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

        if (dataSource.update(query, valuesForDb.map { it.value }) != 1L) {
            error("Concurrent modification of record with id: $id")
        }
    }

    override fun getCount(predicate: Predicate): Long {

        if (columns.isEmpty()) {
            return 0
        }

        val params = mutableListOf<Any?>()
        val sqlCondition = toSqlCondition(predicate, params, columns.associateBy { it.name })

        return getCountImpl(sqlCondition, params)
    }

    private fun getCountImpl(sqlCondition: String, params: List<Any?>): Long {

        updateSchemaCacheIfRequired()

        val selectQuery = createSelectQuery(
            COUNT_COLUMN,
            withAuth = isAuthEnabled(),
            withDeleted = false,
            sqlCondition
        )
        if (selectQuery.contains(WHERE_ALWAYS_FALSE)) {
            return 0
        }

        return dataSource.query(selectQuery, params) { resultSet ->
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

    override fun find(
        predicate: Predicate,
        sort: List<DbFindSort>,
        page: DbFindPage,
        withDeleted: Boolean
    ): DbFindRes<T> {

        if (columns.isEmpty()) {
            return DbFindRes(emptyList(), 0)
        }
        updateSchemaCacheIfRequired()

        val columnsByName = columns.associateBy { it.name }
        val params = mutableListOf<Any?>()
        val sqlCondition = toSqlCondition(predicate, params, columnsByName)
        val query = createSelectQuery(columns, isAuthEnabled(), withDeleted, sqlCondition, sort, page)

        val resultEntities = dataSource.query(query, params) { resultSet ->
            val resultList = mutableListOf<T>()
            while (resultSet.next()) {
                resultList.add(mapper.convertToEntity(convertRowToMap(resultSet, columns)))
            }
            resultList
        }

        val totalCount = if (page.maxItems == -1) {
            resultEntities.size.toLong() + page.skipCount
        } else {
            getCountImpl(sqlCondition, params)
        }
        return DbFindRes(resultEntities, totalCount)
    }

    private fun createSelectQuery(
        selectColumns: List<DbColumnDef>,
        withAuth: Boolean,
        withDeleted: Boolean = false,
        condition: String,
        sort: List<DbFindSort> = emptyList(),
        page: DbFindPage = DbFindPage.ALL,
    ): String {

        val selectColumnsStr = StringBuilder()
        selectColumns.forEach {
            appendRecordColumnName(selectColumnsStr, it.name)
            selectColumnsStr.append(",")
        }
        selectColumnsStr.setLength(selectColumnsStr.length - 1)

        return createSelectQuery(
            selectColumnsStr.toString(),
            withAuth,
            withDeleted,
            condition,
            sort,
            page
        )
    }

    private fun createSelectQuery(
        selectColumns: String,
        withAuth: Boolean,
        withDeleted: Boolean,
        condition: String,
        sort: List<DbFindSort> = emptyList(),
        page: DbFindPage = DbFindPage.ALL,
        innerAuthSelect: Boolean = false
    ): String {

        val delCondition = if (hasDeletedFlag && !withDeleted) {
            "\"${DbEntity.DELETED}\"!=true"
        } else {
            ""
        }

        val query = StringBuilder()
        query.append("SELECT $selectColumns FROM ${tableRef.fullName} \"$RECORD_TABLE_ALIAS\"")

        if (withAuth) {

            if (innerAuthSelect) {
                joinWithAuthorities(query)
            }
            query.append(" WHERE ")
            if (innerAuthSelect) {
                val currentAuthCondition = getCurrentUserAuthoritiesCondition()
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
                    selectColumns = "\"$RECORD_TABLE_ALIAS\".\"id\"",
                    withAuth = withAuth,
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
        addSortAndPage(query, sort, page)

        return query.toString()
    }

    private fun addSortAndPage(query: StringBuilder, sort: List<DbFindSort>, page: DbFindPage) {

        if (sort.isNotEmpty()) {
            query.append(" ORDER BY ")
            val orderBy = sort.joinToString {
                "\"" + it.column + "\" " + if (it.ascending) {
                    "ASC"
                } else {
                    "DESC"
                }
            }
            query.append(orderBy)
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
                    val multiple = column.multiple && column.type != DbColumnType.JSON
                    typesConverter.convert(
                        value,
                        getParamTypeForColumn(column.type, multiple)
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

    private fun joinWithAuthorities(query: StringBuilder) {
        query.append(" INNER JOIN ${config.permsTable.fullName} \"$PERMS_TABLE_ALIAS\" ")
            .append("ON \"$RECORD_TABLE_ALIAS\".id=")
            .append("\"$PERMS_TABLE_ALIAS\".\"${DbPermsEntity.RECORD_ID}\" ")
        query.append("INNER JOIN ${config.authoritiesTable.fullName} \"$AUTHORITIES_TABLE_ALIAS\" ")
            .append("ON \"$PERMS_TABLE_ALIAS\".\"${DbPermsEntity.AUTHORITY_ID}\"=")
            .append("\"$AUTHORITIES_TABLE_ALIAS\".\"${DbAuthorityEntity.ID}\"")
    }

    private fun getCurrentUserAuthoritiesCondition(): String {

        val userAuthorities = AuthContext.getCurrentUserWithAuthorities().toMutableSet()
        userAuthorities.add(RoleConstants.ROLE_EVERYONE)

        val query = StringBuilder()
        query.append("\"$AUTHORITIES_TABLE_ALIAS\".\"${DbAuthorityEntity.EXT_ID}\" IN (")
        for (authority in userAuthorities) {
            query.append("'").append(authority).append("'").append(",")
        }
        query.setLength(query.length - 1)
        query.append(")")
        return query.toString()
    }

    private fun toSqlCondition(
        predicate: Predicate,
        queryParams: MutableList<Any?>,
        columnsByName: Map<String, DbColumnDef>
    ): String {
        val sb = StringBuilder()
        toSqlCondition(sb, predicate, queryParams, columnsByName)
        return sb.toString()
    }

    private fun toSqlCondition(
        query: StringBuilder,
        predicate: Predicate,
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
                    if (toSqlCondition(query, innerPred, queryParams, columnsByName)) {
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
                if (columnDef == null) {
                    query.append(ALWAYS_FALSE_CONDITION)
                    return true
                }
                // todo: add ability to search for multiple values fields
                if (columnDef.multiple) {
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
                        appendRecordColumnName(query, attribute)
                        query.append(" IN (")
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

                        appendRecordColumnName(query, attribute)
                        query.append(' ')
                            .append(operator)
                            .append(" ?")

                        if (value.isTextual() && type == ValuePredicate.Type.CONTAINS) {
                            queryParams.add("%" + value.asText() + "%")
                        } else if (value.isTextual() && columnDef.type == DbColumnType.UUID) {
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
                return true
            }
            is NotPredicate -> {
                val innerPredicate = predicate.getPredicate()
                if (innerPredicate is EmptyPredicate && !columnsByName.containsKey(innerPredicate.getAttribute())) {
                    query.append(ALWAYS_FALSE_CONDITION)
                    return true
                }
                query.append("NOT ")
                return if (toSqlCondition(query, predicate.getPredicate(), queryParams, columnsByName)) {
                    true
                } else {
                    query.setLength(query.length - 4)
                    false
                }
            }
            is EmptyPredicate -> {

                val columnDef = columnsByName[predicate.getAttribute()]
                if (columnDef == null || columnDef.multiple) {
                    return false
                }
                val attribute: String = predicate.getAttribute()
                if (columnDef.type == DbColumnType.TEXT) {
                    query.append("(")
                    appendRecordColumnName(query, attribute)
                    query.append(" IS NULL OR ")
                    appendRecordColumnName(query, attribute)
                    query.append("='')")
                } else {
                    query.append('"')
                    appendRecordColumnName(query, attribute)
                    query.append(" IS NULL")
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

    private fun updateSchemaCacheIfRequired() {
        if (schemaCacheUpdateRequired) {
            dataSource.updateSchema("DEALLOCATE ALL")
            schemaCacheUpdateRequired = false
        }
    }

    override fun getTableRef(): DbTableRef {
        return tableRef
    }

    private data class ValueForDb(
        val name: String,
        val placeholder: String,
        val value: Any?
    )
}
