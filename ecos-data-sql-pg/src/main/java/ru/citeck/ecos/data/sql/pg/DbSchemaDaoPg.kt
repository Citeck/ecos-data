package ru.citeck.ecos.data.sql.pg

import io.github.oshai.kotlinlogging.KotlinLogging
import ru.citeck.ecos.data.sql.datasource.DbDataSource
import ru.citeck.ecos.data.sql.dto.*
import ru.citeck.ecos.data.sql.dto.fk.DbFkConstraint
import ru.citeck.ecos.data.sql.dto.fk.FkCascadeActionOptions
import ru.citeck.ecos.data.sql.schema.DbSchemaDao
import ru.citeck.ecos.data.sql.schema.DbSchemaListener
import java.sql.SQLException
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList

open class DbSchemaDaoPg internal constructor() : DbSchemaDao {

    companion object {
        val log = KotlinLogging.logger {}

        const val COLUMN_TYPE_NAME = "TYPE_NAME"
        const val COLUMN_COLUMN_NAME = "COLUMN_NAME"

        // NAMEDATALEN - 1: PostgreSQL silently truncates longer identifiers, quoting doesn't help
        const val MAX_COLUMN_NAME_BYTES = 63

        /**
         * What [DbSchemaDao.estimateRowsCount] returns when this backend has no answer at all.
         */
        private const val UNKNOWN_ROWS_COUNT = -1L

        /**
         * How far [DbSchemaDao.estimateRowsCount] will count a table no statistics exist for at all.
         * Past this it gives up and answers "unknown": an estimate is only ever shown to an
         * administrator, and no display is worth a sequential scan of an arbitrarily large table.
         *
         * **Deliberately an order of magnitude below
         * [ru.citeck.ecos.data.sql.props.DbEcosDataProps.ColumnsProps.inPlaceAlterMaxRows] (100 000),
         * and it must stay there - do not re-align the two.** A table past that threshold is on the
         * size-triggered deferral path, which is the headline case of the background migration: its
         * `prepare` runs on every drain tick (ten seconds by default) for the whole life of the
         * migration, and for such a table a count bounded by the threshold itself is *guaranteed* to
         * exceed the cap and be thrown away. Equal caps therefore buy a full scan of the cap, every
         * tick, to produce "unknown". Keeping this well below the threshold bounds that to something
         * the deferral path never reaches.
         */
        private const val ESTIMATE_COUNT_LIMIT = 10_000L

        private val INDEXED_COLUMN_TYPES = setOf(
            DbColumnType.DATETIME,
            DbColumnType.DATE,
            DbColumnType.INT,
            DbColumnType.LONG,
            DbColumnType.DOUBLE,
            DbColumnType.TEXT,
            DbColumnType.BIGSERIAL,
            DbColumnType.BOOLEAN,
            DbColumnType.UUID
        )
        private val CONVERTABLE_TO_TEXT_TYPES = setOf(
            DbColumnType.JSON,
            DbColumnType.BOOLEAN,
            DbColumnType.DOUBLE,
            DbColumnType.INT,
            DbColumnType.LONG,
            DbColumnType.UUID
        )

        /**
         * PostgreSQL error codes which mean "your idea of this table's columns is out of date".
         *
         * - 42P01 undefined_table, 42703 undefined_column: the structural cases, previously matched
         *   by a regex over the message text.
         * - 42804 datatype_mismatch: `column "x" is of type bigint but expression is of type
         *   character varying` - a write against a column somebody else has already converted.
         * - 42883 undefined_function: `operator does not exist: bigint = character varying` - the
         *   same, seen from a predicate.
         */
        private val SCHEMA_MISMATCH_SQL_STATES = setOf("42P01", "42703", "42804", "42883")

        /**
         * Stands in for the quoted column name inside a conversion expression, so the expressions
         * can be written once and reused for any column.
         */
        private const val COLUMN = "\${column}"
    }

    private val listeners: MutableMap<String, MutableList<DbSchemaListener>> = ConcurrentHashMap()

    override fun addSchemaListener(schema: String, listener: DbSchemaListener) {
        listeners.computeIfAbsent(schema) { CopyOnWriteArrayList() }.add(listener)
    }

    override fun isTableExists(dataSource: DbDataSource, tableRef: DbTableRef): Boolean {
        val query = "SELECT EXISTS (SELECT FROM pg_tables WHERE schemaname = ? AND tablename = ?)"
        return dataSource.query(query, listOf(tableRef.schema, tableRef.table)) { rs ->
            rs.next()
            rs.getBoolean(1)
        }
    }

    override fun isSchemaExists(dataSource: DbDataSource, schema: String): Boolean {
        return dataSource.query(
            "SELECT EXISTS(SELECT 1 FROM pg_namespace WHERE nspname = ?)",
            listOf(schema)
        ) { it.next() && it.getBoolean(1) }
    }

    override fun getColumns(dataSource: DbDataSource, tableRef: DbTableRef): List<DbColumnDef> {

        return dataSource.withMetaData { metaData ->
            val schema = tableRef.schema.replace("_", "\\_").ifEmpty { "%" }
            val table = tableRef.table.replace("_", "\\_")
            metaData.getColumns(null, schema, table, "%")
                .use {
                    val columns = arrayListOf<DbColumnDef>()
                    while (it.next()) {
                        try {
                            val typeName = it.getString(COLUMN_TYPE_NAME)
                            val (type, multiple) = getColumnType(typeName)
                            columns.add(
                                DbColumnDef.create {
                                    withName(it.getString(COLUMN_COLUMN_NAME))
                                    withType(type)
                                    withMultiple(multiple)
                                }
                            )
                        } catch (e: Exception) {
                            log.warn { "Column error: '${it.getString(COLUMN_COLUMN_NAME)}' ${e.message}" }
                        }
                    }
                    columns
                }
        }
    }

    override fun createTable(dataSource: DbDataSource, tableRef: DbTableRef, columns: List<DbColumnDef>) {
        synchronized(tableRef) {
            createTableInSync(dataSource, tableRef, columns)
        }
    }

    private fun createTableInSync(dataSource: DbDataSource, tableRef: DbTableRef, columns: List<DbColumnDef>) {

        if (tableRef.schema.isNotBlank()) {
            if (!isSchemaExists(dataSource, tableRef.schema)) {
                dataSource.updateSchema("CREATE SCHEMA IF NOT EXISTS \"${tableRef.schema}\"")
                listeners[tableRef.schema]?.forEach { it.onSchemaCreated() }
            }
        }

        val queryBuilder = StringBuilder()

        queryBuilder.append("CREATE TABLE ${tableRef.fullName} (")
        columns.forEach {
            queryBuilder.append("\"").append(it.name).append("\" ")
                .append(getColumnSqlType(it.type, it.multiple))
                .append(getColumnSqlConstraintsWithSpaceIfNotEmpty(it.constraints))
                .append(",")
        }
        queryBuilder.setLength(queryBuilder.length - 1)
        queryBuilder.append(")")

        dataSource.updateSchema(queryBuilder.toString())

        columns.forEach {
            addColumnIndex(dataSource, tableRef, it)
        }
    }

    override fun addColumns(dataSource: DbDataSource, tableRef: DbTableRef, columns: List<DbColumnDef>) {
        synchronized(tableRef) {
            addColumnsInSync(dataSource, tableRef, columns)
        }
    }

    private fun addColumnsInSync(dataSource: DbDataSource, tableRef: DbTableRef, columns: List<DbColumnDef>) {

        if (columns.isEmpty()) {
            return
        }

        columns.forEach { column ->
            val query = "ALTER TABLE ${tableRef.fullName} " +
                "ADD COLUMN \"${column.name}\" " +
                getColumnSqlType(column.type, column.multiple) +
                getColumnSqlConstraintsWithSpaceIfNotEmpty(column.constraints)
            dataSource.updateSchema(query)
            addColumnIndex(dataSource, tableRef, column)
        }
    }

    private fun addColumnIndex(dataSource: DbDataSource, tableRef: DbTableRef, column: DbColumnDef) {

        if (!column.index.enabled || !INDEXED_COLUMN_TYPES.contains(column.type)) {
            return
        }
        val columnToIndex = if (!column.multiple && column.type == DbColumnType.TEXT) {
            "LOWER(\"${column.name}\")"
        } else {
            "\"${column.name}\""
        }
        val query = if (column.multiple) {
            "CREATE INDEX ON ${tableRef.fullName} USING GIN ($columnToIndex);"
        } else {
            "CREATE INDEX ON ${tableRef.fullName} ($columnToIndex);"
        }
        dataSource.updateSchema(query)
    }

    override fun setColumnType(
        dataSource: DbDataSource,
        tableRef: DbTableRef,
        name: String,
        multiple: Boolean,
        newType: DbColumnType
    ) {
        synchronized(tableRef) {
            setColumnTypeInSync(dataSource, tableRef, name, multiple, newType)
        }
    }

    private fun setColumnTypeInSync(
        dataSource: DbDataSource,
        tableRef: DbTableRef,
        name: String,
        multiple: Boolean,
        newType: DbColumnType
    ) {
        val current = getCurrentColumnType(dataSource, tableRef, name)
            ?: error("Column doesn't found in table '$tableRef' with name '$name'")
        val (currentType, currentMultiple) = current

        if (currentType == newType && currentMultiple == multiple) {
            return
        }
        val isArray = isArrayConversion(currentMultiple, multiple, newType)
        // resolved before any ALTER runs: an unsupported pair must fail before the widening step
        // below touches the column, not after it has already been mutated
        val conversion = if (currentType == newType) {
            null
        } else {
            getConversion(currentType, newType, isArray || currentMultiple)
                ?: error(
                    "Conversion of column '$name' in table '$tableRef' from type $currentType " +
                        "to $newType is not supported in place. Multiple flag: $currentMultiple -> $multiple"
                )
        }
        if (multiple && !currentMultiple && newType != DbColumnType.JSON) {
            // automatic conversion of an array to a single value column is not available.
            // it is not a huge problem because it affects only fuzzy searching
            dataSource.updateSchema(
                "ALTER TABLE ${tableRef.fullName} " +
                    "ALTER \"$name\" " +
                    "TYPE ${getColumnSqlType(currentType, true)} " +
                    "USING array[\"$name\"];"
            )
        }
        if (conversion == null) {
            return
        }
        conversion.sessionSettings.forEach { dataSource.updateSchema(it) }
        dataSource.updateSchema(
            "ALTER TABLE ${tableRef.fullName} " +
                "ALTER \"$name\" " +
                "TYPE ${getColumnSqlType(newType, isArray)} " +
                "USING ${conversion.expression.replace(COLUMN, "\"$name\"")};"
        )
    }

    private fun getCurrentColumnType(
        dataSource: DbDataSource,
        tableRef: DbTableRef,
        name: String
    ): Pair<DbColumnType, Boolean>? {
        return dataSource.withMetaData { metaData ->
            metaData.getColumns(null, tableRef.schema.ifEmpty { "%" }, tableRef.table, name).use { rs ->
                if (!rs.next()) {
                    null
                } else {
                    getColumnType(rs.getString(COLUMN_TYPE_NAME))
                }
            }
        }
    }

    override fun renameColumn(dataSource: DbDataSource, tableRef: DbTableRef, name: String, newName: String) {
        synchronized(tableRef) {
            if (getCurrentColumnType(dataSource, tableRef, name) == null) {
                log.debug { "Column '$name' doesn't exist in ${tableRef.fullName}, nothing to rename" }
                return
            }
            dropSingleColumnIndexes(dataSource, tableRef, name)
            dataSource.updateSchema(
                "ALTER TABLE ${tableRef.fullName} RENAME COLUMN \"$name\" TO \"$newName\";"
            )
        }
    }

    /**
     * Drops every index that covers this column and nothing else - [readSingleColumnIndexes] is
     * what finds them.
     *
     * Unique indexes are left in place. Attribute columns never get one - [DbEcosModelService]
     * always reports an empty constraint list - so a unique single-column index here belongs to an
     * entity mapping, and dropping someone else's uniqueness guarantee to save a little write cost
     * is not a trade this method is allowed to make.
     */
    private fun dropSingleColumnIndexes(dataSource: DbDataSource, tableRef: DbTableRef, name: String) {

        val indexesToDrop = readSingleColumnIndexes(dataSource, tableRef, name)
            .filter { !it.primary && !it.unique }
        indexesToDrop.forEach {
            dataSource.updateSchema("DROP INDEX \"${tableRef.schema}\".\"${it.name}\";")
        }
    }

    /**
     * Every index of [tableRef] that covers [column] and nothing else, as the catalog describes it.
     *
     * Indexes on attribute columns are created without a name (`CREATE INDEX ON ...`, see
     * [addColumnIndex]), so they have to be found through the catalog rather than by a naming
     * convention. Both plain and expression indexes count: a single-value TEXT column is indexed as
     * `LOWER("col")`, which the catalog records as an expression, not as a key column. Membership is
     * decided through `pg_depend`, the catalog's own per-column dependency record for both key and
     * expression indexes - not by deparsing the expression to text and searching it for the column
     * name, which false-positives whenever the column is named e.g. `text` or `lower` and another
     * column's expression index happens to contain that word as part of its cast or function call.
     *
     * Shared by [dropSingleColumnIndexes] and [createColumnIndexIfMissing] so that the two can never
     * disagree about what "this column already has an index" means - one of them dropping an index
     * the other would not recognise is how a column ends up with two, or with none.
     */
    private fun readSingleColumnIndexes(
        dataSource: DbDataSource,
        tableRef: DbTableRef,
        column: String
    ): List<SingleColumnIndex> {

        val query = """
            SELECT ic.relname AS index_name,
                   i.indisprimary AS is_primary,
                   i.indisunique AS is_unique,
                   i.indnatts AS natts,
                   EXISTS (
                       SELECT 1 FROM pg_depend d
                       WHERE d.classid = 'pg_class'::regclass
                         AND d.objid = i.indexrelid
                         AND d.refclassid = 'pg_class'::regclass
                         AND d.refobjid = c.oid
                         AND d.refobjsubid = a.attnum
                   ) AS mentions_column
            FROM pg_index i
            JOIN pg_class c ON c.oid = i.indrelid
            JOIN pg_class ic ON ic.oid = i.indexrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            JOIN pg_attribute a ON a.attrelid = c.oid AND a.attname = ?
            WHERE n.nspname = ? AND c.relname = ?
        """.trimIndent()

        return dataSource.query(query, listOf(column, tableRef.schema, tableRef.table)) { rs ->
            val result = ArrayList<SingleColumnIndex>()
            while (rs.next()) {
                if (rs.getInt("natts") == 1 && rs.getBoolean("mentions_column")) {
                    result.add(
                        SingleColumnIndex(
                            rs.getString("index_name"),
                            rs.getBoolean("is_primary"),
                            rs.getBoolean("is_unique")
                        )
                    )
                }
            }
            result
        }
    }

    private class SingleColumnIndex(val name: String, val primary: Boolean, val unique: Boolean)

    override fun createColumnIndexIfMissing(dataSource: DbDataSource, tableRef: DbTableRef, column: DbColumnDef) {
        synchronized(tableRef) {
            // Indexes on attribute columns are created without a name, so `CREATE INDEX IF NOT
            // EXISTS` has nothing to compare against - asked twice it would simply build a second,
            // identically shaped index. The catalog is the only thing that can answer this.
            if (readSingleColumnIndexes(dataSource, tableRef, column.name).isNotEmpty()) {
                log.debug {
                    "Column '${column.name}' of ${tableRef.fullName} is already indexed, " +
                        "nothing to build"
                }
                return
            }
            addColumnIndex(dataSource, tableRef, column)
        }
    }

    override fun isRowsCountGreaterThan(dataSource: DbDataSource, tableRef: DbTableRef, limit: Long): Boolean {

        // null, not 0, when the table itself is not there: a missing table has nothing to count,
        // and running the bounded-count query below against it would just throw. The in-memory
        // backend already answers false for a missing table; this keeps the two backends agreeing
        // without the caller needing to know which one it is talking to.
        val estimate = readReltuples(dataSource, tableRef) ?: return false
        if (estimate > limit) {
            // a stale-high estimate only ever pushes the caller toward the more conservative
            // (background-migration) strategy, so trusting it is safe in the one direction that
            // matters here; a stale-low one proves nothing, which is exactly why the bounded count
            // below exists.
            return true
        }
        // reltuples is -1 for a never-analyzed table and 0 on older majors, so a low estimate proves
        // nothing. Counting up to limit + 1 rows answers exactly, in time bounded by the limit.
        return countUpTo(dataSource, tableRef, limit) > limit
    }

    /**
     * Three sources, cheapest first, and only the last one reads a heap page.
     *
     * 1. `pg_class.reltuples`, the planner's estimate - a catalog read.
     * 2. `pg_stat_all_tables.n_live_tup` when the planner has none. `reltuples` is -1 for a table
     *    that has never been analyzed or vacuumed (0 on older majors), which is the normal state of
     *    a freshly bulk-loaded table - exactly the table a size-triggered migration is deferred for.
     *    The statistics collector maintains `n_live_tup` from the inserts and deletes themselves,
     *    without ANALYZE, and reading it is another catalog/statistics read rather than a scan.
     * 3. A count bounded by [ESTIMATE_COUNT_LIMIT], for a table with no statistics at all. That is
     *    usually a small one - brand new, or with its statistics reset - but it is also a large table
     *    for the first tick or few after a bulk load, because the statistics collector reports
     *    `n_live_tup` asynchronously and is briefly behind the commit. The bound is what makes that
     *    window affordable; see that constant for why it is far below the deferral threshold rather
     *    than equal to it.
     */
    override fun estimateRowsCount(dataSource: DbDataSource, tableRef: DbTableRef): Long {

        val estimate = readReltuples(dataSource, tableRef) ?: return UNKNOWN_ROWS_COUNT
        if (estimate > 0) {
            return estimate
        }
        val liveTuples = readLiveTuples(dataSource, tableRef)
        if (liveTuples > 0) {
            return liveTuples
        }
        val counted = countUpTo(dataSource, tableRef, ESTIMATE_COUNT_LIMIT)
        return if (counted > ESTIMATE_COUNT_LIMIT) UNKNOWN_ROWS_COUNT else counted
    }

    /**
     * The statistics collector's live-row count for the table, or 0 when it has never reported on it.
     * `pg_stat_all_tables` rather than `pg_stat_user_tables` so that a table in a system schema is
     * answered for too.
     */
    private fun readLiveTuples(dataSource: DbDataSource, tableRef: DbTableRef): Long {
        return dataSource.query(
            "SELECT n_live_tup FROM pg_stat_all_tables WHERE schemaname = ? AND relname = ?",
            listOf(tableRef.schema, tableRef.table)
        ) { rs ->
            if (rs.next()) rs.getLong("n_live_tup") else 0L
        }
    }

    /**
     * The planner's row estimate for the table, or null when the table is not in `pg_class` at all.
     * The value itself may be -1 ("never analyzed") or stale in either direction - every caller has
     * to decide what to do about that for itself.
     */
    private fun readReltuples(dataSource: DbDataSource, tableRef: DbTableRef): Long? {
        return dataSource.query(
            "SELECT c.reltuples::bigint AS estimate FROM pg_class c " +
                "JOIN pg_namespace n ON n.oid = c.relnamespace " +
                "WHERE n.nspname = ? AND c.relname = ?",
            listOf(tableRef.schema, tableRef.table)
        ) { rs ->
            if (rs.next()) rs.getLong("estimate") else null
        }
    }

    /**
     * How many rows the table holds, counted no further than [limit] + 1. A result of [limit] + 1
     * means "more than [limit]" and nothing more precise; anything below is exact.
     */
    private fun countUpTo(dataSource: DbDataSource, tableRef: DbTableRef, limit: Long): Long {
        return dataSource.query(
            "SELECT count(*) AS cnt FROM (SELECT 1 FROM ${tableRef.fullName} LIMIT ${limit + 1}) probe",
            emptyList()
        ) { rs ->
            rs.next()
            rs.getLong("cnt")
        }
    }

    override fun isTypeChangeSupported(currentColumn: DbColumnDef, targetColumn: DbColumnDef): Boolean {
        if (currentColumn.type == targetColumn.type) {
            // same physical type: either nothing to do, or the scalar -> array widening, which is
            // always expressible as array[x]. Narrowing an array to a scalar is the one case this
            // answer is not about: the general layer classifies it as class C and routes it to the
            // shadow column and the background transfer before ever asking this. `true` is still
            // the right answer for it, because it is what keeps a narrowing off the "not supported
            // by the backend" error branch - the reason it is deferred is the conversion class, not
            // a missing expression.
            return true
        }
        val isArray = isArrayConversion(currentColumn.multiple, targetColumn.multiple, targetColumn.type)
        return getConversion(currentColumn.type, targetColumn.type, isArray || currentColumn.multiple) != null
    }

    /**
     * Whether a conversion between the given multiple-flags and target type has to be treated as
     * an array cast rather than a scalar one.
     *
     * Shared by [isTypeChangeSupported] and [setColumnTypeInSync] so the two can never compute
     * this differently: a probe and a mutator that disagree on array-ness is worse than either
     * being wrong alone, because nothing downstream can tell which one lied.
     *
     * JSON is the one type this always excludes: [getColumnSqlType] never emits `JSONB[]`, so a
     * JSON-typed column is always physically scalar regardless of its logical multiple flag, and
     * there is no array shape to cast into or out of in place.
     */
    private fun isArrayConversion(currentMultiple: Boolean, targetMultiple: Boolean, newType: DbColumnType): Boolean {
        return (currentMultiple || targetMultiple) && newType != DbColumnType.JSON
    }

    /**
     * The expression that turns a [from] column into a [to] column in place, or null when this
     * backend has none for the pair and the caller has to fall back to a new column plus a
     * background transfer.
     *
     * [isArray] means both sides are arrays. It is not a cosmetic difference: `ALTER ... USING` is
     * not array-aware, and PostgreSQL forbids a subquery in a transform expression
     * (`cannot use subquery in transform expression`), so `ARRAY(SELECT ... FROM unnest(...))` is
     * unavailable here. Only conversions expressible as a plain cast survive the array case; the
     * ones built from functions (`to_char`) are single-value only.
     */
    private fun getConversion(from: DbColumnType, to: DbColumnType, isArray: Boolean): ColumnConversion? {

        if (isArray && from == DbColumnType.JSON) {
            // a JSON-typed column is always physically scalar (see isArrayConversion), so there is
            // no in-place widening from it into an array of anything - that reshaping needs the
            // shadow column and row-wise transfer, same as the TEXT[] -> JSON class C case below.
            return null
        }
        if (to == DbColumnType.JSON) {
            // the physical target is a scalar JSONB whatever the model says, so only a scalar
            // source can be cast into it. An array source is lossy and takes the shadow column.
            if (isArray || from != DbColumnType.TEXT) {
                return null
            }
            return ColumnConversion("$COLUMN::jsonb")
        }
        if (to == DbColumnType.TEXT) {
            if (CONVERTABLE_TO_TEXT_TYPES.contains(from)) {
                val expression = if (isArray) "$COLUMN::text[]" else "$COLUMN::text"
                if (from == DbColumnType.DOUBLE) {
                    // float8out renders the shortest round-trip digits only while extra_float_digits
                    // is at least 1. That is the default from PostgreSQL 12 on, but a deployment can
                    // move it - postgresql.conf, ALTER DATABASE/ROLE SET, a pooler's startup packet -
                    // and at 0 or below the server caps the output at 15 significant digits, so the
                    // same value that DbDoubleText renders as 1.7976931348623157e+308 comes out of
                    // this cast as 1.79769313486232e+308. Every value needing 16 or more significant
                    // digits would then depend on the table's row count for its text, which is
                    // exactly what the two paths agreeing forbids. SET LOCAL pins it for this transaction, the same
                    // idiom - and the same deliberate outliving of this one statement - as the
                    // date[] case below.
                    return ColumnConversion(expression, listOf("SET LOCAL extra_float_digits TO 1;"))
                }
                return ColumnConversion(expression)
            }
            if (isArray) {
                // to_char is not array-aware, see the doc of this method
                return null
            }
            // a bare ::text would follow the session's DateStyle / TimeZone, so both formats are
            // spelled out. They match what the platform writes and reads for these attribute types.
            if (from == DbColumnType.DATE) {
                return ColumnConversion("to_char($COLUMN, 'YYYY-MM-DD')")
            }
            if (from == DbColumnType.DATETIME) {
                // to_char has no fractional-seconds field and would silently truncate sub-second
                // precision, which a conversion classified as lossless is not allowed to do.
                // to_json's date/time rendering is always ISO 8601 regardless of DateStyle, keeps
                // fractional digits when present and trims them when absent, and never appends a
                // zone suffix for a bare timestamp - hence the manual 'Z' once AT TIME ZONE 'UTC'
                // has normalized the value.
                return ColumnConversion("(to_json($COLUMN AT TIME ZONE 'UTC') #>> '{}') || 'Z'")
            }
            return null
        }
        if (from == DbColumnType.DATE && to == DbColumnType.DATETIME) {
            if (!isArray) {
                return ColumnConversion("$COLUMN::timestamp AT TIME ZONE 'UTC'")
            }
            // date[] -> timestamptz[] is a plain cast, so it works on arrays - but the cast reads
            // the session time zone. SET LOCAL pins it for this transaction only; there is no safe
            // way to restore the caller's previous zone afterward (RESET goes to the server
            // default, not to whatever the caller had), so the setting deliberately outlives this
            // statement for the rest of the enclosing transaction. That is acceptable because every
            // other expression this method emits pins UTC explicitly too, so a UTC session for the
            // remainder of a schema migration is harmless.
            return ColumnConversion(
                "$COLUMN::timestamptz[]",
                listOf("SET LOCAL TimeZone TO 'UTC';")
            )
        }
        return null
    }

    /**
     * A conversion expression plus the session settings that have to precede it in the same
     * transaction for the expression to be deterministic.
     */
    private class ColumnConversion(
        val expression: String,
        val sessionSettings: List<String> = emptyList()
    )

    override fun createFkConstraints(
        dataSource: DbDataSource,
        tableRef: DbTableRef,
        constraints: List<DbFkConstraint>
    ) {

        constraints.forEach { constraint ->

            if (constraint.name.isBlank()) {
                error("Constraint name is missing: $constraint")
            }

            val query = StringBuilder()
            query.append("ALTER TABLE ")
                .append(tableRef.fullName)
                .append(" ADD CONSTRAINT \"")
                .append(constraint.name)
                .append("\" FOREIGN KEY (\"")
                .append(constraint.baseColumnName)
                .append("\") REFERENCES ")
                .append(constraint.referencedTable.fullName)
                .append(" (\"")
                .append(constraint.referencedColumn)
                .append("\")")

            if (constraint.onDelete != FkCascadeActionOptions.NO_ACTION) {
                val action = constraint.onDelete.name.replace("_", " ")
                query.append(" ON DELETE ").append(action)
            }
            if (constraint.onUpdate != FkCascadeActionOptions.NO_ACTION) {
                val action = constraint.onUpdate.name.replace("_", " ")
                query.append(" ON UPDATE ").append(action)
            }
            query.append(";")

            dataSource.updateSchema(query.toString())
        }
    }

    override fun setColumnConstraints(
        dataSource: DbDataSource,
        tableRef: DbTableRef,
        columnName: String,
        constraints: List<DbColumnConstraint>
    ) {

        for (constraint in constraints) {

            val query = StringBuilder()
            query.append("ALTER TABLE ")
                .append(tableRef.fullName)
                .append(" ALTER COLUMN \"")
                .append(columnName)
                .append("\" SET ")
                .append(getColumnSqlConstraint(constraint))

            dataSource.updateSchema(query.toString())
        }
    }

    override fun createIndexes(dataSource: DbDataSource, tableRef: DbTableRef, indexes: List<DbIndexDef>) {

        indexes.forEach { index ->

            val query = StringBuilder()
            query.append("CREATE ")
            if (index.unique) {
                query.append("UNIQUE ")
            }
            query.append("INDEX ")
            if (index.name.isNotBlank()) {
                query.append("\"")
                    .append(index.name)
                    .append("\" ")
            }
            query.append("ON ${tableRef.fullName} (")
            query.append(
                index.columns.joinToString(",") {
                    if (!index.caseInsensitive) {
                        "\"$it\""
                    } else {
                        "LOWER(\"$it\")"
                    }
                }
            )
            query.append(")")

            dataSource.updateSchema(query.toString())
        }
    }

    override fun resetCache(dataSource: DbDataSource, tableRef: DbTableRef) {
        dataSource.updateSchema("DEALLOCATE ALL")
    }

    override fun getMaxColumnNameBytes(): Int {
        return MAX_COLUMN_NAME_BYTES
    }

    override fun isSchemaMismatchError(exception: Throwable): Boolean {
        var current: Throwable? = exception
        var depth = 0
        while (current != null && depth++ < 20) {
            if (current is SQLException) {
                var sqlException: SQLException? = current
                var chainDepth = 0
                while (sqlException != null && chainDepth++ < 20) {
                    if (SCHEMA_MISMATCH_SQL_STATES.contains(sqlException.sqlState)) {
                        return true
                    }
                    sqlException = sqlException.nextException
                }
            }
            current = current.cause
        }
        return false
    }

    private fun getColumnType(fullTypeName: String): Pair<DbColumnType, Boolean> {

        val (typeName, multiple) = if (fullTypeName[0] == '_') {
            fullTypeName.substring(1) to true
        } else {
            fullTypeName to false
        }

        return when (typeName) {
            "bigserial" -> DbColumnType.BIGSERIAL
            "int4" -> DbColumnType.INT
            "float8" -> DbColumnType.DOUBLE
            "bool" -> DbColumnType.BOOLEAN
            "date" -> DbColumnType.DATE
            "timestamp" -> DbColumnType.DATETIME
            "timestamptz" -> DbColumnType.DATETIME
            "int8" -> DbColumnType.LONG
            "jsonb" -> DbColumnType.JSON
            "varchar" -> DbColumnType.TEXT
            "bytea" -> DbColumnType.BINARY
            "uuid" -> DbColumnType.UUID
            else -> error("Unknown type: $typeName")
        } to multiple
    }

    private fun getColumnSqlConstraintsWithSpaceIfNotEmpty(constraints: List<DbColumnConstraint>): String {
        if (constraints.isEmpty()) {
            return ""
        }
        return " " + constraints.joinToString(" ") {
            getColumnSqlConstraint(it)
        }
    }

    private fun getColumnSqlConstraint(constraint: DbColumnConstraint): String {
        return when (constraint) {
            DbColumnConstraint.NOT_NULL -> "NOT NULL"
            DbColumnConstraint.PRIMARY_KEY -> "PRIMARY KEY"
            DbColumnConstraint.UNIQUE -> "UNIQUE"
        }
    }

    private fun getColumnSqlType(type: DbColumnType, multiple: Boolean): String {

        val baseType = when (type) {
            DbColumnType.BIGSERIAL -> "BIGSERIAL"
            DbColumnType.INT -> "INT"
            DbColumnType.DOUBLE -> "DOUBLE PRECISION"
            DbColumnType.BOOLEAN -> "BOOLEAN"
            DbColumnType.DATE -> "DATE"
            DbColumnType.DATETIME -> "TIMESTAMPTZ"
            DbColumnType.LONG -> "BIGINT"
            DbColumnType.JSON -> "JSONB"
            DbColumnType.TEXT -> "VARCHAR"
            DbColumnType.BINARY -> "BYTEA"
            DbColumnType.UUID -> "UUID"
        }
        return if (multiple && type != DbColumnType.JSON) {
            "$baseType[]"
        } else {
            baseType
        }
    }
}
