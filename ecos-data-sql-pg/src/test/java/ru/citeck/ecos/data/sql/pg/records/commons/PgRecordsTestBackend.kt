package ru.citeck.ecos.data.sql.pg.records.commons

import org.apache.commons.dbcp2.BasicDataSource
import org.apache.commons.dbcp2.managed.BasicManagedDataSource
import ru.citeck.ecos.data.sql.datasource.DbDataSource
import ru.citeck.ecos.data.sql.datasource.DbDataSourceImpl
import ru.citeck.ecos.data.sql.dto.DbTableRef
import ru.citeck.ecos.data.sql.pg.PgDataServiceFactory
import ru.citeck.ecos.data.sql.service.DbDataServiceFactory
import ru.citeck.ecos.data.sql.test.records.DbRecordsTestBackend
import ru.citeck.ecos.data.sql.test.records.DbRecordsTestBackendFactory
import ru.citeck.ecos.data.sql.test.records.DbTestSchemaCache
import ru.citeck.ecos.test.commons.EcosWebAppApiMock
import ru.citeck.ecos.test.commons.containers.TestContainers
import ru.citeck.ecos.txn.lib.TxnContext
import ru.citeck.ecos.txn.lib.resource.type.xa.JavaXaTxnManagerAdapter
import ru.citeck.ecos.webapp.api.datasource.JdbcDataSource
import java.sql.Connection

/**
 * PostgreSQL [DbRecordsTestBackend], registered as `pg` (the default). This holds exactly the
 * Testcontainers + DBCP managed XA datasource + [PgDataServiceFactory] wiring that used to live
 * inline in [DbRecordsTestBase], so the historical PG run is reproduced byte-for-byte.
 */
class PgRecordsTestBackend(webAppApi: EcosWebAppApiMock) : DbRecordsTestBackend {

    private val dataSource: BasicDataSource

    override val dbDataSource: DbDataSource
    override val dataServiceFactory: DbDataServiceFactory = PgDataServiceFactory()

    companion object {
        private const val COLUMN_TYPE_NAME = "TYPE_NAME"
        private const val COLUMN_COLUMN_NAME = "COLUMN_NAME"
        private const val COLUMN_TABLE_SCHEMA = "TABLE_SCHEM"
        private const val COLUMN_TABLE_NAME = "TABLE_NAME"

        /**
         * What a pristine, freshly migrated suite schema looks like, or null when there is nothing
         * cached and the next [snapshotPristineSchema] should capture it again.
         *
         * JVM-static on purpose. The suite builds a new [PgRecordsTestBackend] - and a new
         * connection pool - for every single test method, so per-instance state would be worthless;
         * what survives between tests is the database inside the shared Testcontainers container,
         * and this is the in-JVM description of it.
         */
        @Volatile
        private var pristine: PristineSchema? = null

        init {
            // Lets @RequiresFreshSchema reach the cached database even from a test class that
            // never builds a DataMockFactory (the schema-migration suites build their own data
            // source). Registered on class load, i.e. the first time a PG backend is needed at all.
            DbTestSchemaCache.registerCacheInvalidator { invalidateCache() }
        }

        /**
         * Physically empties the suite's database and forgets the cache. Uses the container's own
         * plain data source rather than any pool, because this runs from a JUnit
         * `beforeAll`/`afterAll` where no test's data source exists.
         *
         * Deliberately unconditional. Returning early on `pristine == null` would be correct only
         * while every *other* path that nulls the cache also empties the database - an invariant
         * spread over three call sites and true today by convention rather than by construction.
         * The caller asks for an empty database; give it one. The cost is one catalog query per
         * `@RequiresFreshSchema` class boundary.
         */
        private fun invalidateCache() {
            pristine = null
            TestContainers.getPostgres().getDataSource().connection.use { conn ->
                val tables = listTables(conn)
                if (tables.isNotEmpty()) {
                    val dropCommand = "DROP TABLE " + tables.joinToString(",") { it.quoted } + " CASCADE"
                    println("EXEC: $dropCommand")
                    conn.createStatement().use { it.executeUpdate(dropCommand) }
                }
                dropOrphanSequences(conn)
            }
        }

        private fun listTables(conn: Connection): List<TableId> {
            val result = ArrayList<TableId>()
            conn.prepareStatement(
                """
                SELECT n.nspname, c.relname FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE c.relkind = 'r'
                  AND n.nspname NOT IN ('pg_catalog', 'information_schema')
                  AND n.nspname NOT LIKE 'pg_%'
                ORDER BY n.nspname, c.relname
                """.trimIndent()
            ).use { stmt ->
                stmt.executeQuery().use { res ->
                    while (res.next()) {
                        result.add(TableId(res.getString(1), res.getString(2)))
                    }
                }
            }
            return result
        }

        /**
         * Every sequence of the whole database, schema-qualified. Same sweep as [listTables] and
         * for the same reason: nothing is listed in code, so a sequence ecos-data starts creating
         * tomorrow is covered the day it appears.
         */
        private fun listSequences(conn: Connection): List<TableId> {
            val result = ArrayList<TableId>()
            conn.prepareStatement(
                """
                SELECT n.nspname, c.relname FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE c.relkind = 'S'
                  AND n.nspname NOT IN ('pg_catalog', 'information_schema')
                  AND n.nspname NOT LIKE 'pg_%'
                ORDER BY n.nspname, c.relname
                """.trimIndent()
            ).use { stmt ->
                stmt.executeQuery().use { res ->
                    while (res.next()) {
                        result.add(TableId(res.getString(1), res.getString(2)))
                    }
                }
            }
            return result
        }

        /**
         * Drops whatever sequences survived a table sweep. A sequence owned by a column - which is
         * what `BIGSERIAL` produces, and that is every sequence ecos-data creates today - goes away
         * with its table, so on a healthy run this finds nothing. One that is *not* owned by a
         * column does not: `DROP TABLE` has no reason to touch it.
         *
         * That difference is the whole point. "Drop every table" was only ever a stand-in for
         * "leave the database as empty as a fresh one", and the schema cache turned the gap between
         * the two into a correctness problem: the drop path is also the cache's fallback, and the
         * very next test re-captures whatever it left behind *as the pristine baseline*, which then
         * outlives every later test in the JVM. Call this wherever the intent is an empty database.
         */
        private fun dropOrphanSequences(conn: Connection) {
            val sequences = listSequences(conn)
            if (sequences.isEmpty()) {
                return
            }
            val dropCommand = "DROP SEQUENCE " + sequences.joinToString(",") { it.quoted } + " CASCADE"
            println("EXEC: $dropCommand")
            conn.createStatement().use { it.executeUpdate(dropCommand) }
        }

        /**
         * Every column of every table of one schema, in physical order, with the two properties a
         * structural comparison has to include beyond the name: the formatted SQL type (so a type
         * change is seen) and the default expression (so losing or gaining an identity default is
         * seen). One query for the whole schema - the point of the cache is to replace ~100 DDL
         * statements per test with a handful of catalog reads, so the reads have to stay few.
         */
        private fun readColumns(conn: Connection, schema: String): Map<String, List<ColumnInfo>> {
            val result = LinkedHashMap<String, MutableList<ColumnInfo>>()
            conn.prepareStatement(
                """
                SELECT c.relname,
                       a.attname,
                       format_type(a.atttypid, a.atttypmod),
                       a.attnotnull,
                       COALESCE(pg_get_expr(d.adbin, d.adrelid), '')
                FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped
                LEFT JOIN pg_attrdef d ON d.adrelid = c.oid AND d.adnum = a.attnum
                WHERE n.nspname = ? AND c.relkind = 'r'
                ORDER BY c.relname, a.attnum
                """.trimIndent()
            ).use { stmt ->
                stmt.setString(1, schema)
                stmt.executeQuery().use { res ->
                    while (res.next()) {
                        result.computeIfAbsent(res.getString(1)) { ArrayList() }.add(
                            ColumnInfo(res.getString(2), res.getString(3), res.getBoolean(4), res.getString(5))
                        )
                    }
                }
            }
            return result
        }

        /**
         * Index definitions per table. Columns alone would not notice a test that only added or
         * dropped an index - and the suite has tests which do exactly that (unique constraints on
         * attributes) - so the structural fingerprint includes them.
         */
        private fun readIndexes(conn: Connection, schema: String): Map<String, List<String>> {
            val result = LinkedHashMap<String, MutableList<String>>()
            conn.prepareStatement(
                "SELECT tablename, indexdef FROM pg_indexes WHERE schemaname = ? ORDER BY tablename, indexdef"
            ).use { stmt ->
                stmt.setString(1, schema)
                stmt.executeQuery().use { res ->
                    while (res.next()) {
                        result.computeIfAbsent(res.getString(1)) { ArrayList() }.add(res.getString(2))
                    }
                }
            }
            return result
        }

        /**
         * Every sequence of the database with the exact position of its counter.
         *
         * Discovered from the catalog like everything else here, and deliberately *not* derived
         * from the tables: `TRUNCATE ... RESTART IDENTITY` only resets the sequences owned by the
         * tables it truncates, so a sequence that is not owned by one - a plain `CREATE SEQUENCE`,
         * or one whose owning column was dropped - would keep drifting and hand the next test ids
         * a fresh database would never produce. Sweeping `pg_class.relkind = 'S'` covers both kinds
         * and needs no edit when a new one appears.
         */
        private fun readSequences(conn: Connection): Map<TableId, SequenceState> {
            val names = listSequences(conn)
            if (names.isEmpty()) {
                return emptyMap()
            }
            // one round trip for all of them: SELECT (SELECT last_value, is_called FROM seq) x N
            val query = names.joinToString(" UNION ALL ") {
                val key = (it.schema + "." + it.table).replace("'", "''")
                "SELECT '" + key + "' AS name, last_value, is_called FROM " + it.quoted
            }
            val byKey = names.associateBy { it.schema + "." + it.table }
            val result = LinkedHashMap<TableId, SequenceState>()
            conn.createStatement().use { stmt ->
                stmt.executeQuery(query).use { res ->
                    while (res.next()) {
                        result[byKey.getValue(res.getString(1))] =
                            SequenceState(res.getLong(2), res.getBoolean(3))
                    }
                }
            }
            return result
        }

        private fun fingerprint(columns: List<ColumnInfo>?, indexes: List<String>?): String {
            val cols = (columns ?: emptyList()).joinToString(";") {
                it.name + "|" + it.type + "|" + it.notNull + "|" + it.default
            }
            return cols + "||" + (indexes ?: emptyList()).joinToString(";")
        }
    }

    private data class TableId(val schema: String, val table: String) {
        val quoted: String
            get() = "\"$schema\".\"$table\""
    }

    private data class SequenceState(val lastValue: Long, val isCalled: Boolean)

    private data class ColumnInfo(
        val name: String,
        val type: String,
        val notNull: Boolean,
        val default: String
    )

    /**
     * The rows one cached table holds when the schema is pristine, as text.
     *
     * Text, not JDBC objects, deliberately: PostgreSQL can render and parse every type in these
     * tables losslessly through its own I/O functions, and re-inserting through an explicit
     * `?::<type>` cast makes both the value and the NULL unambiguous without this code having to
     * own a JDBC type mapping it would get wrong for exactly one column somewhere.
     */
    private class TableRows(
        val columns: List<ColumnInfo>,
        val rows: List<List<String?>>
    )

    private class PristineSchema(
        val schema: String,
        val fingerprints: Map<String, String>,
        val rows: Map<String, TableRows>,
        val sequences: Map<TableId, SequenceState>
    )

    init {
        val managedDataSource = BasicManagedDataSource()
        managedDataSource.transactionManager = JavaXaTxnManagerAdapter(webAppApi.getProperties())
        managedDataSource.xaDataSourceInstance = TestContainers.getPostgres().getXaDataSource()
        managedDataSource.defaultAutoCommit = false
        managedDataSource.autoCommitOnReturn = false
        this.dataSource = managedDataSource

        val jdbcDataSource = object : JdbcDataSource {
            override fun getKey() = "key"
            override fun getJavaDataSource() = managedDataSource
            override fun isManaged() = true
        }
        this.dbDataSource = DbDataSourceImpl(jdbcDataSource)
    }

    override fun close() {
        dataSource.close()
    }

    override fun dropAllTables() {
        TxnContext.doInTxn {
            dataSource.connection.use { conn ->
                val tables = ArrayList<String>()
                conn.metaData.getTables(null, null, "%", arrayOf("TABLE")).use { res ->
                    while (res.next()) {
                        tables.add("\"${res.getString("TABLE_SCHEM")}\".\"${res.getString("TABLE_NAME")}\"")
                    }
                }
                if (tables.isNotEmpty()) {
                    val dropCommand = "DROP TABLE " + tables.joinToString(",") + " CASCADE"
                    println("EXEC: $dropCommand")
                    conn.createStatement().use { it.executeUpdate(dropCommand) }
                    conn.createStatement().use { it.executeUpdate("DEALLOCATE ALL") }
                }
                dropOrphanSequences(conn)
            }
        }
    }

    override fun snapshotPristineSchema(schema: String) {
        if (pristine != null) {
            return
        }
        TxnContext.doInTxn {
            dataSource.connection.use { conn ->
                val columns = readColumns(conn, schema)
                val indexes = readIndexes(conn, schema)
                val fingerprints = columns.keys.associateWith { fingerprint(columns[it], indexes[it]) }
                val rows = columns.keys.associateWith { table ->
                    TableRows(columns.getValue(table), readRows(conn, schema, table, columns.getValue(table)))
                }
                pristine = PristineSchema(schema, fingerprints, rows, readSequences(conn))
            }
        }
    }

    /**
     * The cheap half of the cache: instead of dropping the schema so the next test can spend ~0.5 s
     * building it again, put the data back the way a freshly built schema has it.
     *
     * Three things have to be true afterwards, and each is handled explicitly below:
     *  1. **No table exists that a fresh schema would not have.** Every table outside the cached
     *     set - the suite's four domain tables, anything a test created itself, anything in another
     *     schema - is dropped. The cached set is exactly what `runSchemaMigrations` builds, every
     *     `ed_*` table included, so it does not depend on which test happened to run first.
     *  2. **The cached tables are structurally untouched.** Their fingerprint is compared against
     *     the pristine one; the suite's whole subject is adding, retyping and renaming columns, so
     *     this is not hypothetical. Any mismatch - or a cached table that has gone missing - falls
     *     back to the full [dropAllTables] and forgets the cache, which costs the next test the
     *     old rebuild and nothing more.
     *  3. **Their content is the pristine content, ids included.** TRUNCATE ... RESTART IDENTITY
     *     clears both rows and identity counters; the rows ecos-data itself wrote while creating
     *     the schema (the schema version in `ed_schema_meta`, the column registry in
     *     `ed_column_meta`) are then re-inserted verbatim and the identity counters advanced past
     *     them, which is exactly where a fresh schema leaves them.
     */
    override fun resetForNextTest() {
        val snapshot = pristine
        if (snapshot == null) {
            dropAllTables()
            return
        }
        var cacheIsStale = false
        TxnContext.doInTxn {
            dataSource.connection.use { conn ->
                val columns = readColumns(conn, snapshot.schema)
                val indexes = readIndexes(conn, snapshot.schema)
                val toDrop = ArrayList<String>()
                for (table in listTables(conn)) {
                    if (table.schema == snapshot.schema && snapshot.fingerprints.containsKey(table.table)) {
                        if (snapshot.fingerprints[table.table] !=
                            fingerprint(columns[table.table], indexes[table.table])
                        ) {
                            cacheIsStale = true
                        }
                    } else {
                        toDrop.add(table.quoted)
                    }
                }
                if (!columns.keys.containsAll(snapshot.fingerprints.keys)) {
                    // a cached table was dropped by the test itself (dropAllTables() is public API
                    // of the factory and a few tests use it mid-test)
                    cacheIsStale = true
                }
                if (cacheIsStale) {
                    return@use
                }
                if (toDrop.isNotEmpty()) {
                    val dropCommand = "DROP TABLE " + toDrop.joinToString(",") + " CASCADE"
                    println("EXEC: $dropCommand")
                    conn.createStatement().use { it.executeUpdate(dropCommand) }
                }
                val cached = snapshot.fingerprints.keys.map { "\"${snapshot.schema}\".\"$it\"" }
                val truncCommand = "TRUNCATE TABLE " + cached.joinToString(",") + " RESTART IDENTITY CASCADE"
                println("EXEC: $truncCommand")
                conn.createStatement().use { it.executeUpdate(truncCommand) }
                restorePristineRows(conn, snapshot)
                if (!restorePristineSequences(conn, snapshot)) {
                    cacheIsStale = true
                    return@use
                }
                conn.createStatement().use { it.executeUpdate("DEALLOCATE ALL") }
            }
        }
        if (cacheIsStale) {
            println("EXEC: schema cache invalidated - the schema is no longer structurally pristine")
            pristine = null
            dropAllTables()
        }
    }

    private fun readRows(
        conn: Connection,
        schema: String,
        table: String,
        columns: List<ColumnInfo>
    ): List<List<String?>> {
        val rows = ArrayList<List<String?>>()
        val columnList = columns.joinToString(",") { "\"${it.name}\"" }
        conn.createStatement().use { stmt ->
            stmt.executeQuery("SELECT $columnList FROM \"$schema\".\"$table\"").use { res ->
                while (res.next()) {
                    rows.add((1..columns.size).map { res.getString(it) })
                }
            }
        }
        return rows
    }

    private fun restorePristineRows(conn: Connection, snapshot: PristineSchema) {
        for ((table, content) in snapshot.rows) {
            if (content.rows.isEmpty()) {
                continue
            }
            val fullName = "\"${snapshot.schema}\".\"$table\""
            val columnList = content.columns.joinToString(",") { "\"${it.name}\"" }
            // one placeholder tuple per row, each value cast to its own column type so that both
            // the value and the NULL are unambiguous to the server whatever the type is
            val tuple = "(" + content.columns.joinToString(",") { "?::${it.type}" } + ")"
            val values = content.rows.joinToString(",") { tuple }
            conn.prepareStatement("INSERT INTO $fullName ($columnList) VALUES $values").use { stmt ->
                var index = 1
                for (row in content.rows) {
                    for (value in row) {
                        stmt.setString(index++, value)
                    }
                }
                stmt.executeUpdate()
            }
        }
    }

    /**
     * Puts every counter of the schema back where a pristine schema has it, and removes any
     * sequence a pristine schema does not have. Returns false when the schema no longer holds the
     * sequences it was cached with, which - like a structural mismatch on a table - means the cache
     * can no longer describe this database and the caller must fall back to a full rebuild.
     *
     * Runs after the rows are restored: those rows occupy the first ids, so a counter left at its
     * start value would hand the next test an id it can already see.
     */
    private fun restorePristineSequences(conn: Connection, snapshot: PristineSchema): Boolean {
        val current = readSequences(conn)
        if (!current.keys.containsAll(snapshot.sequences.keys)) {
            return false
        }
        val extra = current.keys - snapshot.sequences.keys
        if (extra.isNotEmpty()) {
            val dropCommand = "DROP SEQUENCE " + extra.joinToString(",") { it.quoted } + " CASCADE"
            println("EXEC: $dropCommand")
            conn.createStatement().use { it.executeUpdate(dropCommand) }
        }
        if (snapshot.sequences.isEmpty()) {
            return true
        }
        val setvals = snapshot.sequences.entries.joinToString(",") { (id, state) ->
            "setval('" + id.quoted.replace("'", "''") + "', ${state.lastValue}, ${state.isCalled})"
        }
        conn.createStatement().use { stmt ->
            stmt.executeQuery("SELECT $setvals").use { it.next() }
        }
        return true
    }

    override fun cleanRecords(tableRef: DbTableRef) {
        // TxnContext.doInTxn, not a bare dataSource.connection.use: this datasource is a managed
        // XA one (defaultAutoCommit = false), so a connection pulled outside an active transaction
        // is never enlisted and never committed - the TRUNCATE is silently rolled back when the
        // connection returns to the pool. Same pattern as dropAllTables() above.
        TxnContext.doInTxn {
            dataSource.connection.use { conn ->
                val truncCommand = "TRUNCATE TABLE " + tableRef.fullName + " CASCADE"
                println("EXEC: $truncCommand")
                conn.createStatement().use { it.executeUpdate(truncCommand) }
                conn.createStatement().use { it.executeUpdate("DEALLOCATE ALL") }
            }
        }
    }

    override fun selectRecFromDb(tableRef: DbTableRef, recLocalId: String, field: String): Any? {
        return dbDataSource.withTransaction(true) {
            dbDataSource.query(
                "SELECT $field as res FROM ${tableRef.fullName} where __ext_id='$recLocalId'",
                emptyList()
            ) { res ->
                res.next()
                res.getObject("res")
            }
        }
    }

    override fun selectFieldFromDbTable(field: String, table: String, condition: String): Any? {
        return dbDataSource.withTransaction(true) {
            dbDataSource.query(
                "SELECT \"$field\" as res FROM $table WHERE $condition",
                emptyList()
            ) { res ->
                res.next()
                res.getObject("res")
            }
        }
    }

    override fun selectAllFromTable(tableRef: DbTableRef, table: String): List<Map<String, Any?>> {
        val recordsList = ArrayList<Map<String, Any?>>()
        dbDataSource.withTransaction(true) {
            dbDataSource.query(
                "SELECT * FROM ${tableRef.withTable(table).fullName}",
                emptyList()
            ) { res ->
                val columnNames = LinkedHashSet<String>()
                for (i in 1..res.metaData.columnCount) {
                    columnNames.add(res.metaData.getColumnName(i))
                }
                while (res.next()) {
                    val record = LinkedHashMap<String, Any>()
                    for (name in columnNames) {
                        record[name] = res.getObject(name)
                    }
                    recordsList.add(record)
                }
            }
        }
        return recordsList
    }

    override fun sqlUpdate(sql: String): Int {
        return TxnContext.doInTxn {
            dataSource.connection.use { conn ->
                conn.createStatement().use { stmt ->
                    stmt.executeUpdate(sql)
                }
            }
        }
    }

    override fun printQueryRes(sql: String) {
        TxnContext.doInTxn {
            dataSource.connection.use { conn ->
                conn.createStatement().use { stmt ->
                    stmt.executeQuery(sql).use {
                        var line = ""
                        for (i in 1..it.metaData.columnCount) {
                            line += it.metaData.getColumnName(i) + "\t\t\t\t\t"
                        }
                        println(line)
                        while (it.next()) {
                            line = ""
                            for (i in 1..it.metaData.columnCount) {
                                line += (it.getObject(i) ?: "").toString() + "\t\t\t\t\t"
                            }
                            println(line)
                        }
                    }
                }
            }
        }
    }

    override fun printAllColumns() {
        dataSource.connection.use { conn ->
            conn.metaData.getColumns(null, null, null, null).use {
                while (it.next()) {
                    val schema = it.getString(COLUMN_TABLE_SCHEMA)
                    if ("information_schema" != schema && "pg_catalog" != schema) {
                        println(
                            it.getObject(COLUMN_COLUMN_NAME).toString() +
                                "\t\t\t\t" + it.getObject(COLUMN_TYPE_NAME) +
                                "\t\t\t\t" + schema +
                                "\t\t\t\t" + it.getObject(COLUMN_TABLE_NAME)
                        )
                    }
                }
            }
        }
    }
}

/**
 * SPI factory for [PgRecordsTestBackend]. Top-level (not nested) so the standard ServiceLoader can
 * instantiate it without eagerly linking the JDBC/DBCP-heavy [PgRecordsTestBackend] class - this
 * matters when the factory is discovered on a classpath (e.g. the in-mem module) that selects a
 * different backend and does not provide PG's runtime dependencies.
 */
class PgRecordsTestBackendFactory : DbRecordsTestBackendFactory {
    override val id: String = "pg"
    override fun create(webAppApi: EcosWebAppApiMock): DbRecordsTestBackend {
        return PgRecordsTestBackend(webAppApi)
    }
}
