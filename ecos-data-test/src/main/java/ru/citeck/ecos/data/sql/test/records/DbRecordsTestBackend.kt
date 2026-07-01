package ru.citeck.ecos.data.sql.test.records

import ru.citeck.ecos.data.sql.datasource.DbDataSource
import ru.citeck.ecos.data.sql.dto.DbTableRef
import ru.citeck.ecos.data.sql.service.DbDataServiceFactory

/**
 * Backend abstraction for the record-level contract suite ([DbRecordsTestBase]).
 *
 * The suite itself is backend-neutral: it drives everything through the Records API. The only
 * backend-specific concerns are
 *  - **construction** of the storage engine ([dbDataSource] + [dataServiceFactory]), and
 *  - a handful of **DB-direct test helpers** ([selectRecFromDb], [sqlUpdate], [dropAllTables], ...)
 *    that some PG tests use to assert on the raw stored representation.
 *
 * A concrete backend is selected at runtime by [DbRecordsTestBackends] via the
 * `ecos.data.test.backend` system property (default `inmem`), so the same 60+ test classes run on any
 * backend that publishes a [DbRecordsTestBackendFactory] through the standard [java.util.ServiceLoader]
 * SPI. The PG backend reproduces the historical Testcontainers/DBCP/XA wiring exactly, so the PG run
 * is byte-for-byte unchanged.
 *
 * The DB-direct helpers are split out via [supportsRawSql]: a backend that has no SQL engine
 * (e.g. in-memory) returns `false`, and the suite skips raw-SQL assertions with a documented
 * JUnit assumption rather than failing them.
 */
interface DbRecordsTestBackend {

    /** The data source the suite wires into [ru.citeck.ecos.data.sql.context.DbDataSourceContext]. */
    val dbDataSource: DbDataSource

    /** Backend factory (schema dao + entity repo) for [ru.citeck.ecos.data.sql.context.DbDataSourceContext]. */
    val dataServiceFactory: DbDataServiceFactory

    /**
     * Whether the backend can answer the raw-SQL test helpers below. `false` for the in-memory
     * backend, which has no SQL engine; the suite turns such helpers into skipped assumptions.
     */
    val supportsRawSql: Boolean
        get() = true

    /**
     * Whether the backend can evaluate PostgreSQL-internal SQL expressions whose result is defined by
     * the database engine itself - text formatting (`to_char` with a format pattern), timezone-aware
     * date math (`date_trunc`, `date_part('epoch', ...)`) and `interval` arithmetic. The in-memory
     * backend evaluates the portable subset of expressions (arithmetic, numeric/string functions,
     * CASE, coalesce, aggregation) but deliberately does not reimplement Postgres' formatting/timezone
     * engine, so it returns `false` and the suite skips those few assertions with a documented
     * assumption rather than producing a false green.
     */
    val supportsSqlInternalExpressions: Boolean
        get() = true

    /**
     * Whether the backend rolls back mid-transaction writes when a `TxnContext.doInTxn { ... }` block
     * throws. The JDBC/PG backend gets this from the managed connection; the in-memory backend enlists
     * a `TransactionResource` with the active platform transaction (see `InMemDataSource`) and restores
     * its pre-write snapshot on rollback, so it returns `true` by default too. A backend that does not
     * participate in the platform transaction manager overrides this to `false`; tests that assert on
     * post-rollback state declare this requirement and are skipped on such a backend.
     */
    val supportsTransactionRollback: Boolean
        get() = true

    /** Drop every table in the backend (called from `@AfterEach` to isolate tests). */
    fun dropAllTables()

    /** Close/release any resources held by the backend (connection pool, etc.). */
    fun close()

    /** TRUNCATE the given table. */
    fun cleanRecords(tableRef: DbTableRef)

    /** `SELECT <field> FROM <table of rec> WHERE __ext_id = <rec local id>`. */
    fun selectRecFromDb(tableRef: DbTableRef, recLocalId: String, field: String): Any?

    /** `SELECT "<field>" FROM <table> WHERE <condition>`. */
    fun selectFieldFromDbTable(field: String, table: String, condition: String): Any?

    /** `SELECT * FROM <schema of tableRef>.<table>` as a list of column->value maps. */
    fun selectAllFromTable(tableRef: DbTableRef, table: String): List<Map<String, Any?>>

    /** Execute a raw `UPDATE`/DDL statement, returning the affected row count. */
    fun sqlUpdate(sql: String): Int

    /** Debug helper: print the result set of an arbitrary query. */
    fun printQueryRes(sql: String)

    /** Debug helper: print all user columns across the schema. */
    fun printAllColumns()
}
