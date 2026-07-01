package ru.citeck.ecos.data.sql.inmem.records

import ru.citeck.ecos.data.sql.datasource.DbDataSource
import ru.citeck.ecos.data.sql.dto.DbTableRef
import ru.citeck.ecos.data.sql.inmem.InMemDataServiceFactory
import ru.citeck.ecos.data.sql.inmem.datasource.InMemDataSource
import ru.citeck.ecos.data.sql.service.DbDataServiceFactory
import ru.citeck.ecos.data.sql.test.records.DbRecordsTestBackend
import ru.citeck.ecos.data.sql.test.records.DbRecordsTestBackendFactory
import ru.citeck.ecos.test.commons.EcosWebAppApiMock

/**
 * In-memory [DbRecordsTestBackend], registered as `inmem`. Reuses the production
 * [InMemDataSource] + [InMemDataServiceFactory] so the same record-level contract suite that runs
 * on PG runs against the in-memory store.
 *
 * The backend has **no SQL engine** ([supportsRawSql] = `false`), so the raw-SQL test helpers
 * (`selectRecFromDb`, `sqlUpdate`, ...) are not reachable here - the suite turns them into skipped
 * JUnit assumptions instead. The structural drop/truncate helpers operate directly on the store.
 */
class InMemRecordsTestBackend : DbRecordsTestBackend {

    private val inMemDataSource = InMemDataSource()

    override val dbDataSource: DbDataSource = inMemDataSource
    override val dataServiceFactory: DbDataServiceFactory = InMemDataServiceFactory()
    override val supportsRawSql: Boolean = false

    // The in-mem engine evaluates the portable expression subset but not Postgres' formatting /
    // timezone / interval internals (to_char patterns, date_trunc, date_part('epoch'), interval).
    override val supportsSqlInternalExpressions: Boolean = false

    // The in-mem source enlists a TransactionResource with the active platform transaction (see
    // InMemDataSource), so writes made inside a doInTxn block are rolled back when the block throws.
    override val supportsTransactionRollback: Boolean = true

    override fun close() {
        // no pooled resources to release for the in-memory backend
    }

    override fun dropAllTables() {
        // a write transaction so the change is committed into the live store
        inMemDataSource.withTransaction(readOnly = false) {
            inMemDataSource.getStore().dropAllTables()
        }
    }

    override fun cleanRecords(tableRef: DbTableRef) {
        inMemDataSource.withTransaction(readOnly = false) {
            inMemDataSource.getStore().getTable(tableRef)?.truncate()
        }
    }

    override fun selectRecFromDb(tableRef: DbTableRef, recLocalId: String, field: String): Any? {
        throw UnsupportedOperationException(NO_SQL_MSG)
    }

    override fun selectFieldFromDbTable(field: String, table: String, condition: String): Any? {
        throw UnsupportedOperationException(NO_SQL_MSG)
    }

    override fun selectAllFromTable(tableRef: DbTableRef, table: String): List<Map<String, Any?>> {
        throw UnsupportedOperationException(NO_SQL_MSG)
    }

    override fun sqlUpdate(sql: String): Int {
        throw UnsupportedOperationException(NO_SQL_MSG)
    }

    override fun printQueryRes(sql: String) {
        throw UnsupportedOperationException(NO_SQL_MSG)
    }

    override fun printAllColumns() {
        throw UnsupportedOperationException(NO_SQL_MSG)
    }

    companion object {
        private const val NO_SQL_MSG =
            "In-memory backend has no SQL engine; raw-SQL test helpers must be guarded by assumeRawSqlSupported()"
    }
}

/** SPI factory for [InMemRecordsTestBackend], keyed `inmem`. */
class InMemRecordsTestBackendFactory : DbRecordsTestBackendFactory {
    override val id: String = "inmem"
    override fun create(webAppApi: EcosWebAppApiMock): DbRecordsTestBackend {
        return InMemRecordsTestBackend()
    }
}
