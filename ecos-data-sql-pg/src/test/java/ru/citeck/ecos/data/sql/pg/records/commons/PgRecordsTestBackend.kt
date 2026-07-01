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
import ru.citeck.ecos.test.commons.EcosWebAppApiMock
import ru.citeck.ecos.test.commons.containers.TestContainers
import ru.citeck.ecos.txn.lib.TxnContext
import ru.citeck.ecos.txn.lib.resource.type.xa.JavaXaTxnManagerAdapter
import ru.citeck.ecos.webapp.api.datasource.JdbcDataSource

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
    }

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
            }
        }
    }

    override fun cleanRecords(tableRef: DbTableRef) {
        dataSource.connection.use { conn ->
            val truncCommand = "TRUNCATE TABLE " + tableRef.fullName + " CASCADE"
            println("EXEC: $truncCommand")
            conn.createStatement().use { it.executeUpdate(truncCommand) }
            conn.createStatement().use { it.executeUpdate("DEALLOCATE ALL") }
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
