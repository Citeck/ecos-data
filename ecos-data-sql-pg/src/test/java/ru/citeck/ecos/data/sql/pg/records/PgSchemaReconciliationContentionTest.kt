package ru.citeck.ecos.data.sql.pg.records

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertDoesNotThrow
import ru.citeck.ecos.data.sql.modelchange.DbSchemaReconciler
import ru.citeck.ecos.data.sql.service.DbDataServiceImpl
import ru.citeck.ecos.data.sql.test.records.DbRecordsTestBase
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType
import ru.citeck.ecos.txn.lib.TxnContext
import java.time.Duration
import java.util.concurrent.Executors
import java.util.concurrent.Future
import java.util.concurrent.TimeUnit

class PgSchemaReconciliationContentionTest : DbRecordsTestBase() {

    @Test
    fun backgroundLockTimeoutIsLocalToItsTransaction() {
        registerAtts(listOf(AttributeDef.create { withId("value") }))
        createRecord("value" to "before")
        fun connectionState(): Pair<Int, String> = dataSourceCtx.dataSource.query(
            "SELECT pg_backend_pid(), current_setting('lock_timeout')",
            emptyList()
        ) {
            check(it.next())
            it.getInt(1) to it.getString(2)
        }
        lateinit var before: Pair<Int, String>
        TxnContext.doInTxn {
            dataSourceCtx.dataSource.withTransaction(false) {
                before = connectionState()
                dataSourceCtx.schemaDao.lockTableForBackgroundMigration(dataSourceCtx.dataSource, tableRef)
                assertThat(connectionState()).isEqualTo(before.first to "1ms")
            }
        }
        val after = dataSourceCtx.doInNewRoTxn { connectionState() }
        assertThat(after.first).describedAs("reuse the same physical connection, not a fresh default").isEqualTo(before.first)
        assertThat(after.second).isEqualTo(before.second)
    }

    @Test
    fun backgroundReconciliationYieldsToAnUncommittedMetadataWriter() {
        checkMetadataContention(AttributeType.OPTIONS)
    }

    @Test
    fun aBusyMetadataWriteRollsBackThePhysicalColumnTransition() {
        checkMetadataContention(AttributeType.NUMBER)
    }

    private fun checkMetadataContention(targetType: AttributeType) {
        registerAtts(listOf(AttributeDef.create { withId("value") }))
        createRecord("value" to "42")
        val metaService = getTableCtx().getSchemaCtx().columnMetaService
        TxnContext.doInTxn {
            metaService.save(metaService.getByTableAndColumn(tableRef.table, "value")!!)
        }
        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("value")
                    withType(targetType)
                }
            )
        )
        val executor = Executors.newSingleThreadExecutor()
        var background: Future<DbSchemaReconciler.Result>? = null
        try {
            TxnContext.doInTxn {
                dataSourceCtx.dataSource.withTransaction(false) {
                    val metaTable = tableRef.withTable("ed_column_meta").fullName
                    dataSourceCtx.dataSource.query(
                        "SELECT id FROM $metaTable WHERE \"__table\" = ? AND \"__column_name\" = ? FOR UPDATE",
                        listOf(tableRef.table, "value")
                    ) { assertThat(it.next()).isTrue() }
                    // A semantic registry write can precede the transaction's domain-table write.
                    background = executor.submit<DbSchemaReconciler.Result> {
                        val previous = dataSourceCtx.doInNewRoTxn { connectionState() }
                        val result = DbSchemaReconciler().reconcile(mainCtx.dao)
                        val after = dataSourceCtx.doInNewRoTxn { connectionState() }
                        assertThat(after).describedAs("rollback restores the same pooled physical connection").isEqualTo(previous)
                        result
                    }
                    val result = assertDoesNotThrow("Background work must also yield on a metadata row lock") {
                        background!!.get(3, TimeUnit.SECONDS)
                    }
                    assertThat(result.status.name).isEqualTo("BUSY")
                    dataSourceCtx.dataSource.update("UPDATE ${tableRef.fullName} SET \"value\" = ?", listOf("43"))
                }
            }
            assertThat(DbSchemaReconciler().reconcile(mainCtx.dao).status).isEqualTo(DbSchemaReconciler.Status.RECONCILED)
            assertThat(sqlSelect(if (targetType == AttributeType.NUMBER) "__backup_value_text" else "value")).isEqualTo("43")
            assertThat(metaService.getByTableAndColumn(tableRef.table, "value")!!.attType)
                .isEqualTo(ru.citeck.ecos.data.sql.columnmeta.DbColumnSemanticType.Model(targetType))
        } finally {
            runCatching { background?.get(15, TimeUnit.SECONDS) }
            executor.shutdownNow()
            assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue()
        }
    }

    @Test
    fun aBusyTableIsRetriedOnTheNextQueueTickWithoutAnotherModelChange() {
        registerAtts(listOf(AttributeDef.create { withId("value") }))
        createRecord("value" to "42")
        val queue = dataSourceCtx.modelChangeQueue
        queue.drainOnce()
        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("value")
                    withType(AttributeType.NUMBER)
                }
            )
        )
        val executor = Executors.newSingleThreadExecutor()
        var background: Future<Int>? = null
        try {
            TxnContext.doInTxn {
                dataSourceCtx.dataSource.withTransaction(false) {
                    dataSourceCtx.dataSource.updateSchema("LOCK TABLE ${tableRef.fullName} IN ROW EXCLUSIVE MODE")
                    background = executor.submit<Int> { queue.drainOnce() }
                    assertThat(assertDoesNotThrow { background!!.get(3, TimeUnit.SECONDS) }).isEqualTo(1)
                    assertThat(getColumns().first { it.name == "value" }.type.name).isEqualTo("TEXT")
                }
            }
            // No new registry event: BUSY must be remembered as pending, never as reconciled.
            assertThat(queue.drainOnce()).isEqualTo(1)
            assertThat(getColumns().first { it.name == "value" }.type.name).isEqualTo("DOUBLE")
            assertThat(getTableCtx().getAllPhysicalColumns().map { it.name }).contains("__backup_value_text")
            assertThat(sqlSelect("__backup_value_text")).isEqualTo("42")
            assertThat(queue.drainOnce()).isZero()
        } finally {
            runCatching { background?.get(15, TimeUnit.SECONDS) }
            executor.shutdownNow()
            assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue()
        }
    }

    private fun connectionState(): Pair<Int, String> = dataSourceCtx.dataSource.query(
        "SELECT pg_backend_pid(), current_setting('lock_timeout')",
        emptyList()
    ) {
        check(it.next())
        it.getInt(1) to it.getString(2)
    }

    private fun sqlSelect(column: String): String? = dataSourceCtx.doInNewRoTxn {
        dataSourceCtx.dataSource.query("SELECT \"$column\" FROM ${tableRef.fullName}", emptyList()) {
            if (it.next()) it.getString(1) else null
        }
    }

    @Test
    fun backgroundReconciliationAlsoYieldsToAnOccupiedApplicationLock() {
        registerAtts(listOf(AttributeDef.create { withId("value") }))
        createRecord("value" to "before")
        registerAtts(listOf(AttributeDef.create { withId("value") }, AttributeDef.create { withId("lateColumn") }))
        val lock = webAppApi.getAppLockApi().getLock(DbDataServiceImpl.schemaMigrationLockKey(tableRef))
        assertThat(lock.acquire(Duration.ZERO)).isTrue()
        val executor = Executors.newSingleThreadExecutor()
        try {
            val result = executor.submit<DbSchemaReconciler.Result> { DbSchemaReconciler().reconcile(mainCtx.dao) }
            assertThat(assertDoesNotThrow { result.get(3, TimeUnit.SECONDS) }.status.name).isEqualTo("BUSY")
        } finally {
            lock.release()
            executor.shutdownNow()
            assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue()
        }
        assertThat(DbSchemaReconciler().reconcile(mainCtx.dao).status).isEqualTo(DbSchemaReconciler.Status.RECONCILED)
    }

    @Test
    fun backgroundReconciliationYieldsToAnActiveWriter() {
        registerAtts(listOf(AttributeDef.create { withId("value") }))
        val record = createRecord("value" to "before")
        registerAtts(
            listOf(
                AttributeDef.create { withId("value") },
                AttributeDef.create { withId("lateColumn") }
            )
        )
        val executor = Executors.newSingleThreadExecutor()
        var background: Future<DbSchemaReconciler.Result>? = null
        try {
            TxnContext.doInTxn {
                dataSourceCtx.dataSource.withTransaction(false) {
                    // An ordinary UPDATE holds exactly this lock until its transaction ends.
                    // Hold it before launching the worker, so no scheduler timing is required.
                    dataSourceCtx.dataSource.updateSchema(
                        "LOCK TABLE ${tableRef.fullName} IN ROW EXCLUSIVE MODE"
                    )
                    background = executor.submit<DbSchemaReconciler.Result> {
                        DbSchemaReconciler().reconcile(mainCtx.dao)
                    }
                    val deferred = assertDoesNotThrow("Background DDL must yield to the active writer") {
                        background!!.get(3, TimeUnit.SECONDS)
                    }
                    assertThat(deferred.status.name).isEqualTo("BUSY")
                    // Same transaction now needs the migration lock, as a nested activity
                    // position mutation does. The worker must not retain it while waiting on us.
                    records.mutateAtt(record, "lateColumn", "writer-value")
                }
            }
            assertThat(records.getAtt(record, "lateColumn").asText()).isEqualTo("writer-value")
            assertThat(DbSchemaReconciler().reconcile(mainCtx.dao).status)
                .isEqualTo(DbSchemaReconciler.Status.UP_TO_DATE)
        } finally {
            runCatching { background?.get(15, TimeUnit.SECONDS) }
            executor.shutdownNow()
            assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue()
        }
    }
}
