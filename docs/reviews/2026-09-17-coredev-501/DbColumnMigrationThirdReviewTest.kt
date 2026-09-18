package ru.citeck.ecos.data.sql.test.records

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import ru.citeck.ecos.data.sql.batch.DbBatchTaskBatch
import ru.citeck.ecos.data.sql.batch.DbBatchTaskContext
import ru.citeck.ecos.data.sql.batch.DbBatchTaskEngine
import ru.citeck.ecos.data.sql.batch.DbBatchTaskHandler
import ru.citeck.ecos.data.sql.migration.column.DbColumnMigrationHandler
import ru.citeck.ecos.data.sql.migration.column.DbColumnMigrationParams
import ru.citeck.ecos.data.sql.props.DbEcosDataProps
import ru.citeck.ecos.data.sql.records.assocs.DbAssocEntity
import ru.citeck.ecos.data.sql.service.DbDataServiceConfig
import ru.citeck.ecos.data.sql.service.DbDataServiceImpl
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType
import ru.citeck.ecos.records2.predicate.model.Predicates
import ru.citeck.ecos.txn.lib.TxnContext
import ru.citeck.ecos.webapp.api.entity.EntityRef
import java.time.Duration

class DbColumnMigrationThirdReviewTest : DbRecordsTestBase() {

    init {
        dataProps = DbEcosDataProps(
            columns = DbEcosDataProps.ColumnsProps(inPlaceAlterMaxRows = 0),
            batch = DbEcosDataProps.BatchProps(batchPause = Duration.ZERO)
        )
    }

    private fun asType(type: AttributeType) {
        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("att")
                    withType(type)
                    withMultiple(true)
                }
            )
        )
    }

    private fun links(ref: EntityRef): List<Long> = TxnContext.doInTxn {
        val schema = getTableCtx().getSchemaCtx()
        DbDataServiceImpl(
            DbAssocEntity::class.java,
            DbDataServiceConfig.create { withTable(DbAssocEntity.MAIN_TABLE) },
            schema
        ).findAll(Predicates.eq(DbAssocEntity.SOURCE_ID, dbRecordRefService.getIdByEntityRef(ref)))
            .map { it.targetId }
    }

    private fun drain() {
        val schema = getTableCtx().getSchemaCtx()
        DbBatchTaskEngine(schema.dataSourceCtx).drainSchemaOnce(schema)
    }

    @Test
    fun staleRestoreMustNotRunBatchesAfterPrepareReturnsNull() {
        asType(AttributeType.ASSOC)
        val target = createRecord("att" to null)
        val source = createRecord("att" to listOf(target.toString()))
        asType(AttributeType.TEXT)
        createRecord("att" to null)
        drain()
        asType(AttributeType.ASSOC)
        createRecord("att" to null)
        val schema = getTableCtx().getSchemaCtx()
        val obsolete = TxnContext.doInTxn {
            schema.batchTaskService.findByTable(tableRef.table).single { !it.status.isFinal() }
        }
        asType(AttributeType.TEXT)
        createRecord("att" to null)
        assertThat(links(source)).isEmpty()
        assertThat(schema.batchTaskService.restart(obsolete.id)).isTrue()
        DbBatchTaskEngine(schema.dataSourceCtx).runTask(schema, obsolete.id)
        val after = TxnContext.doInTxn { schema.batchTaskService.getById(obsolete.id)!! }
        assertThat(after.errorCount).isZero()
        assertThat(links(source)).isEmpty()
    }

    @Test
    fun supersededDepartureWithoutRestoreMustNotDeleteNewLinks() {
        asType(AttributeType.ASSOC)
        val target = createRecord("att" to null)
        val source = createRecord("att" to listOf(target.toString()))
        asType(AttributeType.TEXT)
        createRecord("att" to null)
        val schema = getTableCtx().getSchemaCtx()
        val obsolete = TxnContext.doInTxn { schema.batchTaskService.findByTable(tableRef.table).single() }
        asType(AttributeType.PERSON)
        createRecord("att" to null)
        records.mutate(source, mapOf("att" to listOf("emodel/person@new-user")))
        val expected = dbRecordRefService.getIdByEntityRef(EntityRef.valueOf("emodel/person@new-user"))
        assertThat(links(source)).containsExactly(expected)
        assertThat(schema.batchTaskService.restart(obsolete.id)).isTrue()
        DbBatchTaskEngine(schema.dataSourceCtx).runTask(schema, obsolete.id)
        assertThat(links(source)).containsExactly(expected)
    }
    @Test
    fun skippedSnapshotMustNotMergeWithTheNextDeparture() {
        asType(AttributeType.ASSOC)
        val oldTarget = createRecord("att" to null)
        val newTarget = createRecord("att" to null)
        val source = createRecord("att" to listOf(oldTarget.toString()))
        asType(AttributeType.TEXT)
        createRecord("att" to null)
        drain()
        asType(AttributeType.ASSOC)
        createRecord("att" to null)
        records.mutate(source, mapOf("att" to listOf(newTarget.toString())))
        drain()
        val expected = dbRecordRefService.getIdByEntityRef(newTarget)
        assertThat(links(source)).containsExactly(expected)
        asType(AttributeType.TEXT)
        createRecord("att" to null)
        drain()
        asType(AttributeType.ASSOC)
        createRecord("att" to null)
        drain()
        assertThat(links(source)).containsExactly(expected)
    }
    @Test
    fun emptyNextDepartureMustReplaceTheSkippedSnapshot() {
        asType(AttributeType.ASSOC)
        val oldTarget = createRecord("att" to null)
        val newTarget = createRecord("att" to null)
        val source = createRecord("att" to listOf(oldTarget.toString()))
        asType(AttributeType.TEXT)
        createRecord("att" to null)
        drain()
        asType(AttributeType.ASSOC)
        createRecord("att" to null)
        records.mutate(source, mapOf("att" to listOf(newTarget.toString())))
        drain()
        assertThat(links(source)).containsExactly(dbRecordRefService.getIdByEntityRef(newTarget))
        records.mutate(source, mapOf("att" to emptyList<String>()))
        assertThat(links(source)).isEmpty()
        asType(AttributeType.TEXT)
        createRecord("att" to null)
        drain()
        asType(AttributeType.ASSOC)
        createRecord("att" to null)
        drain()
        assertThat(links(source)).isEmpty()
    }

    @Test
    fun restoredTargetRowMustNotReactivateAnOlderDeparture() {
        asType(AttributeType.ASSOC)
        val target = createRecord("att" to null)
        val source = createRecord("att" to listOf(target.toString()))
        asType(AttributeType.TEXT)
        createRecord("att" to null)
        val schema = getTableCtx().getSchemaCtx()
        val obsolete = TxnContext.doInTxn { schema.batchTaskService.findByTable(tableRef.table).single() }
        val oldParams = DbColumnMigrationParams.from(obsolete.params)
        asType(AttributeType.PERSON)
        createRecord("att" to null)
        records.mutate(source, mapOf("att" to listOf("emodel/person@new-user")))
        asType(AttributeType.TEXT)
        createRecord("att" to null)
        val sourceId = dbRecordRefService.getIdByEntityRef(source)
        fun snapshot() = schema.assocBackupService.findByColumnMeta(oldParams.backupColumnMetaId, sourceId).map { it.targetId }
        assertThat(snapshot()).containsExactly(dbRecordRefService.getIdByEntityRef(target))
        assertThat(schema.columnMetaService.getByTableAndColumn(tableRef.table, "att")!!.id)
            .isEqualTo(oldParams.targetColumnMetaId)
        assertThat(schema.batchTaskService.restart(obsolete.id)).isTrue()
        DbBatchTaskEngine(schema.dataSourceCtx).runTask(schema, obsolete.id)
        assertThat(snapshot()).containsExactly(dbRecordRefService.getIdByEntityRef(target))
    }
    @Test
    fun adminCancelRestartBetweenReadsMustNotBypassSupersededGuard() {
        org.junit.jupiter.api.Assumptions.assumeTrue(backend.supportsRawSql, "Requires independent PostgreSQL transactions")
        asType(AttributeType.ASSOC)
        val target = createRecord("att" to null)
        createRecord("att" to listOf(target.toString()))
        asType(AttributeType.TEXT)
        createRecord("att" to null)
        val schema = getTableCtx().getSchemaCtx()
        val obsolete = TxnContext.doInTxn { schema.batchTaskService.findByTable(tableRef.table).single() }
        asType(AttributeType.PERSON)
        createRecord("att" to null)
        var batches = 0
        val delegate = DbColumnMigrationHandler(schema.dataSourceCtx)
        val interleaving = object : DbBatchTaskHandler {
            override fun getType() = "review-cancel-restart-interleaving"
            override fun prepare(ctx: DbBatchTaskContext): Long? {
                assertThat(TxnContext.doInNewTxn { schema.batchTaskService.cancel(ctx.task.id) }).isTrue()
                val estimate = delegate.prepare(ctx)
                assertThat(TxnContext.doInNewTxn { schema.batchTaskService.restart(ctx.task.id) }).isTrue()
                return estimate
            }
            override fun processBatch(ctx: DbBatchTaskContext, batch: DbBatchTaskBatch) {
                batches++
            }
        }
        schema.dataSourceCtx.batchTaskHandlers.register(interleaving)
        val task = TxnContext.doInTxn {
            schema.batchTaskService.queue(interleaving.getType(), tableRef.table, obsolete.params)
        }
        DbBatchTaskEngine(schema.dataSourceCtx).runTask(schema, task.id)
        assertThat(batches).isZero()
    }
}
