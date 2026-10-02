package ru.citeck.ecos.data.sql.test.records

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import ru.citeck.ecos.commons.data.MLText
import ru.citeck.ecos.commons.json.Json
import ru.citeck.ecos.data.sql.batch.DbBatchTaskBatch
import ru.citeck.ecos.data.sql.batch.DbBatchTaskContext
import ru.citeck.ecos.data.sql.batch.DbBatchTaskEngine
import ru.citeck.ecos.data.sql.batch.DbBatchTaskStatus
import ru.citeck.ecos.data.sql.columnmeta.DbColumnSemanticType
import ru.citeck.ecos.data.sql.migration.column.DbColumnMigrationHandler
import ru.citeck.ecos.data.sql.migration.column.DbColumnMigrationParams
import ru.citeck.ecos.data.sql.migration.column.DbColumnValueConverter
import ru.citeck.ecos.data.sql.props.DbEcosDataProps
import ru.citeck.ecos.data.sql.records.refs.DbRecordRefEntity
import ru.citeck.ecos.data.sql.repo.entity.DbEntity
import ru.citeck.ecos.data.sql.service.DbDataServiceConfig
import ru.citeck.ecos.data.sql.service.DbDataServiceImpl
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType
import ru.citeck.ecos.records2.predicate.model.Predicates
import ru.citeck.ecos.records3.record.dao.query.dto.query.RecordsQuery
import ru.citeck.ecos.txn.lib.TxnContext
import ru.citeck.ecos.webapp.api.entity.EntityRef

class DbInterruptedColumnMigrationTest : DbRecordsTestBase() {

    init {
        dataProps = DbEcosDataProps(columns = DbEcosDataProps.ColumnsProps(inPlaceAlterMaxRows = 0))
    }

    private fun changeType(type: AttributeType, multiple: Boolean = false) {
        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("value")
                    withType(type)
                    withMultiple(multiple)
                },
                AttributeDef.create { withId("other") }
            )
        )
    }

    private fun transition(type: AttributeType, multiple: Boolean = false) {
        changeType(type, multiple)
        createRecord("other" to "trigger")
    }

    private fun drain() {
        val schema = getTableCtx().getSchemaCtx()
        DbBatchTaskEngine(schema.dataSourceCtx).drainSchemaOnce(schema)
    }

    private fun assertLatestDone() {
        val task = TxnContext.doInTxn {
            getTableCtx().getSchemaCtx().batchTaskService.findByTable(tableRef.table).maxBy { it.id }
        }
        assertThat(task.status).isEqualTo(DbBatchTaskStatus.DONE)
        assertThat(task.failed).isZero()
        assertThat(task.errorCount).isZero()
    }

    private fun assertPhysical(record: EntityRef, expected: Any?) {
        assertLatestDone()
        val value = TxnContext.doInTxn {
            mainCtx.dataService.findByExtId(record.getLocalId())!!.attributes["value"]
        }
        assertThat(value).isEqualTo(expected)
    }

    private fun rawRows(): List<DbEntity> = TxnContext.doInTxn {
        DbDataServiceImpl(
            DbEntity::class.java,
            DbDataServiceConfig.create {
                withTable(tableRef.table)
                withIncludeBackupColumns(true)
            },
            getTableCtx().getSchemaCtx()
        ).findAll()
    }

    private fun assertTextBackup(expected: Map<String, String>) {
        val meta = TxnContext.doInTxn {
            getTableCtx().getSchemaCtx().columnMetaService.getByTable(tableRef.table).single {
                it.attId == "value" && it.backup && it.attType == DbColumnSemanticType.Model(AttributeType.TEXT)
            }
        }
        val actual = rawRows().filter { it.extId in expected }.associate { it.extId to it.attributes[meta.columnName] }
        assertThat(actual).isEqualTo(expected)
    }

    @Test
    fun repeatedChangeKeepsUntransferredValues() {
        changeType(AttributeType.TEXT)
        val record = createRecord("value" to "original")
        transition(AttributeType.JSON)
        transition(AttributeType.MLTEXT)
        drain()
        assertThat(records.getAtt(record, "value?disp").asText()).isEqualTo("original")
        assertPhysical(record, Json.mapper.toString(MLText("original")))
        assertTextBackup(mapOf(record.getLocalId() to "original"))
    }

    @Test
    fun valuesAreReadableBeforeTransfer() {
        changeType(AttributeType.TEXT)
        val record = createRecord("value" to "42")
        transition(AttributeType.NUMBER)
        assertThat(records.getAtt(record, "value?num").asDouble()).isEqualTo(42.0)
    }

    @Test
    fun existingRecordCanBeSavedBeforeReconciliation() {
        changeType(AttributeType.TEXT)
        val record = createRecord("value" to "42")
        changeType(AttributeType.NUMBER)
        records.mutate(record, mapOf("other" to "updated"))
        drain()
        assertThat(records.getAtt(record, "other").asText()).isEqualTo("updated")
        assertThat(records.getAtt(record, "value?num").asDouble()).isEqualTo(42.0)
        assertPhysical(record, 42.0)
        assertTextBackup(mapOf(record.getLocalId() to "42"))
    }

    @Test
    fun partiallyTransferredAndUserEditedValuesHavePriorityOverOlderSources() {
        changeType(AttributeType.TEXT)
        val first = createRecord("value" to "42")
        val second = createRecord("value" to "43")
        val edited = createRecord("value" to "44")
        transition(AttributeType.JSON)
        val schema = getTableCtx().getSchemaCtx()
        val handler = DbColumnMigrationHandler(schema.dataSourceCtx)
        TxnContext.doInTxn {
            val task = schema.batchTaskService.findByTable(tableRef.table).single()
            val context = DbBatchTaskContext(task, schema)
            handler.prepare(context)
            val firstId = mainCtx.dataService.findByExtId(first.getLocalId())!!.id
            handler.processBatch(context, DbBatchTaskBatch(firstId - 1, firstId))
            assertThat(context.processed).isEqualTo(1)
        }
        records.mutate(edited, mapOf("value" to 99))
        transition(AttributeType.MLTEXT)
        assertThat(records.getAtt(first, "value?disp").asText()).isEqualTo("42")
        assertThat(records.getAtt(second, "value?disp").asText()).isEqualTo("43")
        assertThat(records.getAtt(edited, "value?disp").asText()).isEqualTo("99")
        drain()
        assertThat(records.getAtt(first, "value?disp").asText()).isEqualTo("42")
        assertThat(records.getAtt(second, "value?disp").asText()).isEqualTo("43")
        assertThat(records.getAtt(edited, "value?disp").asText()).isEqualTo("99")
        assertPhysical(first, Json.mapper.toString(MLText("42")))
        assertPhysical(second, Json.mapper.toString(MLText("43")))
        assertPhysical(edited, Json.mapper.toString(MLText("99")))
        assertTextBackup(mapOf(first.getLocalId() to "42", second.getLocalId() to "43", edited.getLocalId() to "44"))
    }

    @Test
    fun threeChangesKeepTheOriginalSource() {
        changeType(AttributeType.TEXT)
        val record = createRecord("value" to "42")
        transition(AttributeType.JSON)
        transition(AttributeType.NUMBER)
        transition(AttributeType.MLTEXT)
        assertThat(records.getAtt(record, "value?disp").asText()).isEqualTo("42")
        drain()
        assertThat(records.getAtt(record, "value?disp").asText()).isEqualTo("42")
        assertPhysical(record, Json.mapper.toString(MLText("42")))
        assertTextBackup(mapOf(record.getLocalId() to "42"))
    }

    @Test
    fun journalReadsPendingValuesAndUserWritesWin() {
        changeType(AttributeType.TEXT)
        val record = createRecord("value" to "42")
        transition(AttributeType.NUMBER)
        val query = RecordsQuery.create {
            withSourceId(RECS_DAO_ID)
            withMaxItems(10)
        }
        val rows = records.query(query, listOf("value?num"))
        assertThat(rows.getRecords().first { it.getId() == record }.getAtt("value?num").asDouble())
            .isEqualTo(42.0)
        records.mutate(record, mapOf("value" to 75))
        assertThat(records.getAtt(record, "value?num").asDouble()).isEqualTo(75.0)
        drain()
        assertThat(records.getAtt(record, "value?num").asDouble()).isEqualTo(75.0)
        assertPhysical(record, 75.0)
        assertTextBackup(mapOf(record.getLocalId() to "42"))
    }

    @Test
    fun completedMigrationDoesNotResurrectUnconvertibleValues() {
        changeType(AttributeType.TEXT)
        val record = createRecord("value" to "not-json")
        transition(AttributeType.JSON)
        drain()
        transition(AttributeType.MLTEXT)
        drain()
        assertThat(records.getAtt(record, "value?disp").asText()).isEmpty()
        assertPhysical(record, null)
        assertTextBackup(mapOf(record.getLocalId() to "not-json"))
        transition(AttributeType.TEXT)
        assertThat(records.getAtt(record, "value").asText()).isEqualTo("not-json")
    }

    @Test
    fun tenThousandRowsSurviveAnInterruptedTransfer() {
        changeType(AttributeType.TEXT)
        val seed = createRecord("value" to "original-0")
        val count = 10_000
        TxnContext.doInTxn {
            val template = mainCtx.dataService.findByExtId(seed.getLocalId())!!
            (1 until count).chunked(500).forEach { indices ->
                val refs = indices.map { mainCtx.createRef("migration-$it") }
                val refIds = dbRecordRefService.getOrCreateIdByEntityRefsMap(refs)
                mainCtx.dataService.save(
                    indices.mapIndexed { index, number ->
                        template.copy().apply {
                            id = DbEntity.NEW_REC_ID
                            extId = refs[index].getLocalId()
                            refId = refIds.getValue(refs[index])
                            attributes["value"] = "original-$number"
                        }
                    }
                )
            }
        }
        transition(AttributeType.JSON)
        transition(AttributeType.MLTEXT)
        drain()
        assertLatestDone()
        val expected = (1 until count).associate { "migration-$it" to "original-$it" } +
            (seed.getLocalId() to "original-0")
        val values = rawRows().filter { it.extId in expected }.associate { it.extId to it.attributes["value"] }
        assertThat(values).isEqualTo(expected.mapValues { Json.mapper.toString(MLText(it.value)) })
        assertTextBackup(expected)
    }

    @ParameterizedTest
    @ValueSource(booleans = [false, true])
    fun inheritedAssociationSourceKeepsLinksBeyondTheColumnCache(failedBeforePrepare: Boolean) {
        changeType(AttributeType.ASSOC, true)
        val targets = (0 until 12).map { createRecord("other" to "target-$it") }
        val record = createRecord("value" to targets)
        transition(AttributeType.TEXT, true)
        if (failedBeforePrepare) {
            TxnContext.doInTxn {
                val service = getTableCtx().getSchemaCtx().batchTaskService
                service.save(service.findByTable(tableRef.table).single().copy(status = DbBatchTaskStatus.FAILED))
            }
        }
        transition(AttributeType.PERSON, true)
        drain()
        assertThat(records.getAtt(record, "value[]?id").asStrList())
            .containsExactlyInAnyOrderElementsOf(targets.map { it.toString() })
        assertLatestDone()
        TxnContext.doInTxn {
            val schema = getTableCtx().getSchemaCtx()
            val refId = dbRecordRefService.getIdByEntityRef(record)
            val ids = targets.map { dbRecordRefService.getIdByEntityRef(it) }
            val meta = schema.columnMetaService.getByTable(tableRef.table).single {
                it.backup && it.attType == DbColumnSemanticType.Model(AttributeType.ASSOC)
            }
            assertThat(schema.assocBackupService.findByColumnMeta(meta.id, refId).map { it.targetId })
                .containsExactlyElementsOf(ids)
            assertThat(schema.assocsService.getTargetAssocs(listOf(refId), "value")[refId]!!.map { it.targetId })
                .containsExactlyElementsOf(ids)
        }
    }

    @Test
    fun unchangedSchemaDoesNotAcquireMigrationLocksForExistingRecords() {
        changeType(AttributeType.TEXT)
        val record = createRecord("value" to "42")
        val before = schemaMigrationLockCallCount()
        repeat(3) { records.mutate(record, mapOf("other" to "updated-$it")) }
        assertThat(schemaMigrationLockCallCount()).isEqualTo(before)
    }

    @Test
    fun multiAssociationValuesAreReadableBeforePrepare() {
        changeType(AttributeType.ASSOC, true)
        val targets = (0 until 12).map { createRecord("other" to "target-$it") }
        val record = createRecord("value" to targets)
        transition(AttributeType.TEXT, true)
        val before = records.getAtt(record, "value[]").asStrList()
        drain()
        assertThat(records.getAtt(record, "value[]").asStrList())
            .containsExactlyElementsOf(targets.map { it.toString() })
        assertThat(before).containsExactlyElementsOf(targets.map { it.toString() })
        assertPhysical(record, targets.map { it.toString() })
    }

    @Test
    fun failedUnfinishedMigrationStillHasASourceForTheNextType() {
        changeType(AttributeType.TEXT)
        val record = createRecord("value" to "original")
        transition(AttributeType.JSON)
        val schema = getTableCtx().getSchemaCtx()
        TxnContext.doInTxn {
            val task = schema.batchTaskService.findByTable(tableRef.table).single()
            schema.batchTaskService.save(task.copy(status = DbBatchTaskStatus.FAILED, error = "batch failed"))
        }
        transition(AttributeType.MLTEXT)
        drain()
        assertPhysical(record, Json.mapper.toString(MLText("original")))
        assertTextBackup(mapOf(record.getLocalId() to "original"))
        val after = records.getAtt(record, "value?disp").asText()
        transition(AttributeType.TEXT)
        assertThat(records.getAtt(record, "value").asText()).isEqualTo("original")
        assertThat(after).isEqualTo("original")
    }

    @Test
    fun malformedPendingTaskDoesNotBreakUnrelatedAttributes() {
        changeType(AttributeType.TEXT)
        val record = createRecord("value" to "42", "other" to "unchanged")
        transition(AttributeType.NUMBER)
        val schema = getTableCtx().getSchemaCtx()
        TxnContext.doInTxn {
            val task = schema.batchTaskService.findByTable(tableRef.table).single()
            val params = task.params
            params.set("targetType", "INVALID_TYPE")
            schema.batchTaskService.save(task.copy(params = params))
        }
        assertThat(records.getAtt(record, "other").asText()).isEqualTo("unchanged")
    }
    @Test
    fun failedTaskStillProvidesReadValues() {
        changeType(AttributeType.TEXT)
        val record = createRecord("value" to "42")
        transition(AttributeType.NUMBER)
        TxnContext.doInTxn {
            val service = getTableCtx().getSchemaCtx().batchTaskService
            service.save(service.findByTable(tableRef.table).single().copy(status = DbBatchTaskStatus.FAILED))
            assertThat(mainCtx.dataService.findByExtId(record.getLocalId())!!.attributes["value"]).isNull()
        }
        assertThat(records.getAtt(record, "value?num").asDouble()).isEqualTo(42.0)
        assertTextBackup(mapOf(record.getLocalId() to "42"))
    }

    @Test
    fun cancelledTaskIsNotResumedByAnotherTypeChange() {
        changeType(AttributeType.TEXT)
        val record = createRecord("value" to "original")
        transition(AttributeType.JSON)
        TxnContext.doInTxn {
            val service = getTableCtx().getSchemaCtx().batchTaskService
            assertThat(service.cancel(service.findByTable(tableRef.table).single().id)).isTrue()
        }
        transition(AttributeType.MLTEXT)
        drain()
        assertPhysical(record, null)
        assertTextBackup(mapOf(record.getLocalId() to "original"))
    }

    @Test
    fun journalResolvesDistinctReferenceSourcesInOneBatchWithAColdCache() {
        changeType(AttributeType.ASSOC, true)
        val targetsByRecord = (0 until 5).associate {
            val targets = (0 until 12).map { index -> createRecord("other" to "target-$it-$index") }
            createRecord("value" to targets, "other" to "parent") to targets
        }
        transition(AttributeType.TEXT, true)
        val targetIds = targetsByRecord.values.flatten().map { dbRecordRefService.getIdByEntityRef(it) }.toSet()
        targetIds.forEach { dbRecordRefService.invalidateById(it) }
        val targetReads = ArrayList<Set<Long>>()
        afterStorageRead = { table, _, rows ->
            if (table.table == DbRecordRefEntity.TABLE) {
                val ids = rows.mapNotNull { it[DbEntity.ID] as? Long }.filter { it in targetIds }.toSet()
                if (ids.isNotEmpty()) targetReads.add(ids)
            }
        }
        try {
            val result = records.query(
                RecordsQuery.create {
                    withSourceId(RECS_DAO_ID)
                    withMaxItems(5)
                    withQuery(Predicates.eq("other", "parent"))
                },
                listOf("value[]")
            )
            assertThat(result.getRecords().associate { it.getId() to it.getAtt("value[]").asStrList() })
                .isEqualTo(targetsByRecord.mapValues { (_, targets) -> targets.map { it.toString() } })
            assertThat(targetReads).containsExactly(targetIds)
        } finally {
            afterStorageRead = null
        }
    }

    @Test
    fun anAbsentReferenceInABatchMapDoesNotFallBackToPerIdLookup() {
        changeType(AttributeType.ASSOC)
        val target = createRecord("other" to "target")
        createRecord("value" to target)
        transition(AttributeType.TEXT)
        TxnContext.doInTxn {
            val schema = getTableCtx().getSchemaCtx()
            val params = DbColumnMigrationParams.from(schema.batchTaskService.findByTable(tableRef.table).single().params)
            val id = dbRecordRefService.getIdByEntityRef(target)
            assertThat(dbRecordRefService.getEntityRefsByIdsMap(listOf(id))[id]).isEqualTo(target)
            assertThrows<IllegalStateException> {
                DbColumnValueConverter.convert(id, params, getTableCtx(), emptyMap())
            }
        }
    }
}
