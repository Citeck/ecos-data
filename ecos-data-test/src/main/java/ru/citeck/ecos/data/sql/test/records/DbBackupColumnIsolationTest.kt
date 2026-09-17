package ru.citeck.ecos.data.sql.test.records

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import ru.citeck.ecos.data.sql.columnmeta.DbColumnMetaDto
import ru.citeck.ecos.data.sql.columnmeta.DbColumnSemanticType
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType
import ru.citeck.ecos.txn.lib.TxnContext
import java.time.Instant

/**
 * A backup column holds the only copy of the user's pre-migration data, so it must be
 * unreachable by anything that could overwrite it and invisible to anything that reports what a
 * record has.
 */
class DbBackupColumnIsolationTest : DbRecordsTestBase() {

    private fun moveColumnAside(attId: String, backupName: String) {
        val schemaCtx = getTableCtx().getSchemaCtx()
        TxnContext.doInTxn {
            // DbSchemaDaoPg.renameColumn needs a live JDBC connection scoped to this transaction
            // (it reads the current column type off the metadata) - the bare TxnContext.doInTxn
            // above is not enough on its own, exactly as DbSchemaDaoPrimitivesTest does it.
            schemaCtx.dataSourceCtx.dataSource.withTransaction(false) {
                schemaCtx.dataSourceCtx.schemaDao.renameColumn(
                    schemaCtx.dataSourceCtx.dataSource,
                    tableRef,
                    attId,
                    backupName
                )
            }
            schemaCtx.columnMetaService.save(
                DbColumnMetaDto(
                    id = DbColumnMetaDto.NEW_REC_ID,
                    table = tableRef.table,
                    columnName = backupName,
                    attId = attId,
                    attType = DbColumnSemanticType.Model(AttributeType.TEXT),
                    multiple = false,
                    backup = true,
                    created = Instant.now(),
                    creator = "system"
                )
            )
        }
        // schema-level reset is not enough for this table's own DbDataService - see
        // DataMockFactory.resetColumnsCache for why the direct DDL rename needs both.
        resetColumnsCache()
    }

    @Test
    fun aBackupColumnIsNotOneOfTheTablesColumnsAnyMoreTest() {

        registerAtts(listOf(AttributeDef.create { withId("someAtt") }))
        createRecord("someAtt" to "value-0")

        moveColumnAside("someAtt", "__backup_someAtt_text")

        val ctx = getTableCtx()
        assertThat(ctx.getColumns().map { it.name })
            .describedAs("a backup must not be offered to reads, writes or the schema diff")
            .doesNotContain("__backup_someAtt_text")
        assertThat(ctx.getAllPhysicalColumns().map { it.name })
            .describedAs("but the migration machinery has to be able to find it")
            .contains("__backup_someAtt_text")
        assertThat(ctx.hasColumn("__backup_someAtt_text"))
            .describedAs("hasColumn answers for the visible set, so a write cannot address a backup")
            .isFalse()
    }

    @Test
    fun readingARecordDoesNotResurrectTheBackedUpValueTest() {

        registerAtts(listOf(AttributeDef.create { withId("someAtt") }))
        val rec = createRecord("someAtt" to "value-0")

        moveColumnAside("someAtt", "__backup_someAtt_text")

        val value = records.getAtt(rec, "someAtt").asText()
        assertThat(value)
            .describedAs("the attribute's column is gone; the read must degrade, not return the backup")
            .isEmpty()
    }

    @Test
    fun theColumnIsRecreatedEmptyAndTheBackupIsLeftAloneTest() {

        registerAtts(listOf(AttributeDef.create { withId("someAtt") }))
        val rec = createRecord("someAtt" to "value-0")

        moveColumnAside("someAtt", "__backup_someAtt_text")

        // the ordinary write path notices the column is missing and adds it back, empty
        updateRecord(rec, "someAtt" to "value-1")

        val ctx = getTableCtx()
        assertThat(ctx.getColumns().map { it.name })
            .describedAs("the attribute's own column comes back")
            .contains("someAtt")
        assertThat(ctx.getAllPhysicalColumns().map { it.name })
            .describedAs("and the backup is still there, untouched, alongside it")
            .contains("__backup_someAtt_text", "someAtt")
        assertThat(records.getAtt(rec, "someAtt").asText()).isEqualTo("value-1")
    }

    @Test
    fun theSchemaDiffNeverTriesToConvertABackupTest() {

        registerAtts(listOf(AttributeDef.create { withId("someAtt") }))
        createRecord("someAtt" to "value-0")
        moveColumnAside("someAtt", "__backup_someAtt_text")

        // change the model under it: a backup must not be dragged along by the new type
        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("someAtt")
                    withType(AttributeType.NUMBER)
                }
            )
        )
        createRecord("someAtt" to 42)

        val backup = getTableCtx().getAllPhysicalColumns().first { it.name == "__backup_someAtt_text" }
        assertThat(backup.type.name)
            .describedAs("the backup keeps the physical type it had when it was moved aside")
            .isEqualTo("TEXT")
    }

    @Test
    fun theRegistryCanFindTheBackupsOfAnAttributeNewestFirstTest() {

        registerAtts(listOf(AttributeDef.create { withId("someAtt") }))
        createRecord("someAtt" to "value-0")

        moveColumnAside("someAtt", "__backup_someAtt_text")
        createRecord("someAtt" to "value-1")
        moveColumnAside("someAtt", "__backup_someAtt_text_2")

        val schemaCtx = getTableCtx().getSchemaCtx()
        val backups = TxnContext.doInTxn { schemaCtx.columnMetaService.findBackups(tableRef.table, "someAtt") }
        assertThat(backups.map { it.columnName })
            .describedAs("the freshest backup is the one restored, so the order is part of the contract")
            .containsExactly("__backup_someAtt_text_2", "__backup_someAtt_text")
        assertThat(backups).allMatch { it.backup }
    }
}
