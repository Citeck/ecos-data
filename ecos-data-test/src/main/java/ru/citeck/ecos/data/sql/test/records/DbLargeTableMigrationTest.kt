package ru.citeck.ecos.data.sql.test.records

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import ru.citeck.ecos.data.sql.columnmeta.DbColumnSemanticType
import ru.citeck.ecos.data.sql.dto.DbColumnType
import ru.citeck.ecos.data.sql.migration.column.DbColumnMigrationParams
import ru.citeck.ecos.data.sql.props.DbEcosDataProps
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType
import ru.citeck.ecos.txn.lib.TxnContext
import java.io.ByteArrayOutputStream
import java.io.PrintStream

/**
 * Above the in-place threshold an `ALTER ... USING` would rewrite the whole table under an
 * exclusive lock, inside whichever user's mutation happened to trigger it. The column is therefore
 * moved aside instead - a `RENAME` and an `ADD COLUMN`, both O(1) metadata work - and the values
 * are transferred in the background: deferred, never failed, never silently marked as done, and
 * never dropped.
 *
 * The threshold is set to a value a test can reach; the production default is 100 000.
 */
class DbLargeTableMigrationTest : DbRecordsTestBase() {

    companion object {
        private const val IN_PLACE_MAX_ROWS = 2L
    }

    init {
        dataProps = DbEcosDataProps(
            columns = DbEcosDataProps.ColumnsProps(
                inPlaceAlterMaxRows = IN_PLACE_MAX_ROWS
            )
        )
    }

    /**
     * Captures stderr around a block of code and returns what was logged to it. slf4j-simple (the
     * test binding) writes there by default, and this project has no in-JVM log-capture utility -
     * see [ru.citeck.ecos.data.sql.records.utils.DbReadToleranceLog].
     */
    private fun captureStderr(action: () -> Unit): String {
        val original = System.err
        val captured = ByteArrayOutputStream()
        System.setErr(PrintStream(captured, true))
        try {
            action()
        } finally {
            System.setErr(original)
        }
        return captured.toString()
    }

    @Test
    fun conversionIsDeferredAboveTheThresholdTest() {

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("numAtt")
                    withType(AttributeType.NUMBER)
                },
                AttributeDef.create { withId("otherAtt") }
            )
        )
        repeat(IN_PLACE_MAX_ROWS.toInt() + 1) { createRecord("numAtt" to it, "otherAtt" to "v-$it") }
        assertThat(getColumns().first { it.name == "numAtt" }.type).isEqualTo(DbColumnType.DOUBLE)

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("numAtt")
                    withType(AttributeType.TEXT)
                },
                AttributeDef.create { withId("otherAtt") }
            )
        )

        // the mutation goes through: a deferred conversion must not block writes to the table
        val log = captureStderr {
            val record = createRecord("otherAtt" to "after")
            assertThat(records.getAtt(record, "otherAtt").asText()).isEqualTo("after")
        }

        // DOUBLE -> TEXT is a supported conversion (see DbRecordsDaoColumnUpdateTest), so a column
        // left at DOUBLE is ambiguous between this test's deferral branch and the unsupported-
        // conversion branch - only the logged reason tells them apart
        assertThat(log)
            .describedAs("must name the deferral branch this test is pinning, not the unsupported-conversion branch")
            .contains("Column type change was deferred")
            .doesNotContain("is not supported by the backend")

        val ctx = getTableCtx()
        assertThat(ctx.getAllPhysicalColumns().first { it.name == "__backup_numAtt_number" }.type)
            .describedAs("the table is above the threshold, so the numbers are not rewritten in place - they are moved aside")
            .isEqualTo(DbColumnType.DOUBLE)
        assertThat(ctx.getColumns().first { it.name == "numAtt" }.type)
            .describedAs("and a fresh column of the model's type stands in their place")
            .isEqualTo(DbColumnType.TEXT)

        val metaByColumn = getTableCtx().getSchemaCtx().columnMetaService
            .getByTable(tableRef.table).associateBy { it.columnName }
        assertThat(metaByColumn["__backup_numAtt_number"]!!.attType)
            .describedAs("the registry describes the backup, and the backup is still a number")
            .isEqualTo(DbColumnSemanticType.Model(AttributeType.NUMBER))
        assertThat(metaByColumn["numAtt"]!!.attType)
            .describedAs("and describes the new column, which really is text")
            .isEqualTo(DbColumnSemanticType.Model(AttributeType.TEXT))

        val tasks = TxnContext.doInTxn {
            getTableCtx().getSchemaCtx().batchTaskService.findByTable(tableRef.table)
        }
        assertThat(tasks)
            .describedAs("deferred means handed to the background engine, not forgotten")
            .hasSize(1)
        val params = DbColumnMigrationParams.from(tasks.single().params)
        assertThat(params.attId).isEqualTo("numAtt")
        assertThat(params.backupColumn).isEqualTo("__backup_numAtt_number")
        assertThat(params.targetType).isEqualTo(AttributeType.TEXT)
    }

    /**
     * The threshold exists to keep a table rewrite out of a user's mutation. A change that rewrites
     * nothing therefore must not be subject to it: `TEXT <-> OPTIONS` and
     * any move inside `G_ASSOC` - are the same bytes with the same meaning, and
     * `setColumnTypeInSync` emits no SQL for them at all. Deferring one of those would rename a
     * column full of values into a permanent backup, leave the attribute reading empty for every
     * record until a full-table transfer copied the values back onto themselves, and double the
     * table's storage for the attribute for good.
     */
    @Test
    fun aChangeThatRewritesNothingIsNotDeferredByTheRowCountTest() {

        registerAtts(
            listOf(
                AttributeDef.create { withId("someAtt") },
                AttributeDef.create { withId("otherAtt") }
            )
        )
        val records0 = (0..IN_PLACE_MAX_ROWS.toInt()).map {
            createRecord("someAtt" to "v-$it", "otherAtt" to "o-$it")
        }

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("someAtt")
                    withType(AttributeType.OPTIONS)
                },
                AttributeDef.create { withId("otherAtt") }
            )
        )
        // any mutation of this table reaches the migration decision; writing someAtt itself would
        // only add an options-value validation this test has no interest in
        createRecord("otherAtt" to "trigger")

        assertThat(getTableCtx().getAllPhysicalColumns().map { it.name })
            .describedAs("nothing was rewritten, so nothing had to be moved out of the way")
            .doesNotContain("__backup_someAtt_text")
        assertThat(
            TxnContext.doInTxn {
                getTableCtx().getSchemaCtx().batchTaskService.findByTable(tableRef.table)
            }
        )
            .describedAs("and there is nothing for a background transfer to carry")
            .isEmpty()
        assertThat(records.getAtt(records0.first(), "someAtt").asText())
            .describedAs("the values stay readable throughout - no window where the attribute is empty")
            .isEqualTo("v-0")
        assertThat(
            getTableCtx().getSchemaCtx().columnMetaService
                .getByTable(tableRef.table)
                .first { it.columnName == "someAtt" }.attType
        )
            .describedAs("the change is a registry correction, and it still happens above the threshold")
            .isEqualTo(DbColumnSemanticType.Model(AttributeType.OPTIONS))
    }

    @Test
    fun conversionRunsBelowTheThresholdTest() {

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("numAtt")
                    withType(AttributeType.NUMBER)
                }
            )
        )
        createRecord("numAtt" to 1)

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("numAtt")
                    withType(AttributeType.TEXT)
                }
            )
        )
        createRecord("numAtt" to "two")

        assertThat(getColumns().first { it.name == "numAtt" }.type).isEqualTo(DbColumnType.TEXT)
        assertThat(
            getTableCtx().getSchemaCtx().columnMetaService
                .getByTable(tableRef.table)
                .first { it.columnName == "numAtt" }.attType
        ).isEqualTo(DbColumnSemanticType.Model(AttributeType.TEXT))
    }
}
