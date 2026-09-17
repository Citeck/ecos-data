package ru.citeck.ecos.data.sql.test.records

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import ru.citeck.ecos.data.sql.dto.DbColumnType
import ru.citeck.ecos.data.sql.dto.DbTableRef
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.txn.lib.TxnContext

/**
 * The two backend primitives the column migration is built on, asserted through the portable
 * [ru.citeck.ecos.data.sql.schema.DbSchemaDao] contract so that both backends answer the same way.
 */
class DbSchemaDaoPrimitivesTest : DbRecordsTestBase() {

    @Test
    fun renameColumnMovesTheDefinitionTest() {

        registerAtts(listOf(AttributeDef.create { withId("someAtt") }))
        createRecord("someAtt" to "value")

        assertThat(getColumns().map { it.name }).contains("someAtt")

        // TxnContext.doInTxn, not a bare dbDataSource.withTransaction: the records test harness
        // runs on a managed datasource, where DbDataSourceImpl commits only when !isManaged()
        // (see DbDataSourceImpl). Without an enclosing TxnContext transaction the connection
        // is never enlisted and is rolled back when it returns to the pool, so the DDL silently
        // disappears - the defect that made DbRecordsColumnsCacheTest vacuous.
        TxnContext.doInTxn {
            dbDataSource.withTransaction(false) {
                dbSchemaDao.renameColumn(dbDataSource, tableRef, "someAtt", "__backup_someAtt_text")
            }
        }

        val columns = getColumns()
        assertThat(columns.map { it.name })
            .contains("__backup_someAtt_text")
            .doesNotContain("someAtt")
        assertThat(columns.first { it.name == "__backup_someAtt_text" }.type)
            .describedAs("a rename changes the name and nothing else")
            .isEqualTo(DbColumnType.TEXT)
    }

    @Test
    fun renameOfAnAbsentColumnIsANoOpTest() {

        registerAtts(listOf(AttributeDef.create { withId("someAtt") }))
        createRecord("someAtt" to "value")

        val before = getColumns().map { it.name }.toSet()
        TxnContext.doInTxn {
            dbDataSource.withTransaction(false) {
                dbSchemaDao.renameColumn(dbDataSource, tableRef, "no-such-column", "whatever")
            }
        }
        assertThat(getColumns().map { it.name }.toSet()).isEqualTo(before)
    }

    @Test
    fun rowsCountProbeAnswersAroundTheLimitTest() {

        registerAtts(listOf(AttributeDef.create { withId("someAtt") }))
        repeat(5) { createRecord("someAtt" to "value-$it") }

        val probe = { limit: Long ->
            dbDataSource.withTransaction(true) {
                dbSchemaDao.isRowsCountGreaterThan(dbDataSource, tableRef, limit)
            }
        }

        assertThat(probe(0L)).isTrue()
        assertThat(probe(4L)).isTrue()
        assertThat(probe(5L)).isFalse()
        assertThat(probe(100L)).isFalse()
    }

    /**
     * [ru.citeck.ecos.data.sql.schema.DbSchemaDao.estimateRowsCount] - what the column migration's
     * `prepare` shows an administrator as "of how many".
     *
     * Only the two properties that path depends on are asserted, because the value is explicitly
     * allowed to be an estimate: a table that holds rows must never answer 0 or "unknown" (that is
     * what would blank the progress column), and a table that is not there must answer -1 rather
     * than pretend to be empty. Neither an exact value nor an upper bound is asserted - on
     * PostgreSQL the answer may come from `reltuples`, from `n_live_tup` or from a bounded count
     * depending on what statistics exist at that moment, and `reltuples` in particular is allowed to
     * be stale-high. Pinning a ceiling would pin the statistics collector's timing rather than this
     * method's contract. The lower bound is what discriminates: it is what a broken estimate (0, or
     * -1 for "unknown") would blank the administrator's progress column with.
     */
    @Test
    fun rowsCountEstimateAnswersForATableAndDeclinesForAMissingOneTest() {

        registerAtts(listOf(AttributeDef.create { withId("someAtt") }))
        repeat(5) { createRecord("someAtt" to "value-$it") }

        val estimate = { ref: DbTableRef ->
            dbDataSource.withTransaction(true) {
                dbSchemaDao.estimateRowsCount(dbDataSource, ref)
            }
        }

        assertThat(estimate(tableRef))
            .describedAs("a table with rows in it never answers 0, and never answers 'unknown'")
            .isGreaterThanOrEqualTo(1L)
        assertThat(estimate(tableRef.withTable("no-such-table")))
            .describedAs("-1 is 'unknown' everywhere this value travels; 0 would read as 'nothing to do'")
            .isEqualTo(-1L)
    }

    @Test
    fun rowsCountProbeOnAnEmptyTableTest() {

        registerAtts(listOf(AttributeDef.create { withId("someAtt") }))
        createRecord("someAtt" to "value")
        cleanRecords()

        assertThat(
            dbDataSource.withTransaction(true) {
                dbSchemaDao.isRowsCountGreaterThan(dbDataSource, tableRef, 0L)
            }
        ).isFalse()
    }
}
