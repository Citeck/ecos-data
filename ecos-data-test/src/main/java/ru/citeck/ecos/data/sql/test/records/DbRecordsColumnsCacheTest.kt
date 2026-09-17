package ru.citeck.ecos.data.sql.test.records

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import ru.citeck.ecos.data.sql.dto.DbColumnType
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType
import ru.citeck.ecos.records2.predicate.model.Predicates
import java.util.*

class DbRecordsColumnsCacheTest : DbRecordsTestBase() {

    @Test
    fun test() {

        // PG-direct: drives raw "ALTER TABLE ... DROP COLUMN" DDL through the data source to verify
        // the prepared-statement column cache is invalidated. No SQL engine on the in-mem backend.
        assumeRawSqlSupported()

        registerAtts(
            Array(10) {
                AttributeDef.create {
                    withId("att-$it")
                }
            }.toList()
        )

        val record = createRecord(
            *Array(10) {
                "att-$it" to "val-$it"
            }
        )

        assertThat(records.getAtt(record, "att-0").asText()).isEqualTo("val-0")

        // sqlUpdate runs in its own committed transaction (TxnContext.doInTxn), unlike a bare
        // dbDataSource.withTransaction(false) with no enclosing TxnContext transaction: on the
        // managed PG datasource that leaves the connection unenlisted, and it is rolled back on
        // return to the pool - the DDL would silently never happen
        sqlUpdate("ALTER TABLE ${tableRef.fullName} DROP COLUMN \"att-5\"")

        // this instance's cache still lists "att-5": the very next read selects it and fails -
        // that failure is what resets the cache. The attempt itself is lost (rethrown), not silently
        // absorbed.
        assertThatThrownBy { records.getAtt(record, "att-0") }

        // the cache is reset now, so the read that follows succeeds again
        assertThat(records.getAtt(record, "att-0").asText()).isEqualTo("val-0")

        sqlUpdate("ALTER TABLE ${tableRef.fullName} DROP COLUMN \"att-6\"")

        assertThatThrownBy {
            records.query(
                baseQuery.copy {
                    withQuery(Predicates.eq("att-1", "val-1"))
                }
            )
        }

        val queryRes = records.query(
            baseQuery.copy {
                withQuery(Predicates.eq("att-1", "val-1"))
            }
        ).getRecords()
        assertThat(queryRes).containsExactly(record)
    }

    /**
     * The cluster case the column migration makes routine: another instance converted the column
     * while this one still holds the old definition. The old message regex did not match the errors
     * a type change produces, so the cache stayed stale until the process restarted.
     */
    @Test
    fun columnTypeChangedByAnotherInstanceTest() {

        // drives raw DDL through the data source, which only a real SQL backend exposes
        assumeRawSqlSupported()

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("numAtt")
                    withType(AttributeType.NUMBER)
                }
            )
        )
        val record = createRecord("numAtt" to 42)
        assertThat(getTableCtx().getColumnByName("numAtt")?.type).isEqualTo(DbColumnType.DOUBLE)

        // another instance converts the column; this instance's cache knows nothing about it.
        // sqlUpdate (TxnContext.doInTxn) is required here, not a bare dbDataSource.withTransaction:
        // on the managed PG datasource, a transaction with no enclosing TxnContext transaction never
        // gets enlisted and is rolled back when the connection returns to the pool - the ALTER would
        // never actually take effect.
        sqlUpdate(
            "ALTER TABLE ${tableRef.fullName} " +
                "ALTER COLUMN \"numAtt\" TYPE VARCHAR USING \"numAtt\"::varchar"
        )
        assertThat(getTableCtx().getColumnByName("numAtt")?.type)
            .describedAs("cache must still be stale before the query")
            .isEqualTo(DbColumnType.DOUBLE)

        // the query fails on the type mismatch - that is expected and is not what this test pins
        runCatching {
            records.query(
                baseQuery.copy {
                    withQuery(Predicates.eq("numAtt", 42))
                }
            )
        }

        // what it pins: the failure invalidated the cache instead of leaving it stale forever
        assertThat(getTableCtx().getColumnByName("numAtt")?.type)
            .describedAs("the type mismatch must have reset the column cache")
            .isEqualTo(DbColumnType.TEXT)

        // and reading still works against the new physical type
        val afterReset = records.query(
            baseQuery.copy {
                withQuery(Predicates.eq("_type", REC_TEST_TYPE_REF))
            }
        ).getRecords()
        assertThat(afterReset).containsExactly(record)
    }
}
