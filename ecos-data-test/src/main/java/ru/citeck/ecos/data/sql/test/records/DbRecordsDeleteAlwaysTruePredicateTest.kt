package ru.citeck.ecos.data.sql.test.records

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import ru.citeck.ecos.data.sql.repo.entity.DbEntity
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.records2.predicate.model.Predicate
import ru.citeck.ecos.records2.predicate.model.Predicates
import ru.citeck.ecos.records2.predicate.model.ValuePredicate
import ru.citeck.ecos.txn.lib.TxnContext

/**
 * `DbDataServiceImpl.delete(Predicate)` treats its two degenerate predicates asymmetrically on
 * purpose: an always-false predicate can never match anything, so it stays a documented silent
 * no-op, while an always-true predicate is rejected up front - "delete every row in the table" is
 * not an operation this API offers by design; a caller that means that should say so explicitly
 * via `delete(entityId)` / `delete(entities: List<Long>)`.
 *
 * Before this guard, an always-true delete silently wiped the whole table on the in-memory
 * backend, while the SQL backend generated `DELETE FROM "schema"."table" "r" WHERE ` with an
 * empty condition and blew up with a raw `PSQLException: syntax error at end of input` - two
 * different kinds of wrong for the exact same call. This test runs unmodified against both
 * backends via `dependenciesToScan`.
 */
class DbRecordsDeleteAlwaysTruePredicateTest : DbRecordsTestBase() {

    @Test
    fun deleteWithAlwaysTruePredicateThrowsAndLeavesRowsInPlaceTest() {

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("textAtt")
                }
            )
        )

        createRecord("textAtt" to "idx-0")
        createRecord("textAtt" to "idx-1")
        createRecord("textAtt" to "idx-2")

        assertThat(records.query(baseQuery).getRecords()).hasSize(3)

        assertThrows<IllegalArgumentException> {
            TxnContext.doInTxn {
                mainCtx.dataService.delete(Predicates.alwaysTrue())
            }
        }

        assertThat(records.query(baseQuery).getRecords())
            .describedAs("rejecting the call must happen before any row is touched")
            .hasSize(3)
    }

    /**
     * The three shapes that mean "every row" without being a bare `VoidPredicate`.
     * `PredicateUtils.isAlwaysTrue` is literally `predicate is VoidPredicate`, so before the guard
     * was normalised through `PredicateUtils.optimize` every one of these walked straight past it
     * and wiped the table - the in-memory backend silently, the SQL backend with a raw syntax error
     * from an empty `WHERE`.
     */
    private fun disguisedAlwaysTruePredicates(): List<Pair<String, Predicate>> {
        return listOf(
            "and(alwaysTrue())" to Predicates.and(Predicates.alwaysTrue()),
            "not(alwaysFalse())" to Predicates.not(Predicates.alwaysFalse()),
            "an empty and()" to Predicates.and()
        )
    }

    @Test
    fun deleteWithADisguisedAlwaysTruePredicateIsRejectedByTheDataServiceTest() {

        registerAtts(listOf(AttributeDef.create { withId("textAtt") }))

        createRecord("textAtt" to "idx-0")
        createRecord("textAtt" to "idx-1")
        createRecord("textAtt" to "idx-2")

        for ((name, predicate) in disguisedAlwaysTruePredicates()) {
            assertThatThrownBy {
                TxnContext.doInTxn { mainCtx.dataService.delete(predicate) }
            }
                .describedAs("$name means every row just as surely as a bare always-true predicate does")
                .isInstanceOf(IllegalArgumentException::class.java)
        }

        assertThat(records.query(baseQuery).getRecords())
            .describedAs("rejecting the call must happen before any row is touched")
            .hasSize(3)
    }

    /**
     * The same three shapes one layer down, against whichever `DbEntityRepo` the backend under test
     * provides - `DbEntityRepoPg` for PostgreSQL, `InMemEntityRepo` for the in-memory one. Each
     * carries its own copy of the guard (a caller holding the repo bypasses `DbDataServiceImpl`
     * entirely), so each has to be normalised the same way.
     */
    @Test
    fun deleteWithADisguisedAlwaysTruePredicateIsRejectedByTheEntityRepoTest() {

        registerAtts(listOf(AttributeDef.create { withId("textAtt") }))

        createRecord("textAtt" to "idx-0")
        createRecord("textAtt" to "idx-1")

        val entityRepo = getTableCtx().getSchemaCtx().dataSourceCtx.entityRepo
        for ((name, predicate) in disguisedAlwaysTruePredicates()) {
            assertThatThrownBy {
                TxnContext.doInTxn { entityRepo.delete(getTableCtx(), predicate) }
            }
                .describedAs("$name must be refused by the repository itself, not only by DbDataServiceImpl")
                .isInstanceOf(IllegalArgumentException::class.java)
        }

        assertThat(records.query(baseQuery).getRecords())
            .describedAs("the guard runs before the repository touches anything")
            .hasSize(2)
    }

    @Test
    fun deleteWithNormalPredicateStillWorksTest() {

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("textAtt")
                }
            )
        )

        val recToKeep = createRecord("textAtt" to "idx-0")
        val recToDelete = createRecord("textAtt" to "idx-1")

        TxnContext.doInTxn {
            mainCtx.dataService.delete(ValuePredicate.eq(DbEntity.EXT_ID, recToDelete.getLocalId()))
        }

        assertThat(mainCtx.dataService.findByExtId(recToDelete.getLocalId())).isNull()
        assertThat(mainCtx.dataService.findByExtId(recToKeep.getLocalId())).isNotNull()
    }

    @Test
    fun deleteWithAlwaysFalsePredicateIsStillANoOpTest() {

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("textAtt")
                }
            )
        )

        createRecord("textAtt" to "idx-0")
        createRecord("textAtt" to "idx-1")

        TxnContext.doInTxn {
            mainCtx.dataService.delete(Predicates.alwaysFalse())
        }

        assertThat(records.query(baseQuery).getRecords()).hasSize(2)
    }
}
