package ru.citeck.ecos.data.sql.test.records

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import ru.citeck.ecos.data.sql.repo.find.DbFindPage
import ru.citeck.ecos.data.sql.repo.find.DbFindQuery
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.records2.predicate.model.Predicates

/**
 * What `DbDataService.find` answers when no items are asked for.
 *
 * `maxItems = 0` is how a caller asks for the total alone, and the total is built by its own query
 * rather than by counting what a select returned. Whether that query runs at all is the
 * `withTotalCount` flag - the in-memory backend has always read it there, PostgreSQL used to run
 * the count regardless and report it, so the two disagreed for a caller that asked for neither.
 */
class DbDataServiceCountContractTest : DbRecordsTestBase() {

    @Test
    fun theTotalIsReturnedOnlyWhenItIsAskedForTest() {

        registerAtts(listOf(AttributeDef.create { withId("textAtt") }))

        createRecord("textAtt" to "abc")
        createRecord("textAtt" to "abc")
        createRecord("textAtt" to "def")

        val query = DbFindQuery.create()
            .withPredicate(Predicates.alwaysTrue())
            .build()

        val withTotal = mainCtx.dataService.find(query, DbFindPage.ZERO, true)
        assertThat(withTotal.entities).isEmpty()
        assertThat(withTotal.totalCount).isEqualTo(3)

        val withoutTotal = mainCtx.dataService.find(query, DbFindPage.ZERO, false)
        assertThat(withoutTotal.entities).isEmpty()
        assertThat(withoutTotal.totalCount)
            .describedAs("a caller that does not ask for the total does not get one")
            .isEqualTo(0)

        // the overload without the flag is the same thing said shorter
        assertThat(mainCtx.dataService.find(query, DbFindPage.ZERO).totalCount).isEqualTo(0)

        // and the dedicated way of asking still answers
        assertThat(mainCtx.dataService.getCount(query)).isEqualTo(3)

        val narrowed = DbFindQuery.create()
            .withPredicate(Predicates.eq("textAtt", "abc"))
            .build()
        assertThat(mainCtx.dataService.find(narrowed, DbFindPage.ZERO, true).totalCount).isEqualTo(2)
        assertThat(mainCtx.dataService.getCount(narrowed)).isEqualTo(2)
    }

    /**
     * A query built by hand, naming something the table cannot read.
     *
     * `find` has always refused such a query outright. The count did not: it leaves `find` through
     * the "no items asked for" branch, which returned before the check ran, so a group-by over a
     * column that is not there reached PostgreSQL as `column r.noSuchColumn does not exist` and the
     * in-memory backend answered a number. The check now runs before either of them can leave.
     *
     * Loud here on purpose: at this level the query is assembled in code, so an unreadable name is
     * a mistake in that code. The records DAO one layer up answers the same situation with an empty
     * result, because there the name comes from a saved view - see
     * [DbRecordsGroupingTest.groupingByANameTheQueryCannotReadTest].
     */
    @Test
    fun aQueryNamingSomethingItCannotReadIsRefusedByTheCountAsWellTest() {

        registerAtts(listOf(AttributeDef.create { withId("textAtt") }))

        createRecord("textAtt" to "abc")
        createRecord("textAtt" to "def")

        val invalid = DbFindQuery.create()
            .withPredicate(Predicates.alwaysTrue())
            .withGroupBy(listOf("noSuchColumn"))
            .build()

        assertThatThrownBy { mainCtx.dataService.getCount(invalid) }
            .describedAs("the count refuses it the same way find does")
            .isInstanceOf(IllegalStateException::class.java)
            .hasMessageContaining("Grouping by columns [noSuchColumn] is not allowed")

        assertThatThrownBy { mainCtx.dataService.find(invalid, DbFindPage.ZERO, true) }
            .isInstanceOf(IllegalStateException::class.java)
            .hasMessageContaining("Grouping by columns [noSuchColumn] is not allowed")

        assertThatThrownBy { mainCtx.dataService.find(invalid, DbFindPage.ALL, true) }
            .isInstanceOf(IllegalStateException::class.java)
            .hasMessageContaining("Grouping by columns [noSuchColumn] is not allowed")

        // a grouping the table can read still counts its groups
        val valid = DbFindQuery.create()
            .withPredicate(Predicates.alwaysTrue())
            .withGroupBy(listOf("textAtt"))
            .build()
        assertThat(mainCtx.dataService.getCount(valid)).isEqualTo(2)
    }
}
