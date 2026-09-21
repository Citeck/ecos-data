package ru.citeck.ecos.data.sql.test.records

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import ru.citeck.ecos.commons.data.ObjectData
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType
import ru.citeck.ecos.model.lib.type.dto.TypeInfo
import ru.citeck.ecos.model.lib.type.dto.TypeModelDef
import ru.citeck.ecos.model.lib.utils.ModelUtils
import ru.citeck.ecos.records3.record.dao.query.dto.query.SortBy

/**
 * What a query does with a name it cannot read - in an aggregate, and in a sort key.
 *
 * Three shapes an independent review found regressed while this work was in progress; the bodies
 * are that review's reproductions, kept as they were written.
 */
class DbUnreadableAttributeDegradationTest : DbRecordsTestBase() {

    /**
     * `groupBy = ["*"]` is one group over everything, but no `GROUP BY` is emitted for it: the
     * rows are folded by the aggregate standing in the select list. Answering an aggregate over an
     * attribute that is not there with a constant removes the last aggregate from that list and
     * the query turns back into one row per record - only when every selected expression is such
     * an aggregate, which is why a `count(*)` next to it hides this.
     */
    @Test
    fun missingAggregateMustStillCollapseAllRows() {
        registerAtts(listOf(AttributeDef.create { withId("text0") }))
        createRecord("text0" to "a")
        createRecord("text0" to "b")
        createRecord("text0" to "c")
        val result = records.query(
            baseQuery.copy {
                withSortBy(emptyList())
                withGroupBy(listOf("*"))
            },
            mapOf("value" to "max(noSuchColumn)")
        ).getRecords()
        assertThat(result).hasSize(1)
    }

    /**
     * `null` written by the caller is not an attribute that is missing. Both become the same token
     * once the columns are looked up, so the two have to be told apart before that - `max(null)` is
     * a question with an answer, and the answer is NULL.
     */
    @Test
    fun explicitNullAggregateMustRemainNull() {
        registerAtts(listOf(AttributeDef.create { withId("text0") }))
        createRecord("text0" to "a")
        createRecord("text0" to "a")
        val result = records.query(
            baseQuery.copy {
                withSortBy(emptyList())
                withGroupBy(listOf("text0"))
            },
            mapOf("value" to "max(null)")
        ).getRecords()
        assertThat(result).hasSize(1)
        assertThat(result[0]["value"].isNull()).isTrue()
    }

    /**
     * A sort key that cannot be prepared is dropped, and it must not leave its joins behind. The
     * target type here has never held a record, so its table does not exist yet: a join emitted
     * for the refused key fails the whole query with `relation ... does not exist` instead of the
     * query running without that sort.
     */
    @Test
    fun failedSortMustNotRegisterJoinToMissingTable() {
        val targetTypeRef = ModelUtils.getTypeRef("peer-review-target")
        createRecordsDao(tableRef.withTable("peer-review-target"), targetTypeRef, "peer-review-target")
        registerType(
            TypeInfo.create {
                withId(targetTypeRef.getLocalId())
                withSourceId("peer-review-target")
                withModel(TypeModelDef.create().withAttributes(listOf(AttributeDef.create { withId("name") })).build())
            }
        )
        registerAtts(
            listOf(
                AttributeDef.create { withId("text0") },
                AttributeDef.create {
                    withId("assoc")
                    withType(AttributeType.ASSOC)
                    withConfig(ObjectData.create().set("typeRef", targetTypeRef))
                }
            )
        )
        val source = createRecord("text0" to "a")
        val result = records.query(
            baseQuery.copy { withSortBy(listOf(SortBy("assoc.noSuchColumn", true))) }
        ).getRecords()
        assertThat(result.map { it.toString() }).containsExactly(source.toString())
    }
}
