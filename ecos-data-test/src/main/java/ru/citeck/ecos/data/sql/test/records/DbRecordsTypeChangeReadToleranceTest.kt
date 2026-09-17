package ru.citeck.ecos.data.sql.test.records

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import ru.citeck.ecos.commons.data.ObjectData
import ru.citeck.ecos.data.sql.dto.DbColumnType
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType
import ru.citeck.ecos.model.lib.type.dto.TypeInfo
import ru.citeck.ecos.model.lib.type.dto.TypeModelDef
import ru.citeck.ecos.model.lib.utils.ModelUtils
import ru.citeck.ecos.records2.predicate.model.Predicates
import ru.citeck.ecos.records3.record.dao.query.dto.query.SortBy
import ru.citeck.ecos.webapp.api.entity.EntityRef

/**
 * Reading must never fail because the type model and the physical schema disagree.
 *
 * Such a mismatch is normal today - a failed column conversion leaves the model and the schema
 * out of sync forever - and it becomes an expected transient state once background column
 * migration lands. Either way the journal has to keep returning rows.
 *
 * The contract these tests pin down: a model/schema disagreement never fails a read. At worst the
 * attribute comes back empty and the filter finds nothing, with a warning in the log. The exact
 * filtering result under a mismatch is deliberately left unspecified. The write path keeps no such
 * tolerance - failing there is correct.
 */
class DbRecordsTypeChangeReadToleranceTest : DbRecordsTestBase() {

    /**
     * Leaves the table in the broken state the customer hits: the column is VARCHAR holding
     * stringified ref ids, while the model says the attribute is an association.
     *
     * Going ASSOC -> TEXT is allowed (BIGINT -> VARCHAR is a supported cast), the way back is not,
     * so the schema is stuck as VARCHAR. No mutation may happen after the model is switched back -
     * it would fail on the unsupported TEXT -> LONG conversion, which is a different defect.
     *
     * @return the records that exist in the table afterwards
     */
    private fun createAssocToTextMismatch(): List<EntityRef> {

        registerAssocAtt(AttributeType.ASSOC)

        val target = createRecord()
        val withAssoc = createRecord("assocAtt" to target)

        registerAssocAtt(AttributeType.TEXT)

        // any mutation triggers ensureColumnsExist, which performs the BIGINT -> VARCHAR migration
        val plainText = createRecord("assocAtt" to "plain-text")
        assertThat(getColumns().first { it.name == "assocAtt" }.type)
            .isEqualTo(DbColumnType.TEXT)

        registerAssocAtt(AttributeType.ASSOC)

        return listOf(target, withAssoc, plainText)
    }

    private fun registerAssocAtt(type: AttributeType) {
        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("assocAtt")
                    withType(type)
                }
            )
        )
    }

    @Test
    fun queryReturnsRecordsWhenAssocColumnIsTextTest() {

        val expected = createAssocToTextMismatch()

        val queryRes = records.query(
            baseQuery.copy()
                .withQuery(Predicates.eq("_type", REC_TEST_TYPE_REF))
                .build()
        )

        assertThat(queryRes.getRecords()).containsExactlyInAnyOrderElementsOf(expected)
    }

    @Test
    fun attributeIsEmptyInsteadOfFailingWhenAssocColumnIsTextTest() {

        // Skipped on the in-memory backend, but not because the column definition stays BIGINT
        // there - it does flip to TEXT. If it did not, the getColumns() assertion inside the
        // shared createAssocToTextMismatch() would already fail the first test of this class,
        // which is not skipped on in-mem. What in-mem does not do is convert the stored VALUES:
        // they stay Long, so assocMapping still resolves them and the attribute comes back as a
        // real ref instead of being empty. Reproducing this assertion there needs a value
        // conversion on column type change, which the column-type migration plan adds to
        // DbSchemaDao for both backends.
        assumeRawSqlSupported()

        val expected = createAssocToTextMismatch()

        val atts = records.getAtts(expected, listOf("assocAtt?id"))

        assertThat(atts).hasSize(expected.size)
        atts.forEach {
            assertThat(it.getAtt("assocAtt?id").asText()).isEmpty()
        }
    }

    @Test
    fun queryReturnsRecordsWhenAssocRefIsDanglingTest() {

        // This test writes the column value directly via raw SQL, which only a real SQL backend
        // exposes.
        assumeRawSqlSupported()

        registerAssocAtt(AttributeType.ASSOC)

        val target = createRecord()
        val withAssoc = createRecord("assocAtt" to target)

        // No sequence in ed_record_ref will ever reach this id, so it is a dangling reference:
        // the column is still BIGINT (a supported, healthy type), but nothing backs this id -
        // e.g. the referenced row was hard-deleted from ed_record_ref out of band.
        val danglingId = 999999999999L
        sqlUpdate(
            "UPDATE ${tableRef.fullName} SET \"assocAtt\" = $danglingId " +
                "WHERE __ext_id = '${withAssoc.getLocalId()}'"
        )

        val queryRes = records.query(
            baseQuery.copy()
                .withQuery(Predicates.eq("_type", REC_TEST_TYPE_REF))
                .build()
        )

        assertThat(queryRes.getRecords()).containsExactlyInAnyOrder(target, withAssoc)

        val atts = records.getAtts(listOf(withAssoc), listOf("assocAtt?id"))
        assertThat(atts).hasSize(1)
        assertThat(atts[0].getAtt("assocAtt?id").asText()).isEmpty()
    }

    @Test
    fun queryWithAssocPredicateDoesNotFailWhenAssocColumnIsTextTest() {

        val expected = createAssocToTextMismatch()
        val target = expected[0]

        // The exact matching semantics under a mismatch are deliberately unspecified - a text
        // column holding stringified ref ids may or may not match a ref predicate. What is
        // guaranteed is that the query completes instead of failing the whole journal.
        val queryRes = records.query(
            baseQuery.copy()
                .withQuery(
                    Predicates.and(
                        Predicates.eq("_type", REC_TEST_TYPE_REF),
                        Predicates.eq("assocAtt", target)
                    )
                )
                .build()
        )

        assertThat(queryRes.getRecords()).isSubsetOf(expected)
    }

    /**
     * The target type/table for createAssocWithTargetTypeToTextMismatch(). Unlike
     * createAssocToTextMismatch()'s assocAtt (which deliberately has no "typeRef" config, since
     * the older tests only read the raw column value), this attribute resolves to a real,
     * separately registered type - required for the dotted-attribute code paths under test here
     * to ever reach the guarded code instead of bailing out on "target type unknown".
     */
    private val mismatchTargetTypeRef = ModelUtils.getTypeRef("assoc-mismatch-target-type")

    private fun registerAssocAttWithTargetType(type: AttributeType) {
        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("assocAtt")
                    withType(type)
                    if (type == AttributeType.ASSOC) {
                        withConfig(ObjectData.create().set("typeRef", mismatchTargetTypeRef.toString()))
                    }
                }
            )
        )
    }

    /**
     * Same ASSOC -> TEXT column drift as createAssocToTextMismatch(), but the association points
     * at a real target type with its own DAO/table, configured with "typeRef" - see
     * mismatchTargetTypeRef's kdoc for why createAssocToTextMismatch() itself can't be reused for
     * these tests.
     *
     * @return the records of the main (REC_TEST_TYPE_REF) type that exist in the table afterwards
     */
    private fun createAssocWithTargetTypeToTextMismatch(): List<EntityRef> {

        val targetDao = createRecordsDao(
            DEFAULT_TABLE_REF.withTable("assoc-mismatch-target-table"),
            mismatchTargetTypeRef,
            mismatchTargetTypeRef.getLocalId()
        )
        registerType(
            TypeInfo.create {
                withId(mismatchTargetTypeRef.getLocalId())
                withSourceId(mismatchTargetTypeRef.getLocalId())
                withModel(
                    TypeModelDef.create()
                        .withAttributes(
                            listOf(
                                AttributeDef.create()
                                    .withId("targetText")
                                    .build()
                            )
                        )
                        .build()
                )
            }
        )

        registerAssocAttWithTargetType(AttributeType.ASSOC)

        val target = targetDao.createRecord("targetText" to "target-value")
        val withAssoc = createRecord("assocAtt" to target)

        registerAssocAttWithTargetType(AttributeType.TEXT)

        // any mutation triggers ensureColumnsExist, which performs the BIGINT -> VARCHAR migration
        val plainText = createRecord("assocAtt" to "plain-text")
        assertThat(getColumns().first { it.name == "assocAtt" }.type)
            .isEqualTo(DbColumnType.TEXT)

        registerAssocAttWithTargetType(AttributeType.ASSOC)

        return listOf(withAssoc, plainText)
    }

    @Test
    fun queryWithDottedAssocAttDoesNotFailWhenAssocColumnIsTextTest() {

        val expected = createAssocWithTargetTypeToTextMismatch()

        // Requesting "assocAtt.targetText" through the query's attributes list does NOT reach
        // prepareAssocSelectJoin: Records3's schema resolver splits it into a hierarchy (a node
        // named "assocAtt" with "targetText" as its own inner node), so DbRecordsQueryDao only
        // ever calls registerSelectAtt("assocAtt", ...) - never with an embedded dot - and the
        // actual value is served later through generic per-record dereference, bypassing the SQL
        // join optimization entirely. sortBy is different: RecordsQuery.sortBy.attribute is a flat
        // string handed to registerSelectAtt as-is (DbRecordsQueryDao.kt, sortBy handling), which is
        // exactly how the existing DbRecordsAssocTableJoinTest exercises this same join (e.g.
        // withSortBy(SortBy("assocAtt.targetNum", ascending))). Without the guard in
        // DbFindQueryContext.prepareAssocSelectJoin this query fails while building the SQL,
        // because the join compares the target's BIGINT __ref_id against a VARCHAR column.
        val queryRes = records.query(
            baseQuery.copy()
                .withQuery(Predicates.eq("_type", REC_TEST_TYPE_REF))
                .withSortBy(listOf(SortBy("assocAtt.targetText", true)))
                .build()
        )

        assertThat(queryRes.getRecords()).hasSize(expected.size)
    }

    @Test
    fun queryWithDottedAssocPredicateDoesNotFailWhenAssocColumnIsTextTest() {

        createAssocWithTargetTypeToTextMismatch()

        // Filtering through a single (non-multiple) association goes through
        // processAssocJoinWithPredicates, not through prepareAssocSelectJoin. Same invariant: the
        // query must complete. Here the degradation is not merely "unspecified" - the code maps
        // the inner value predicate to Predicates.alwaysFalse(), so the deterministic contract is
        // an empty result, and that is what is asserted.
        val queryRes = records.query(
            baseQuery.copy()
                .withQuery(
                    Predicates.and(
                        Predicates.eq("_type", REC_TEST_TYPE_REF),
                        Predicates.eq("assocAtt.targetText", "whatever")
                    )
                )
                .build()
        )

        assertThat(queryRes.getRecords()).isEmpty()
    }

    /**
     * The schema is generated from the type model by the WRITE path only (ensureColumnsExist), so
     * an attribute added to the model and never saved since has no column at all.
     *
     * At the top level of a query such a condition never reaches the SQL builder:
     * DbDataServiceImpl.preparePredicate already rewrites a predicate on an unknown column into
     * Predicates.alwaysFalse(). This test pins that behaviour down - in particular that the
     * condition degrades to "matches nothing" and not to "is dropped", which would widen the
     * result set. The inner predicates of an association join do NOT go through preparePredicate;
     * they are covered by queryWithDottedFilterByNeverMaterializedColumnDoesNotFailTest below.
     */
    @Test
    fun queryWithFilterByNeverMaterializedColumnDoesNotFailTest() {

        registerAtts(listOf(AttributeDef.create { withId("textAtt") }))
        val rec = createRecord("textAtt" to "value")

        // the new attribute is added to the model, and nothing is saved afterwards
        registerAtts(
            listOf(
                AttributeDef.create { withId("textAtt") },
                AttributeDef.create { withId("newTextAtt") }
            )
        )
        assertThat(getColumns().map { it.name }).doesNotContain("newTextAtt")

        val queryRes = records.query(
            baseQuery.copy()
                .withQuery(Predicates.eq("newTextAtt", "anything"))
                .build()
        )
        assertThat(queryRes.getRecords()).isEmpty()

        val andRes = records.query(
            baseQuery.copy()
                .withQuery(
                    Predicates.and(
                        Predicates.eq("textAtt", "value"),
                        Predicates.eq("newTextAtt", "anything")
                    )
                )
                .build()
        )
        assertThat(andRes.getRecords()).isEmpty()

        // sanity check: the healthy half of the same condition does find the record
        val healthyRes = records.query(
            baseQuery.copy()
                .withQuery(Predicates.eq("textAtt", "value"))
                .build()
        )
        assertThat(healthyRes.getRecords()).containsExactly(rec)
    }

    /**
     * Registers a separate target type/table, points "assocAtt" at it, and creates one linked pair
     * of records. Afterwards the target type gets two more attributes which are never saved, so
     * they exist in the model and not in the schema.
     *
     * @return the record of the main type which references the target one
     */
    private fun createTargetTypeWithNeverMaterializedAtts(): EntityRef {

        val targetDao = createRecordsDao(
            DEFAULT_TABLE_REF.withTable("assoc-mismatch-target-table"),
            mismatchTargetTypeRef,
            mismatchTargetTypeRef.getLocalId()
        )
        fun registerTargetType(atts: List<AttributeDef>) {
            registerType(
                TypeInfo.create {
                    withId(mismatchTargetTypeRef.getLocalId())
                    withSourceId(mismatchTargetTypeRef.getLocalId())
                    withModel(TypeModelDef.create().withAttributes(atts).build())
                }
            )
        }
        registerTargetType(listOf(AttributeDef.create().withId("targetText").build()))
        registerAssocAttWithTargetType(AttributeType.ASSOC)

        val target = targetDao.createRecord("targetText" to "target-value")
        val withAssoc = createRecord("assocAtt" to target)

        // the target type gets two more attributes, and nothing is saved into the target table
        // afterwards - so ensureColumnsExist never materializes them
        registerTargetType(
            listOf(
                AttributeDef.create().withId("targetText").build(),
                AttributeDef.create().withId("newTargetText").build(),
                AttributeDef.create()
                    .withId("newTargetAssoc")
                    .withType(AttributeType.ASSOC)
                    .withMultiple(true)
                    .build()
            )
        )
        assertThat(targetDao.getColumns().map { it.name })
            .doesNotContain("newTargetText", "newTargetAssoc")

        return withAssoc
    }

    /**
     * A dotted journal filter - "assocAtt.newTargetText" - where the inner attribute exists in the
     * target type model but was never materialized as a column.
     *
     * Unlike a top-level condition, the inner predicate of an association join is handed straight
     * to the SQL builder (DbEntityRepoPg.addAssocTableCondition), bypassing
     * DbDataServiceImpl.preparePredicate. Without the guard it fails the whole listing with
     * "column is not found".
     */
    @Test
    fun queryWithDottedFilterByNeverMaterializedColumnDoesNotFailTest() {

        val withAssoc = createTargetTypeWithNeverMaterializedAtts()

        val queryRes = records.query(
            baseQuery.copy()
                .withQuery(Predicates.eq("assocAtt.newTargetText", "anything"))
                .build()
        )
        assertThat(queryRes.getRecords()).isEmpty()

        // the listing itself is healthy: the same journal without the broken condition works
        val healthyRes = records.query(
            baseQuery.copy()
                .withQuery(Predicates.eq("assocAtt.targetText", "target-value"))
                .build()
        )
        assertThat(healthyRes.getRecords()).containsExactly(withAssoc)
    }

    /**
     * Same as above, but the never-materialized inner attribute is an association. Association
     * conditions are answered through ed_associations and never touch the source column, so a
     * missing column is not even a reason to refuse the condition - which is why the
     * "column is not found" check has to sit below the association branches, not above them.
     */
    @Test
    fun queryWithDottedFilterByNeverMaterializedAssocColumnDoesNotFailTest() {

        val withAssoc = createTargetTypeWithNeverMaterializedAtts()

        val queryRes = records.query(
            baseQuery.copy()
                .withQuery(Predicates.eq("assocAtt.newTargetAssoc", withAssoc))
                .build()
        )
        assertThat(queryRes.getRecords()).isEmpty()
    }

    /**
     * A journal column with an aggregation over an association - sum(assocAtt.targetNum) - after
     * the admin changed assocAtt from Association to Text. The association can no longer be
     * joined, so the expression is dropped; the journal itself must still load.
     */
    @Test
    fun queryWithExpressionOverChangedAssocAttDoesNotFailTest() {

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("assocAtt")
                    withType(AttributeType.TEXT)
                }
            )
        )
        val rec = createRecord("assocAtt" to "plain-text")

        val queryRes = records.query(
            baseQuery.copy()
                .withQuery(Predicates.eq("_type", REC_TEST_TYPE_REF))
                .build(),
            listOf("assocAtt", "sum(assocAtt.targetNum)")
        )

        assertThat(queryRes.getRecords()).hasSize(1)
        assertThat(queryRes.getRecords()[0].getId()).isEqualTo(rec)
        assertThat(queryRes.getRecords()[0]["assocAtt"].asText()).isEqualTo("plain-text")
        // the expression is refused, so the column is simply empty
        assertThat(queryRes.getRecords()[0]["sum(assocAtt.targetNum)"].asText()).isEmpty()
    }

    /**
     * Backward associations (assoc_src_*) are resolved from ed_associations. A row whose source id
     * has no counterpart in ed_record_ref anymore must be skipped, not fail the record read - the
     * same tolerance the forward direction already has.
     */
    @Test
    fun sourceAssocsAreReadWhenSourceRefIsDanglingTest() {

        // the broken row is produced with raw SQL, which only a real SQL backend exposes
        assumeRawSqlSupported()

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("assocAtt")
                    withType(AttributeType.ASSOC)
                    withMultiple(true)
                }
            )
        )
        val target = createRecord()
        val source = createRecord("assocAtt" to listOf(target))

        assertThat(records.getAtt(target, "assoc_src_assocAtt[]?id").asStrList())
            .containsExactly(source.toString())

        // No sequence in ed_record_ref will ever reach this id: the association row now points at
        // a source which does not exist - e.g. it was hard-deleted out of band.
        val danglingId = 999999999999L
        sqlUpdate(
            "UPDATE ${tableRef.withTable("ed_associations").fullName} " +
                "SET \"__source_id\" = $danglingId WHERE \"__source_id\" = " +
                "(SELECT \"__ref_id\" FROM ${tableRef.fullName} " +
                "WHERE __ext_id = '${source.getLocalId()}')"
        )

        assertThat(records.getAtt(target, "assoc_src_assocAtt[]?id").asStrList()).isEmpty()
    }

    /**
     * A "_type.*" condition makes the DAO resolve every __type value present in the table. A row
     * whose __type has no counterpart in ed_record_ref must not fail the building of the query.
     */
    @Test
    fun queryWithComplexTypeConditionDoesNotFailWhenTypeRefIsDanglingTest() {

        // the broken row is produced with raw SQL, which only a real SQL backend exposes
        assumeRawSqlSupported()

        registerAtts(listOf(AttributeDef.create { withId("textAtt") }))
        val healthy = createRecord("textAtt" to "value")
        val broken = createRecord("textAtt" to "value2")

        val danglingId = 999999999999L
        sqlUpdate(
            "UPDATE ${tableRef.fullName} SET \"__type\" = $danglingId " +
                "WHERE __ext_id = '${broken.getLocalId()}'"
        )

        val queryRes = records.query(
            baseQuery.copy()
                .withQuery(Predicates.eq("_type.id", REC_TEST_TYPE_ID))
                .build()
        )

        assertThat(queryRes.getRecords()).containsExactly(healthy)
    }
}
