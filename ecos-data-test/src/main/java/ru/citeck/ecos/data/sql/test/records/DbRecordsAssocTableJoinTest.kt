package ru.citeck.ecos.data.sql.test.records

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import ru.citeck.ecos.commons.data.ObjectData
import ru.citeck.ecos.model.lib.aspect.dto.AspectInfo
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType
import ru.citeck.ecos.model.lib.type.dto.TypeInfo
import ru.citeck.ecos.model.lib.type.dto.TypeModelDef
import ru.citeck.ecos.model.lib.utils.ModelUtils
import ru.citeck.ecos.records2.predicate.model.Predicate
import ru.citeck.ecos.records2.predicate.model.Predicates
import ru.citeck.ecos.records3.record.dao.query.dto.query.SortBy
import ru.citeck.ecos.webapp.api.entity.EntityRef

class DbRecordsAssocTableJoinTest : DbRecordsTestBase() {

    private val mainTypeRef = REC_TEST_TYPE_REF
    private val targetTypeRef = ModelUtils.getTypeRef("target-type")
    private val childTypeRef = ModelUtils.getTypeRef("child-type")

    private lateinit var targetDao: RecordsDaoTestCtx
    private lateinit var childDao: RecordsDaoTestCtx

    @Test
    fun testExpressionWithJoin() {

        val targetRec0 = targetDao.createRecord("_name" to "first", "targetText" to "abc", "targetNum" to 10)
        val targetRec1 = targetDao.createRecord("_name" to "second", "targetText" to "def", "targetNum" to 100)

        val rec0 = createRecord("assocAtt" to targetRec0, "numAtt" to 22)
        val rec1 = createRecord("assocAtt" to targetRec0, "numAtt" to 23)
        createRecord("assocAtt" to targetRec0, "numAtt" to 24)
        createRecord("assocAtt" to targetRec0, "numAtt" to 25)
        createRecord("assocAtt" to targetRec1, "numAtt" to 44)
        createRecord("assocAtt" to targetRec1, "numAtt" to 44)
        createRecord("assocAtt" to targetRec1, "numAtt" to 44)
        createRecord("assocAtt" to targetRec1, "numAtt" to 45)
        val rec4 = createRecord("assocAtt" to targetRec1, "numAtt" to 100)

        val res = records.query(
            baseQuery.copy()
                .withQuery(Predicates.ge("(numAtt - assocAtt.targetNum)", 0))
                .withSortBy(SortBy("(numAtt - assocAtt.targetNum)", true))
                // Test COUNT(*) query. If maxItems > (result records).size, then COUNT(*) won't be executed
                .withMaxItems(3)
                .build(),
            listOf(
                "assocAtt._name?disp",
                "assocAtt.targetNum",
                "(numAtt - assocAtt.targetNum)"
            )
        )

        assertThat(res.getRecords().map { it.getId() }).containsExactly(rec4, rec0, rec1)

        val res1 = records.query(
            baseQuery.copy()
                // test with expression which is not part of groupBy
                .withQuery(Predicates.gt("(numAtt - assocAtt.targetNum)", 0))
                .withGroupBy(listOf("assocAtt.targetText"))
                .build(),
            mapOf("text" to "assocAtt.targetText", "count" to "count(*)")
        ).getRecords()

        assertThat(res1).hasSize(1)
        assertThat(res1[0]["text"].asText()).isEqualTo("abc")
        assertThat(res1[0]["count"].asInt()).isEqualTo(4)

        val res2 = records.query(
            baseQuery.copy()
                .withGroupBy(listOf("assocAtt._name"))
                .withSortBy(SortBy("assocAtt._name", true))
                .build(),
            mapOf("name" to "assocAtt._name", "count" to "count(*)")
        ).getRecords()

        assertThat(res2).hasSize(2)
        assertThat(res2[0]["name"].asText()).isEqualTo("first")
        assertThat(res2[0]["count"].asInt()).isEqualTo(4)
        assertThat(res2[1]["name"].asText()).isEqualTo("second")
        assertThat(res2[1]["count"].asInt()).isEqualTo(5)
    }

    @Test
    fun testWithNonExistentColumn() {

        val targetRec0 = targetDao.createRecord("targetText" to "abc", "targetNum" to 1)
        val targetRec1 = targetDao.createRecord("targetText" to "abc", "targetNum" to 2)
        val mainRec = createRecord("multiAssocAtt" to listOf(targetRec0, targetRec1))

        val res0 = records.getAtt(mainRec, "sum(multiAssocAtt.targetNum)").asInt()
        assertThat(res0).isEqualTo(3)

        addAttribute(
            mainTypeRef.getLocalId(),
            AttributeDef.create()
                .withId("number")
                .withType(AttributeType.NUMBER)
                .build()
        )

        val res1 = records.getAtt(mainRec, "sum(multiAssocAtt.number)").asInt()
        assertThat(res1).isEqualTo(0)

        updateRecord(mainRec, "number" to 10)

        val res2 = records.getAtt(mainRec, "sum(multiAssocAtt.number)").asInt()
        assertThat(res2).isEqualTo(0)

        addAttribute(
            targetTypeRef.getLocalId(),
            AttributeDef.create()
                .withId("number")
                .withType(AttributeType.NUMBER)
                .build()
        )

        val res3 = records.getAtt(mainRec, "sum(multiAssocAtt.number)").asInt()
        assertThat(res3).isEqualTo(0)

        updateRecord(targetRec0, "number" to 123)

        val res4 = records.getAtt(mainRec, "sum(multiAssocAtt.number)").asInt()
        assertThat(res4).isEqualTo(123)
    }

    @Test
    fun testWithNonExistentTargetTable() {

        val rec0 = createRecord("textAtt" to "abc")
        val rec1 = createRecord("textAtt" to "def")

        // target table doesn't exist
        val queryRes = records.query(
            baseQuery.copy()
                .withQuery(
                    Predicates.or(
                        Predicates.eq("textAtt", "abc"),
                        Predicates.inVals("multiAssocAtt.targetAssoc", listOf(rec1)),
                        Predicates.inVals("multiAssocAtt.targetText", listOf(rec1))
                    )
                ).build()
        ).getRecords()

        assertThat(queryRes).containsExactly(rec0)

        // Nothing can be linked when the target table is not there, so an emptiness question is
        // true of every record - in either of its two spellings. Answered by predicate class, the
        // `eq(att, null)` one came out as "true of none", the opposite of `empty`.
        listOf(
            Predicates.empty("multiAssocAtt.targetText"),
            Predicates.eq("multiAssocAtt.targetText", null)
        ).forEach { predicate ->
            assertThat(
                records.query(baseQuery.copy { withQuery(predicate) }).getRecords()
            ).describedAs("Predicate: $predicate").containsExactlyInAnyOrder(rec0, rec1)
            assertThat(
                records.query(baseQuery.copy { withQuery(Predicates.not(predicate)) }).getRecords()
            ).describedAs("Predicate: not($predicate)").isEmpty()
        }

        val targetRef0 = targetDao.createRecord("targetText" to "ttt")

        // target table exists but attribute 'targetAssoc' doesn't exist
        val queryRes1 = records.query(
            baseQuery.copy()
                .withQuery(
                    Predicates.or(
                        Predicates.eq("textAtt", "abc"),
                        Predicates.inVals("multiAssocAtt.targetAssoc", listOf(rec1))
                    )
                ).build()
        ).getRecords()

        assertThat(queryRes1).containsExactly(rec0)

        val rec2 = createRecord("multiAssocAtt" to targetRef0)

        val queryRes2 = records.query(
            baseQuery.copy()
                .withQuery(
                    Predicates.or(
                        Predicates.eq("textAtt", "abc"),
                        Predicates.inVals("multiAssocAtt.targetText", listOf("ttt"))
                    )
                ).build()
        ).getRecords()

        assertThat(queryRes2).containsExactly(rec0, rec2)
    }

    @ParameterizedTest
    @ValueSource(strings = ["multiAssocAtt", "aspect0:multiAssocAtt", "multiAuthorityAtt"])
    fun testWithMultipleAssoc(multiAssocName: String) {

        val targetRec0 = targetDao.createRecord("targetText" to "abc", "targetNum" to 10)
        val targetRec1 = targetDao.createRecord(
            "targetText" to "def",
            "targetNum" to 100,
            "targetAssoc" to targetRec0,
            "targetAuthority" to targetRec0
        )

        val targetRec2 = targetDao.createRecord("targetText" to "hij", "targetNum" to 5)
        val targetRec3 = targetDao.createRecord(
            "targetText" to "klm",
            "targetNum" to 50,
            "targetAssoc" to targetRec2,
            "targetAuthority" to targetRec0
        )

        val record0 = createRecord(multiAssocName to listOf(targetRec0, targetRec1))
        val record1 = createRecord(multiAssocName to listOf(targetRec2, targetRec3))

        val queryRes0 = records.query(
            baseQuery.copy {
                withQuery(Predicates.eq("$multiAssocName.targetText", "klm"))
            }
        ).getRecords()

        assertThat(queryRes0).containsExactly(record1)

        val queryRes1 = records.query(
            baseQuery.copy {
                withQuery(Predicates.inVals("$multiAssocName.targetText", listOf("klm", "abc")))
            }
        ).getRecords()

        assertThat(queryRes1).containsExactlyInAnyOrder(record0, record1)

        val queryRes2 = records.query(
            baseQuery.copy {
                withQuery(Predicates.inVals("$multiAssocName.targetAssoc", listOf(targetRec0)))
            }
        ).getRecords()

        assertThat(queryRes2).containsExactly(record0)

        val queryRes3 = records.query(
            baseQuery.copy {
                withQuery(Predicates.inVals("$multiAssocName.targetAuthority", listOf(targetRec0)))
            }
        ).getRecords()

        assertThat(queryRes3).containsExactly(record0, record1)

        val queryRes4 = records.query(baseQuery, mapOf("sum" to "sum(\"$multiAssocName.targetNum\")"))
            .getRecords()
            .associate { it.getId() to it["sum"].asInt() }

        assertThat(queryRes4[record0]).isEqualTo(110)
        assertThat(queryRes4[record1]).isEqualTo(55)

        fun sortTest(asc: Boolean) {
            val queryRes = records.query(
                baseQuery.copy()
                    .withSortBy(SortBy("sum(\"$multiAssocName.targetNum\")", asc))
                    .build(),
                mapOf("sum" to "sum(\"$multiAssocName.targetNum\")")
            )
                .getRecords()
                .map { it["sum"].asInt() }

            if (asc) {
                assertThat(queryRes).containsExactly(55, 110)
            } else {
                assertThat(queryRes).containsExactly(110, 55)
            }
        }
        sortTest(true)
        sortTest(false)

        val value = records.getAtt(record0, "sum(\"$multiAssocName.targetNum\")?num").asInt()
        assertThat(value).isEqualTo(110)

        val queryRes5 = records.query(
            baseQuery.copy {
                withQuery(Predicates.eq("sum(\"$multiAssocName.targetNum\")", 110))
            }
        ).getRecords()

        assertThat(queryRes5).containsExactly(record0)
    }

    @Test
    fun groupByParentAttTest() {

        val srcRec0 = createRecord("numAtt" to 1, "textAtt" to "abc")
        val srcRec1 = createRecord("numAtt" to 2, "textAtt" to "def")

        childDao.createRecord("_parent" to srcRec0, "_parentAtt" to "childAssocAtt")
        childDao.createRecord("_parent" to srcRec1, "_parentAtt" to "childAssocAtt")
        childDao.createRecord("_parent" to srcRec1, "_parentAtt" to "childAssocAtt")

        val recsResult = records.query(
            baseQuery.copy {
                withSourceId(childDao.dao.getId())
                withQuery(Predicates.eq("_parent._type", REC_TEST_TYPE_REF))
                withSortBy(listOf(SortBy("_parent.textAtt", true)))
                withGroupBy(listOf("_parent.textAtt"))
            },
            mapOf(
                "textAtt" to "_parent.textAtt",
                "numSum" to "sum(_parent.numAtt)",
                "count" to "count(*)"
            )
        ).getRecords()

        assertThat(recsResult).hasSize(2)
        assertThat(recsResult.map { it["textAtt"].asText() }).containsExactly("abc", "def")
        assertThat(recsResult.map { it["numSum"].asInt() }).containsExactly(1, 4)
        assertThat(recsResult.map { it["count"].asInt() }).containsExactly(1, 2)
    }

    @Test
    fun groupByParentTest() {

        val targetRec0 = targetDao.createRecord("targetText" to "abc", "targetNum" to 10)
        val targetRec1 = targetDao.createRecord("targetText" to "def", "targetNum" to 100)

        val srcRec0 = createRecord("assocAtt" to targetRec0)
        val srcRec1 = createRecord("assocAtt" to targetRec1)

        childDao.createRecord("_parent" to srcRec0, "_parentAtt" to "childAssocAtt")
        childDao.createRecord("_parent" to srcRec1, "_parentAtt" to "childAssocAtt")
        childDao.createRecord("_parent" to srcRec1, "_parentAtt" to "childAssocAtt")

        val recsResult = records.query(
            baseQuery.copy {
                withSourceId(childDao.dao.getId())
                withQuery(Predicates.eq("_parent._type", REC_TEST_TYPE_REF))
                withSortBy(listOf(SortBy("_parent.assocAtt", true)))
                withGroupBy(listOf("_parent.assocAtt"))
            },
            mapOf(
                "assocAtt" to "_parent.assocAtt?id",
                "count" to "count(*)"
            )
        ).getRecords()

        assertThat(recsResult).hasSize(2)
        assertThat(recsResult.map { it["assocAtt"].asText() }).containsExactly(
            targetRec0.toString(),
            targetRec1.toString()
        )
        assertThat(recsResult.map { it["count"].asInt() }).containsExactly(1, 2)
    }

    @Test
    fun test() {

        val targetRec0 = targetDao.createRecord("targetText" to "abc", "targetNum" to 10)
        val targetRec1 = targetDao.createRecord("targetText" to "def", "targetNum" to 100)

        val srcRec0 = createRecord("assocAtt" to targetRec0)
        val srcRec1 = createRecord("assocAtt" to targetRec1)

        fun queryTest(predicate: Predicate, expected: List<EntityRef>) {
            val result = records.query(
                baseQuery.copy {
                    withQuery(predicate)
                }
            ).getRecords()
            assertThat(result).describedAs("Predicate: $predicate")
                .containsExactlyInAnyOrderElementsOf(expected.map { EntityRef.valueOf(it) })
        }

        queryTest(Predicates.eq("assocAtt.targetText", "abc"), listOf(srcRec0))
        queryTest(Predicates.eq("assocAtt.targetText", "def"), listOf(srcRec1))

        queryTest(Predicates.not(Predicates.eq("assocAtt.targetText", "def")), listOf(srcRec0))
        queryTest(Predicates.not(Predicates.contains("assocAtt.targetText", "de")), listOf(srcRec0))
        queryTest(Predicates.contains("assocAtt.targetText", "de"), listOf(srcRec1))

        fun sortByTest(ascending: Boolean) {
            val result = records.query(
                baseQuery.copy {
                    withSortBy(SortBy("assocAtt.targetNum", ascending))
                }
            ).getRecords()
            val expected = if (ascending) {
                listOf(srcRec0, srcRec1)
            } else {
                listOf(srcRec1, srcRec0)
            }
            assertThat(result).describedAs("Ascending: $ascending")
                .containsExactlyElementsOf(expected.map { EntityRef.valueOf(it) })
        }
        sortByTest(true)
        sortByTest(false)

        val result = records.query(
            baseQuery.copy {
                withGroupBy(listOf("assocAtt.targetText"))
                withSortBy(listOf(SortBy("assocAtt.targetText", true)))
            },
            mapOf(
                "text" to "assocAtt.targetText",
                "count" to "count(*)"
            )
        ).getRecords()

        assertThat(result).hasSize(2)
        assertThat(result[0]["text"].asText()).isEqualTo("abc")
        assertThat(result[0]["count"].asInt()).isEqualTo(1)
        assertThat(result[1]["text"].asText()).isEqualTo("def")
        assertThat(result[1]["count"].asInt()).isEqualTo(1)
    }

    @Test
    fun testNotEqPredicateForNullValues() {

        val targetRec0 = targetDao.createRecord("targetText" to "abc", "targetNum" to 10, "targetDate" to "2024-02-26")
        val targetRec1 = targetDao.createRecord("targetText" to "def", "targetNum" to 100, "targetDate" to "2023-03-27")
        val targetRecWithNullValues = targetDao.createRecord("targetText" to null, "targetNum" to null, "targetDate" to null)

        val srcRec0 = createRecord("assocAtt" to targetRec0)
        val srcRec1 = createRecord("assocAtt" to targetRec1)
        val srcRec2 = createRecord("assocAtt" to targetRecWithNullValues)

        fun queryTest(predicate: Predicate, expected: List<EntityRef>) {
            val result = records.query(
                baseQuery.copy {
                    withQuery(predicate)
                }
            ).getRecords()
            assertThat(result).describedAs("Predicate: $predicate")
                .containsExactlyInAnyOrderElementsOf(expected.map { EntityRef.valueOf(it) })
        }

        queryTest(Predicates.not(Predicates.eq("assocAtt.targetText", "abc")), listOf(srcRec1, srcRec2))
        queryTest(Predicates.not(Predicates.eq("assocAtt.targetNum", 100)), listOf(srcRec0, srcRec2))
        queryTest(Predicates.not(Predicates.eq("assocAtt.targetDate", "2024-02-26")), listOf(srcRec1, srcRec2))
    }

    /**
     * A negated condition over a path, asked of a record that holds no association at all.
     *
     * The negation of a path condition lives outside the EXISTS, so what is asked is "no linked
     * record satisfies it". A record with nothing linked has no such record, so it matches - the
     * answer `NOT EXISTS` gives over an empty set. Every source record in the neighbouring tests
     * holds an association, so a rewrite that pushed the negation inside the EXISTS would drop
     * exactly these records and nothing here would have noticed.
     */
    @Test
    fun aNegatedConditionOverAnAssocPathWhenTheAssociationIsAbsentTest() {

        val targetAbc = targetDao.createRecord("targetText" to "abc", "targetNum" to 10)

        val srcWithAssoc = createRecord("assocAtt" to targetAbc)
        val srcWithoutAssoc = createRecord("textAtt" to "no assoc at all")
        val srcWithMultiAssoc = createRecord("multiAssocAtt" to listOf(targetAbc))
        val srcWithoutMultiAssoc = createRecord("textAtt" to "no links")

        fun queryTest(predicate: Predicate, expected: List<EntityRef>) {
            val result = records.query(
                baseQuery.copy {
                    withQuery(predicate)
                }
            ).getRecords()
            assertThat(result).describedAs("Predicate: $predicate")
                .containsExactlyInAnyOrderElementsOf(expected.map { EntityRef.valueOf(it) })
        }

        val withoutTheAssoc = listOf(srcWithoutAssoc, srcWithMultiAssoc, srcWithoutMultiAssoc)

        // the linked record satisfies the inner condition, so only it is excluded
        queryTest(Predicates.not(Predicates.eq("assocAtt.targetText", "abc")), withoutTheAssoc)
        queryTest(Predicates.not(Predicates.contains("assocAtt.targetText", "ab")), withoutTheAssoc)
        // a non-equality operator takes the same route: the record with no association is not a
        // record whose linked number is greater than five
        queryTest(Predicates.not(Predicates.gt("assocAtt.targetNum", 5)), withoutTheAssoc)

        // the linked record does not satisfy it, so every record matches, linked or not
        queryTest(
            Predicates.not(Predicates.eq("assocAtt.targetText", "def")),
            listOf(srcWithAssoc) + withoutTheAssoc
        )

        // the same through a multiple association, where the link lives in the assoc table rather
        // than in a column of the source row
        queryTest(
            Predicates.not(Predicates.eq("multiAssocAtt.targetText", "abc")),
            listOf(srcWithAssoc, srcWithoutAssoc, srcWithoutMultiAssoc)
        )
        queryTest(
            Predicates.not(Predicates.eq("multiAssocAtt.targetText", "def")),
            listOf(srcWithAssoc) + withoutTheAssoc
        )
    }

    /**
     * The same over a two-hop path, where the link can be missing at either step.
     *
     * The inner condition of one EXISTS is built by the same code that built the outer one, so a
     * negation that stays outside at the first level could still be pushed inside at the second.
     */
    @Test
    fun aNegatedConditionOverATwoHopAssocPathWhenALinkIsAbsentTest() {

        val leafWithText = createRecord("textAtt" to "leaf-value", "numAtt" to 1)

        val midWithValue = childDao.createRecord("testTypeTargetAssoc" to leafWithText)
        val midWithoutAssoc = childDao.createRecord("childText" to "no link onwards")

        // the leaf records live in the same table as the source ones, so the source set is named
        // explicitly rather than left to be everything the table holds
        val srcWithValue = createRecord("childAssocAtt" to midWithValue, "numAtt" to 2)
        val srcWithBrokenPath = createRecord("childAssocAtt" to midWithoutAssoc, "numAtt" to 2)
        val srcWithoutAssoc = createRecord("numAtt" to 2)

        fun queryTest(predicate: Predicate, expected: List<EntityRef>) {
            val result = records.query(
                baseQuery.copy {
                    withQuery(Predicates.and(predicate, Predicates.eq("numAtt", 2)))
                }
            ).getRecords()
            assertThat(result).describedAs("Predicate: $predicate")
                .containsExactlyInAnyOrderElementsOf(expected.map { EntityRef.valueOf(it) })
        }

        // the path leads to the value for one record only; the one whose middle record links
        // nowhere and the one with no middle record at all are both "no linked record satisfies it"
        queryTest(
            Predicates.not(Predicates.eq("childAssocAtt.testTypeTargetAssoc.textAtt", "leaf-value")),
            listOf(srcWithBrokenPath, srcWithoutAssoc)
        )
        queryTest(
            Predicates.not(Predicates.eq("childAssocAtt.testTypeTargetAssoc.textAtt", "other-value")),
            listOf(srcWithValue, srcWithBrokenPath, srcWithoutAssoc)
        )
    }

    @Test
    fun joinInFunctionTest() {

        val target10 = targetDao.createRecord("targetNum" to 10)
        val target30 = targetDao.createRecord("targetNum" to 30)

        mainCtx.createRecord("numAtt" to 50, "assocAtt" to target10)
        mainCtx.createRecord("numAtt" to 100, "assocAtt" to target30)

        val queryRes0 = records.query(
            baseQuery.copy()
                .withSortBy(emptyList())
                .build(),
            mapOf("funcAmount" to "(assocAtt.targetNum - numAtt)")
        ).getRecords().map { it["funcAmount"].asInt() }.sorted()

        assertThat(queryRes0).hasSize(2)
        assertThat(queryRes0[0]).isEqualTo(-70)
        assertThat(queryRes0[1]).isEqualTo(-40)

        val queryRes1 = records.query(
            baseQuery.copy()
                .withQuery(Predicates.gt("(assocAtt.targetNum - numAtt)", -50))
                .withSortBy(emptyList())
                .build(),
            mapOf("funcAmount" to "(assocAtt.targetNum - numAtt)")
        ).getRecords().map { it["funcAmount"].asInt() }.sorted()

        assertThat(queryRes1).hasSize(1)
        assertThat(queryRes1[0]).isEqualTo(-40)
    }

    @Test
    fun joinedEmptyPredicateTest() {

        val targetRec0 = targetDao.createRecord("targetText" to "abc", "targetNum" to 10)
        val targetRec1 = targetDao.createRecord("targetText" to "def", "targetNum" to 100)

        val srcRec0 = createRecord("assocAtt" to null)
        val srcRec1 = createRecord("assocAtt" to targetRec1)
        val srcRec2 = createRecord("assocAtt" to null)
        val srcRec3 = createRecord("assocAtt" to targetRec0)

        val child0 = childDao.createRecord("_parent" to srcRec0, "_parentAtt" to "childAssocAtt", "testTypeTargetAssoc" to srcRec1)
        val child1 = childDao.createRecord("_parent" to srcRec1, "_parentAtt" to "childAssocAtt", "testTypeTargetAssoc" to srcRec0)
        val child2 = childDao.createRecord("_parent" to srcRec2, "_parentAtt" to "childAssocAtt", "testTypeTargetAssoc" to srcRec2)
        val child3 = childDao.createRecord("_parent" to srcRec3, "_parentAtt" to "childAssocAtt", "testTypeTargetAssoc" to srcRec1)

        val recsResult0 = records.query(
            baseQuery.copy {
                withSourceId(childDao.dao.getId())
                withQuery(
                    Predicates.and(
                        Predicates.eq("_parent._type", REC_TEST_TYPE_REF),
                        Predicates.empty("_parent.assocAtt")
                    )
                )
            }
        ).getRecords()

        assertThat(recsResult0).hasSize(2)
        assertThat(recsResult0).containsExactlyInAnyOrder(child0, child2)

        val recsResult1 = records.query(
            baseQuery.copy {
                withSourceId(childDao.dao.getId())
                withQuery(
                    Predicates.and(
                        Predicates.empty("testTypeTargetAssoc.assocAtt")
                    )
                )
            }
        ).getRecords()

        assertThat(recsResult1).hasSize(2)
        assertThat(recsResult1).containsExactlyInAnyOrder(child1, child2)
    }

    /**
     * COREDEV-527: `empty` and `not-empty` over a path through an association.
     *
     * A path condition becomes `EXISTS(SELECT 1 FROM target WHERE <linked> AND <inner>)`, and a
     * `not-empty` arrives as `NOT(empty(path))` - so the negation used to land *outside* that
     * EXISTS, producing `NOT EXISTS(... AND targetText IS EMPTY)`. For a record with no association
     * at all the subquery has no rows, so the whole thing is true and the record comes back: the
     * filter looks like it works, raises nothing, and returns everything.
     *
     * The record with no association is what makes this observable. Every other test here gives all
     * of its source records an association, and with that row missing the two readings of the
     * negation agree.
     *
     * The two conditions are complements over the source records - a path that leads nowhere holds
     * no value, so it is empty - and that is asserted rather than assumed.
     */
    @Test
    fun emptyAndNotEmptyOverAnAssocPathTest() {

        val targetWithText = targetDao.createRecord("targetText" to "abc")
        val targetWithoutText = targetDao.createRecord("targetNum" to 10)

        val srcWithValue = createRecord("assocAtt" to targetWithText)
        val srcWithEmptyValue = createRecord("assocAtt" to targetWithoutText)
        val srcWithoutAssoc = createRecord("textAtt" to "no assoc at all")

        fun queryTest(predicate: Predicate, expected: List<EntityRef>) {
            val result = records.query(
                baseQuery.copy {
                    withQuery(predicate)
                }
            ).getRecords()
            assertThat(result).describedAs("Predicate: $predicate")
                .containsExactlyInAnyOrderElementsOf(expected.map { EntityRef.valueOf(it) })
        }

        queryTest(
            Predicates.notEmpty("assocAtt.targetText"),
            listOf(srcWithValue)
        )
        queryTest(
            Predicates.empty("assocAtt.targetText"),
            listOf(srcWithEmptyValue, srcWithoutAssoc)
        )

        // the same through a multiple association, where EXISTS reads as "at least one linked
        // record holds a value"
        val srcMultiWithValue = createRecord("multiAssocAtt" to listOf(targetWithoutText, targetWithText))
        val srcMultiWithoutValue = createRecord("multiAssocAtt" to listOf(targetWithoutText))
        val srcMultiEmpty = createRecord("textAtt" to "no links")

        queryTest(
            Predicates.and(
                Predicates.notEmpty("multiAssocAtt.targetText"),
                Predicates.empty("assocAtt")
            ),
            listOf(srcMultiWithValue)
        )
        queryTest(
            Predicates.and(
                Predicates.empty("multiAssocAtt.targetText"),
                Predicates.empty("assocAtt")
            ),
            // srcWithoutAssoc holds neither association, so it satisfies both halves
            listOf(srcMultiWithoutValue, srcMultiEmpty, srcWithoutAssoc)
        )

        // eq(att, null) is the other spelling of the same question, and this codebase already
        // treats the two alike - it must not answer differently over a path
        queryTest(
            Predicates.eq("assocAtt.targetText", null),
            listOf(srcWithEmptyValue, srcWithoutAssoc, srcMultiWithValue, srcMultiWithoutValue, srcMultiEmpty)
        )
        queryTest(
            Predicates.not(Predicates.eq("assocAtt.targetText", null)),
            listOf(srcWithValue)
        )
    }

    /**
     * The same over a two-hop path, because the rewriting is recursive: the inner condition of one
     * EXISTS is itself built by the same code, and a `not-empty` that collapsed correctly at one
     * level could still come out inverted at the next.
     */
    @Test
    fun emptyAndNotEmptyOverATwoHopAssocPathTest() {

        val leafWithText = createRecord("textAtt" to "leaf-value")
        val leafWithoutText = createRecord("numAtt" to 10)

        val midWithValue = childDao.createRecord("testTypeTargetAssoc" to leafWithText)
        val midWithoutValue = childDao.createRecord("testTypeTargetAssoc" to leafWithoutText)
        val midWithoutAssoc = childDao.createRecord("childText" to "no link onwards")

        val srcWithValue = createRecord("childAssocAtt" to midWithValue)
        val srcWithoutValue = createRecord("childAssocAtt" to midWithoutValue)
        val srcWithBrokenPath = createRecord("childAssocAtt" to midWithoutAssoc)

        // the leaf records live in the same table as the source ones, so the source set is named
        // explicitly rather than left to be everything the table holds
        fun queryTest(predicate: Predicate, expected: List<EntityRef>) {
            val result = records.query(
                baseQuery.copy {
                    withQuery(Predicates.and(predicate, Predicates.notEmpty("childAssocAtt")))
                }
            ).getRecords()
            assertThat(result).describedAs("Predicate: $predicate")
                .containsExactlyInAnyOrderElementsOf(expected.map { EntityRef.valueOf(it) })
        }

        queryTest(
            Predicates.notEmpty("childAssocAtt.testTypeTargetAssoc.textAtt"),
            listOf(srcWithValue)
        )
        queryTest(
            Predicates.empty("childAssocAtt.testTypeTargetAssoc.textAtt"),
            // the path leads nowhere for the middle record without a link, and stops at an empty
            // value for the one whose leaf has none - both hold no value at the end of the path
            listOf(srcWithoutValue, srcWithBrokenPath)
        )
    }

    /**
     * A condition whose last step names an attribute the target type does not have.
     *
     * The inner condition of an assoc join names attributes of the target table, and it used to
     * reach the backends without the preparation the main predicate gets against its own table -
     * so a column the target has never had produced no condition at all. PostgreSQL was handed
     * `WHERE <link> AND )` and refused the query outright; the in-memory backend, which models a
     * condition that produces nothing as one that was dropped, quietly answered "every record that
     * has an association". Both of those are the failure this ticket is about: a filter that does
     * not filter, or does not run.
     *
     * The answers here are the ones the same conditions get on a plain attribute of the main table,
     * where `DbDataServiceImpl.getPredicateForMissingColumn` has always given them: an attribute
     * that does not exist holds no value, so `empty` is true of everything and nothing else matches.
     */
    @Test
    fun aConditionOnAnAttributeTheTargetTypeDoesNotHaveTest() {

        val target = targetDao.createRecord("targetText" to "abc")

        val srcWithAssoc = createRecord("assocAtt" to target)
        val srcWithoutAssoc = createRecord("textAtt" to "none")

        fun queryTest(predicate: Predicate, expected: List<EntityRef>) {
            val result = records.query(
                baseQuery.copy {
                    withQuery(predicate)
                }
            ).getRecords()
            assertThat(result).describedAs("Predicate: $predicate")
                .containsExactlyInAnyOrderElementsOf(expected.map { EntityRef.valueOf(it) })
        }

        queryTest(Predicates.notEmpty("assocAtt.noSuchAttribute"), emptyList())
        queryTest(Predicates.empty("assocAtt.noSuchAttribute"), listOf(srcWithAssoc, srcWithoutAssoc))
        queryTest(Predicates.eq("assocAtt.noSuchAttribute", "abc"), emptyList())
        queryTest(Predicates.contains("assocAtt.noSuchAttribute", "ab"), emptyList())

        // the same through a multiple association, which builds the EXISTS differently
        val srcMulti = createRecord("multiAssocAtt" to listOf(target))
        queryTest(Predicates.notEmpty("multiAssocAtt.noSuchAttribute"), emptyList())
        queryTest(
            Predicates.empty("multiAssocAtt.noSuchAttribute"),
            listOf(srcWithAssoc, srcWithoutAssoc, srcMulti)
        )
        queryTest(Predicates.eq("multiAssocAtt.noSuchAttribute", "abc"), emptyList())
    }

    /**
     * Sorting and grouping by a path of more than one association.
     *
     * A path used to be refused past the first step - "Assoc select can't be executed for multiple
     * joins" - and the refusal is silent outside strict mode, so a two-step sort key was ignored
     * (ascending and descending returned the same order) and a two-step group key returned nothing
     * at all. A join is now registered per step, keyed by the path walked so far and joined to the
     * step before it, which is what the ORDER BY and the GROUP BY read.
     *
     * Ascending *and* descending are asserted: a sort key that is quietly dropped leaves the rows
     * in insertion order, which one of the two directions agrees with by chance.
     *
     * **Reading a multi-step path is deliberately not asserted here.** A plain select never reaches
     * these joins at all - the schema reader has already split `a.b.c` into nested schema atts, so
     * the value comes back through ordinary record traversal and the assertion would hold with the
     * one-step limit still in place. Where the join really does serve the value is a grouped query,
     * which `groupingByBothAPathAndItsPrefixTest` and
     * `aPathEndingInAnAssociationResolvesItsReferenceTest` cover.
     */
    @Test
    fun sortAndGroupByAMultiStepAssocPathTest() {

        val tgtA = targetDao.createRecord("targetText" to "AAA")
        val tgtB = targetDao.createRecord("targetText" to "BBB")

        val leafA = createRecord("textAtt" to "leaf-A", "assocAtt" to tgtA)
        val leafB = createRecord("textAtt" to "leaf-B", "assocAtt" to tgtB)

        // childAssocAtt is a child association and a record has one parent, so each source gets
        // its own middle record - two of which lead to the same leaf, which is what gives grouping
        // something to group
        val midA0 = childDao.createRecord("childText" to "mid-A0", "testTypeTargetAssoc" to leafA)
        val midA1 = childDao.createRecord("childText" to "mid-A1", "testTypeTargetAssoc" to leafA)
        val midB = childDao.createRecord("childText" to "mid-B", "testTypeTargetAssoc" to leafB)

        val srcA0 = createRecord("textAtt" to "src-A0", "childAssocAtt" to midA0)
        val srcA1 = createRecord("textAtt" to "src-A1", "childAssocAtt" to midA1)
        val srcB = createRecord("textAtt" to "src-B", "childAssocAtt" to midB)

        val onlySources = Predicates.notEmpty("childAssocAtt")
        val twoSteps = "childAssocAtt.testTypeTargetAssoc.textAtt"
        val threeSteps = "childAssocAtt.testTypeTargetAssoc.assocAtt.targetText"

        // sort
        fun sorted(att: String, ascending: Boolean): List<EntityRef> {
            return records.query(
                baseQuery.copy {
                    withQuery(onlySources)
                    withSortBy(listOf(SortBy(att, ascending), SortBy("textAtt", true)))
                }
            ).getRecords()
        }
        assertThat(sorted(twoSteps, true)).containsExactly(srcA0, srcA1, srcB)
        assertThat(sorted(twoSteps, false)).containsExactly(srcB, srcA0, srcA1)
        assertThat(sorted(threeSteps, true)).containsExactly(srcA0, srcA1, srcB)
        assertThat(sorted(threeSteps, false)).containsExactly(srcB, srcA0, srcA1)

        // group
        val grouped = records.query(
            baseQuery.copy {
                withQuery(onlySources)
                withGroupBy(listOf(twoSteps))
                withSortBy(SortBy(twoSteps, true))
            },
            mapOf("value" to twoSteps, "count" to "count(*)")
        ).getRecords()

        assertThat(grouped).hasSize(2)
        assertThat(grouped[0]["value"].asText()).isEqualTo("leaf-A")
        assertThat(grouped[0]["count"].asInt()).isEqualTo(2)
        assertThat(grouped[1]["value"].asText()).isEqualTo("leaf-B")
        assertThat(grouped[1]["count"].asInt()).isEqualTo(1)
    }

    /**
     * Grouping by a path and by a proper prefix of it in the same query - a shape that only became
     * possible now that a path may be longer than one step.
     *
     * Grouping is what puts a dotted path into a record as a flat key; an ordinary select walks the
     * records instead. Both keys then name the same slot in the nested values the record builds for
     * the reader: `a.b` wants the reference it resolved to, `a.b.c` wants a map to walk into.
     *
     * The reference wins, because it answers both questions - `a.b?id` still resolves, and `a.b.c`
     * resolves through it by loading the record it points at. That is what both assertions below
     * hold together: with a map in that slot instead, the prefix is no longer a record.
     */
    @Test
    fun groupingByBothAPathAndItsPrefixTest() {

        val leaf = createRecord("textAtt" to "leaf-value")
        val mid = childDao.createRecord("childText" to "mid", "testTypeTargetAssoc" to leaf)
        createRecord("textAtt" to "src", "childAssocAtt" to mid)

        val prefix = "childAssocAtt.testTypeTargetAssoc"
        val full = "$prefix.textAtt"

        val grouped = records.query(
            baseQuery.copy {
                withQuery(Predicates.notEmpty("childAssocAtt"))
                withGroupBy(listOf(full, prefix))
            },
            mapOf("value" to full, "ref" to "$prefix?id", "count" to "count(*)")
        ).getRecords()

        assertThat(grouped).hasSize(1)
        assertThat(grouped[0]["value"].asText())
            .describedAs("the deeper path resolves through the reference the shorter one left")
            .isEqualTo("leaf-value")
        assertThat(grouped[0]["ref"].asText())
            .describedAs("and the shorter path is still a record, not a map")
            .isEqualTo(leaf.toString())
        assertThat(grouped[0]["count"].asInt()).isEqualTo(1)
    }

    /**
     * A path whose last step is itself an association: its reference has to resolve.
     *
     * The ref-ids a record needs to turn into references are collected from its flat keys, and that
     * collection used to cut a key at the first dot - fine while a path was one step, useless for a
     * longer one, whose last step it then looked up as an attribute id containing a dot. The value
     * came back as an empty reference with a "Ref doesn't found" warning: a silently wrong answer in
     * exactly the shape that longer paths made possible.
     */
    @Test
    fun aPathEndingInAnAssociationResolvesItsReferenceTest() {

        val target = targetDao.createRecord("targetText" to "AAA")
        val leaf = createRecord("textAtt" to "leaf", "assocAtt" to target)
        val mid = childDao.createRecord("childText" to "mid", "testTypeTargetAssoc" to leaf)
        createRecord("textAtt" to "src", "childAssocAtt" to mid)

        val twoStepsToAssoc = "childAssocAtt.testTypeTargetAssoc.assocAtt"

        val grouped = records.query(
            baseQuery.copy {
                withQuery(Predicates.notEmpty("childAssocAtt"))
                withGroupBy(listOf(twoStepsToAssoc))
            },
            mapOf("ref" to "$twoStepsToAssoc?id", "count" to "count(*)")
        ).getRecords()

        assertThat(grouped).hasSize(1)
        assertThat(grouped[0]["ref"].asText())
            .describedAs("the association at the end of a two-step path is a real reference")
            .isEqualTo(target.toString())
        assertThat(grouped[0]["count"].asInt()).isEqualTo(1)
    }

    /**
     * A path whose last step is an attribute the type carries through an aspect.
     *
     * The join registration resolves a step with the aspect and child-type fallbacks, so such a path
     * registers its joins and produces its flat key like any other. Resolving the definition at the
     * end of it against the type model alone would not find the attribute, and the value would come
     * back as a raw id instead of a reference - the same symptom as a path ending in an association,
     * narrowed to the attributes a type does not declare itself.
     */
    @Test
    fun aPathEndingInAnAspectAttributeResolvesItsReferenceTest() {

        val target = targetDao.createRecord("targetText" to "AAA")
        val leaf = createRecord("textAtt" to "leaf", "aspect0:assocAtt" to target)
        val mid = childDao.createRecord("childText" to "mid", "testTypeTargetAssoc" to leaf)
        createRecord("textAtt" to "src", "childAssocAtt" to mid)

        val att = "childAssocAtt.testTypeTargetAssoc.aspect0:assocAtt"

        val grouped = records.query(
            baseQuery.copy {
                withQuery(Predicates.notEmpty("childAssocAtt"))
                withGroupBy(listOf(att))
            },
            mapOf("ref" to "$att?id", "count" to "count(*)")
        ).getRecords()

        assertThat(grouped).hasSize(1)
        assertThat(grouped[0]["ref"].asText())
            .describedAs("an aspect attribute at the end of a path is a real reference")
            .isEqualTo(target.toString())
        assertThat(grouped[0]["count"].asInt()).isEqualTo(1)
    }

    /**
     * Sorting and grouping by a path when some records do not have the association.
     *
     * The path is reached by a LEFT JOIN, so a record without the association keeps the row and
     * carries no value at the end of the path. That places it with the empty values - last when
     * ascending, first when descending - and gives it a group of its own rather than dropping it.
     *
     * Nothing asserted this: every source record in the neighbouring sort/group tests has the
     * association, and the multi-step one filters the rest out with `notEmpty("childAssocAtt")`.
     * An INNER JOIN, or a join skipped altogether, would pass those tests and lose records here.
     */
    @Test
    fun sortingAndGroupingByAnAssocPathKeepRecordsWithoutTheAssociationTest() {

        val tgtA = targetDao.createRecord("targetText" to "AAA")
        val tgtB = targetDao.createRecord("targetText" to "BBB")

        // the two-hop path lands back in this table, so the participating records are marked with
        // a number of their own instead of being taken as "everything the table holds"
        val inQuery = 7
        val leafA = createRecord("textAtt" to "leaf-A", "numAtt" to 1)
        val midA = childDao.createRecord("childText" to "mid-A", "testTypeTargetAssoc" to leafA)
        val midWithoutLink = childDao.createRecord("childText" to "mid-none")

        val withA = createRecord("textAtt" to "s1", "numAtt" to inQuery, "assocAtt" to tgtA, "childAssocAtt" to midA)
        val withB = createRecord("textAtt" to "s2", "numAtt" to inQuery, "assocAtt" to tgtB, "childAssocAtt" to midWithoutLink)
        val withoutAssoc = createRecord("textAtt" to "s3", "numAtt" to inQuery)

        val onlyMarked = Predicates.eq("numAtt", inQuery)

        fun sorted(att: String, ascending: Boolean): List<EntityRef> {
            return records.query(
                baseQuery.copy {
                    withQuery(onlyMarked)
                    withSortBy(listOf(SortBy(att, ascending), SortBy("textAtt", true)))
                }
            ).getRecords()
        }

        val oneStep = "assocAtt.targetText"
        assertThat(sorted(oneStep, true)).containsExactly(withA, withB, withoutAssoc)
        assertThat(sorted(oneStep, false)).containsExactly(withoutAssoc, withB, withA)

        // the same when the link is missing at the second step rather than the first
        val twoSteps = "childAssocAtt.testTypeTargetAssoc.textAtt"
        assertThat(sorted(twoSteps, true)).containsExactly(withA, withB, withoutAssoc)
        assertThat(sorted(twoSteps, false)).containsExactly(withB, withoutAssoc, withA)

        fun grouped(att: String): Map<String, Int> {
            return records.query(
                baseQuery.copy {
                    withQuery(onlyMarked)
                    withGroupBy(listOf(att))
                },
                mapOf("value" to att, "count" to "count(*)")
            ).getRecords().associate { it["value"].asText() to it["count"].asInt() }
        }

        assertThat(grouped(oneStep))
            .describedAs("a record without the association is its own group, not a dropped row")
            .isEqualTo(mapOf("AAA" to 1, "BBB" to 1, "" to 1))
        assertThat(grouped(twoSteps))
            .describedAs("a path that stops at either step holds no value, and those share a group")
            .isEqualTo(mapOf("leaf-A" to 1, "" to 2))
    }

    /**
     * A three-step path as a condition. The sort and group tests already walk three steps; the
     * condition side only ever walked two, and it is built by different code - a chain of nested
     * EXISTS rather than a chain of joins.
     */
    @Test
    fun aThreeStepPathFiltersAndAnswersEmptinessTest() {

        val tgtA = targetDao.createRecord("targetText" to "AAA")

        val inQuery = 7
        val leafWithTarget = createRecord("textAtt" to "leaf-A", "numAtt" to 1, "assocAtt" to tgtA)
        val leafWithoutTarget = createRecord("textAtt" to "leaf-B", "numAtt" to 1)

        val midToTarget = childDao.createRecord("childText" to "mid-A", "testTypeTargetAssoc" to leafWithTarget)
        val midToLeafWithoutTarget = childDao.createRecord("childText" to "mid-B", "testTypeTargetAssoc" to leafWithoutTarget)
        val midWithoutLink = childDao.createRecord("childText" to "mid-none")

        val full = createRecord("textAtt" to "s-full", "numAtt" to inQuery, "childAssocAtt" to midToTarget)
        val brokenAtThird = createRecord("textAtt" to "s-3", "numAtt" to inQuery, "childAssocAtt" to midToLeafWithoutTarget)
        val brokenAtSecond = createRecord("textAtt" to "s-2", "numAtt" to inQuery, "childAssocAtt" to midWithoutLink)
        val brokenAtFirst = createRecord("textAtt" to "s-1", "numAtt" to inQuery)

        fun queryTest(predicate: Predicate, expected: List<EntityRef>) {
            val result = records.query(
                baseQuery.copy {
                    withQuery(Predicates.and(predicate, Predicates.eq("numAtt", inQuery)))
                }
            ).getRecords()
            assertThat(result).describedAs("Predicate: $predicate")
                .containsExactlyInAnyOrderElementsOf(expected.map { EntityRef.valueOf(it) })
        }

        val att = "childAssocAtt.testTypeTargetAssoc.assocAtt.targetText"

        queryTest(Predicates.eq(att, "AAA"), listOf(full))
        queryTest(Predicates.notEmpty(att), listOf(full))
        // the path can stop at any of the three steps, and all three are "holds no value"
        queryTest(Predicates.empty(att), listOf(brokenAtThird, brokenAtSecond, brokenAtFirst))
        queryTest(
            Predicates.not(Predicates.eq(att, "AAA")),
            listOf(brokenAtThird, brokenAtSecond, brokenAtFirst)
        )
    }

    /**
     * Two conditions over the same multiple association are independent questions.
     *
     * Each one becomes its own EXISTS - the join is registered per condition, keyed by a counter -
     * so `and` asks "some linked record satisfies this *and* some linked record satisfies that",
     * not "one linked record satisfies both". Over a multiple association the two may be different
     * records, and a pair of conditions no single record could satisfy still matches.
     *
     * Pinned because it is surprising enough to be "fixed" by accident, and because merging the
     * two joins is exactly what a well-meaning optimisation would do.
     */
    @Test
    fun conditionsOverTheSameMultipleAssociationAreIndependentTest() {

        val small = targetDao.createRecord("targetText" to "AAA", "targetNum" to 10)
        val large = targetDao.createRecord("targetText" to "BBB", "targetNum" to 20)

        val linkedToBoth = createRecord("textAtt" to "both", "multiAssocAtt" to listOf(small, large))
        val linkedToSmall = createRecord("textAtt" to "small", "multiAssocAtt" to listOf(small))
        val singleToSmall = createRecord("textAtt" to "single", "assocAtt" to small)

        fun queryTest(predicate: Predicate, expected: List<EntityRef>) {
            val result = records.query(baseQuery.copy { withQuery(predicate) }).getRecords()
            assertThat(result).describedAs("Predicate: $predicate")
                .containsExactlyInAnyOrderElementsOf(expected.map { EntityRef.valueOf(it) })
        }

        // no single linked record is both above and below 15, yet the record linked to two is a
        // match: one link answers each half
        queryTest(
            Predicates.and(
                Predicates.gt("multiAssocAtt.targetNum", 15),
                Predicates.lt("multiAssocAtt.targetNum", 15)
            ),
            listOf(linkedToBoth)
        )
        queryTest(
            Predicates.and(
                Predicates.eq("multiAssocAtt.targetText", "AAA"),
                Predicates.eq("multiAssocAtt.targetNum", 20)
            ),
            listOf(linkedToBoth)
        )

        // through a non-multiple association there is only one linked record, so the same shape
        // asks both questions of it
        queryTest(
            Predicates.and(
                Predicates.eq("assocAtt.targetText", "AAA"),
                Predicates.eq("assocAtt.targetNum", 10)
            ),
            listOf(singleToSmall)
        )
        queryTest(
            Predicates.and(
                Predicates.eq("assocAtt.targetText", "AAA"),
                Predicates.eq("assocAtt.targetNum", 20)
            ),
            emptyList()
        )
        queryTest(Predicates.eq("multiAssocAtt.targetNum", 10), listOf(linkedToBoth, linkedToSmall))
    }

    /**
     * Negation around a composed condition, not around a single one.
     *
     * `not(and(...))` and `not(or(...))` keep the negation at the top while each path condition
     * below it turns into its own EXISTS. De Morgan has to come out right through that rewriting,
     * and records with no association at all have to land on the true side of it.
     */
    @Test
    fun negationOverComposedPathConditionsTest() {

        val tgtA = targetDao.createRecord("targetText" to "AAA", "targetNum" to 10)
        val tgtB = targetDao.createRecord("targetText" to "BBB", "targetNum" to 20)

        val withA = createRecord("textAtt" to "withA", "assocAtt" to tgtA)
        val withB = createRecord("textAtt" to "withB", "assocAtt" to tgtB)
        val withoutAssoc = createRecord("textAtt" to "without")
        val withMultiB = createRecord("textAtt" to "multiB", "multiAssocAtt" to listOf(tgtB))

        fun queryTest(predicate: Predicate, expected: List<EntityRef>) {
            val result = records.query(baseQuery.copy { withQuery(predicate) }).getRecords()
            assertThat(result).describedAs("Predicate: $predicate")
                .containsExactlyInAnyOrderElementsOf(expected.map { EntityRef.valueOf(it) })
        }

        val isA = Predicates.eq("assocAtt.targetText", "AAA")
        val isTen = Predicates.eq("assocAtt.targetNum", 10)
        val isB = Predicates.eq("assocAtt.targetText", "BBB")

        queryTest(Predicates.or(isA, Predicates.eq("multiAssocAtt.targetText", "BBB")), listOf(withA, withMultiB))
        queryTest(Predicates.not(Predicates.and(isA, isTen)), listOf(withB, withoutAssoc, withMultiB))
        queryTest(Predicates.not(Predicates.or(isA, isB)), listOf(withoutAssoc, withMultiB))
        queryTest(
            Predicates.and(Predicates.not(isA), Predicates.not(isB)),
            listOf(withoutAssoc, withMultiB)
        )
    }

    /**
     * The total count of a query filtered by a path, asked with no items.
     *
     * `maxItems = 0` takes a different route through the repository: the count is built by its own
     * query rather than by counting what a select returned, and it has its own copy of the
     * condition-building call. A condition that works in a select and is dropped from the count is
     * a shape this codebase can produce.
     */
    @Test
    fun theTotalCountOfAQueryFilteredByAPathTest() {

        val tgtA = targetDao.createRecord("targetText" to "AAA")
        val tgtB = targetDao.createRecord("targetText" to "BBB")

        createRecord("textAtt" to "withA", "assocAtt" to tgtA)
        createRecord("textAtt" to "withB", "assocAtt" to tgtB)
        createRecord("textAtt" to "without")
        createRecord("textAtt" to "multiA", "multiAssocAtt" to listOf(tgtA))

        fun countOf(predicate: Predicate): Long {
            return records.query(
                baseQuery.copy {
                    withQuery(predicate)
                    withMaxItems(0)
                }
            ).getTotalCount()
        }

        assertThat(countOf(Predicates.notEmpty("assocAtt.targetText"))).isEqualTo(2)
        assertThat(countOf(Predicates.eq("assocAtt.targetText", "AAA"))).isEqualTo(1)
        // the negated form, where the records with no association are the ones counted
        assertThat(countOf(Predicates.not(Predicates.eq("assocAtt.targetText", "AAA")))).isEqualTo(3)
        assertThat(countOf(Predicates.empty("assocAtt.targetText"))).isEqualTo(2)
        assertThat(countOf(Predicates.eq("multiAssocAtt.targetText", "AAA"))).isEqualTo(1)
        assertThat(countOf(Predicates.eq("childAssocAtt.testTypeTargetAssoc.textAtt", "nothing"))).isEqualTo(0)
    }

    /**
     * Two sibling paths whose first 63 bytes are the same.
     *
     * PostgreSQL truncates an identifier at 63 bytes and says nothing about it, so a join alias
     * taken from the path itself would make these two joins the same alias and both columns would
     * be read from whichever one was joined last. The alias is the join's position in the ordered
     * map instead, which is why they stay apart.
     *
     * The in-memory backend does not build SQL and cannot exhibit this, but it answers the same
     * query and is worth holding to the same answer.
     */
    @Test
    fun longJoinKeysThatShareAPrefixDoNotCollideTest() {

        val one = targetDao.createRecord("targetText" to "ONE")
        val two = targetDao.createRecord("targetText" to "TWO")

        val leaf = createRecord(
            "textAtt" to "leaf",
            "assocAttWithALongNameNumberOne" to one,
            "assocAttWithALongNameNumberTwo" to two
        )
        val mid = childDao.createRecord("childText" to "mid", "targetAssocWithAVeryLongNameToo" to leaf)
        createRecord("textAtt" to "src", "childAssocAttWithAVeryLongNameToApproachTheLimit" to mid)

        val prefix = "childAssocAttWithAVeryLongNameToApproachTheLimit.targetAssocWithAVeryLongNameToo"
        val pathOne = "$prefix.assocAttWithALongNameNumberOne.targetText"
        val pathTwo = "$prefix.assocAttWithALongNameNumberTwo.targetText"

        assertThat(prefix.length)
            .describedAs("the shared prefix has to be longer than an identifier may be")
            .isGreaterThan(63)

        val grouped = records.query(
            baseQuery.copy {
                withQuery(Predicates.notEmpty("childAssocAttWithAVeryLongNameToApproachTheLimit"))
                withGroupBy(listOf(pathOne, pathTwo))
            },
            mapOf("one" to pathOne, "two" to pathTwo, "count" to "count(*)")
        ).getRecords()

        assertThat(grouped).hasSize(1)
        assertThat(grouped[0]["one"].asText()).isEqualTo("ONE")
        assertThat(grouped[0]["two"].asText())
            .describedAs("the second join is not the first one under a truncated alias")
            .isEqualTo("TWO")
        assertThat(grouped[0]["count"].asInt()).isEqualTo(1)
    }

    /**
     * A path whose first step is a multiple association: it filters, and it does not select.
     *
     * Filtering walks the association table with EXISTS and handles many links naturally. Selecting
     * - which sorting and grouping both are - is a LEFT JOIN on the column holding the link, and a
     * multiple association has no such single value: its column is an array. The registration
     * refuses the step, the query as a whole is refused with it, and the caller gets an empty
     * result and a message in the debug log.
     *
     * Asserted as the current limitation rather than assumed: a refusal is a deliberate answer
     * here, and the alternative the refusal replaced was PostgreSQL failing the entire query on
     * `bigint = bigint[]`. Making such a path select properly - a row per link - is a change of
     * what the query means, not a fix.
     */
    @Test
    fun aPathThroughAMultipleAssociationFiltersButIsNotSelectableTest() {

        val tgt = targetDao.createRecord("targetText" to "AAA")
        val linked = createRecord("textAtt" to "linked", "multiAssocAtt" to listOf(tgt))
        createRecord("textAtt" to "not-linked")

        val att = "multiAssocAtt.targetText"

        assertThat(records.query(baseQuery.copy { withQuery(Predicates.eq(att, "AAA")) }).getRecords())
            .describedAs("filtering through a multiple association works")
            .containsExactly(linked)

        assertThat(records.query(baseQuery.copy { withGroupBy(listOf(att)) }).getRecords())
            .describedAs("grouping by it is refused, and a refused query is an empty result")
            .isEmpty()
    }

    /**
     * Negation over the association itself rather than over a path through it.
     *
     * Another mechanism answers this: an association predicate is rewritten against the association
     * table whether or not the attribute is multiple - `DbRecordsUtils.isStoredInAssocsTable` looks
     * at the type, not at the multiplicity - so the condition becomes its own EXISTS and the
     * negation stays outside it. A record with no link has no row in that table and therefore is
     * not a record whose association is that one, which is the answer the path case gives too.
     *
     * The column holding the link is not what answers it, so the NULL-safe comparison PostgreSQL
     * gets for `not(eq(column, value))` is not what carries this - it only takes over if the join
     * is dropped.
     */
    @Test
    fun negationOverTheAssociationItselfWhenItIsAbsentTest() {

        val tgtA = targetDao.createRecord("targetText" to "AAA")
        val tgtB = targetDao.createRecord("targetText" to "BBB")

        val withA = createRecord("textAtt" to "withA", "assocAtt" to tgtA)
        val withoutAssoc = createRecord("textAtt" to "without")
        val withMultiA = createRecord("textAtt" to "multiA", "multiAssocAtt" to listOf(tgtA))
        val withMultiB = createRecord("textAtt" to "multiB", "multiAssocAtt" to listOf(tgtB))

        fun queryTest(predicate: Predicate, expected: List<EntityRef>) {
            val result = records.query(baseQuery.copy { withQuery(predicate) }).getRecords()
            assertThat(result).describedAs("Predicate: $predicate")
                .containsExactlyInAnyOrderElementsOf(expected.map { EntityRef.valueOf(it) })
        }

        queryTest(Predicates.eq("assocAtt", tgtA), listOf(withA))
        queryTest(
            Predicates.not(Predicates.eq("assocAtt", tgtA)),
            listOf(withoutAssoc, withMultiA, withMultiB)
        )
        queryTest(Predicates.eq("multiAssocAtt", tgtA), listOf(withMultiA))
        queryTest(
            Predicates.not(Predicates.eq("multiAssocAtt", tgtA)),
            listOf(withA, withoutAssoc, withMultiB)
        )
    }

    /**
     * The operators a path condition can carry beyond equality, in both polarities.
     *
     * Each of these takes its own branch when the condition is rendered inside the EXISTS, and the
     * record with no association has to come out of the negated form of every one of them.
     */
    @Test
    fun theOtherOperatorsOverAnAssocPathTest() {

        val tgtA = targetDao.createRecord("targetText" to "AAA", "targetNum" to 10)
        val tgtB = targetDao.createRecord("targetText" to "BBB", "targetNum" to 20)

        val withA = createRecord("textAtt" to "withA", "assocAtt" to tgtA)
        val withB = createRecord("textAtt" to "withB", "assocAtt" to tgtB)
        val withoutAssoc = createRecord("textAtt" to "without")

        fun queryTest(predicate: Predicate, expected: List<EntityRef>) {
            val result = records.query(baseQuery.copy { withQuery(predicate) }).getRecords()
            assertThat(result).describedAs("Predicate: $predicate")
                .containsExactlyInAnyOrderElementsOf(expected.map { EntityRef.valueOf(it) })
        }

        queryTest(Predicates.like("assocAtt.targetText", "A%"), listOf(withA))
        queryTest(Predicates.not(Predicates.like("assocAtt.targetText", "A%")), listOf(withB, withoutAssoc))

        queryTest(Predicates.inVals("assocAtt.targetText", listOf("AAA", "CCC")), listOf(withA))
        queryTest(
            Predicates.not(Predicates.inVals("assocAtt.targetText", listOf("AAA", "CCC"))),
            listOf(withB, withoutAssoc)
        )

        // an empty list matches nothing, so its negation matches everything - including the record
        // the association of which does not exist
        queryTest(Predicates.inVals("assocAtt.targetText", emptyList<String>()), emptyList())
        queryTest(
            Predicates.not(Predicates.inVals("assocAtt.targetText", emptyList<String>())),
            listOf(withA, withB, withoutAssoc)
        )

        queryTest(Predicates.ge("assocAtt.targetNum", 20), listOf(withB))
        queryTest(Predicates.not(Predicates.ge("assocAtt.targetNum", 20)), listOf(withA, withoutAssoc))
        queryTest(Predicates.le("assocAtt.targetNum", 10), listOf(withA))
        queryTest(Predicates.not(Predicates.le("assocAtt.targetNum", 10)), listOf(withB, withoutAssoc))
    }

    /**
     * The total count of a query grouped by a path through an association.
     *
     * The count builds its own query, and it used to hand the group-by names straight to SQL: a
     * path reached the database as a column name with dots in it and PostgreSQL refused the whole
     * query with `column r.assocAtt.targetText does not exist`, while the in-memory backend
     * answered. Counting the grouped select as a whole registers the joins on the way, because it
     * is the very call the query itself makes.
     */
    @Test
    fun theTotalCountOfAQueryGroupedByAnAssocPathTest() {

        val tgtA = targetDao.createRecord("targetText" to "AAA")
        val tgtB = targetDao.createRecord("targetText" to "BBB")

        val inQuery = 7
        val leaf = createRecord("textAtt" to "leaf", "numAtt" to 1, "assocAtt" to tgtA)
        val mid = childDao.createRecord("childText" to "mid", "testTypeTargetAssoc" to leaf)

        createRecord("textAtt" to "s1", "numAtt" to inQuery, "assocAtt" to tgtA, "childAssocAtt" to mid)
        createRecord("textAtt" to "s2", "numAtt" to inQuery, "assocAtt" to tgtA)
        createRecord("textAtt" to "s3", "numAtt" to inQuery, "assocAtt" to tgtB)
        createRecord("textAtt" to "s4", "numAtt" to inQuery)

        fun countGroupedBy(vararg atts: String): Long {
            return records.query(
                baseQuery.copy {
                    withQuery(Predicates.eq("numAtt", inQuery))
                    withGroupBy(atts.toList())
                    withMaxItems(0)
                }
            ).getTotalCount()
        }

        // AAA, BBB and the records that have no association at all
        assertThat(countGroupedBy("assocAtt.targetText")).isEqualTo(3)
        // one record reaches the leaf, three stop before it and share a group
        assertThat(countGroupedBy("childAssocAtt.testTypeTargetAssoc.textAtt")).isEqualTo(2)
        assertThat(countGroupedBy("assocAtt.targetText", "textAtt")).isEqualTo(4)

        // and the count agrees with the number of rows the same grouping returns
        val att = "assocAtt.targetText"
        val groups = records.query(
            baseQuery.copy {
                withQuery(Predicates.eq("numAtt", inQuery))
                withGroupBy(listOf(att))
            },
            mapOf("value" to att, "count" to "count(*)")
        ).getRecords()
        assertThat(groups).hasSize(3)
    }

    /**
     * Filtering by a field of a linked record when the path passes through a multiple association -
     * at the first step, in the middle, at the end, or at more than one of them.
     *
     * This is the mechanism that has nothing to do with select joins: a condition over a path is a
     * nested EXISTS, and a multiple step walks the association table instead of a column, so many
     * links are natural to it and the depth is just another level of nesting. What "matches" means
     * stays the same at every step - *some* linked record satisfies the rest of the path.
     *
     * Only the one-step form was covered. A multiple association deeper in the path is the shape
     * that multi-step paths made reachable, and nothing exercised it.
     */
    @Test
    fun filteringThroughAMultipleAssociationAtAnyStepOfAPathTest() {

        val leafAbc = targetDao.createRecord("targetText" to "abc")
        val leafDef = targetDao.createRecord("targetText" to "def")

        val midToAbc = childDao.createRecord("childText" to "mid-abc", "childMultiAssoc" to listOf(leafAbc))
        val midToBoth = childDao.createRecord(
            "childText" to "mid-both",
            "childMultiAssoc" to listOf(leafAbc, leafDef)
        )
        val midToNone = childDao.createRecord("childText" to "mid-none")

        // a target record linked to several middle records, to walk a multiple step first
        val targetToMids = targetDao.createRecord(
            "targetText" to "hub",
            "targetMultiAssoc" to listOf(midToAbc, midToNone)
        )
        val targetToNoMids = targetDao.createRecord("targetText" to "hub-empty")

        val inQuery = 7
        // first step multiple, second and third single/multiple
        val viaMultiFirst = createRecord(
            "textAtt" to "multi-first",
            "numAtt" to inQuery,
            "multiAssocAtt" to listOf(targetToMids)
        )
        val viaMultiFirstEmpty = createRecord(
            "textAtt" to "multi-first-empty",
            "numAtt" to inQuery,
            "multiAssocAtt" to listOf(targetToNoMids)
        )
        // first step single, second step multiple
        val viaMultiMiddle = createRecord(
            "textAtt" to "multi-middle",
            "numAtt" to inQuery,
            "childAssocAtt" to midToBoth
        )
        val viaMultiMiddleEmpty = createRecord(
            "textAtt" to "multi-middle-empty",
            "numAtt" to inQuery,
            "childAssocAtt" to midToNone
        )
        val withoutAnything = createRecord("textAtt" to "nothing", "numAtt" to inQuery)

        fun queryTest(predicate: Predicate, expected: List<EntityRef>) {
            val result = records.query(
                baseQuery.copy {
                    withQuery(Predicates.and(predicate, Predicates.eq("numAtt", inQuery)))
                }
            ).getRecords()
            assertThat(result).describedAs("Predicate: $predicate")
                .containsExactlyInAnyOrderElementsOf(expected.map { EntityRef.valueOf(it) })
        }

        // multiple in the middle of a two-step path
        val middle = "childAssocAtt.childMultiAssoc.targetText"
        queryTest(Predicates.eq(middle, "abc"), listOf(viaMultiMiddle))
        queryTest(Predicates.eq(middle, "def"), listOf(viaMultiMiddle))
        queryTest(Predicates.eq(middle, "ghi"), emptyList())
        queryTest(Predicates.notEmpty(middle), listOf(viaMultiMiddle))
        // the middle record with no links, and the records with no path at all, hold no value
        queryTest(
            Predicates.empty(middle),
            listOf(viaMultiMiddleEmpty, viaMultiFirst, viaMultiFirstEmpty, withoutAnything)
        )
        queryTest(
            Predicates.not(Predicates.eq(middle, "abc")),
            listOf(viaMultiMiddleEmpty, viaMultiFirst, viaMultiFirstEmpty, withoutAnything)
        )

        // multiple at the first step, single after it
        val firstStep = "multiAssocAtt.targetMultiAssoc.childText"
        queryTest(Predicates.eq(firstStep, "mid-abc"), listOf(viaMultiFirst))
        queryTest(Predicates.eq(firstStep, "mid-none"), listOf(viaMultiFirst))
        queryTest(Predicates.notEmpty(firstStep), listOf(viaMultiFirst))

        // two multiple steps in the same path, three links deep
        val twoMultiSteps = "multiAssocAtt.targetMultiAssoc.childMultiAssoc.targetText"
        queryTest(Predicates.eq(twoMultiSteps, "abc"), listOf(viaMultiFirst))
        queryTest(Predicates.eq(twoMultiSteps, "def"), emptyList())
        queryTest(Predicates.notEmpty(twoMultiSteps), listOf(viaMultiFirst))
        queryTest(
            Predicates.not(Predicates.eq(twoMultiSteps, "abc")),
            listOf(viaMultiFirstEmpty, viaMultiMiddle, viaMultiMiddleEmpty, withoutAnything)
        )

        // the operators that are not equality take the same route at depth
        queryTest(Predicates.contains(middle, "ab"), listOf(viaMultiMiddle))
        queryTest(Predicates.inVals(middle, listOf("def", "ghi")), listOf(viaMultiMiddle))
    }

    @BeforeEach
    fun prepare() {

        registerAspect(
            AspectInfo.create {
                withId("aspect0")
                withAttributes(
                    listOf(
                        AttributeDef.create {
                            withId("aspect0:assocAtt")
                            withType(AttributeType.ASSOC)
                            withConfig(ObjectData.create().set("typeRef", targetTypeRef.toString()))
                        },
                        AttributeDef.create {
                            withId("aspect0:multiAssocAtt")
                            withType(AttributeType.ASSOC)
                            withMultiple(true)
                            withConfig(ObjectData.create().set("typeRef", targetTypeRef.toString()))
                        }
                    )
                )
            }
        )

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("textAtt")
                },
                AttributeDef.create {
                    withId("numAtt")
                    withType(AttributeType.NUMBER)
                },
                AttributeDef.create {
                    withId("assocAtt")
                    withType(AttributeType.ASSOC)
                    withConfig(ObjectData.create().set("typeRef", targetTypeRef.toString()))
                },
                AttributeDef.create {
                    withId("multiAssocAtt")
                    withType(AttributeType.ASSOC)
                    withMultiple(true)
                    withConfig(ObjectData.create().set("typeRef", targetTypeRef.toString()))
                },
                AttributeDef.create {
                    withId("multiAuthorityAtt")
                    withType(AttributeType.AUTHORITY)
                    withMultiple(true)
                    withConfig(ObjectData.create().set("typeRef", targetTypeRef.toString()))
                },
                AttributeDef.create {
                    withId("childAssocAtt")
                    withType(AttributeType.ASSOC)
                    withConfig(
                        ObjectData.create()
                            .set("typeRef", childTypeRef.toString())
                            .set("child", true)
                    )
                },
                // long enough that the paths built from them cross PostgreSQL's 63-byte
                // identifier limit - see longJoinKeysThatShareAPrefixDoNotCollideTest
                AttributeDef.create {
                    withId("childAssocAttWithAVeryLongNameToApproachTheLimit")
                    withType(AttributeType.ASSOC)
                    withConfig(
                        ObjectData.create()
                            .set("typeRef", childTypeRef.toString())
                            .set("child", true)
                    )
                },
                AttributeDef.create {
                    withId("assocAttWithALongNameNumberOne")
                    withType(AttributeType.ASSOC)
                    withConfig(ObjectData.create().set("typeRef", targetTypeRef.toString()))
                },
                AttributeDef.create {
                    withId("assocAttWithALongNameNumberTwo")
                    withType(AttributeType.ASSOC)
                    withConfig(ObjectData.create().set("typeRef", targetTypeRef.toString()))
                }
            )
        )

        targetDao = createRecordsDao(
            DEFAULT_TABLE_REF.withTable(targetTypeRef.getLocalId()),
            targetTypeRef,
            targetTypeRef.getLocalId()
        )

        registerType(
            TypeInfo.create {
                withId(targetTypeRef.getLocalId())
                withSourceId(targetTypeRef.getLocalId())
                withModel(
                    TypeModelDef.create()
                        .withAttributes(
                            listOf(
                                AttributeDef.create()
                                    .withId("name")
                                    .withType(AttributeType.MLTEXT)
                                    .build(),
                                AttributeDef.create()
                                    .withId("targetText")
                                    .build(),
                                AttributeDef.create()
                                    .withId("targetAssoc")
                                    .withType(AttributeType.ASSOC)
                                    .build(),
                                AttributeDef.create()
                                    .withId("targetAuthority")
                                    .withType(AttributeType.AUTHORITY)
                                    .build(),
                                AttributeDef.create()
                                    .withId("targetNum")
                                    .withType(AttributeType.NUMBER)
                                    .build(),
                                AttributeDef.create()
                                    .withId("targetMultiAssoc")
                                    .withType(AttributeType.ASSOC)
                                    .withMultiple(true)
                                    .withConfig(ObjectData.create().set("typeRef", childTypeRef.toString()))
                                    .build(),
                                AttributeDef.create()
                                    .withId("targetDate")
                                    .withType(AttributeType.DATE)
                                    .build()
                            )
                        )
                        .build()
                )
            }
        )

        childDao = createRecordsDao(
            DEFAULT_TABLE_REF.withTable(childTypeRef.getLocalId()),
            childTypeRef,
            childTypeRef.getLocalId()
        )

        registerType(
            TypeInfo.create {
                withId(childTypeRef.getLocalId())
                withSourceId(childTypeRef.getLocalId())
                withModel(
                    TypeModelDef.create()
                        .withAttributes(
                            listOf(
                                AttributeDef.create()
                                    .withId("childText")
                                    .build(),
                                AttributeDef.create()
                                    .withId("childNum")
                                    .withType(AttributeType.NUMBER)
                                    .build(),
                                AttributeDef.create()
                                    .withId("testTypeTargetAssoc")
                                    .withType(AttributeType.ASSOC)
                                    .withConfig(ObjectData.create().set("typeRef", REC_TEST_TYPE_REF))
                                    .build(),
                                AttributeDef.create()
                                    .withId("childMultiAssoc")
                                    .withType(AttributeType.ASSOC)
                                    .withMultiple(true)
                                    .withConfig(ObjectData.create().set("typeRef", targetTypeRef.toString()))
                                    .build(),
                                AttributeDef.create()
                                    .withId("targetAssocWithAVeryLongNameToo")
                                    .withType(AttributeType.ASSOC)
                                    .withConfig(ObjectData.create().set("typeRef", REC_TEST_TYPE_REF))
                                    .build()
                            )
                        )
                        .build()
                )
            }
        )
    }
}
