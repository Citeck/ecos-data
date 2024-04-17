package ru.citeck.ecos.data.sql.pg.records

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

        val targetRec0 = targetDao.createRecord("targetText" to "abc", "targetNum" to 10)
        val targetRec1 = targetDao.createRecord("targetText" to "def", "targetNum" to 100)

        val rec0 = createRecord("assocAtt" to targetRec0, "numAtt" to 22)
        val rec1 = createRecord("assocAtt" to targetRec0, "numAtt" to 23)
        createRecord("assocAtt" to targetRec0, "numAtt" to 24)
        createRecord("assocAtt" to targetRec0, "numAtt" to 25)
        createRecord("assocAtt" to targetRec1, "numAtt" to 44)
        createRecord("assocAtt" to targetRec1, "numAtt" to 44)
        createRecord("assocAtt" to targetRec1, "numAtt" to 44)
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

    @ParameterizedTest
    @ValueSource(strings = ["multiAssocAtt", "aspect0:multiAssocAtt"])
    fun testWithMultipleAssoc(multiAssocName: String) {

        val targetRec0 = targetDao.createRecord("targetText" to "abc", "targetNum" to 10)
        val targetRec1 = targetDao.createRecord("targetText" to "def", "targetNum" to 100)

        val targetRec2 = targetDao.createRecord("targetText" to "hij", "targetNum" to 5)
        val targetRec3 = targetDao.createRecord("targetText" to "klm", "targetNum" to 50)

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

        val queryRes2 = records.query(baseQuery, mapOf("sum" to "sum(\"$multiAssocName.targetNum\")"))
            .getRecords()
            .associate { it.getId() to it["sum"].asInt() }

        assertThat(queryRes2[record0]).isEqualTo(110)
        assertThat(queryRes2[record1]).isEqualTo(55)

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

        val queryRes3 = records.query(
            baseQuery.copy {
                withQuery(Predicates.eq("sum(\"$multiAssocName.targetNum\")", 110))
            }
        ).getRecords()

        assertThat(queryRes3).containsExactly(record0)
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
                    withId("childAssocAtt")
                    withType(AttributeType.ASSOC)
                    withConfig(
                        ObjectData.create()
                            .set("typeRef", childTypeRef.toString())
                            .set("child", true)
                    )
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
                                    .withId("targetText")
                                    .build(),
                                AttributeDef.create()
                                    .withId("targetNum")
                                    .withType(AttributeType.NUMBER)
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
                                    .build()
                            )
                        )
                        .build()
                )
            }
        )
    }
}
