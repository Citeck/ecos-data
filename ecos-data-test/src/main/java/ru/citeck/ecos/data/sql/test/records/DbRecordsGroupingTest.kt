package ru.citeck.ecos.data.sql.test.records

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import ru.citeck.ecos.commons.data.DataValue
import ru.citeck.ecos.commons.data.ObjectData
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType
import ru.citeck.ecos.model.lib.status.dto.StatusDef
import ru.citeck.ecos.model.lib.type.dto.TypeInfo
import ru.citeck.ecos.model.lib.type.dto.TypeModelDef
import ru.citeck.ecos.model.lib.utils.ModelUtils
import ru.citeck.ecos.records2.RecordConstants
import ru.citeck.ecos.records2.predicate.model.Predicates
import ru.citeck.ecos.records3.record.atts.dto.RecordAtts
import ru.citeck.ecos.records3.record.atts.schema.annotation.AttName
import ru.citeck.ecos.records3.record.dao.query.dto.query.RecordsQuery
import ru.citeck.ecos.records3.record.dao.query.dto.query.SortBy
import ru.citeck.ecos.records3.record.dao.query.dto.res.RecsQueryRes
import ru.citeck.ecos.webapp.api.entity.toEntityRef
import java.util.TreeMap
import java.util.concurrent.atomic.AtomicInteger
import kotlin.random.Random

class DbRecordsGroupingTest : DbRecordsTestBase() {

    companion object {
        private const val TASK_ID_FIELD = "taskId"
        private const val AMOUNT_FIELD = "amount"
    }

    @Test
    fun testWithPropOfAssoc() {

        val targetTypeRef = ModelUtils.getTypeRef("target")
        val targetDao = createRecordsDao(
            tableRef.withTable("target"),
            targetTypeRef,
            "target"
        )
        registerType(
            TypeInfo.create()
                .withId(targetTypeRef.getLocalId())
                .withSourceId("target")
                .withModel(
                    TypeModelDef.create()
                        .withAttributes(
                            listOf(
                                AttributeDef.create()
                                    .withId("name")
                                    .build(),
                                AttributeDef.create()
                                    .withId("number")
                                    .withType(AttributeType.NUMBER)
                                    .build()
                            )
                        ).build()
                ).build()
        )
        registerAtts(
            listOf(
                AttributeDef.create()
                    .withId("assoc")
                    .withType(AttributeType.ASSOC)
                    .withConfig(ObjectData.create().set("typeRef", targetTypeRef))
                    .build()
            )
        )

        val target0 = targetDao.createRecord("name" to "abc", "number" to 123)

        createRecord("assoc" to target0)
        createRecord()

        val queryRes = records.query(
            baseQuery.copy()
                .withGroupBy(listOf("assoc"))
                .build(),
            mapOf(
                "assocAlias" to "assoc{disp:?disp,value:?assoc}",
                "sumAlias" to "sum(assoc.number)"
            )
        ).getRecords()

        assertThat(queryRes).hasSize(2)
        val nonNullRec = queryRes.first { it.getAtt("$.assocAlias.value").asText() == target0.toString() }
        assertThat(nonNullRec.getAtt("$.assocAlias.disp").asText()).isEqualTo("abc")
        val nullRec = queryRes.first { it !== nonNullRec }
        assertThat(nullRec.getAtt("$.assocAlias")).isEqualTo(DataValue.NULL)
    }

    @Test
    fun testGroupingWithInvalidAttsToLoad() {

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("text0")
                },
                AttributeDef.create {
                    withId("text1")
                },
                AttributeDef.create {
                    withId("num0")
                    withType(AttributeType.NUMBER)
                },
                AttributeDef.create {
                    withId("num1")
                    withType(AttributeType.NUMBER)
                }
            )
        )

        createRecord("text0" to "abc", "text1" to "qwe", "num0" to 10, "num1" to 20)
        createRecord("text0" to "abc", "text1" to "ert", "num0" to 5, "num1" to 30)
        createRecord("text0" to "def", "text1" to "tyu", "num0" to 2, "num1" to 15)

        val query = baseQuery.copy()
            .withSortBy(emptyList())
            .withGroupBy(listOf("text0"))
            .withSortBy(listOf(SortBy("text0", true)))
            .build()

        val res = records.query(query, listOf("text0", "text1", "num0", "sum(num0)", "(num0 - 10)")).getRecords()

        assertThat(res).hasSize(2)
        assertThat(res[0]["text0"].asText()).isEqualTo("abc")
        assertThat(res[0]["text1"].asText()).isEqualTo("")
        assertThat(res[0]["num0"].asInt()).isEqualTo(0)
        assertThat(res[0]["sum(num0)"].asInt()).isEqualTo(15)
        assertThat(res[0]["(num0 - 10)"].asInt()).isEqualTo(0)
        assertThat(res[1]["text0"].asText()).isEqualTo("def")
        assertThat(res[1]["text1"].asText()).isEqualTo("")
        assertThat(res[1]["num0"].asInt()).isEqualTo(0)
        assertThat(res[1]["sum(num0)"].asInt()).isEqualTo(2)
        assertThat(res[1]["(num0 - 10)"].asInt()).isEqualTo(0)

        val query2 = baseQuery.copy()
            .withSortBy(emptyList())
            .withGroupBy(listOf("(num0 -   10)"))
            .withSortBy(listOf(SortBy("(num0   - 10)", true)))
            .build()

        val res2 = records.query(
            query2,
            listOf(
                "text1",
                "sum(num0)",
                "(num0 - 10)"
            )
        ).getRecords()

        assertThat(res2).hasSize(3)
        assertThat(res2[0]["text1"].asText()).isEqualTo("")
        assertThat(res2[0]["sum(num0)"].asInt()).isEqualTo(2)
        assertThat(res2[0]["(num0 - 10)"].asInt()).isEqualTo(-8)

        assertThat(res2[1]["text1"].asText()).isEqualTo("")
        assertThat(res2[1]["sum(num0)"].asInt()).isEqualTo(5)
        assertThat(res2[1]["(num0 - 10)"].asInt()).isEqualTo(-5)

        assertThat(res2[2]["text1"].asText()).isEqualTo("")
        assertThat(res2[2]["sum(num0)"].asInt()).isEqualTo(10)
        assertThat(res2[2]["(num0 - 10)"].asInt()).isEqualTo(0)
    }

    @Test
    fun testGroupAndSortByMultipleAtts() {

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("text0")
                },
                AttributeDef.create {
                    withId("text1")
                }
            )
        )
        val recordsInMemData = ArrayList<Pair<String, String>>()
        repeat(30) {
            val att0 = Random.nextInt(10).toString()
            val att1 = Random.nextInt(10).toString()
            createRecord("text0" to att0, "text1" to att1)
            recordsInMemData.add(att0 to att1)
        }
        val groupedInMemWithCount = HashMap<Pair<String, String>, AtomicInteger>()
        recordsInMemData.forEach {
            groupedInMemWithCount.computeIfAbsent(it) { AtomicInteger() }.incrementAndGet()
        }
        fun queryTest(asc0: Boolean, asc1: Boolean) {
            val sortedGroupedInMem = TreeMap<Pair<String, String>, AtomicInteger> { v0, v1 ->
                var compareRes = v0.first.compareTo(v1.first)
                if (!asc0) {
                    compareRes = -compareRes
                }
                if (compareRes == 0) {
                    compareRes = v0.second.compareTo(v1.second)
                    if (!asc1) {
                        compareRes = -compareRes
                    }
                }
                compareRes
            }
            sortedGroupedInMem.putAll(groupedInMemWithCount)
            val result = records.query(
                baseQuery.copy {
                    withGroupBy(listOf("text0", "text1"))
                    withSortBy(
                        listOf(
                            SortBy("text0", asc0),
                            SortBy("text1", asc1)
                        )
                    )
                },
                listOf("text0", "text1", "count(*)")
            ).getRecords()
            assertThat(result).hasSize(sortedGroupedInMem.size)
            sortedGroupedInMem.entries.forEachIndexed { idx, expected ->
                val atts = result[idx]
                assertThat(atts["text0"].asText()).isEqualTo(expected.key.first)
                assertThat(atts["text1"].asText()).isEqualTo(expected.key.second)
                assertThat(atts["count(*)"].asInt()).isEqualTo(expected.value.get())
            }
        }

        queryTest(asc0 = false, asc1 = false)
        queryTest(asc0 = false, asc1 = true)
        queryTest(asc0 = true, asc1 = false)
        queryTest(asc0 = true, asc1 = true)
    }

    @Test
    fun testGroupingByCreatedWithSorting() {

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("text")
                }
            )
        )

        val createdAtt = RecordConstants.ATT_CREATED
        val recordsCount = 10

        repeat(recordsCount) {
            Thread.sleep(50)
            createRecord().let { it to records.getAtt(it, createdAtt).getAsInstantOrEpoch() }
        }

        fun testQuery(ascendingSort: Boolean) {

            val queryRes = records.query(
                baseQuery.copy {
                    withSortBy(listOf(SortBy(createdAtt, ascendingSort)))
                    withGroupBy(listOf(createdAtt))
                },
                listOf(createdAtt)
            ).getRecords().map { it.getAtt(createdAtt).getAsInstantOrEpoch() }

            assertThat(queryRes).hasSize(recordsCount)

            for (i in 0 until queryRes.lastIndex) {
                if (ascendingSort) {
                    assertThat(queryRes[i]).isBefore(queryRes[i + 1])
                } else {
                    assertThat(queryRes[i]).isAfter(queryRes[i + 1])
                }
            }
        }
        testQuery(false)
        testQuery(true)
    }

    @Test
    fun test() {

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId(TASK_ID_FIELD)
                },
                AttributeDef.create {
                    withId(AMOUNT_FIELD)
                    withType(AttributeType.NUMBER)
                }
            )
        )

        createRecord(TASK_ID_FIELD to "task-0", AMOUNT_FIELD to 10)
        createRecord(TASK_ID_FIELD to "task-1", AMOUNT_FIELD to 20)
        createRecord(TASK_ID_FIELD to "task-1", AMOUNT_FIELD to 30)
        createRecord(TASK_ID_FIELD to "task-1", AMOUNT_FIELD to 40)

        val result = records.query(
            createQuery {
                withSortBy(emptyList())
                withQuery(Predicates.alwaysTrue())
                withGroupBy(listOf(TASK_ID_FIELD))
            },
            TaskIdWithCount::class.java
        ).getRecords()

        assertThat(result).hasSize(2)
        val assertTask = { taskId: String, action: (TaskIdWithCount) -> Unit ->
            val task = result.find { it.taskId == taskId }
            assertThat(task).describedAs(taskId).isNotNull
            action.invoke(task!!)
        }
        assertTask("task-0") {
            assertThat(it.count).isEqualTo(1)
            assertThat(it.amountSum).isEqualTo(10.0)
            assertThat(it.minSum).isEqualTo(10.0)
            assertThat(it.maxSum).isEqualTo(10.0)
            assertThat(it.avgSum).isEqualTo(10.0)
        }
        assertTask("task-1") {
            assertThat(it.count).isEqualTo(3)
            assertThat(it.amountSum).isEqualTo(90.0)
            assertThat(it.minSum).isEqualTo(20.0)
            assertThat(it.maxSum).isEqualTo(40.0)
            assertThat(it.avgSum).isEqualTo(30.0)
        }
    }

    @Test
    fun testWithGroupingByGlobalAtts() {

        registerType(
            TypeInfo.create {
                withId(REC_TEST_TYPE_ID)
                withModel(
                    TypeModelDef.create {
                        withStatuses(
                            listOf(
                                StatusDef.create().withId("draft").build(),
                                StatusDef.create().withId("confirm").build()
                            )
                        )
                        withAttributes(
                            listOf(
                                AttributeDef.create().withId(TASK_ID_FIELD).build(),
                                AttributeDef.create().withId(AMOUNT_FIELD).withType(AttributeType.NUMBER).build()
                            )
                        )
                    }
                )
            }
        )

        createRecord(TASK_ID_FIELD to "task-0", AMOUNT_FIELD to 10, "_status" to "draft")
        createRecord(TASK_ID_FIELD to "task-0", AMOUNT_FIELD to 20, "_status" to "draft")
        createRecord(TASK_ID_FIELD to "task-0", AMOUNT_FIELD to 30, "_status" to "draft")
        createRecord(TASK_ID_FIELD to "task-0", AMOUNT_FIELD to 11, "_status" to "confirm")
        createRecord(TASK_ID_FIELD to "task-0", AMOUNT_FIELD to 22, "_status" to "confirm")

        val result = records.query(
            createQuery {
                withSortBy(emptyList())
                withQuery(Predicates.alwaysTrue())
                withGroupBy(listOf("_status"))
            },
            listOf("_status?str", "count(*)")
        ).getRecords()

        assertThat(result).hasSize(2)
        val countByStatus = result.associate {
            it.getAtt("_status?str").asText() to it.getAtt("count(*)").asInt()
        }
        assertThat(countByStatus).hasSize(2)
        assertThat(countByStatus["draft"]).isEqualTo(3)
        assertThat(countByStatus["confirm"]).isEqualTo(2)
    }

    @Test
    fun testWithGroupingByMultipleAttsFromAssoc() {

        val assocTypeRef = ModelUtils.getTypeRef("assoc-type")
        val assocSourceId = "assoc-src-id"
        registerAtts(
            listOf(
                AttributeDef.create()
                    .withId("assoc")
                    .withType(AttributeType.ASSOC)
                    .withConfig(ObjectData.create().set("typeRef", assocTypeRef))
                    .build()
            )
        )
        registerType(
            TypeInfo.create()
                .withId(assocTypeRef.getLocalId())
                .withSourceId(assocSourceId)
                .withModel(
                    TypeModelDef.create()
                        .withAttributes(
                            listOf(
                                AttributeDef.create()
                                    .withId("assocTextField0")
                                    .build(),
                                AttributeDef.create()
                                    .withId("assocTextField1")
                                    .build()
                            )
                        ).build()
                ).build()
        )
        val assocDaoCtx = createRecordsDao(
            DEFAULT_TABLE_REF.withTable("assoc-table"),
            assocTypeRef,
            assocSourceId
        )

        val assocRef0 = assocDaoCtx.createRecord(
            "assocTextField0" to "field0text0",
            "assocTextField1" to "field1text0"
        )
        val assocRef1 = assocDaoCtx.createRecord(
            "assocTextField0" to "field0text1",
            "assocTextField1" to "field1text1"
        )

        mainCtx.createRecord("assoc" to assocRef0)
        mainCtx.createRecord("assoc" to assocRef0)
        mainCtx.createRecord("assoc" to assocRef0)

        mainCtx.createRecord("assoc" to assocRef1)
        mainCtx.createRecord("assoc" to assocRef1)

        val countAtt = "count(*)"
        val assocAtt = "assoc?id"

        val assocTextField0Att = "assoc.assocTextField0"
        val assocTextField1Att = "assoc.assocTextField1"

        val queryRes0 = records.query(
            baseQuery.copy()
                .withGroupBy(listOf("assoc"))
                .withSortBy(listOf(SortBy(countAtt, true)))
                .build(),
            listOf(assocAtt, countAtt)
        ).getRecords()

        assertThat(queryRes0).hasSize(2)
        assertThat(queryRes0[0][countAtt].asInt()).isEqualTo(2)
        assertThat(queryRes0[0][assocAtt].toEntityRef()).isEqualTo(assocRef1)
        assertThat(queryRes0[1][countAtt].asInt()).isEqualTo(3)
        assertThat(queryRes0[1][assocAtt].toEntityRef()).isEqualTo(assocRef0)

        val queryRes1 = records.query(
            baseQuery.copy()
                .withGroupBy(listOf(assocTextField0Att))
                .withSortBy(listOf(SortBy(countAtt, true)))
                .build(),
            listOf(assocTextField0Att, countAtt)
        ).getRecords()

        assertThat(queryRes1).hasSize(2)
        assertThat(queryRes1[0][countAtt].asInt()).isEqualTo(2)
        assertThat(queryRes1[0][assocTextField0Att].asText()).isEqualTo("field0text1")
        assertThat(queryRes1[1][countAtt].asInt()).isEqualTo(3)
        assertThat(queryRes1[1][assocTextField0Att].asText()).isEqualTo("field0text0")

        val queryRes2 = records.query(
            baseQuery.copy()
                .withGroupBy(listOf(assocTextField0Att, assocTextField1Att))
                .withSortBy(listOf(SortBy(countAtt, true)))
                .build(),
            listOf(assocTextField0Att, assocTextField1Att, countAtt)
        ).getRecords()

        assertThat(queryRes2).hasSize(2)
        assertThat(queryRes2[0][countAtt].asInt()).isEqualTo(2)
        assertThat(queryRes2[0][assocTextField0Att].asText()).isEqualTo("field0text1")
        assertThat(queryRes2[0][assocTextField1Att].asText()).isEqualTo("field1text1")
        assertThat(queryRes2[1][countAtt].asInt()).isEqualTo(3)
        assertThat(queryRes2[1][assocTextField0Att].asText()).isEqualTo("field0text0")
        assertThat(queryRes2[1][assocTextField1Att].asText()).isEqualTo("field1text0")

        val queryRes3 = records.query(
            baseQuery.copy()
                // legacy grouping with '&' delimiter. groupBy(field0&field1) should work as groupBy(field0, field1)
                // see ru.citeck.ecos.records3.record.dao.impl.group.RecordsGroupDao
                .withGroupBy(listOf("$assocTextField0Att&$assocTextField1Att"))
                .withSortBy(listOf(SortBy(countAtt, true)))
                .build(),
            listOf(assocTextField0Att, assocTextField1Att, countAtt)
        ).getRecords()

        assertThat(queryRes3).hasSize(2)
        assertThat(queryRes3[0][countAtt].asInt()).isEqualTo(2)
        assertThat(queryRes3[0][assocTextField0Att].asText()).isEqualTo("field0text1")
        assertThat(queryRes3[0][assocTextField1Att].asText()).isEqualTo("field1text1")
        assertThat(queryRes3[1][countAtt].asInt()).isEqualTo(3)
        assertThat(queryRes3[1][assocTextField0Att].asText()).isEqualTo("field0text0")
        assertThat(queryRes3[1][assocTextField1Att].asText()).isEqualTo("field1text0")

        val queryRes4 = records.query(
            baseQuery.copy()
                .withGroupBy(listOf("assoc", assocTextField1Att))
                .withSortBy(listOf(SortBy(countAtt, true)))
                .build(),
            listOf("assoc", assocAtt, assocTextField1Att, countAtt)
        ).getRecords()

        assertThat(queryRes4).hasSize(2)
        assertThat(queryRes4[0][countAtt].asInt()).isEqualTo(2)
        assertThat(queryRes4[0][assocAtt].toEntityRef()).isEqualTo(assocRef1)
        assertThat(queryRes4[0][assocTextField1Att].asText()).isEqualTo("field1text1")
        assertThat(queryRes4[1][countAtt].asInt()).isEqualTo(3)
        assertThat(queryRes4[1][assocAtt].toEntityRef()).isEqualTo(assocRef0)
        assertThat(queryRes4[1][assocTextField1Att].asText()).isEqualTo("field1text0")
    }

    @Test
    fun testGroupingByAllRecords() {

        registerAtts(
            listOf(
                AttributeDef.create()
                    .withId("num")
                    .withType(AttributeType.NUMBER)
                    .build()
            )
        )
        createRecord("num" to 1)
        createRecord("num" to 2)
        createRecord("num" to 3)

        val query = baseQuery.copy { withGroupBy(listOf("*")) }

        fun test(func: String, expected: Double) {
            val sum = records.queryOne(query, func)
            assertThat(sum.asDouble()).describedAs(func).isEqualTo(expected)
        }
        test("sum(num)", 6.0)
        test("avg(num)", 2.0)
        test("count(*)", 3.0)
        test("max(num)", 3.0)
        test("min(num)", 1.0)
    }

    @Test
    fun testWithMetaFieldToLoad() {

        registerAtts(
            listOf(
                AttributeDef.create()
                    .withId("assoc")
                    .withType(AttributeType.ASSOC)
                    .build()
            )
        )
        createRecord()
        val rec1 = createRecord()
        createRecord("assoc" to rec1)
        createRecord("assoc" to rec1)
        createRecord("assoc" to rec1)

        val recs0 = records.query(
            baseQuery.copy()
                .withGroupBy(listOf("assoc"))
                .build(),
            listOf(
                "count(*)",
                "assoc?localId"
            )
        ).getRecords().map { it.getAtts() }.associate {
            it["assoc?localId"].asText() to it["count(*)"].asInt()
        }

        assertThat(recs0).hasSize(2)
        assertThat(recs0[""]).isEqualTo(2)
        assertThat(recs0[rec1.getLocalId()]).isEqualTo(3)

        createRecord()

        val recs1 = records.query(
            baseQuery.copy()
                .withGroupBy(listOf("assoc"))
                .build(),
            listOf(
                "count(*)",
                "assoc?localId",
                "_creator{id:?id,disp:?disp}",
                "_modifier{id:?id,disp:?disp}"
            )
        ).getRecords().map { it.getAtts() }.associate {
            it["assoc?localId"].asText() to it["count(*)"].asInt()
        }

        assertThat(recs1).hasSize(2)
        assertThat(recs1[""]).isEqualTo(3)
        assertThat(recs1[rec1.getLocalId()]).isEqualTo(3)
    }

    /**
     * The total count of a grouped query.
     *
     * A grouped query answers with a row per group, so "how many" has to mean how many groups -
     * that is what the caller pages through. It used to mean something else on PostgreSQL:
     * `SELECT COUNT(*) ... GROUP BY x` returns a row per group holding that group's size, and the
     * first of them was read as the total, with no ORDER BY to say which group that was. The
     * in-memory backend has always answered with the number of groups, so the two disagreed on
     * every grouped count.
     */
    @Test
    fun theTotalCountOfAGroupedQueryIsTheNumberOfGroupsTest() {

        registerAtts(
            listOf(
                AttributeDef.create { withId("text0") },
                AttributeDef.create {
                    withId("num0")
                    withType(AttributeType.NUMBER)
                }
            )
        )

        createRecord("text0" to "abc", "num0" to 1)
        createRecord("text0" to "abc", "num0" to 2)
        createRecord("text0" to "def", "num0" to 3)
        createRecord("text0" to "ghi", "num0" to 4)

        fun countOf(action: RecordsQuery.Builder.() -> Unit): Long {
            return records.query(
                baseQuery.copy {
                    withSortBy(emptyList())
                    withMaxItems(0)
                    action()
                }
            ).getTotalCount()
        }

        // three groups of sizes 2, 1 and 1 - the old answer was the size of one of them
        assertThat(countOf { withGroupBy(listOf("text0")) }).isEqualTo(3)
        // grouping by something every record differs in makes group count and record count agree,
        // which is why a test that only ever grouped by a unique column could not tell them apart
        assertThat(countOf { withGroupBy(listOf("num0")) }).isEqualTo(4)
        assertThat(countOf { withGroupBy(listOf("text0", "num0")) }).isEqualTo(4)

        // no grouping at all still counts records
        assertThat(countOf { }).isEqualTo(4)

        // "*" is one group over everything, and it exists only if anything matches
        assertThat(countOf { withGroupBy(listOf("*")) }).isEqualTo(1)
        assertThat(
            countOf {
                withGroupBy(listOf("*"))
                withQuery(Predicates.eq("text0", "nothing"))
            }
        ).isEqualTo(0)
        assertThat(
            countOf {
                withGroupBy(listOf("text0"))
                withQuery(Predicates.eq("text0", "nothing"))
            }
        ).isEqualTo(0)

        // a condition narrows the groups, not only the records in them
        assertThat(
            countOf {
                withGroupBy(listOf("text0"))
                withQuery(Predicates.eq("text0", "abc"))
            }
        ).isEqualTo(1)

        // an aggregate in the select does not change what is counted
        assertThat(
            countOf {
                withGroupBy(listOf("text0"))
                withSortBy(listOf(SortBy("sum(num0)", true)))
            }
        ).isEqualTo(3)

        // and the count agrees with what the query actually returns
        val groups = records.query(
            baseQuery.copy {
                withSortBy(emptyList())
                withGroupBy(listOf("text0"))
            },
            mapOf("value" to "text0", "count" to "count(*)")
        ).getRecords()
        assertThat(groups).hasSize(3)
    }

    /**
     * Grouping by a column that holds many values.
     *
     * Such a column is an array, and the whole array is the group: two records carrying the same
     * values in the same order belong together, a record carrying a subset does not, and a record
     * carrying nothing has a group of its own. That is what PostgreSQL does with `GROUP BY` over an
     * array column, and the in-memory backend has to say the same.
     *
     * Both backends got it wrong, in different ways, and both had shipped:
     *  - PostgreSQL read the grouped value back as the column's scalar type, ignoring that the
     *    column is multiple, and failed the whole query with `Can't convert class kotlin.Array to
     *    class kotlin.Long` - an error reaching the caller, not an empty result;
     *  - the in-memory backend keyed its buckets by a list holding the array itself, and an array
     *    compares by identity, so equal arrays never met and every record became its own group.
     */
    @Test
    fun groupingByAMultipleColumnTest() {

        val targetTypeRef = ModelUtils.getTypeRef("group-target-type")
        val targetDao = createRecordsDao(
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
                        .withAttributes(listOf(AttributeDef.create().withId("targetText").build()))
                        .build()
                )
            }
        )

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("multiText")
                    withMultiple(true)
                },
                AttributeDef.create {
                    withId("multiAssoc")
                    withType(AttributeType.ASSOC)
                    withMultiple(true)
                    withConfig(ObjectData.create().set("typeRef", targetTypeRef.toString()))
                }
            )
        )

        val tgtA = targetDao.createRecord("targetText" to "AAA")
        val tgtB = targetDao.createRecord("targetText" to "BBB")

        createRecord("multiText" to listOf("a", "b"), "multiAssoc" to listOf(tgtA, tgtB))
        createRecord("multiText" to listOf("a", "b"), "multiAssoc" to listOf(tgtA, tgtB))
        createRecord("multiText" to listOf("b", "a"), "multiAssoc" to listOf(tgtB, tgtA))
        createRecord("multiText" to listOf("a"), "multiAssoc" to listOf(tgtA))
        createRecord()

        fun groupSizes(att: String): List<Int> {
            return records.query(
                baseQuery.copy {
                    withSortBy(emptyList())
                    withGroupBy(listOf(att))
                },
                mapOf("count" to "count(*)")
            ).getRecords().map { it["count"].asInt() }.sorted()
        }

        fun groupCount(att: String): Long {
            return records.query(
                baseQuery.copy {
                    withSortBy(emptyList())
                    withGroupBy(listOf(att))
                    withMaxItems(0)
                }
            ).getTotalCount()
        }

        // ["a","b"] twice, ["b","a"] once - order is part of the value, so they are not the same
        // group - ["a"] once, and nothing once
        assertThat(groupSizes("multiText")).containsExactly(1, 1, 1, 2)
        assertThat(groupCount("multiText")).isEqualTo(4)

        assertThat(groupSizes("multiAssoc")).containsExactly(1, 1, 1, 2)
        assertThat(groupCount("multiAssoc")).isEqualTo(4)

        // the values a group reports are still the whole array, not a key built for comparing
        val grouped = records.query(
            baseQuery.copy {
                withSortBy(emptyList())
                withGroupBy(listOf("multiText"))
            },
            mapOf("value" to "multiText[]", "count" to "count(*)")
        ).getRecords()
        val byValue = grouped.associate { it["value"].toString() to it["count"].asInt() }
        assertThat(byValue).isEqualTo(
            mapOf(
                """["a","b"]""" to 2,
                """["b","a"]""" to 1,
                """["a"]""" to 1,
                "[]" to 1
            )
        )
    }

    /**
     * An aggregate over an attribute the table does not have, in a grouped query.
     *
     * An unresolvable argument becomes a bare `NULL` in SQL, and an aggregate cannot be typed by
     * one: PostgreSQL refused `sum(null)` with `function sum(unknown) is not unique` and failed the
     * whole query - a journal grouped by one field and totalling another would stop working the
     * moment that other field was renamed away. The in-memory backend answered `null` instead, so
     * nothing here noticed.
     *
     * A query without grouping never reached this: its aggregates are resolved away before the
     * argument is looked up at all, which is why only the grouped form broke.
     *
     * The answer is the one this codebase already gives for an aggregate over an attribute that is
     * not there - zero for the numeric ones - and both backends give it now.
     */
    @Test
    fun anAggregateOverAnAttributeThatIsNotThereInAGroupedQueryTest() {

        val targetTypeRef = ModelUtils.getTypeRef("agg-target-type")
        val targetDao = createRecordsDao(
            DEFAULT_TABLE_REF.withTable(targetTypeRef.getLocalId()),
            targetTypeRef,
            targetTypeRef.getLocalId()
        )
        registerType(
            TypeInfo.create {
                withId(targetTypeRef.getLocalId())
                withSourceId(targetTypeRef.getLocalId())
                withModel(
                    TypeModelDef.create().withAttributes(
                        listOf(
                            AttributeDef.create()
                                .withId("targetNum")
                                .withType(AttributeType.NUMBER)
                                .build()
                        )
                    ).build()
                )
            }
        )

        registerAtts(
            listOf(
                AttributeDef.create { withId("text0") },
                AttributeDef.create {
                    withId("num0")
                    withType(AttributeType.NUMBER)
                },
                AttributeDef.create {
                    withId("assoc")
                    withType(AttributeType.ASSOC)
                    withConfig(ObjectData.create().set("typeRef", targetTypeRef.toString()))
                },
                AttributeDef.create {
                    withId("multiAssoc")
                    withType(AttributeType.ASSOC)
                    withMultiple(true)
                    withConfig(ObjectData.create().set("typeRef", targetTypeRef.toString()))
                }
            )
        )

        val target = targetDao.createRecord("targetNum" to 5)

        createRecord("text0" to "abc", "num0" to 1)
        createRecord("text0" to "abc", "num0" to 2)
        createRecord("text0" to "def", "num0" to 4)
        createRecord("text0" to "ghi", "assoc" to target, "multiAssoc" to listOf(target))

        val res = records.query(
            baseQuery.copy {
                withSortBy(emptyList())
                withQuery(Predicates.inVals("text0", listOf("abc", "def")))
                withGroupBy(listOf("text0"))
                withSortBy(listOf(SortBy("text0", true)))
            },
            mapOf(
                "text" to "text0",
                "validSum" to "sum(num0)",
                "missingSum" to "sum(noSuchColumn)",
                "missingAvg" to "avg(noSuchColumn)",
                "missingMax" to "max(noSuchColumn)",
                "missingCount" to "count(noSuchColumn)"
            )
        ).getRecords()

        assertThat(res).hasSize(2)

        assertThat(res[0]["text"].asText()).isEqualTo("abc")
        assertThat(res[0]["validSum"].asInt())
            .describedAs("an aggregate over an attribute that is there still works")
            .isEqualTo(3)
        // compared as text on purpose: `asInt()` turns a null into 0 as well, and "answered zero"
        // is exactly what has to be told apart from "answered nothing" here
        assertThat(res[0]["missingSum"].asText()).isEqualTo("0")
        assertThat(res[0]["missingMax"].asText()).isEqualTo("0")
        assertThat(res[0]["missingCount"].asText()).isEqualTo("0")
        // `avg` is asked numerically instead: PostgreSQL answers it with a scaled numeric - `0E-20`
        // here, as it scales the average of any real column - while the in-memory backend answers
        // a plain `0`. The two agree on the number, which is what this is about; the scale is how
        // PostgreSQL renders every average, not something this fix introduced
        assertThat(res[0]["missingAvg"].isNull()).isFalse()
        assertThat(res[0]["missingAvg"].asDouble()).isEqualTo(0.0)

        assertThat(res[1]["text"].asText()).isEqualTo("def")
        assertThat(res[1]["validSum"].asInt()).isEqualTo(4)
        assertThat(res[1]["missingSum"].asText()).isEqualTo("0")

        // the same when the attribute is missing at the end of a path, and when the path itself
        // cannot be walked for selecting at all - a multiple association
        val res2 = records.query(
            baseQuery.copy {
                withSortBy(emptyList())
                withQuery(Predicates.eq("text0", "ghi"))
                withGroupBy(listOf("text0"))
            },
            mapOf(
                "onTarget" to """sum("assoc.targetNum")""",
                "missingOnTarget" to """sum("assoc.noSuchColumn")""",
                "throughMultiple" to """sum("multiAssoc.targetNum")"""
            )
        ).getRecords()

        assertThat(res2).hasSize(1)
        assertThat(res2[0]["onTarget"].asInt())
            .describedAs("a path that can be walked still reaches the value")
            .isEqualTo(5)
        assertThat(res2[0]["missingOnTarget"].asText()).isEqualTo("0")
        assertThat(res2[0]["throughMultiple"].asText())
            .describedAs("a multiple association cannot be walked for selecting, so there is no value")
            .isEqualTo("0")
    }

    /**
     * Grouping by a name the query cannot read.
     *
     * An attribute renamed away leaves such a group-by behind in every saved view that used it, so
     * this is a question with no answer rather than a mistake worth raising at whoever opened the
     * journal: the records DAO refuses it the way it refuses the other preparing failures - an
     * empty result and a line in the debug log. PostgreSQL used to raise from underneath, which
     * reached the caller as a failure, while the in-memory backend quietly put every record into a
     * single group and answered that.
     *
     * The repository below still refuses loudly, because a query built by hand there has a mistake
     * in it rather than an unanswerable question - see [DbDataServiceCountContractTest].
     */
    @Test
    fun groupingByANameTheQueryCannotReadTest() {

        registerAtts(
            listOf(
                AttributeDef.create { withId("text0") },
                AttributeDef.create {
                    withId("num0")
                    withType(AttributeType.NUMBER)
                }
            )
        )

        createRecord("text0" to "abc", "num0" to 1)
        createRecord("text0" to "abc", "num0" to 2)
        createRecord("text0" to "def", "num0" to 3)

        fun queryGroupedBy(vararg atts: String): RecsQueryRes<RecordAtts> {
            return records.query(
                baseQuery.copy {
                    withSortBy(emptyList())
                    withGroupBy(atts.toList())
                },
                mapOf("count" to "count(*)")
            )
        }

        val absent = queryGroupedBy("noSuchColumn")
        assertThat(absent.getRecords()).isEmpty()
        assertThat(absent.getTotalCount()).isEqualTo(0)

        // one unreadable name is enough to leave the whole grouping unanswerable
        assertThat(queryGroupedBy("text0", "noSuchColumn").getRecords()).isEmpty()
        assertThat(queryGroupedBy("noSuchAssoc.someAtt").getRecords()).isEmpty()

        // asking for the total alone takes another route through the repository, and gets the
        // same answer
        assertThat(
            records.query(
                baseQuery.copy {
                    withSortBy(emptyList())
                    withGroupBy(listOf("noSuchColumn"))
                    withMaxItems(0)
                }
            ).getTotalCount()
        ).isEqualTo(0)

        // and a grouping that can be read is untouched
        val valid = queryGroupedBy("text0")
        assertThat(valid.getRecords()).hasSize(2)
        assertThat(valid.getRecords().map { it["count"].asInt() }.sorted()).containsExactly(1, 2)
    }

    open class TaskIdWithCount(
        val taskId: String,
        @AttName("count(*)")
        val count: Int,
        @AttName("sum(amount)")
        val amountSum: Double,
        @AttName("max(amount)")
        val maxSum: Double,
        @AttName("min(amount)")
        val minSum: Double,
        @AttName("avg(amount)")
        val avgSum: Double
    )
}
