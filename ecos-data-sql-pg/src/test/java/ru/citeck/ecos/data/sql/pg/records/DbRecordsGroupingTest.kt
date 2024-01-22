package ru.citeck.ecos.data.sql.pg.records

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import ru.citeck.ecos.commons.data.ObjectData
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType
import ru.citeck.ecos.model.lib.status.dto.StatusDef
import ru.citeck.ecos.model.lib.type.dto.TypeInfo
import ru.citeck.ecos.model.lib.type.dto.TypeModelDef
import ru.citeck.ecos.model.lib.utils.ModelUtils
import ru.citeck.ecos.records2.RecordConstants
import ru.citeck.ecos.records2.predicate.model.Predicates
import ru.citeck.ecos.records3.record.atts.schema.annotation.AttName
import ru.citeck.ecos.records3.record.dao.query.dto.query.SortBy
import ru.citeck.ecos.webapp.api.entity.toEntityRef
import java.util.TreeMap
import java.util.concurrent.atomic.AtomicInteger
import kotlin.random.Random

class DbRecordsGroupingTest : DbRecordsTestBase() {

    companion object {
        private const val taskIdField = "taskId"
        private const val amountField = "amount"
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
                    withId(taskIdField)
                },
                AttributeDef.create {
                    withId(amountField)
                    withType(AttributeType.NUMBER)
                }
            )
        )

        createRecord(taskIdField to "task-0", amountField to 10)
        createRecord(taskIdField to "task-1", amountField to 20)
        createRecord(taskIdField to "task-1", amountField to 30)
        createRecord(taskIdField to "task-1", amountField to 40)

        val result = records.query(
            createQuery {
                withSortBy(emptyList())
                withQuery(Predicates.alwaysTrue())
                withGroupBy(listOf(taskIdField))
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
                                AttributeDef.create().withId(taskIdField).build(),
                                AttributeDef.create().withId(amountField).withType(AttributeType.NUMBER).build()
                            )
                        )
                    }
                )
            }
        )

        createRecord(taskIdField to "task-0", amountField to 10, "_status" to "draft")
        createRecord(taskIdField to "task-0", amountField to 20, "_status" to "draft")
        createRecord(taskIdField to "task-0", amountField to 30, "_status" to "draft")
        createRecord(taskIdField to "task-0", amountField to 11, "_status" to "confirm")
        createRecord(taskIdField to "task-0", amountField to 22, "_status" to "confirm")

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
