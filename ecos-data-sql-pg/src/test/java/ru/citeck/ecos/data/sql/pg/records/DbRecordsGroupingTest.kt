package ru.citeck.ecos.data.sql.pg.records

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType
import ru.citeck.ecos.model.lib.status.dto.StatusDef
import ru.citeck.ecos.model.lib.type.dto.TypeInfo
import ru.citeck.ecos.model.lib.type.dto.TypeModelDef
import ru.citeck.ecos.records2.RecordConstants
import ru.citeck.ecos.records2.predicate.model.Predicates
import ru.citeck.ecos.records3.record.atts.schema.annotation.AttName
import ru.citeck.ecos.records3.record.dao.query.dto.query.SortBy
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
