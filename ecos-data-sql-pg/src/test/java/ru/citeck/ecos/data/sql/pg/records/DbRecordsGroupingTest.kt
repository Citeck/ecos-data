package ru.citeck.ecos.data.sql.pg.records

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType
import ru.citeck.ecos.records2.predicate.model.Predicates
import ru.citeck.ecos.records3.record.atts.schema.annotation.AttName

class DbRecordsGroupingTest : DbRecordsTestBase() {

    @Test
    fun test() {
        val taskIdField = "taskId"
        val amountField = "amount"

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
            baseQuery.copy {
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

    class TaskIdWithCount(
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
