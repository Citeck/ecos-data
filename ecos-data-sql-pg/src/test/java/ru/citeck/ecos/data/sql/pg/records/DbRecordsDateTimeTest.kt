package ru.citeck.ecos.data.sql.pg.records

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import ru.citeck.ecos.commons.data.DataValue
import ru.citeck.ecos.data.sql.pg.records.commons.DbRecordsTestBase
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType
import ru.citeck.ecos.records2.predicate.model.ValuePredicate
import ru.citeck.ecos.webapp.api.entity.EntityRef
import java.time.Instant
import java.time.temporal.ChronoUnit

class DbRecordsDateTimeTest : DbRecordsTestBase() {

    @Test
    fun test() {

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("date")
                    withType(AttributeType.DATE)
                },
                AttributeDef.create {
                    withId("datetime")
                    withType(AttributeType.DATETIME)
                }
            )
        )

        val record = createRecord(
            "date" to "2023-01-01",
            "datetime" to "2023-01-01T11:11:11Z"
        )

        fun assertQuery(field: String, type: ValuePredicate.Type, value: String, expected: List<EntityRef>) {
            val records = records.query(
                baseQuery.withQuery(
                    DataValue.create(
                        ValuePredicate(field, type, value)
                    )
                )
            ).getRecords()
            assertThat(records as List<EntityRef>)
                .describedAs("$field $type '$value'")
                .containsExactlyInAnyOrderElementsOf(expected)
        }

        fun assertEqQuery(field: String, value: String, expected: List<EntityRef>) {
            assertQuery(field, ValuePredicate.Type.EQ, value, expected)
        }

        fun assertGtQuery(field: String, value: String, expected: List<EntityRef>) {
            assertQuery(field, ValuePredicate.Type.GT, value, expected)
        }

        fun assertLtQuery(field: String, value: String, expected: List<EntityRef>) {
            assertQuery(field, ValuePredicate.Type.LT, value, expected)
        }

        listOf("_created", "_modified").forEach { field ->
            assertGtQuery(field, "\$TODAY", listOf(record))
            assertLtQuery(field, "P1D", listOf(record))
            assertEqQuery(field, "-PT1H/\$NOW", listOf(record))
            assertEqQuery(field, "-P1D/P1D", listOf(record))
        }

        assertEqQuery("date", "2022-05-01T00:00:00.000Z/2023-05-16T17:00:00.000Z", listOf(record))
        assertEqQuery("date", "2022-05-01/2023-05-16", listOf(record))
        assertEqQuery("date", "2023-05-01T00:00:00.000Z/2023-05-16T17:00:00.000Z", emptyList())
        assertEqQuery("date", "2023-05-01/2023-05-16", emptyList())

        assertEqQuery("datetime", "2022-05-01T00:00:00.000Z/2023-05-16T17:00:00.000Z", listOf(record))
        assertEqQuery("datetime", "2022-05-01/2023-05-16", listOf(record))
        assertEqQuery("datetime", "2023-05-01T00:00:00.000Z/2023-05-16T17:00:00.000Z", emptyList())
        assertEqQuery("datetime", "2023-05-01/2023-05-16", emptyList())

        val record1 = createRecord("date" to Instant.now().truncatedTo(ChronoUnit.DAYS))
        assertEqQuery("date", "\$TODAY", listOf(record1))
    }
}
