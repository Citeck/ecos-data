package ru.citeck.ecos.data.sql.pg.records

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import ru.citeck.ecos.commons.data.DataValue
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType
import ru.citeck.ecos.records2.predicate.model.Predicate
import ru.citeck.ecos.records2.predicate.model.Predicates
import ru.citeck.ecos.webapp.api.entity.EntityRef
import java.time.Instant

class DbRecordsPredicateTest : DbRecordsTestBase() {

    @Test
    fun testWithBoolean() {

        val att0 = "attBool0"

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId(att0)
                    withType(AttributeType.BOOLEAN)
                }
            )
        )

        val falseRef = createRecord(att0 to false)
        val nullRef = createRecord(att0 to null)
        val trueRef = createRecord(att0 to true)
        val refToValue = mapOf(
            falseRef to false,
            trueRef to true,
            nullRef to null
        )

        val queryTest = { predicate: Predicate, expected: List<EntityRef> ->
            val queryRes = records.query(
                baseQuery.withQuery(DataValue.create(predicate)),
            )
            val expectedBools = expected.map { refToValue[it] }
            val actualBools = queryRes.getRecords().map { refToValue[it] }
            assertThat(queryRes.getRecords())
                .describedAs("predicate: $predicate expected: $expectedBools actual: $actualBools")
                .containsExactlyInAnyOrderElementsOf(expected)
        }

        queryTest(Predicates.not(Predicates.eq(att0, false)), listOf(trueRef, nullRef))
        queryTest(Predicates.eq(att0, null), listOf(nullRef))
        queryTest(Predicates.eq(att0, false), listOf(falseRef))
        queryTest(Predicates.eq(att0, true), listOf(trueRef))
        queryTest(Predicates.empty(att0), listOf(nullRef))
        queryTest(Predicates.notEmpty(att0), listOf(trueRef, falseRef))
        queryTest(Predicates.not(Predicates.eq(att0, true)), listOf(falseRef, nullRef))
        queryTest(Predicates.not(Predicates.eq(att0, null)), listOf(falseRef, trueRef))
        queryTest(Predicates.empty(att0), listOf(nullRef))
        queryTest(Predicates.not(Predicates.empty(att0)), listOf(falseRef, trueRef))
    }

    @Test
    fun testWithComplexCondition() {

        val attBool0 = "attBool0"
        val attDateTime0 = "attDateTime0"
        val attDateTime1 = "attDateTime1"

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId(attBool0)
                    withType(AttributeType.BOOLEAN)
                },
                AttributeDef.create {
                    withId(attDateTime0)
                    withType(AttributeType.DATETIME)
                },
                AttributeDef.create {
                    withId(attDateTime1)
                    withType(AttributeType.DATETIME)
                }
            )
        )

        val time0 = Instant.parse("2020-01-01T00:00:00Z")
        val time1 = Instant.parse("2020-06-01T00:00:00Z")

        createRecord(
            attBool0 to false,
            attDateTime0 to time0,
            attDateTime1 to time1
        )

        val lastActivityTime = Instant.parse("2019-12-01T00:00:00Z")

        val predicate = Predicates.and(
            Predicates.not(Predicates.eq("id", "admin")),
            Predicates.or(
                Predicates.empty(attBool0),
                Predicates.eq(attBool0, "false")
            ),
            Predicates.notEmpty(attDateTime0),
            Predicates.or(
                Predicates.and(
                    Predicates.empty(attDateTime1),
                    Predicates.le(attDateTime0, lastActivityTime),
                ),
                Predicates.and(
                    Predicates.notEmpty(attDateTime1),
                    Predicates.le(attDateTime1, lastActivityTime),
                    Predicates.le(attDateTime0, lastActivityTime),
                )
            )
        )

        val queryRes = records.query(
            baseQuery.copy {
                withQuery(predicate)
            }
        )

        assertThat(queryRes.getRecords()).isEmpty()
    }
}
