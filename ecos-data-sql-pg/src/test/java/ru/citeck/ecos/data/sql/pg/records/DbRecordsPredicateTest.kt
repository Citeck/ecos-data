package ru.citeck.ecos.data.sql.pg.records

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import ru.citeck.ecos.commons.data.DataValue
import ru.citeck.ecos.commons.data.ObjectData
import ru.citeck.ecos.data.sql.pg.records.commons.DbRecordsTestBase
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType
import ru.citeck.ecos.records2.predicate.model.Predicate
import ru.citeck.ecos.records2.predicate.model.Predicates
import ru.citeck.ecos.records3.record.dao.impl.mem.InMemDataRecordsDao
import ru.citeck.ecos.records3.record.dao.query.dto.query.SortBy
import ru.citeck.ecos.webapp.api.entity.EntityRef
import java.time.Instant
import java.time.LocalDate

class DbRecordsPredicateTest : DbRecordsTestBase() {

    @Test
    fun testWithType() {
        records.register(InMemDataRecordsDao("emodel/type"))
        records.mutate(
            "emodel/type@",
            mapOf(
                "id" to REC_TEST_TYPE_ID,
                "name" to ObjectData.create().set("ru", "Русский").set("en", "English"),
                "config" to ObjectData.create()
                    .set("key", "value")
                    .set("inner", ObjectData.create().set("innerKey", "innerValue"))
            )
        )

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("text")
                    withType(AttributeType.TEXT)
                }
            )
        )
        val rec0 = createRecord("text" to "abc")

        fun queryTest(condition: Predicate, vararg expected: EntityRef) {
            val recs = records.query(
                baseQuery.copy()
                    .withQuery(condition)
                    .withSortBy(SortBy("dbid", true))
                    .build()
            ).getRecords()
            assertThat(recs).containsExactlyElementsOf(expected.toList())
        }

        queryTest(Predicates.contains("_type", "Русс"), rec0)
        queryTest(Predicates.contains("_type", "Русс1"))
        queryTest(Predicates.contains("_type", "emodel/type@$REC_TEST_TYPE_ID"), rec0)
        queryTest(Predicates.contains("_type", REC_TEST_TYPE_ID), rec0)
        queryTest(Predicates.eq("_type.config.key", "value"), rec0)
        queryTest(Predicates.eq("_type.config.key", "value1"))
    }

    @Test
    fun testWithText2() {

        val textAtt = "textAtt"
        val mlTextAtt = "mlTextAtt"
        registerAtts(
            listOf(
                AttributeDef.create {
                    withId(textAtt)
                    withType(AttributeType.TEXT)
                },
                AttributeDef.create {
                    withId(mlTextAtt)
                    withType(AttributeType.MLTEXT)
                }
            )
        )

        val rec0 = createRecord(
            textAtt to "text0",
            mlTextAtt to mapOf("ru" to "mltext_ru0", "en" to "mltext_en0")
        )
        val rec1 = createRecord(
            textAtt to "text1",
            mlTextAtt to mapOf("ru" to "mltext_ru1", "en" to "mltext_en1")
        )

        fun queryTest(condition: Predicate, vararg expected: EntityRef) {
            val recs = records.query(
                baseQuery.copy()
                    .withQuery(condition)
                    .withSortBy(SortBy("dbid", true))
                    .build()
            ).getRecords()
            assertThat(recs).containsExactlyElementsOf(expected.toList())
        }

        queryTest(Predicates.eq(textAtt, "text0"), rec0)
        queryTest(Predicates.like(textAtt, "%xt0"), rec0)
        queryTest(Predicates.eq(mlTextAtt, "mltext_ru0"), rec0)
        queryTest(Predicates.eq(mlTextAtt, "mltext_en0"), rec0)
        queryTest(Predicates.like(mlTextAtt, "mltext_%"), rec0, rec1)
        queryTest(Predicates.like(mlTextAtt, "%mlTex%"), rec0, rec1)
        queryTest(Predicates.like(mlTextAtt, "%mlQTex%"))

        queryTest(Predicates.eq(textAtt, "text1"), rec1)
    }

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
    fun testWithText() {

        val att0 = "attText0"

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId(att0)
                    withType(AttributeType.TEXT)
                }
            )
        )

        val textRef = createRecord(att0 to "someText")
        val nullRef = createRecord(att0 to null)
        val emptyRef = createRecord(att0 to "")
        val refToValue = mapOf(
            textRef to "someText",
            nullRef to null,
            emptyRef to ""
        )

        val queryTest = { predicate: Predicate, expected: List<EntityRef> ->
            val queryRes = records.query(
                baseQuery.withQuery(DataValue.create(predicate)),
            )
            val expectedValues = expected.map { refToValue[it] }
            val actualValues = queryRes.getRecords().map { refToValue[it] }
            assertThat(queryRes.getRecords())
                .describedAs("predicate: $predicate expected: $expectedValues actual: $actualValues")
                .containsExactlyInAnyOrderElementsOf(expected)
        }

        queryTest(Predicates.not(Predicates.eq(att0, "someText")), listOf(nullRef, emptyRef))
        queryTest(Predicates.eq(att0, null), listOf(nullRef))
        queryTest(Predicates.eq(att0, "someText"), listOf(textRef))
        queryTest(Predicates.eq(att0, ""), listOf(emptyRef))
        queryTest(Predicates.empty(att0), listOf(nullRef, emptyRef))
        queryTest(Predicates.notEmpty(att0), listOf(textRef))
        queryTest(Predicates.not(Predicates.eq(att0, "")), listOf(textRef, nullRef))
        queryTest(Predicates.not(Predicates.eq(att0, null)), listOf(textRef, emptyRef))
        queryTest(Predicates.empty(att0), listOf(nullRef, emptyRef))
        queryTest(Predicates.not(Predicates.empty(att0)), listOf(textRef))
    }

    @Test
    fun testWithNumber() {

        val att0 = "attNumber0"

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId(att0)
                    withType(AttributeType.NUMBER)
                }
            )
        )

        val doubleRef = createRecord(att0 to 10.5)
        val nullRef = createRecord(att0 to null)
        val zeroRef = createRecord(att0 to 0)
        val refToValue = mapOf(
            doubleRef to 10.5,
            nullRef to null,
            zeroRef to 0
        )

        val queryTest = { predicate: Predicate, expected: List<EntityRef> ->
            val queryRes = records.query(
                baseQuery.withQuery(DataValue.create(predicate)),
            )
            val expectedValues = expected.map { refToValue[it] }
            val actualValues = queryRes.getRecords().map { refToValue[it] }
            assertThat(queryRes.getRecords())
                .describedAs("predicate: $predicate expected: $expectedValues actual: $actualValues")
                .containsExactlyInAnyOrderElementsOf(expected)
        }

        queryTest(Predicates.not(Predicates.eq(att0, 10.5)), listOf(nullRef, zeroRef))
        queryTest(Predicates.eq(att0, null), listOf(nullRef))
        queryTest(Predicates.eq(att0, 10.5), listOf(doubleRef))
        queryTest(Predicates.eq(att0, 0), listOf(zeroRef))
        queryTest(Predicates.empty(att0), listOf(nullRef))
        queryTest(Predicates.notEmpty(att0), listOf(doubleRef, zeroRef))
        queryTest(Predicates.not(Predicates.eq(att0, 0)), listOf(doubleRef, nullRef))
        queryTest(Predicates.not(Predicates.eq(att0, null)), listOf(doubleRef, zeroRef))
        queryTest(Predicates.empty(att0), listOf(nullRef))
        queryTest(Predicates.not(Predicates.empty(att0)), listOf(doubleRef, zeroRef))
    }

    @Test
    fun testWithDate() {

        val att0 = "attDate0"

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId(att0)
                    withType(AttributeType.DATE)
                }
            )
        )

        val dateRef = createRecord(att0 to LocalDate.parse("2024-02-23"))
        val nullRef = createRecord(att0 to null)
        val refToValue = mapOf(
            dateRef to LocalDate.parse("2024-02-23"),
            nullRef to null
        )

        val queryTest = { predicate: Predicate, expected: List<EntityRef> ->
            val queryRes = records.query(
                baseQuery.withQuery(DataValue.create(predicate)),
            )
            val expectedValues = expected.map { refToValue[it] }
            val actualValues = queryRes.getRecords().map { refToValue[it] }
            assertThat(queryRes.getRecords())
                .describedAs("predicate: $predicate expected: $expectedValues actual: $actualValues")
                .containsExactlyInAnyOrderElementsOf(expected)
        }

        queryTest(Predicates.not(Predicates.eq(att0, LocalDate.parse("2024-02-23"))), listOf(nullRef))
        queryTest(Predicates.eq(att0, null), listOf(nullRef))
        queryTest(Predicates.eq(att0, LocalDate.parse("2024-02-23")), listOf(dateRef))
        queryTest(Predicates.empty(att0), listOf(nullRef))
        queryTest(Predicates.notEmpty(att0), listOf(dateRef))
        queryTest(Predicates.not(Predicates.eq(att0, null)), listOf(dateRef))
        queryTest(Predicates.empty(att0), listOf(nullRef))
        queryTest(Predicates.not(Predicates.empty(att0)), listOf(dateRef))
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
