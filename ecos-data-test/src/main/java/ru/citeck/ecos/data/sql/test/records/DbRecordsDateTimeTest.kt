package ru.citeck.ecos.data.sql.test.records

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import ru.citeck.ecos.commons.data.DataValue
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

    /**
     * A multi-valued DATE, which is a `date[]` column and the one array shape nothing ever bound.
     *
     * `DbDataSourceImpl.setParams` converts a `Timestamp[]` into a JDBC array explicitly, because
     * the driver cannot infer the element type of an object array; a `LocalDate[]` - what a DATE
     * column's values are converted to on the way in - had no such branch, so `setObject` was handed
     * a raw `LocalDate[]` and PostgreSQL answered `Cannot cast an instance of [Ljava.time.LocalDate;
     * to type Types.ARRAY`. Every write of a multi-valued DATE attribute failed, whether or not any
     * migration was involved; the conversion matrix found it because it is the only test that writes
     * a value of every attribute type at both multiplicities.
     */
    @Test
    fun aMultiValuedDateAttributeStoresEveryDateTest() {

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("dates")
                    withType(AttributeType.DATE)
                    withMultiple(true)
                }
            )
        )

        val record = createRecord("dates" to listOf("2023-01-01", "2024-02-29", "1970-01-01"))

        assertThat(records.getAtt(record, "dates[]").asStrList().map { it.substring(0, 10) })
            .describedAs("a date[] column holds every date it was given, in the order it was given them")
            .containsExactly("2023-01-01", "2024-02-29", "1970-01-01")
    }

    /**
     * A DATE from before the Gregorian cutover must come back out of the store exactly as it went in.
     *
     * The PostgreSQL read path used to pull a `date` out of the `ResultSet` with the untyped
     * `getObject`, which hands back a [java.sql.Date]. That type holds its value as epoch millis and
     * reads its fields back through [java.util.Date]'s hybrid calendar - Julian before the
     * 1582-10-15 cutover, Gregorian after it - while PostgreSQL and [java.time] are proleptic
     * Gregorian throughout. Every date older than the cutover therefore came back shifted by the
     * Julian/Gregorian difference of its own epoch: two days at year 1, ten days in the 1500s.
     *
     * The write path binds a [java.time.LocalDate] and is not shifted, which is what made this
     * corrupting rather than cosmetic: a read-modify-write cycle read the shifted date and stored
     * it, replacing the user's date for good and shifting it again on every following save.
     *
     * Which driver shifts which form is not the same on every driver - see
     * `ru.citeck.ecos.data.sql.pg.records.PgProlepticDateReadTest` for the read of each form
     * measured against what the database itself holds.
     */
    @Test
    fun aDateFromBeforeTheGregorianCutoverSurvivesARoundTripTest() {

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("date")
                    withType(AttributeType.DATE)
                }
            )
        )

        val record = createRecord("date" to "0001-01-01T00:00:00Z")

        assertThat(records.getAtt(record, "date").asText())
            .describedAs("a DATE before the Gregorian cutover reads back as it was written")
            .isEqualTo("0001-01-01T00:00:00Z")
    }

    /**
     * The same shift, on the array form of a DATE column - `date[]`.
     *
     * A `date[]` is read as one JDBC array whose elements are [java.sql.Date], so each element
     * crosses the same hybrid-calendar boundary as a single DATE does, and the per-element read has
     * to be typed the same way. Only AD dates are used here: binding a `LocalDate[]` renders each
     * element with `toString`, and PostgreSQL rejects the `-4712-01-01` that a BC year produces -
     * an array *write* defect that this test deliberately stays clear of.
     */
    @Test
    fun aMultiValuedDateFromBeforeTheGregorianCutoverSurvivesARoundTripTest() {

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("dates")
                    withType(AttributeType.DATE)
                    withMultiple(true)
                }
            )
        )

        val record = createRecord("dates" to listOf("0001-01-01", "1500-03-04", "2023-01-01"))

        assertThat(records.getAtt(record, "dates[]").asStrList().map { it.substring(0, 10) })
            .describedAs("every element of a date[] reads back as it was written")
            .containsExactly("0001-01-01", "1500-03-04", "2023-01-01")
    }
}
