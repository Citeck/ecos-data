package ru.citeck.ecos.data.sql.pg.records

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import ru.citeck.ecos.data.sql.test.records.DbRecordsTestBase
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType
import ru.citeck.ecos.webapp.api.entity.EntityRef

/**
 * What the read path gives back must be what PostgreSQL actually holds, for every date column, on
 * every driver.
 *
 * `DbEntityRepoPg.convertRowToMap` used to take every column out of the `ResultSet` with the untyped
 * `getObject`, which answers a [java.sql.Date] for a `date` and a [java.sql.Timestamp] for a
 * `timestamptz`. Both types hold their value as epoch millis and read their fields back through
 * [java.util.Date]'s hybrid calendar - Julian before the 1582-10-15 cutover, Gregorian after it -
 * while PostgreSQL and `java.time` are proleptic Gregorian throughout, so every value older than the
 * cutover crossed shifted by the Julian/Gregorian difference of its own epoch: two days at year 1,
 * ten days in the 1500s, thirty-eight days at 4713 BC.
 *
 * The drivers disagree about *which half* of those two types they fill in faithfully, which is why a
 * fix proven on one driver is still a defect on the other. Measured on PostgreSQL 12.6:
 *
 * - 42.7.9 builds them from the instant, so the millis are right and the calendar fields are wrong:
 *   a `date` read as `Date.toLocalDate()` came back two days late, a `timestamptz` read as
 *   `Timestamp.toInstant()` was right.
 * - 42.7.13 (and 42.2.14 / 42.7.3 / 42.7.10-42.7.13) builds them from the calendar fields, so it is
 *   the other way round: the `date` was right and the `timestamptz` instant came back two days early.
 *
 * The values are planted with raw SQL on purpose. A round trip through the ordinary write path
 * cannot tell a read defect from a write one, and both exist here - see the report on this branch:
 * the DATETIME attribute conversion turns `0001-01-01T11:22:33Z` into an instant two days earlier
 * before the SQL layer ever sees it, and 42.7.10+ shifts a bound [java.sql.Timestamp] back the other
 * way, so the two cancel on one driver and not on the other. Planting the value states the one thing
 * a read path owns: the database holds X, so the read has to say X.
 */
class PgProlepticDateReadTest : DbRecordsTestBase() {

    companion object {
        private val DATE_ATTS = listOf(
            AttributeDef.create {
                withId("date")
                withType(AttributeType.DATE)
            },
            AttributeDef.create {
                withId("dates")
                withType(AttributeType.DATE)
                withMultiple(true)
            },
            AttributeDef.create {
                withId("datetime")
                withType(AttributeType.DATETIME)
            },
            AttributeDef.create {
                withId("datetimes")
                withType(AttributeType.DATETIME)
                withMultiple(true)
            }
        )
    }

    private fun plantPreGregorianDates(): EntityRef {

        registerAtts(DATE_ATTS)

        val record = createRecord(
            "date" to "2023-01-01",
            "dates" to listOf("2023-01-01"),
            "datetime" to "2023-01-01T11:22:33Z",
            "datetimes" to listOf("2023-01-01T11:22:33Z")
        )

        sqlUpdate(
            "UPDATE ${tableRef.fullName} SET " +
                "\"date\" = '0001-01-01'::date, " +
                "\"dates\" = '{0001-01-01,1500-03-04,2023-01-01}'::date[], " +
                "\"datetime\" = '0001-01-01 11:22:33+00'::timestamptz, " +
                "\"datetimes\" = '{\"0001-01-01 11:22:33+00\",\"1500-03-04 11:22:33+00\"}'::timestamptz[] " +
                "WHERE __ext_id = '${record.getLocalId()}'"
        )
        return record
    }

    @Test
    fun aDateColumnReadsBackWhatTheDatabaseHoldsTest() {
        assertThat(records.getAtt(plantPreGregorianDates(), "date").asText())
            .describedAs("a date holding 0001-01-01 reads back as 0001-01-01")
            .isEqualTo("0001-01-01T00:00:00Z")
    }

    @Test
    fun aDateArrayColumnReadsBackWhatTheDatabaseHoldsTest() {
        assertThat(records.getAtt(plantPreGregorianDates(), "dates[]").asStrList().map { it.substring(0, 10) })
            .describedAs("a date[] reads back every element as the database holds it")
            .containsExactly("0001-01-01", "1500-03-04", "2023-01-01")
    }

    @Test
    fun aTimestamptzColumnReadsBackWhatTheDatabaseHoldsTest() {
        assertThat(records.getAtt(plantPreGregorianDates(), "datetime").asText())
            .describedAs("a timestamptz holding 0001-01-01 11:22:33+00 reads back as that instant")
            .isEqualTo("0001-01-01T11:22:33Z")
    }

    @Test
    fun aTimestamptzArrayColumnReadsBackWhatTheDatabaseHoldsTest() {
        assertThat(records.getAtt(plantPreGregorianDates(), "datetimes[]").asStrList())
            .describedAs("a timestamptz[] reads back every element as the database holds it")
            .containsExactly("0001-01-01T11:22:33Z", "1500-03-04T11:22:33Z")
    }

    /**
     * A null date column and a null element inside a date array stay null: the typed read replaces
     * only how a value crosses out of the result set, never whether there is one.
     */
    @Test
    fun aNullDateColumnAndANullArrayElementStayNullTest() {

        registerAtts(DATE_ATTS)

        val record = createRecord(
            "date" to "2023-01-01",
            "dates" to listOf("2023-01-01"),
            "datetime" to "2023-01-01T11:22:33Z",
            "datetimes" to listOf("2023-01-01T11:22:33Z")
        )

        sqlUpdate(
            "UPDATE ${tableRef.fullName} SET " +
                "\"date\" = NULL, " +
                "\"datetime\" = NULL, " +
                "\"dates\" = '{0001-01-01,NULL,2023-01-01}'::date[], " +
                "\"datetimes\" = '{NULL,\"0001-01-01 11:22:33+00\"}'::timestamptz[] " +
                "WHERE __ext_id = '${record.getLocalId()}'"
        )

        assertThat(records.getAtt(record, "date").isNull())
            .describedAs("a null date stays null")
            .isTrue()

        assertThat(records.getAtt(record, "datetime").isNull())
            .describedAs("a null timestamptz stays null")
            .isTrue()

        assertThat(records.getAtt(record, "dates[]").asStrList().map { it.substring(0, 10) })
            .describedAs("a null element of a date[] is dropped, as an untyped read dropped it")
            .containsExactly("0001-01-01", "2023-01-01")

        assertThat(records.getAtt(record, "datetimes[]").asStrList())
            .describedAs("a null element of a timestamptz[] is dropped, as an untyped read dropped it")
            .containsExactly("0001-01-01T11:22:33Z")
    }
}
