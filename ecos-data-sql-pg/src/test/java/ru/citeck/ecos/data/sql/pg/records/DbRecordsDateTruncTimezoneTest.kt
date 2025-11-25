package ru.citeck.ecos.data.sql.pg.records

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import ru.citeck.ecos.context.lib.time.TimeZoneContext
import ru.citeck.ecos.data.sql.pg.records.commons.DbRecordsTestBase
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType
import ru.citeck.ecos.records3.record.dao.query.dto.query.SortBy
import java.time.Duration
import java.time.Instant
import java.time.ZoneOffset

//TODO Fix local timezone
@Disabled("Fix local timezone")
class DbRecordsDateTruncTimezoneTest : DbRecordsTestBase() {

    @Test
    fun dateTruncWithDifferentTimezonesTest() {
        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("dateReceived")
                    withType(AttributeType.DATETIME)
                }
            )
        )

        createRecord("dateReceived" to Instant.parse("2025-11-01T03:00:00Z"))
        createRecord("dateReceived" to Instant.parse("2025-11-05T12:30:00Z"))
        createRecord("dateReceived" to Instant.parse("2025-11-10T21:00:00Z"))
        createRecord("dateReceived" to Instant.parse("2025-11-11T07:30:00Z"))
        createRecord("dateReceived" to Instant.parse("2025-11-12T08:15:00Z"))
        createRecord("dateReceived" to Instant.parse("2025-11-15T18:45:00Z"))
        createRecord("dateReceived" to Instant.parse("2025-11-25T23:30:00Z"))
        createRecord("dateReceived" to Instant.parse("2025-11-30T23:30:00Z"))

        createRecord("dateReceived" to Instant.parse("2025-10-01T00:15:00Z"))
        createRecord("dateReceived" to Instant.parse("2025-10-10T10:00:00Z"))
        createRecord("dateReceived" to Instant.parse("2025-10-15T14:30:00Z"))
        createRecord("dateReceived" to Instant.parse("2025-10-20T19:00:00Z"))
        createRecord("dateReceived" to Instant.parse("2025-10-31T20:00:00Z"))
        createRecord("dateReceived" to Instant.parse("2025-10-31T20:30:00Z"))
        createRecord("dateReceived" to Instant.parse("2025-10-31T21:00:00Z"))
        createRecord("dateReceived" to Instant.parse("2025-10-31T21:05:00Z"))
        createRecord("dateReceived" to Instant.parse("2025-10-31T21:45:00Z"))

        createRecord("dateReceived" to Instant.parse("2025-09-01T06:00:00Z"))
        createRecord("dateReceived" to Instant.parse("2025-09-10T13:20:00Z"))
        createRecord("dateReceived" to Instant.parse("2025-09-15T16:40:00Z"))
        createRecord("dateReceived" to Instant.parse("2025-09-20T22:00:00Z"))
        createRecord("dateReceived" to Instant.parse("2025-09-30T20:00:00Z"))
        createRecord("dateReceived" to Instant.parse("2025-09-30T21:40:00Z"))

        TimeZoneContext.doWithUtcOffset(Duration.ZERO) {
            val utcResult = queryDateTruncGrouping()
            assertThat(utcResult["2025-09"]).isEqualTo(6)
            assertThat(utcResult["2025-10"]).isEqualTo(9)
            assertThat(utcResult["2025-11"]).isEqualTo(8)
        }

        TimeZoneContext.doWithUtcOffset(Duration.ofHours(3)) {
            val utc3Result = queryDateTruncGrouping()
            assertThat(utc3Result["2025-09"]).isEqualTo(5)
            assertThat(utc3Result["2025-10"]).isEqualTo(7)
            assertThat(utc3Result["2025-11"]).isEqualTo(10)
            assertThat(utc3Result["2025-12"]).isEqualTo(1)
        }

        TimeZoneContext.doWithUtcOffset(Duration.ofHours(5)) {
            val utc5Result = queryDateTruncGrouping()
            assertThat(utc5Result["2025-09"]).isEqualTo(4)
            assertThat(utc5Result["2025-10"]).isEqualTo(6)
            assertThat(utc5Result["2025-11"]).isEqualTo(12)
            assertThat(utc5Result["2025-12"]).isEqualTo(1)
        }

        TimeZoneContext.doWithUtcOffset(Duration.ofHours(-5)) {
            val utcMinus5Result = queryDateTruncGrouping()
            assertThat(utcMinus5Result["2025-09"]).isEqualTo(7)
            assertThat(utcMinus5Result["2025-10"]).isEqualTo(9)
            assertThat(utcMinus5Result["2025-11"]).isEqualTo(7)
        }

        TimeZoneContext.doWithUtcOffset(Duration.ofHours(-8)) {
            val utcMinus8Result = queryDateTruncGrouping()
            assertThat(utcMinus8Result["2025-08"]).isEqualTo(1)
            assertThat(utcMinus8Result["2025-09"]).isEqualTo(6)
            assertThat(utcMinus8Result["2025-10"]).isEqualTo(9)
            assertThat(utcMinus8Result["2025-11"]).isEqualTo(7)
        }
    }

    private fun queryDateTruncGrouping(): Map<String, Int> {
        val expression = "date_trunc('month', dateReceived)"

        val queryRes = this.records.query(
            baseQuery.copy {
                withGroupBy(listOf(expression))
                withSortBy(listOf(SortBy(expression, true)))
            },
            mapOf(
                "date_trunc" to expression,
                "count" to "count(*)"
            )
        ).getRecords()

        val result = mutableMapOf<String, Int>()
        for (record in queryRes) {
            val monthStart = record.getAtt("date_trunc").getAsInstantOrEpoch()
            val dateTime = monthStart.atZone(ZoneOffset.UTC)
            val monthKey = String.format("%04d-%02d", dateTime.year, dateTime.monthValue)
            val count = record.getAtt("count").asInt()
            result[monthKey] = count
        }

        return result
    }
}
