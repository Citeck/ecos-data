package ru.citeck.ecos.data.utils

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource
import org.mockito.MockedStatic
import org.mockito.Mockito
import ru.citeck.ecos.context.lib.time.TimeZoneContext
import ru.citeck.ecos.data.sql.records.utils.DbDateUtils
import java.time.Duration
import java.time.Instant
import java.time.temporal.ChronoUnit

class DbDateUtilsTest {

    private lateinit var instantMock: MockedStatic<Instant>

    @BeforeEach
    fun setup() {
        mockInstant(Instant.parse("2023-06-15T14:30:45Z"))
    }

    @AfterEach
    fun destroy() {
        instantMock.close()
    }

    private fun setupWithInstant(instant: Instant) {
        instantMock.close()
        mockInstant(instant)
    }

    private fun mockInstant(expectedInstant: Instant) {
        instantMock = Mockito.mockStatic(Instant::class.java, Mockito.CALLS_REAL_METHODS)
        instantMock.`when`<Instant> { Instant.now() }.thenReturn(expectedInstant)
    }

    @ParameterizedTest
    @CsvSource(
        "2h, HOURS",
        "2s, SECONDS",
        "2m, MINUTES",
        "2d, DAYS",
        "123, NANOS",
        "2h 3m, MINUTES",
        "2h 3s, SECONDS",
        "3s 2h, SECONDS"
    )
    fun minDimensionTest(value: String, expectedUnit: ChronoUnit) {
        assertThat(DbDateUtils.getMinDimension(value)).isEqualTo(expectedUnit)
        assertThat(DbDateUtils.getMinDimension(value.uppercase())).isEqualTo(expectedUnit)
    }

    @Test
    fun normalizeDateTimePredicateValueTodayWithoutTimeTest() {
        TimeZoneContext.doWithUtcOffset(Duration.ZERO) {
            val result = DbDateUtils.normalizeDateTimePredicateValue(
                DbDateUtils.TODAY,
                withTime = false
            )
            val expected = "2023-06-15T00:00:00Z"
            assertThat(result).isEqualTo(expected)
        }

        TimeZoneContext.doWithUtcOffset(Duration.ofHours(3)) {
            val result = DbDateUtils.normalizeDateTimePredicateValue(
                DbDateUtils.TODAY,
                withTime = false
            )
            val expected = "2023-06-15T00:00:00Z"
            assertThat(result).isEqualTo(expected)
        }

        TimeZoneContext.doWithUtcOffset(Duration.ofHours(-5)) {
            val result = DbDateUtils.normalizeDateTimePredicateValue(
                DbDateUtils.TODAY,
                withTime = false
            )
            val expected = "2023-06-15T00:00:00Z"
            assertThat(result).isEqualTo(expected)
        }

        TimeZoneContext.doWithUtcOffset(Duration.ofHours(20)) {
            val result = DbDateUtils.normalizeDateTimePredicateValue(
                DbDateUtils.TODAY,
                withTime = false
            )
            val expected = "2023-06-16T00:00:00Z"
            assertThat(result).isEqualTo(expected)
        }

        TimeZoneContext.doWithUtcOffset(Duration.ofHours(-20)) {
            val result = DbDateUtils.normalizeDateTimePredicateValue(
                DbDateUtils.TODAY,
                withTime = false
            )
            val expected = "2023-06-14T00:00:00Z"
            assertThat(result).isEqualTo(expected)
        }
    }

    @Test
    fun normalizeDateTimePredicateValueTodayWithTimeTest() {
        TimeZoneContext.doWithUtcOffset(Duration.ZERO) {
            val result = DbDateUtils.normalizeDateTimePredicateValue(
                DbDateUtils.TODAY,
                withTime = true
            )
            val expected = "2023-06-15T00:00:00Z/2023-06-16T00:00:00Z"
            assertThat(result).isEqualTo(expected)
        }

        TimeZoneContext.doWithUtcOffset(Duration.ZERO) {
            val result = DbDateUtils.normalizeDateTimePredicateValue(
                DbDateUtils.TODAY,
                withTime = true,
                DbDateUtils.RangePart.START
            )
            val expected = "2023-06-15T00:00:00Z"
            assertThat(result).isEqualTo(expected)
        }

        TimeZoneContext.doWithUtcOffset(Duration.ZERO) {
            val result = DbDateUtils.normalizeDateTimePredicateValue(
                DbDateUtils.TODAY,
                withTime = true,
                DbDateUtils.RangePart.END
            )
            val expected = "2023-06-16T00:00:00Z"
            assertThat(result).isEqualTo(expected)
        }

        TimeZoneContext.doWithUtcOffset(Duration.ofHours(8)) {
            val resultNone = DbDateUtils.normalizeDateTimePredicateValue(
                DbDateUtils.TODAY,
                withTime = true
            )
            val expectedNone = "2023-06-14T16:00:00Z/2023-06-15T16:00:00Z"
            assertThat(resultNone).isEqualTo(expectedNone)

            val resultStart = DbDateUtils.normalizeDateTimePredicateValue(
                DbDateUtils.TODAY,
                withTime = true,
                DbDateUtils.RangePart.START
            )
            val expectedStart = "2023-06-14T16:00:00Z"
            assertThat(resultStart).isEqualTo(expectedStart)

            val resultEnd = DbDateUtils.normalizeDateTimePredicateValue(
                DbDateUtils.TODAY,
                withTime = true,
                DbDateUtils.RangePart.END
            )
            val expectedEnd = "2023-06-15T16:00:00Z"
            assertThat(resultEnd).isEqualTo(expectedEnd)
        }

        TimeZoneContext.doWithUtcOffset(Duration.ofHours(-7)) {
            val result = DbDateUtils.normalizeDateTimePredicateValue(
                DbDateUtils.TODAY,
                withTime = true
            )
            val expected = "2023-06-15T07:00:00Z/2023-06-16T07:00:00Z"
            assertThat(result).isEqualTo(expected)
        }
    }

    @Test
    fun normalizeDateTimePredicateValueCurrentWeekWithoutTimeTest() {
        setupWithInstant(Instant.parse("2025-10-15T14:30:45Z"))
        TimeZoneContext.doWithUtcOffset(Duration.ZERO) {
            val result = DbDateUtils.normalizeDateTimePredicateValue(
                DbDateUtils.CURRENT_WEEK,
                withTime = false
            )
            val expected = "2025-10-13T00:00:00Z"
            assertThat(result).isEqualTo(expected)
        }

        setupWithInstant(Instant.parse("2025-10-23T10:15:30Z"))
        TimeZoneContext.doWithUtcOffset(Duration.ofHours(20)) {
            val result = DbDateUtils.normalizeDateTimePredicateValue(
                DbDateUtils.CURRENT_WEEK,
                withTime = false
            )
            val expected = "2025-10-20T00:00:00Z"
            assertThat(result).isEqualTo(expected)
        }

        setupWithInstant(Instant.parse("2025-10-30T18:45:00Z"))
        TimeZoneContext.doWithUtcOffset(Duration.ofHours(-20)) {
            val result = DbDateUtils.normalizeDateTimePredicateValue(
                DbDateUtils.CURRENT_WEEK,
                withTime = false
            )
            val expected = "2025-10-27T00:00:00Z"
            assertThat(result).isEqualTo(expected)
        }
    }

    @Test
    fun normalizeDateTimePredicateValueCurrentWeekWithTimeTest() {
        setupWithInstant(Instant.parse("2025-10-15T14:30:45Z"))
        TimeZoneContext.doWithUtcOffset(Duration.ZERO) {
            val result = DbDateUtils.normalizeDateTimePredicateValue(
                DbDateUtils.CURRENT_WEEK,
                withTime = true
            )
            val expected = "2025-10-13T00:00:00Z/2025-10-20T00:00:00Z"
            assertThat(result).isEqualTo(expected)
        }

        setupWithInstant(Instant.parse("2025-10-08T10:20:30Z"))
        TimeZoneContext.doWithUtcOffset(Duration.ZERO) {
            val resultStart = DbDateUtils.normalizeDateTimePredicateValue(
                DbDateUtils.CURRENT_WEEK,
                withTime = true,
                DbDateUtils.RangePart.START
            )
            val expectedStart = "2025-10-06T00:00:00Z"
            assertThat(resultStart).isEqualTo(expectedStart)
        }

        setupWithInstant(Instant.parse("2025-10-17T16:45:15Z"))
        TimeZoneContext.doWithUtcOffset(Duration.ZERO) {
            val resultEnd = DbDateUtils.normalizeDateTimePredicateValue(
                DbDateUtils.CURRENT_WEEK,
                withTime = true,
                DbDateUtils.RangePart.END
            )
            val expectedEnd = "2025-10-20T00:00:00Z"
            assertThat(resultEnd).isEqualTo(expectedEnd)
        }

        setupWithInstant(Instant.parse("2025-10-23T12:00:00Z"))
        TimeZoneContext.doWithUtcOffset(Duration.ofHours(8)) {
            val resultNone = DbDateUtils.normalizeDateTimePredicateValue(
                DbDateUtils.CURRENT_WEEK,
                withTime = true
            )
            val expectedNone = "2025-10-19T16:00:00Z/2025-10-26T16:00:00Z"
            assertThat(resultNone).isEqualTo(expectedNone)

            val resultStart = DbDateUtils.normalizeDateTimePredicateValue(
                DbDateUtils.CURRENT_WEEK,
                withTime = true,
                DbDateUtils.RangePart.START
            )
            val expectedStart = "2025-10-19T16:00:00Z"
            assertThat(resultStart).isEqualTo(expectedStart)

            val resultEnd = DbDateUtils.normalizeDateTimePredicateValue(
                DbDateUtils.CURRENT_WEEK,
                withTime = true,
                DbDateUtils.RangePart.END
            )
            val expectedEnd = "2025-10-26T16:00:00Z"
            assertThat(resultEnd).isEqualTo(expectedEnd)
        }

        setupWithInstant(Instant.parse("2025-10-19T20:00:00Z"))
        TimeZoneContext.doWithUtcOffset(Duration.ofHours(5)) {
            val result = DbDateUtils.normalizeDateTimePredicateValue(
                DbDateUtils.CURRENT_WEEK,
                withTime = true
            )
            val expected = "2025-10-19T19:00:00Z/2025-10-26T19:00:00Z"
            assertThat(result).isEqualTo(expected)
        }

        setupWithInstant(Instant.parse("2025-10-06T03:00:00Z"))
        TimeZoneContext.doWithUtcOffset(Duration.ofHours(-5)) {
            val result = DbDateUtils.normalizeDateTimePredicateValue(
                DbDateUtils.CURRENT_WEEK,
                withTime = true
            )
            val expected = "2025-09-29T05:00:00Z/2025-10-06T05:00:00Z"
            assertThat(result).isEqualTo(expected)
        }
    }

    @Test
    fun normalizeDateTimePredicateValueCurrentMonthWithoutTimeTest() {
        setupWithInstant(Instant.parse("2025-10-15T14:30:45Z"))
        TimeZoneContext.doWithUtcOffset(Duration.ZERO) {
            val result = DbDateUtils.normalizeDateTimePredicateValue(
                DbDateUtils.CURRENT_MONTH,
                withTime = false
            )
            val expected = "2025-10-01T00:00:00Z"
            assertThat(result).isEqualTo(expected)
        }

        setupWithInstant(Instant.parse("2025-10-08T10:20:30Z"))
        TimeZoneContext.doWithUtcOffset(Duration.ofHours(3)) {
            val result = DbDateUtils.normalizeDateTimePredicateValue(
                DbDateUtils.CURRENT_MONTH,
                withTime = false
            )
            val expected = "2025-10-01T00:00:00Z"
            assertThat(result).isEqualTo(expected)
        }

        setupWithInstant(Instant.parse("2025-10-23T18:45:00Z"))
        TimeZoneContext.doWithUtcOffset(Duration.ofHours(-5)) {
            val result = DbDateUtils.normalizeDateTimePredicateValue(
                DbDateUtils.CURRENT_MONTH,
                withTime = false
            )
            val expected = "2025-10-01T00:00:00Z"
            assertThat(result).isEqualTo(expected)
        }

        setupWithInstant(Instant.parse("2025-10-12T05:15:30Z"))
        TimeZoneContext.doWithUtcOffset(Duration.ofHours(20)) {
            val result = DbDateUtils.normalizeDateTimePredicateValue(
                DbDateUtils.CURRENT_MONTH,
                withTime = false
            )
            val expected = "2025-10-01T00:00:00Z"
            assertThat(result).isEqualTo(expected)
        }

        setupWithInstant(Instant.parse("2025-10-28T22:30:00Z"))
        TimeZoneContext.doWithUtcOffset(Duration.ofHours(-20)) {
            val result = DbDateUtils.normalizeDateTimePredicateValue(
                DbDateUtils.CURRENT_MONTH,
                withTime = false
            )
            val expected = "2025-10-01T00:00:00Z"
            assertThat(result).isEqualTo(expected)
        }
    }

    @Test
    fun normalizeDateTimePredicateValueCurrentMonthWithTimeTest() {
        setupWithInstant(Instant.parse("2025-10-15T14:30:45Z"))
        TimeZoneContext.doWithUtcOffset(Duration.ZERO) {
            val result = DbDateUtils.normalizeDateTimePredicateValue(
                DbDateUtils.CURRENT_MONTH,
                withTime = true
            )
            val expected = "2025-10-01T00:00:00Z/2025-11-01T00:00:00Z"
            assertThat(result).isEqualTo(expected)
        }

        setupWithInstant(Instant.parse("2025-10-08T10:20:30Z"))
        TimeZoneContext.doWithUtcOffset(Duration.ZERO) {
            val resultStart = DbDateUtils.normalizeDateTimePredicateValue(
                DbDateUtils.CURRENT_MONTH,
                withTime = true,
                DbDateUtils.RangePart.START
            )
            val expectedStart = "2025-10-01T00:00:00Z"
            assertThat(resultStart).isEqualTo(expectedStart)
        }

        setupWithInstant(Instant.parse("2025-10-23T18:45:00Z"))
        TimeZoneContext.doWithUtcOffset(Duration.ZERO) {
            val resultEnd = DbDateUtils.normalizeDateTimePredicateValue(
                DbDateUtils.CURRENT_MONTH,
                withTime = true,
                DbDateUtils.RangePart.END
            )
            val expectedEnd = "2025-11-01T00:00:00Z"
            assertThat(resultEnd).isEqualTo(expectedEnd)
        }

        setupWithInstant(Instant.parse("2025-10-17T12:00:00Z"))
        TimeZoneContext.doWithUtcOffset(Duration.ofHours(8)) {
            val resultNone = DbDateUtils.normalizeDateTimePredicateValue(
                DbDateUtils.CURRENT_MONTH,
                withTime = true
            )
            val expectedNone = "2025-09-30T16:00:00Z/2025-10-31T16:00:00Z"
            assertThat(resultNone).isEqualTo(expectedNone)

            val resultStart = DbDateUtils.normalizeDateTimePredicateValue(
                DbDateUtils.CURRENT_MONTH,
                withTime = true,
                DbDateUtils.RangePart.START
            )
            val expectedStart = "2025-09-30T16:00:00Z"
            assertThat(resultStart).isEqualTo(expectedStart)

            val resultEnd = DbDateUtils.normalizeDateTimePredicateValue(
                DbDateUtils.CURRENT_MONTH,
                withTime = true,
                DbDateUtils.RangePart.END
            )
            val expectedEnd = "2025-10-31T16:00:00Z"
            assertThat(resultEnd).isEqualTo(expectedEnd)
        }

        setupWithInstant(Instant.parse("2025-10-28T09:30:00Z"))
        TimeZoneContext.doWithUtcOffset(Duration.ofHours(-7)) {
            val result = DbDateUtils.normalizeDateTimePredicateValue(
                DbDateUtils.CURRENT_MONTH,
                withTime = true
            )
            val expected = "2025-10-01T07:00:00Z/2025-11-01T07:00:00Z"
            assertThat(result).isEqualTo(expected)
        }

        setupWithInstant(Instant.parse("2025-10-31T18:00:00Z"))
        TimeZoneContext.doWithUtcOffset(Duration.ofHours(7)) {
            val result = DbDateUtils.normalizeDateTimePredicateValue(
                DbDateUtils.CURRENT_MONTH,
                withTime = true
            )
            val expected = "2025-10-31T17:00:00Z/2025-11-30T17:00:00Z"
            assertThat(result).isEqualTo(expected)
        }

        setupWithInstant(Instant.parse("2025-10-01T03:00:00Z"))
        TimeZoneContext.doWithUtcOffset(Duration.ofHours(-5)) {
            val result = DbDateUtils.normalizeDateTimePredicateValue(
                DbDateUtils.CURRENT_MONTH,
                withTime = true
            )
            val expected = "2025-09-01T05:00:00Z/2025-10-01T05:00:00Z"
            assertThat(result).isEqualTo(expected)
        }
    }

    @Test
    fun normalizeDateTimePredicateValueCurrentYearWithoutTimeTest() {
        setupWithInstant(Instant.parse("2023-03-15T14:30:45Z"))
        TimeZoneContext.doWithUtcOffset(Duration.ZERO) {
            val result = DbDateUtils.normalizeDateTimePredicateValue(
                DbDateUtils.CURRENT_YEAR,
                withTime = false
            )
            val expected = "2023-01-01T00:00:00Z"
            assertThat(result).isEqualTo(expected)
        }

        setupWithInstant(Instant.parse("2024-07-08T10:20:30Z"))
        TimeZoneContext.doWithUtcOffset(Duration.ofHours(3)) {
            val result = DbDateUtils.normalizeDateTimePredicateValue(
                DbDateUtils.CURRENT_YEAR,
                withTime = false
            )
            val expected = "2024-01-01T00:00:00Z"
            assertThat(result).isEqualTo(expected)
        }

        setupWithInstant(Instant.parse("2026-11-23T18:45:00Z"))
        TimeZoneContext.doWithUtcOffset(Duration.ofHours(-5)) {
            val result = DbDateUtils.normalizeDateTimePredicateValue(
                DbDateUtils.CURRENT_YEAR,
                withTime = false
            )
            val expected = "2026-01-01T00:00:00Z"
            assertThat(result).isEqualTo(expected)
        }

        setupWithInstant(Instant.parse("2022-02-12T05:15:30Z"))
        TimeZoneContext.doWithUtcOffset(Duration.ofHours(20)) {
            val result = DbDateUtils.normalizeDateTimePredicateValue(
                DbDateUtils.CURRENT_YEAR,
                withTime = false
            )
            val expected = "2022-01-01T00:00:00Z"
            assertThat(result).isEqualTo(expected)
        }

        setupWithInstant(Instant.parse("2027-09-28T22:30:00Z"))
        TimeZoneContext.doWithUtcOffset(Duration.ofHours(-20)) {
            val result = DbDateUtils.normalizeDateTimePredicateValue(
                DbDateUtils.CURRENT_YEAR,
                withTime = false
            )
            val expected = "2027-01-01T00:00:00Z"
            assertThat(result).isEqualTo(expected)
        }
    }

    @Test
    fun normalizeDateTimePredicateValueCurrentYearWithTimeTest() {
        setupWithInstant(Instant.parse("2023-05-15T14:30:45Z"))
        TimeZoneContext.doWithUtcOffset(Duration.ZERO) {
            val result = DbDateUtils.normalizeDateTimePredicateValue(
                DbDateUtils.CURRENT_YEAR,
                withTime = true
            )
            val expected = "2023-01-01T00:00:00Z/2024-01-01T00:00:00Z"
            assertThat(result).isEqualTo(expected)
        }

        setupWithInstant(Instant.parse("2024-08-08T10:20:30Z"))
        TimeZoneContext.doWithUtcOffset(Duration.ZERO) {
            val resultStart = DbDateUtils.normalizeDateTimePredicateValue(
                DbDateUtils.CURRENT_YEAR,
                withTime = true,
                DbDateUtils.RangePart.START
            )
            val expectedStart = "2024-01-01T00:00:00Z"
            assertThat(resultStart).isEqualTo(expectedStart)
        }

        setupWithInstant(Instant.parse("2026-04-23T18:45:00Z"))
        TimeZoneContext.doWithUtcOffset(Duration.ZERO) {
            val resultEnd = DbDateUtils.normalizeDateTimePredicateValue(
                DbDateUtils.CURRENT_YEAR,
                withTime = true,
                DbDateUtils.RangePart.END
            )
            val expectedEnd = "2027-01-01T00:00:00Z"
            assertThat(resultEnd).isEqualTo(expectedEnd)
        }

        setupWithInstant(Instant.parse("2022-06-17T12:00:00Z"))
        TimeZoneContext.doWithUtcOffset(Duration.ofHours(8)) {
            val resultNone = DbDateUtils.normalizeDateTimePredicateValue(
                DbDateUtils.CURRENT_YEAR,
                withTime = true
            )
            val expectedNone = "2021-12-31T16:00:00Z/2022-12-31T16:00:00Z"
            assertThat(resultNone).isEqualTo(expectedNone)

            val resultStart = DbDateUtils.normalizeDateTimePredicateValue(
                DbDateUtils.CURRENT_YEAR,
                withTime = true,
                DbDateUtils.RangePart.START
            )
            val expectedStart = "2021-12-31T16:00:00Z"
            assertThat(resultStart).isEqualTo(expectedStart)

            val resultEnd = DbDateUtils.normalizeDateTimePredicateValue(
                DbDateUtils.CURRENT_YEAR,
                withTime = true,
                DbDateUtils.RangePart.END
            )
            val expectedEnd = "2022-12-31T16:00:00Z"
            assertThat(resultEnd).isEqualTo(expectedEnd)
        }

        setupWithInstant(Instant.parse("2027-03-28T09:30:00Z"))
        TimeZoneContext.doWithUtcOffset(Duration.ofHours(-7)) {
            val result = DbDateUtils.normalizeDateTimePredicateValue(
                DbDateUtils.CURRENT_YEAR,
                withTime = true
            )
            val expected = "2027-01-01T07:00:00Z/2028-01-01T07:00:00Z"
            assertThat(result).isEqualTo(expected)
        }

        setupWithInstant(Instant.parse("2021-12-31T18:00:00Z"))
        TimeZoneContext.doWithUtcOffset(Duration.ofHours(7)) {
            val result = DbDateUtils.normalizeDateTimePredicateValue(
                DbDateUtils.CURRENT_YEAR,
                withTime = true
            )
            val expected = "2021-12-31T17:00:00Z/2022-12-31T17:00:00Z"
            assertThat(result).isEqualTo(expected)
        }

        setupWithInstant(Instant.parse("2020-01-01T03:00:00Z"))
        TimeZoneContext.doWithUtcOffset(Duration.ofHours(-5)) {
            val result = DbDateUtils.normalizeDateTimePredicateValue(
                DbDateUtils.CURRENT_YEAR,
                withTime = true
            )
            val expected = "2019-01-01T05:00:00Z/2020-01-01T05:00:00Z"
            assertThat(result).isEqualTo(expected)
        }
    }

    @Test
    fun normalizeDateTimePredicateValueCurrentQuarterWithoutTimeTest() {
        setupWithInstant(Instant.parse("2025-02-15T14:30:45Z"))
        TimeZoneContext.doWithUtcOffset(Duration.ZERO) {
            val result = DbDateUtils.normalizeDateTimePredicateValue(
                DbDateUtils.CURRENT_QUARTER,
                withTime = false
            )
            val expected = "2025-01-01T00:00:00Z"
            assertThat(result).isEqualTo(expected)
        }

        setupWithInstant(Instant.parse("2025-05-08T10:20:30Z"))
        TimeZoneContext.doWithUtcOffset(Duration.ofHours(3)) {
            val result = DbDateUtils.normalizeDateTimePredicateValue(
                DbDateUtils.CURRENT_QUARTER,
                withTime = false
            )
            val expected = "2025-04-01T00:00:00Z"
            assertThat(result).isEqualTo(expected)
        }

        setupWithInstant(Instant.parse("2025-08-23T18:45:00Z"))
        TimeZoneContext.doWithUtcOffset(Duration.ofHours(-5)) {
            val result = DbDateUtils.normalizeDateTimePredicateValue(
                DbDateUtils.CURRENT_QUARTER,
                withTime = false
            )
            val expected = "2025-07-01T00:00:00Z"
            assertThat(result).isEqualTo(expected)
        }

        setupWithInstant(Instant.parse("2025-11-12T05:15:30Z"))
        TimeZoneContext.doWithUtcOffset(Duration.ofHours(20)) {
            val result = DbDateUtils.normalizeDateTimePredicateValue(
                DbDateUtils.CURRENT_QUARTER,
                withTime = false
            )
            val expected = "2025-10-01T00:00:00Z"
            assertThat(result).isEqualTo(expected)
        }

        setupWithInstant(Instant.parse("2025-12-28T22:30:00Z"))
        TimeZoneContext.doWithUtcOffset(Duration.ofHours(-20)) {
            val result = DbDateUtils.normalizeDateTimePredicateValue(
                DbDateUtils.CURRENT_QUARTER,
                withTime = false
            )
            val expected = "2025-10-01T00:00:00Z"
            assertThat(result).isEqualTo(expected)
        }
    }

    @Test
    fun normalizeDateTimePredicateValueCurrentQuarterWithTimeTest() {
        setupWithInstant(Instant.parse("2025-03-15T14:30:45Z"))
        TimeZoneContext.doWithUtcOffset(Duration.ZERO) {
            val result = DbDateUtils.normalizeDateTimePredicateValue(
                DbDateUtils.CURRENT_QUARTER,
                withTime = true
            )
            val expected = "2025-01-01T00:00:00Z/2025-04-01T00:00:00Z"
            assertThat(result).isEqualTo(expected)
        }

        setupWithInstant(Instant.parse("2025-04-08T10:20:30Z"))
        TimeZoneContext.doWithUtcOffset(Duration.ZERO) {
            val resultStart = DbDateUtils.normalizeDateTimePredicateValue(
                DbDateUtils.CURRENT_QUARTER,
                withTime = true,
                DbDateUtils.RangePart.START
            )
            val expectedStart = "2025-04-01T00:00:00Z"
            assertThat(resultStart).isEqualTo(expectedStart)
        }

        setupWithInstant(Instant.parse("2025-06-23T18:45:00Z"))
        TimeZoneContext.doWithUtcOffset(Duration.ZERO) {
            val resultEnd = DbDateUtils.normalizeDateTimePredicateValue(
                DbDateUtils.CURRENT_QUARTER,
                withTime = true,
                DbDateUtils.RangePart.END
            )
            val expectedEnd = "2025-07-01T00:00:00Z"
            assertThat(resultEnd).isEqualTo(expectedEnd)
        }

        setupWithInstant(Instant.parse("2025-07-17T12:00:00Z"))
        TimeZoneContext.doWithUtcOffset(Duration.ofHours(8)) {
            val resultNone = DbDateUtils.normalizeDateTimePredicateValue(
                DbDateUtils.CURRENT_QUARTER,
                withTime = true
            )
            val expectedNone = "2025-06-30T16:00:00Z/2025-09-30T16:00:00Z"
            assertThat(resultNone).isEqualTo(expectedNone)

            val resultStart = DbDateUtils.normalizeDateTimePredicateValue(
                DbDateUtils.CURRENT_QUARTER,
                withTime = true,
                DbDateUtils.RangePart.START
            )
            val expectedStart = "2025-06-30T16:00:00Z"
            assertThat(resultStart).isEqualTo(expectedStart)

            val resultEnd = DbDateUtils.normalizeDateTimePredicateValue(
                DbDateUtils.CURRENT_QUARTER,
                withTime = true,
                DbDateUtils.RangePart.END
            )
            val expectedEnd = "2025-09-30T16:00:00Z"
            assertThat(resultEnd).isEqualTo(expectedEnd)
        }

        setupWithInstant(Instant.parse("2025-11-28T09:30:00Z"))
        TimeZoneContext.doWithUtcOffset(Duration.ofHours(-7)) {
            val result = DbDateUtils.normalizeDateTimePredicateValue(
                DbDateUtils.CURRENT_QUARTER,
                withTime = true
            )
            val expected = "2025-10-01T07:00:00Z/2026-01-01T07:00:00Z"
            assertThat(result).isEqualTo(expected)
        }

        setupWithInstant(Instant.parse("2025-09-30T20:00:00Z"))
        TimeZoneContext.doWithUtcOffset(Duration.ofHours(5)) {
            val result = DbDateUtils.normalizeDateTimePredicateValue(
                DbDateUtils.CURRENT_QUARTER,
                withTime = true
            )
            val expected = "2025-09-30T19:00:00Z/2025-12-31T19:00:00Z"
            assertThat(result).isEqualTo(expected)
        }

        setupWithInstant(Instant.parse("2025-10-01T03:00:00Z"))
        TimeZoneContext.doWithUtcOffset(Duration.ofHours(-5)) {
            val result = DbDateUtils.normalizeDateTimePredicateValue(
                DbDateUtils.CURRENT_QUARTER,
                withTime = true
            )
            val expected = "2025-07-01T05:00:00Z/2025-10-01T05:00:00Z"
            assertThat(result).isEqualTo(expected)
        }
    }

    @Test
    fun normalizeDateTimePredicateValueQuarterPeriodWithoutTimeTest() {
        setupWithInstant(Instant.parse("2025-06-15T14:30:45Z"))
        TimeZoneContext.doWithUtcOffset(Duration.ZERO) {
            val result = DbDateUtils.normalizeDateTimePredicateValue("-P1Q", withTime = false)
            val expected = "2025-01-01T00:00:00Z"
            assertThat(result).isEqualTo(expected)
        }

        setupWithInstant(Instant.parse("2025-09-15T18:30:00Z"))
        TimeZoneContext.doWithUtcOffset(Duration.ofHours(5)) {
            val result = DbDateUtils.normalizeDateTimePredicateValue("-P2Q", withTime = false)
            val expected = "2025-01-01T00:00:00Z"
            assertThat(result).isEqualTo(expected)
        }

        setupWithInstant(Instant.parse("2025-11-10T14:00:00Z"))
        TimeZoneContext.doWithUtcOffset(Duration.ofHours(-8)) {
            val result = DbDateUtils.normalizeDateTimePredicateValue("-P3Q", withTime = false)
            val expected = "2025-01-01T00:00:00Z"
            assertThat(result).isEqualTo(expected)
        }

        setupWithInstant(Instant.parse("2025-10-15T14:30:45Z"))
        TimeZoneContext.doWithUtcOffset(Duration.ZERO) {
            val result = DbDateUtils.normalizeDateTimePredicateValue("-P6Q", withTime = false)
            val expected = "2024-04-01T00:00:00Z"
            assertThat(result).isEqualTo(expected)
        }

        setupWithInstant(Instant.parse("2025-12-05T16:45:00Z"))
        TimeZoneContext.doWithUtcOffset(Duration.ofHours(10)) {
            val result = DbDateUtils.normalizeDateTimePredicateValue("-P8Q", withTime = false)
            val expected = "2023-10-01T00:00:00Z"
            assertThat(result).isEqualTo(expected)
        }
    }

    @Test
    fun normalizeDateTimePredicateValueQuarterPeriodWithTimeTest() {
        setupWithInstant(Instant.parse("2025-03-20T10:15:30Z"))
        TimeZoneContext.doWithUtcOffset(Duration.ZERO) {
            val result = DbDateUtils.normalizeDateTimePredicateValue("-P1Q", withTime = true)
            val expected = "2024-10-01T00:00:00Z/2025-01-01T00:00:00Z"
            assertThat(result).isEqualTo(expected)
        }

        setupWithInstant(Instant.parse("2025-05-15T14:30:45Z"))
        TimeZoneContext.doWithUtcOffset(Duration.ZERO) {
            val resultStart = DbDateUtils.normalizeDateTimePredicateValue(
                "-P1Q",
                withTime = true,
                DbDateUtils.RangePart.START
            )
            val expectedStart = "2025-01-01T00:00:00Z"
            assertThat(resultStart).isEqualTo(expectedStart)

            val resultEnd = DbDateUtils.normalizeDateTimePredicateValue(
                "-P1Q",
                withTime = true,
                DbDateUtils.RangePart.END
            )
            val expectedEnd = "2025-04-01T00:00:00Z"
            assertThat(resultEnd).isEqualTo(expectedEnd)
        }

        setupWithInstant(Instant.parse("2025-12-28T22:45:15Z"))
        TimeZoneContext.doWithUtcOffset(Duration.ZERO) {
            val result = DbDateUtils.normalizeDateTimePredicateValue("-P2Q", withTime = true)
            val expected = "2025-04-01T00:00:00Z/2025-07-01T00:00:00Z"
            assertThat(result).isEqualTo(expected)
        }

        setupWithInstant(Instant.parse("2025-08-15T14:30:45Z"))
        TimeZoneContext.doWithUtcOffset(Duration.ofHours(8)) {
            val result = DbDateUtils.normalizeDateTimePredicateValue("-P1Q", withTime = true)
            val expected = "2025-03-31T16:00:00Z/2025-06-30T16:00:00Z"
            assertThat(result).isEqualTo(expected)
        }

        setupWithInstant(Instant.parse("2025-11-20T18:00:00Z"))
        TimeZoneContext.doWithUtcOffset(Duration.ofHours(-7)) {
            val result = DbDateUtils.normalizeDateTimePredicateValue("-P1Q", withTime = true)
            val expected = "2025-07-01T07:00:00Z/2025-10-01T07:00:00Z"
            assertThat(result).isEqualTo(expected)
        }

        setupWithInstant(Instant.parse("2025-02-20T10:30:00Z"))
        TimeZoneContext.doWithUtcOffset(Duration.ZERO) {
            val result = DbDateUtils.normalizeDateTimePredicateValue("-P6Q", withTime = true)
            val expected = "2023-07-01T00:00:00Z/2023-10-01T00:00:00Z"
            assertThat(result).isEqualTo(expected)
        }

        setupWithInstant(Instant.parse("2025-04-10T08:20:00Z"))
        TimeZoneContext.doWithUtcOffset(Duration.ofHours(3)) {
            val result = DbDateUtils.normalizeDateTimePredicateValue("-P4Q", withTime = true)
            val expected = "2024-03-31T21:00:00Z/2024-06-30T21:00:00Z"
            assertThat(result).isEqualTo(expected)
        }
    }

    @Test
    fun normalizeDateTimePredicateValueQuarterPeriodInvalidFormatTest() {
        setupWithInstant(Instant.parse("2025-07-15T14:30:45Z"))
        TimeZoneContext.doWithUtcOffset(Duration.ZERO) {
            val forwardException = org.junit.jupiter.api.assertThrows<IllegalArgumentException> {
                DbDateUtils.normalizeDateTimePredicateValue("P1Q", withTime = false)
            }
            assertThat(forwardException.message).contains("Forward quarter periods are not supported. Use CURRENT_QUARTER for current quarter. Got: P1Q")

            val invalidFormatException = org.junit.jupiter.api.assertThrows<IllegalArgumentException> {
                DbDateUtils.normalizeDateTimePredicateValue("-PXQ", withTime = false)
            }
            assertThat(invalidFormatException.message).contains("Invalid quarter period format: -PXQ. Expected format: -P{number}Q")
        }
    }

    @Test
    fun normalizeDateTimePredicateValueQuarterPeriodRangeTest() {
        setupWithInstant(Instant.parse("2025-08-15T14:30:45Z"))
        TimeZoneContext.doWithUtcOffset(Duration.ZERO) {
            val result = DbDateUtils.normalizeDateTimePredicateValue(
                "-P2Q/-P1Q",
                withTime = true
            )
            val expected = "2025-01-01T00:00:00Z/2025-07-01T00:00:00Z"
            assertThat(result).isEqualTo(expected)
        }

        setupWithInstant(Instant.parse("2025-11-20T10:30:00Z"))
        TimeZoneContext.doWithUtcOffset(Duration.ZERO) {
            val resultWithoutTime = DbDateUtils.normalizeDateTimePredicateValue(
                "-P2Q/-P1Q",
                withTime = false
            )
            val expected = "2025-04-01T00:00:00Z/2025-07-01T00:00:00Z"
            assertThat(resultWithoutTime).isEqualTo(expected)
        }

        setupWithInstant(Instant.parse("2025-03-10T16:45:00Z"))
        TimeZoneContext.doWithUtcOffset(Duration.ZERO) {
            val result = DbDateUtils.normalizeDateTimePredicateValue(
                "-P4Q/-P2Q",
                withTime = true
            )
            val expected = "2024-01-01T00:00:00Z/2024-10-01T00:00:00Z"
            assertThat(result).isEqualTo(expected)
        }
    }
}
