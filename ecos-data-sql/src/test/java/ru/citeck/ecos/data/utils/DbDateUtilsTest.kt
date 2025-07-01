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
            val expected = "2023-06-14T21:00:00Z"
            assertThat(result).isEqualTo(expected)
        }

        TimeZoneContext.doWithUtcOffset(Duration.ofHours(-5)) {
            val result = DbDateUtils.normalizeDateTimePredicateValue(
                DbDateUtils.TODAY,
                withTime = false
            )
            val expected = "2023-06-15T05:00:00Z"
            assertThat(result).isEqualTo(expected)
        }

        TimeZoneContext.doWithUtcOffset(Duration.ofMinutes(330)) {
            val result = DbDateUtils.normalizeDateTimePredicateValue(
                DbDateUtils.TODAY,
                withTime = false
            )
            val expected = "2023-06-14T18:30:00Z"
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
}
