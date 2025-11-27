package ru.citeck.ecos.data.sql.records.utils

import ru.citeck.ecos.context.lib.time.TimeZoneContext
import java.time.*
import java.time.temporal.ChronoUnit
import java.time.temporal.IsoFields
import java.time.temporal.TemporalAdjusters

object DbDateUtils {

    const val TODAY = "\$TODAY"
    const val NOW = "\$NOW"
    const val CURRENT_WEEK = "\$CURRENT_WEEK"
    const val CURRENT_MONTH = "\$CURRENT_MONTH"
    const val CURRENT_YEAR = "\$CURRENT_YEAR"
    const val CURRENT_QUARTER = "\$CURRENT_QUARTER"

    private val DIMENSIONS = listOf(
        'S' to ChronoUnit.SECONDS,
        'M' to ChronoUnit.MINUTES,
        'H' to ChronoUnit.HOURS,
        'D' to ChronoUnit.DAYS
    )

    private val QUARTER_PERIOD_REGEX = Regex("(-?)P(\\d+)Q")

    enum class RangePart {
        START,
        END,
        NONE
    }

    fun normalizeDateTimePredicateValue(
        value: String,
        withTime: Boolean,
        rangePart: RangePart = RangePart.NONE
    ): String {
        if (value.isBlank()) {
            return value
        }
        val offset = TimeZoneContext.getUtcOffset()
        return when (value) {
            NOW -> Instant.now().toString()
            TODAY -> {
                val todayTime = Instant.now().plus(offset).truncatedTo(ChronoUnit.DAYS)
                if (withTime) {
                    val start = todayTime.minus(offset)
                    val end = start.plus(1, ChronoUnit.DAYS)
                    formatRange(start, end, rangePart)
                } else {
                    todayTime.toString()
                }
            }

            CURRENT_WEEK -> {
                val nowWithOffset = Instant.now().plus(offset).atZone(ZoneOffset.UTC)
                val mondayOfWeek = nowWithOffset.with(TemporalAdjusters.previousOrSame(DayOfWeek.MONDAY))
                    .truncatedTo(ChronoUnit.DAYS)
                    .toInstant()

                if (withTime) {
                    val weekStart = mondayOfWeek.minus(offset)
                    val weekEnd = weekStart.plus(7, ChronoUnit.DAYS)
                    formatRange(weekStart, weekEnd, rangePart)
                } else {
                    mondayOfWeek.toString()
                }
            }

            CURRENT_MONTH -> {
                val nowWithOffset = Instant.now().plus(offset).atZone(ZoneOffset.UTC)
                val firstDayOfMonth = nowWithOffset.with(TemporalAdjusters.firstDayOfMonth())
                    .truncatedTo(ChronoUnit.DAYS)
                    .toInstant()

                if (withTime) {
                    val monthStart = firstDayOfMonth.minus(offset)
                    val firstDayOfNextMonth = nowWithOffset.with(TemporalAdjusters.firstDayOfNextMonth())
                        .truncatedTo(ChronoUnit.DAYS)
                        .toInstant()
                    val monthEnd = firstDayOfNextMonth.minus(offset)
                    formatRange(monthStart, monthEnd, rangePart)
                } else {
                    firstDayOfMonth.toString()
                }
            }

            CURRENT_YEAR -> {
                val nowWithOffset = Instant.now().plus(offset).atZone(ZoneOffset.UTC)
                val firstDayOfYear = nowWithOffset.with(TemporalAdjusters.firstDayOfYear())
                    .truncatedTo(ChronoUnit.DAYS)
                    .toInstant()

                if (withTime) {
                    val yearStart = firstDayOfYear.minus(offset)
                    val firstDayOfNextYear = nowWithOffset.with(TemporalAdjusters.firstDayOfNextYear())
                        .truncatedTo(ChronoUnit.DAYS)
                        .toInstant()
                    val yearEnd = firstDayOfNextYear.minus(offset)
                    formatRange(yearStart, yearEnd, rangePart)
                } else {
                    firstDayOfYear.toString()
                }
            }

            CURRENT_QUARTER -> {
                calculateQuarterPeriod(0, offset, withTime, rangePart)
            }

            else -> {
                if (rangePart == RangePart.NONE) {
                    val rangeDelimIdx = value.indexOf('/')
                    if (rangeDelimIdx > 0) {
                        val rangeFrom = normalizeDateTimePredicateValue(
                            value.substring(0, rangeDelimIdx),
                            withTime,
                            RangePart.START
                        )
                        val rangeTo = normalizeDateTimePredicateValue(
                            value.substring(rangeDelimIdx + 1),
                            withTime,
                            RangePart.END
                        )
                        return "$rangeFrom/$rangeTo"
                    }
                }
                if (value[0] == 'P' || value.length > 1 && value[1] == 'P') {
                    if (value.contains('Q')) {
                        parseQuarterPeriod(value, offset, withTime, rangePart)
                    } else {
                        parsePeriodOrDuration(value, offset)
                    }
                } else if (withTime && !value.contains("T")) {
                    val date = Instant.parse(value + "T00:00:00Z")
                    when (rangePart) {
                        RangePart.END -> date.plus(1, ChronoUnit.DAYS).toString()
                        else -> date.toString()
                    }
                } else {
                    value
                }
            }
        }
    }

    private fun parsePeriodOrDuration(value: String, offset: Duration): String {
        val isPeriod = isPeriodFormat(value)

        return if (isPeriod) {
            val period = Period.parse(value)
            val nowWithOffset = Instant.now().plus(offset).atZone(ZoneOffset.UTC)
            nowWithOffset
                .truncatedTo(ChronoUnit.DAYS)
                .plus(period)
                .toInstant()
                .toString()
        } else {
            val duration = Duration.parse(value)
            Instant.now()
                .truncatedTo(getMinDimension(value))
                .plus(duration)
                .toString()
        }
    }

    private fun isPeriodFormat(value: String): Boolean {
        if (value.contains('T')) {
            return false
        }
        return value.contains('Y') || value.contains('M') || value.contains('W')
    }

    fun getMinDimension(value: String): ChronoUnit {
        return DIMENSIONS.firstOrNull {
            value.contains(it.first, ignoreCase = true)
        }?.second ?: ChronoUnit.NANOS
    }

    private fun formatRange(start: Instant, end: Instant, rangePart: RangePart): String {
        return when (rangePart) {
            RangePart.START -> start.toString()
            RangePart.END -> end.toString()
            else -> "$start/$end"
        }
    }

    private fun parseQuarterPeriod(
        value: String,
        offset: Duration,
        withTime: Boolean,
        rangePart: RangePart
    ): String {
        val matchResult = QUARTER_PERIOD_REGEX.matchEntire(value)
            ?: throw IllegalArgumentException("Invalid quarter period format: $value. Expected format: -P{number}Q")

        val isNegative = matchResult.groupValues[1] == "-"
        val quarters = matchResult.groupValues[2].toInt()

        if (!isNegative) {
            throw IllegalArgumentException(
                "Forward quarter periods are not supported. Use CURRENT_QUARTER for current quarter. Got: $value"
            )
        }

        return calculateQuarterPeriod(quarters, offset, withTime, rangePart)
    }

    private fun calculateQuarterPeriod(
        quartersBack: Int,
        offset: Duration,
        withTime: Boolean,
        rangePart: RangePart
    ): String {
        val nowWithOffset = Instant.now().plus(offset).atZone(ZoneOffset.UTC)
        val targetQuarterDate = nowWithOffset.minus(quartersBack.toLong(), IsoFields.QUARTER_YEARS)

        val firstDayOfQuarter = targetQuarterDate
            .with(IsoFields.DAY_OF_QUARTER, 1)
            .truncatedTo(ChronoUnit.DAYS)

        return if (withTime) {
            val quarterStart = firstDayOfQuarter.toInstant().minus(offset)
            val quarterEnd = firstDayOfQuarter.plus(1, IsoFields.QUARTER_YEARS).toInstant().minus(offset)
            formatRange(quarterStart, quarterEnd, rangePart)
        } else {
            firstDayOfQuarter.toInstant().toString()
        }
    }
}
