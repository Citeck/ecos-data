package ru.citeck.ecos.data.sql.records.utils

import java.time.Duration
import java.time.Instant
import java.time.temporal.ChronoUnit

object DbDateUtils {

    const val TODAY = "\$TODAY"
    const val NOW = "\$NOW"

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
        return when (value) {
            NOW -> Instant.now().toString()
            TODAY -> {
                val todayTime = Instant.now().truncatedTo(ChronoUnit.DAYS)
                if (withTime) {
                    when (rangePart) {
                        RangePart.START -> todayTime.toString()
                        RangePart.END -> todayTime.plus(1, ChronoUnit.DAYS).toString()
                        else -> todayTime.toString() + "/" + todayTime.plus(1, ChronoUnit.DAYS)
                    }
                } else {
                    todayTime.toString()
                }
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
                    val duration = Duration.parse(value)
                    Instant.now()
                        .truncatedTo(getMinDimension(value))
                        .plus(duration)
                        .toString()
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

    fun getMinDimension(value: String): ChronoUnit = when {
        value.last() == 'S' -> ChronoUnit.SECONDS
        value.last() == 'M' -> ChronoUnit.MINUTES
        value.last() == 'H' -> ChronoUnit.HOURS
        value.last() == 'D' -> ChronoUnit.DAYS
        else -> ChronoUnit.NANOS
    }
}
