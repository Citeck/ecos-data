package ru.citeck.ecos.data.sql.records.utils

import java.time.Duration
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.concurrent.TimeUnit

object DbDateUtils {

    const val TODAY = "\$TODAY"
    const val NOW = "\$NOW"

    enum class RangePart {
        START, END, NONE
    }

    fun normalizeDateTimePredicateValue(
        value: String,
        withTime: Boolean,
        rangePart: RangePart = RangePart.NONE
    ): String {
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
                if (value[0] == 'P' || value[1] == 'P') {
                    val duration = Duration.parse(value)
                    Instant.now()
                        .truncatedTo(getMinDimension(duration))
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

    fun getMinDimension(value: Duration): ChronoUnit = when {
        value.nano != 0 -> ChronoUnit.NANOS
        value.seconds.mod(TimeUnit.DAYS.toSeconds(1)) == 0L -> ChronoUnit.DAYS
        value.seconds.mod(TimeUnit.HOURS.toSeconds(1)) == 0L -> ChronoUnit.HOURS
        value.seconds.mod(TimeUnit.MINUTES.toSeconds(1)) == 0L -> ChronoUnit.MINUTES
        value.seconds != 0L -> ChronoUnit.SECONDS
        else -> ChronoUnit.NANOS
    }
}
