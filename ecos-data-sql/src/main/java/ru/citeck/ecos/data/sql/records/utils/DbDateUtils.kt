package ru.citeck.ecos.data.sql.records.utils

import ru.citeck.ecos.context.lib.time.TimeZoneContext
import java.time.Duration
import java.time.Instant
import java.time.ZoneOffset
import java.time.temporal.ChronoUnit

object DbDateUtils {

    const val TODAY = "\$TODAY"
    const val NOW = "\$NOW"

    private val DIMENSIONS = listOf(
        'S' to ChronoUnit.SECONDS,
        'M' to ChronoUnit.MINUTES,
        'H' to ChronoUnit.HOURS,
        'D' to ChronoUnit.DAYS
    )

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
                val offset = TimeZoneContext.getUtcOffset()
                val zone = ZoneOffset.ofTotalSeconds(offset.seconds.toInt())
                val todayTime = Instant.now().atZone(zone).truncatedTo(ChronoUnit.DAYS).toInstant()
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

    fun getMinDimension(value: String): ChronoUnit {
        return DIMENSIONS.firstOrNull {
            value.contains(it.first, ignoreCase = true)
        }?.second ?: ChronoUnit.NANOS
    }
}
