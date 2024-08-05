package ru.citeck.ecos.data.utils

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource
import ru.citeck.ecos.data.sql.records.utils.DbDateUtils
import java.time.temporal.ChronoUnit

class DbDateUtilsTest {

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
}
