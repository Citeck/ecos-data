package ru.citeck.ecos.data.sql.props

import org.assertj.core.api.Assertions.assertThatCode
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import java.time.Duration

/**
 * [DbEcosDataProps.BatchProps] used to be a set of hard-coded constants in [DbBatchTaskEngine], so
 * a nonsensical value was simply not reachable. Now that every one of them is configuration, a bad
 * value has to be rejected here instead of surfacing as a wedged table: `batchSize <= 0` in
 * particular builds an empty id window that the engine re-issues forever, holding the table's
 * distributed lock the whole time and logging nothing.
 */
class DbEcosDataPropsBatchPropsTest {

    @Test
    fun defaultsConstructWithoutComplaintTest() {
        assertThatCode { DbEcosDataProps.BatchProps() }.doesNotThrowAnyException()
    }

    @Test
    fun zeroMaxAttemptsIsLegalTest() {
        // unlike batchSize, 0 is the documented "never give up" default and must stay legal
        assertThatCode { DbEcosDataProps.BatchProps(maxAttempts = 0) }.doesNotThrowAnyException()
    }

    @Test
    fun aZeroBatchSizeIsRejectedTest() {
        assertThatThrownBy { DbEcosDataProps.BatchProps(batchSize = 0) }
            .describedAs(
                "a batch size of 0 makes runOneBatch build the window (cursor, cursor] - always " +
                    "empty, so the engine would spin on it forever instead of failing fast here"
            )
            .isInstanceOf(IllegalArgumentException::class.java)
    }

    @Test
    fun aNegativeBatchSizeIsRejectedTest() {
        assertThatThrownBy { DbEcosDataProps.BatchProps(batchSize = -1) }
            .isInstanceOf(IllegalArgumentException::class.java)
    }

    @Test
    fun aNegativeBatchPauseIsRejectedTest() {
        assertThatThrownBy { DbEcosDataProps.BatchProps(batchPause = Duration.ofMillis(-1)) }
            .isInstanceOf(IllegalArgumentException::class.java)
    }

    @Test
    fun aNegativeMaxAttemptsIsRejectedTest() {
        assertThatThrownBy { DbEcosDataProps.BatchProps(maxAttempts = -1) }
            .isInstanceOf(IllegalArgumentException::class.java)
    }

    @Test
    fun aZeroOrNegativeRetryInitialDelayIsRejectedTest() {
        assertThatThrownBy { DbEcosDataProps.BatchProps(retryInitialDelay = Duration.ZERO) }
            .isInstanceOf(IllegalArgumentException::class.java)
        assertThatThrownBy { DbEcosDataProps.BatchProps(retryInitialDelay = Duration.ofSeconds(-1)) }
            .isInstanceOf(IllegalArgumentException::class.java)
    }

    @Test
    fun aZeroOrNegativeRetryMaxDelayIsRejectedTest() {
        assertThatThrownBy { DbEcosDataProps.BatchProps(retryMaxDelay = Duration.ZERO) }
            .isInstanceOf(IllegalArgumentException::class.java)
        assertThatThrownBy { DbEcosDataProps.BatchProps(retryMaxDelay = Duration.ofSeconds(-1)) }
            .isInstanceOf(IllegalArgumentException::class.java)
    }

    @Test
    fun aRetryMaxDelayBelowTheInitialDelayIsRejectedTest() {
        assertThatThrownBy {
            DbEcosDataProps.BatchProps(
                retryInitialDelay = Duration.ofSeconds(10),
                retryMaxDelay = Duration.ofSeconds(5)
            )
        }
            .describedAs("a ceiling below the starting point would make the backoff shrink instead of grow")
            .isInstanceOf(IllegalArgumentException::class.java)
    }

    @Test
    fun aNonPositiveRetryErrorEscalationAfterIsRejectedTest() {
        assertThatThrownBy { DbEcosDataProps.BatchProps(retryErrorEscalationAfter = 0) }
            .isInstanceOf(IllegalArgumentException::class.java)
    }

    @Test
    fun aZeroOrNegativeDrainIntervalIsRejectedTest() {
        assertThatThrownBy { DbEcosDataProps.BatchProps(drainInterval = Duration.ZERO) }
            .isInstanceOf(IllegalArgumentException::class.java)
        assertThatThrownBy { DbEcosDataProps.BatchProps(drainInterval = Duration.ofSeconds(-1)) }
            .isInstanceOf(IllegalArgumentException::class.java)
    }

    /**
     * Unlike `maxAttempts`, zero is not a documented "unlimited" here - it would mean a run that
     * makes no pass at all, does no work, and reports itself finished.
     */
    @Test
    fun aZeroOrNegativeMaxPassesPerRunIsRejectedTest() {
        assertThatThrownBy { DbEcosDataProps.BatchProps(maxPassesPerRun = 0) }
            .isInstanceOf(IllegalArgumentException::class.java)
        assertThatThrownBy { DbEcosDataProps.BatchProps(maxPassesPerRun = -1) }
            .isInstanceOf(IllegalArgumentException::class.java)
    }
}
