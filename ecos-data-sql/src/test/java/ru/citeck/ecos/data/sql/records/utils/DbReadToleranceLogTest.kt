package ru.citeck.ecos.data.sql.records.utils

import io.github.oshai.kotlinlogging.KotlinLogging
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger

/**
 * [DbReadToleranceLog] is a JVM-wide singleton shared with every other test in this module (and
 * with production code), so these tests never assume it starts empty - only that it behaves
 * correctly from whatever state it is already in. Keys are always randomised so a previous test's
 * (or a previous run's, in the same fork) usage can never collide with this test's own counting.
 */
class DbReadToleranceLogTest {

    private val log = KotlinLogging.logger {}

    companion object {
        // warnOnce's cap is 10_000 as of the final-review fix wave that raised it to match
        // errorOnce's (it used to be 100) - past-the-cap tests drive this many-plus-a-margin brand
        // new keys through warnOnce specifically, not a number tied to the old value.
        private const val KEYS_PAST_WARN_CAP = 10_100

        // comfortably under both caps (both are 10_000 now) - big enough to have been a convincing
        // "past the old 100-key warnOnce cap" sample before this fix, small enough to stay clear of
        // errorOnce's own cap regardless of what other tests in this JVM fork already put in it.
        private const val ERROR_SAMPLE_SIZE = 300
    }

    private fun uniqueKey(prefix: String): String = "$prefix-${UUID.randomUUID()}"

    @Test
    fun errorOnceIsNotBoundedByWarnOnceKeysTest() {

        // drive warnOnce's own key set well past its cap with brand new keys - this is what
        // a large installation with many tolerated read-side mismatches looks like in production
        val warnCount = AtomicInteger()
        repeat(KEYS_PAST_WARN_CAP) {
            DbReadToleranceLog.warnOnce(log, uniqueKey("warn")) {
                warnCount.incrementAndGet()
                "test warn message"
            }
        }
        // warnOnce's cap must still be enforced (it is unchanged by this fix) - of the brand new
        // keys just added, at most its cap can ever be logged by warnOnce across the whole JVM, so
        // strictly fewer than all of these calls actually logged
        assertThat(warnCount.get())
            .describedAs("warnOnce must still cap itself - this fix must not have removed that")
            .isLessThan(KEYS_PAST_WARN_CAP)

        // now hand errorOnce a batch of brand new keys, well past warnOnce's cap. Before the shared cap,
        // fix, errorOnce shared warnOnce's key set and its (then 100-key) cap: once that shared set
        // filled up (which the loop above guarantees, and which a real installation's read-side
        // tolerance logging does on its own), every errorOnce call afterwards logged nothing at all
        // - the installation's only signal of an unsupported column conversion would have been
        // silently swallowed. With independent key sets, all of these must log.
        val errorCount = AtomicInteger()
        repeat(ERROR_SAMPLE_SIZE) {
            DbReadToleranceLog.errorOnce(log, uniqueKey("error")) {
                errorCount.incrementAndGet()
                "test error message"
            }
        }
        assertThat(errorCount.get())
            .describedAs(
                "the mandated ERROR must not be suppressed by warnOnce's key set filling up - " +
                    "that is exactly the installation-size scenario the ERROR exists for"
            )
            .isEqualTo(ERROR_SAMPLE_SIZE)
    }

    @Test
    fun eachKeyStillLogsOnlyOnceTest() {
        val key = uniqueKey("dedup")
        val count = AtomicInteger()
        repeat(5) {
            DbReadToleranceLog.errorOnce(log, key) {
                count.incrementAndGet()
                "test error message"
            }
        }
        assertThat(count.get()).isEqualTo(1)
    }
}
