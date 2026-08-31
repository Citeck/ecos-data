package ru.citeck.ecos.data.sql.content.upload

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import java.time.Duration

class ChunkedUploadPolicyTest {

    /**
     * The completion lease has to stay below the idle timeout, so its default is derived from that
     * timeout rather than fixed: a caller which never mentions the lease must never be refused for
     * it, however short an idle timeout it chose.
     */
    @Test
    fun `the default completion lease fits inside any valid idle timeout`() {
        listOf(
            Duration.ofSeconds(1),
            Duration.ofMinutes(1),
            Duration.ofMinutes(10),
            Duration.ofMinutes(30),
            Duration.ofHours(6)
        ).forEach { idleTimeout ->
            val policy = ChunkedUploadPolicy(maxActiveSessionsPerUser = 1, sessionIdleTimeout = idleTimeout)
            assertThat(policy.completionLease)
                .describedAs("default completion lease for an idle timeout of $idleTimeout")
                .isPositive
                .isLessThanOrEqualTo(idleTimeout.dividedBy(2))
                .isLessThanOrEqualTo(ChunkedUploadPolicy.MAX_DEFAULT_COMPLETION_LEASE)
        }
    }

    @Test
    fun `a long idle timeout does not stretch the default lease past its cap`() {
        val policy = ChunkedUploadPolicy(
            maxActiveSessionsPerUser = 1,
            sessionIdleTimeout = Duration.ofDays(1)
        )
        assertThat(policy.completionLease).isEqualTo(ChunkedUploadPolicy.MAX_DEFAULT_COMPLETION_LEASE)
    }

    @Test
    fun `an explicit lease longer than half the idle timeout is refused`() {
        // releasing a lease moves the session a whole lease closer to expiry, so a longer one would
        // retire the session instead of freeing it for the retry
        assertThatThrownBy {
            ChunkedUploadPolicy(
                maxActiveSessionsPerUser = 1,
                sessionIdleTimeout = Duration.ofMinutes(10),
                completionLease = Duration.ofMinutes(6)
            )
        }.isInstanceOf(IllegalArgumentException::class.java)

        assertThatThrownBy {
            ChunkedUploadPolicy(
                maxActiveSessionsPerUser = 1,
                sessionIdleTimeout = Duration.ofMinutes(5),
                completionLease = Duration.ofMinutes(5)
            )
        }.isInstanceOf(IllegalArgumentException::class.java)
    }

    /**
     * The floor exists so that the two rules about the lease stay satisfiable together. Under it the
     * caller would be refused with a complaint about a lease it never named, which is the one thing
     * the derived default is there to prevent - so the idle timeout is what the message must blame.
     */
    @Test
    fun `an idle timeout too short to hold a lease is refused as an idle timeout`() {
        assertThatThrownBy {
            ChunkedUploadPolicy(
                maxActiveSessionsPerUser = 1,
                sessionIdleTimeout = ChunkedUploadPolicy.MIN_SESSION_IDLE_TIMEOUT.minusMillis(1)
            )
        }.isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("Session idle timeout")

        // the floor itself is a valid timeout, and leaves a valid lease behind it
        val atTheFloor = ChunkedUploadPolicy(
            maxActiveSessionsPerUser = 1,
            sessionIdleTimeout = ChunkedUploadPolicy.MIN_SESSION_IDLE_TIMEOUT
        )
        assertThat(atTheFloor.completionLease).isPositive
    }

    @Test
    fun `a non-positive explicit lease is refused`() {
        listOf(Duration.ZERO, Duration.ofMinutes(-1)).forEach { lease ->
            assertThatThrownBy {
                ChunkedUploadPolicy(
                    maxActiveSessionsPerUser = 1,
                    sessionIdleTimeout = Duration.ofMinutes(5),
                    completionLease = lease
                )
            }.describedAs("policy with a completion lease of $lease")
                .isInstanceOf(IllegalArgumentException::class.java)
        }
    }
}
