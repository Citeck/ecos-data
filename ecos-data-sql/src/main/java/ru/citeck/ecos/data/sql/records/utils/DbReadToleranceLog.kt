package ru.citeck.ecos.data.sql.records.utils

import io.github.oshai.kotlinlogging.KLogger
import java.util.concurrent.ConcurrentHashMap

/**
 * Deduplicating WARN/ERROR log for the points where the type model and the physical schema disagree.
 *
 * [warnOnce] is the read side - a failed column conversion, a dangling reference, a column never
 * materialized. Reading degrades instead of failing, and that degradation still has to be visible,
 * or a journal silently stops finding records with nothing in the log to explain it. One mismatch
 * affects every row of every page, so each message is reported once per key.
 *
 * [errorOnce] is the write side - a conversion the backend cannot perform, two types claiming one
 * attribute id. The mutation deliberately does not fail, so the ERROR is the operator's only signal.
 *
 * Both key sets are capped ([MAX_WARN_KEYS], [MAX_ERROR_KEYS]) and the cap is **flat, for the JVM's
 * whole life** rather than rolling: once it is reached no further warning of that kind is printed,
 * including for an unrelated mismatch found months later. A generous budget rather than
 * time-windowed eviction, which would be new machinery with its own clock. The read side is keyed
 * per table x attribute and the write side per table x column, so the write side fills far more
 * slowly - deliberately, because the ERROR must not go silent on exactly the installations big
 * enough to fill the read-side cap.
 *
 * The caller passes its own logger, so the log category stays the class that detected the problem.
 */
object DbReadToleranceLog {

    private const val MAX_WARN_KEYS = 10_000
    private const val MAX_ERROR_KEYS = 10_000

    private val loggedWarnProblems = ConcurrentHashMap.newKeySet<String>()
    private val loggedErrorProblems = ConcurrentHashMap.newKeySet<String>()

    fun warnOnce(log: KLogger, key: String, message: () -> String) {
        if (loggedWarnProblems.size < MAX_WARN_KEYS && loggedWarnProblems.add(key)) {
            log.warn { message() }
        }
    }

    /**
     * The same, for a tolerated *exception*. A swallowed throwable whose stack trace never reaches
     * the log is a defect nobody can locate, and the message alone rarely says where it came from.
     */
    fun warnOnce(log: KLogger, key: String, cause: Throwable, message: () -> String) {
        if (loggedWarnProblems.size < MAX_WARN_KEYS && loggedWarnProblems.add(key)) {
            log.warn(cause) { message() }
        }
    }

    fun errorOnce(log: KLogger, key: String, message: () -> String) {
        if (loggedErrorProblems.size < MAX_ERROR_KEYS && loggedErrorProblems.add(key)) {
            log.error { message() }
        }
    }
}
