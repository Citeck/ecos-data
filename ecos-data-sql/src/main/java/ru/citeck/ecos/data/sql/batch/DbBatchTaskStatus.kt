package ru.citeck.ecos.data.sql.batch

/**
 * Lifecycle of a row in `ed_batch_task`.
 *
 * [RUNNING] is a hint, not a barrier: an instance can die without getting the chance to move the
 * row out of it, so the drain treats [PENDING] and [RUNNING] identically and relies on the
 * distributed lock for mutual exclusion.
 */
enum class DbBatchTaskStatus {

    PENDING,
    RUNNING,
    DONE,
    FAILED,
    CANCELLED;

    fun isFinal(): Boolean {
        return this == DONE || this == FAILED || this == CANCELLED
    }

    companion object {
        /**
         * Falls back to [FAILED] rather than throwing: an unreadable status must not make the whole
         * admin list unloadable, and [FAILED] is the one value that neither hides the row nor lets
         * the drain pick it up.
         */
        fun parse(value: String): DbBatchTaskStatus {
            return entries.firstOrNull { it.name == value } ?: FAILED
        }
    }
}
