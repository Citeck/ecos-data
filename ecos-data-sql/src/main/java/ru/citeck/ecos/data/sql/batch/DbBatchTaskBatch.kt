package ru.citeck.ecos.data.sql.batch

/**
 * One id window, half-open at the bottom: `id > fromExclusive AND id <= toInclusive`.
 *
 * Iterating by primary key instead of `OFFSET` is what makes the engine both cheap and correct
 * under concurrent inserts: rows created while the task runs land in the tail and are
 * picked up naturally, and nothing shifts underneath a page the way `OFFSET` pages do.
 */
data class DbBatchTaskBatch(
    val fromExclusive: Long,
    val toInclusive: Long
)
