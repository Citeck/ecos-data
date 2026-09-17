package ru.citeck.ecos.data.sql.batch

import ru.citeck.ecos.commons.data.ObjectData
import java.time.Instant

/**
 * Immutable view of an `ed_batch_task` row. `total` is `-1` while unknown: a handler's `prepare`
 * may legitimately decline to estimate, and `-1` says "no estimate" without pretending the job is
 * empty the way `0` would.
 */
data class DbBatchTaskDto(
    val id: Long = DbBatchTaskEntity.NEW_REC_ID,
    val handler: String,
    val table: String,
    val params: ObjectData = ObjectData.create(),
    val status: DbBatchTaskStatus = DbBatchTaskStatus.PENDING,
    val cursor: Long = 0,
    val total: Long = -1,
    val processed: Long = 0,
    val skipped: Long = 0,
    val failed: Long = 0,
    val error: String = "",
    val errorCount: Int = 0,
    val nextAttemptAt: Instant = Instant.EPOCH,
    val created: Instant = Instant.EPOCH,
    val creator: String = "",
    val started: Instant = Instant.EPOCH,
    val finished: Instant = Instant.EPOCH
)
