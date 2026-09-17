package ru.citeck.ecos.data.sql.batch

import ru.citeck.ecos.commons.data.ObjectData
import ru.citeck.ecos.data.sql.context.DbSchemaContext
import ru.citeck.ecos.data.sql.dto.DbTableRef

/**
 * What a handler is given for one task.
 *
 * The three counters are `var`s the handler increments as it goes, and the engine persists them
 * with the cursor in the same transaction as the batch. A handler never writes to `ed_batch_task`
 * itself - if it did, its write and the engine's cursor write could disagree after a rollback.
 */
class DbBatchTaskContext(
    val task: DbBatchTaskDto,
    val schemaCtx: DbSchemaContext
) {
    var processed: Long = task.processed
    var skipped: Long = task.skipped
    var failed: Long = task.failed

    fun getParams(): ObjectData = task.params

    fun getTableRef(): DbTableRef = schemaCtx.getTableRef(task.table)
}
