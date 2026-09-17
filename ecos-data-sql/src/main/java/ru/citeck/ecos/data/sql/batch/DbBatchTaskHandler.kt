package ru.citeck.ecos.data.sql.batch

/**
 * The work a batch task actually does. The engine owns the cursor, the lock, the retries and the
 * transaction; a handler owns what happens to the rows.
 *
 * How a handler saves a record is its own business, not the engine's rule. Two shapes
 * are expected: bypassing mutation through the repository, which is right for a column migration
 * because moving a value out of a backup is not a user edit; and going through mutation when the
 * records DAO machinery is needed, silencing the noise with
 * `DbRecordsControlAtts.DISABLE_AUDIT` / `DISABLE_EVENTS` - available because the engine runs
 * under `AuthContext.runAsSystem`.
 */
interface DbBatchTaskHandler {

    fun getType(): String

    /**
     * Called once per run, inside the engine's transaction, before the first batch.
     *
     * @return an estimate of how many records will be processed, or null if the handler has no
     *         cheap estimate. The value is only shown to the administrator; the engine never uses
     *         it to decide anything, so an estimate is allowed to be wrong.
     */
    fun prepare(ctx: DbBatchTaskContext): Long?

    /**
     * Processes one id window, inside the engine's transaction for that batch.
     *
     * Throwing fails the whole batch: the transaction rolls back, the cursor does not move, the
     * attempt counter goes up and the engine retries. A *record* that cannot be processed must not
     * throw - increment [DbBatchTaskContext.failed] and carry on, so one bad row cannot stop a
     * migration.
     */
    fun processBatch(ctx: DbBatchTaskContext, batch: DbBatchTaskBatch)

    fun onFinish(ctx: DbBatchTaskContext) {}

    fun onCancel(ctx: DbBatchTaskContext) {}
}
