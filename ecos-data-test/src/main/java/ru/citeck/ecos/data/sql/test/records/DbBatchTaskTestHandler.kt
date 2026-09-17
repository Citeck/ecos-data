package ru.citeck.ecos.data.sql.test.records

import ru.citeck.ecos.data.sql.batch.DbBatchTaskBatch
import ru.citeck.ecos.data.sql.batch.DbBatchTaskContext
import ru.citeck.ecos.data.sql.batch.DbBatchTaskHandler
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicInteger

/**
 * Fully controllable [DbBatchTaskHandler] for the engine tests.
 *
 * Every hook records what it was called with, so a test can assert on the engine's observable
 * behaviour - which batches it asked for, in which order, how many times - instead of reaching
 * into the engine. [failBatchesUntilAttempt] and [failRecordIndexes] let a test inject the two
 * failure modes the engine has to tell apart: a batch that fails as a whole, and a single record
 * that is broken while its neighbours are fine; [skipRecordIndexes] covers the third per-record
 * outcome, a record the handler decides needs no work at all.
 */
class DbBatchTaskTestHandler(
    private val type: String,
    /**
     * ids reported by [prepare]; null means "no estimate"
     */
    var totalEstimate: Long? = null,
    /**
     * while `attempts <= this`, [processBatch] throws
     */
    var failBatchesUntilAttempt: Int = 0,
    /**
     * record ordinals (0-based, across the whole run) that [processBatch] counts as failed.
     *
     * Named for what it does: it does **not** throw, it increments `ctx.failed` - a per-record
     * failure that must not stop the migration. [failBatchesUntilAttempt] is the
     * knob that throws.
     */
    var failRecordIndexes: Set<Int> = emptySet(),
    /**
     * record ordinals (0-based, across the whole run) that [processBatch] counts as skipped
     */
    var skipRecordIndexes: Set<Int> = emptySet(),
    /**
     * how many records each batch pretends to find; the engine only cares that the cursor moves
     */
    var recordsPerBatch: Int = 0
) : DbBatchTaskHandler {

    val prepareCalls = AtomicInteger()
    val finishCalls = AtomicInteger()
    val cancelCalls = AtomicInteger()
    val attempts = AtomicInteger()
    val seenBatches = CopyOnWriteArrayList<DbBatchTaskBatch>()

    /**
     * set by a test to run arbitrary code in the middle of a batch, e.g. cancel the task
     */
    var beforeBatch: ((DbBatchTaskContext, DbBatchTaskBatch) -> Unit)? = null

    /**
     * set by a test to run arbitrary code at the start of [onFinish], e.g. commit a cancel into the
     * window between the engine's read and the write that publishes DONE
     */
    var beforeFinish: ((DbBatchTaskContext) -> Unit)? = null

    /**
     * set by a test to run arbitrary code at the start of [prepare], e.g. throw the way a handler
     * given malformed `__params` would - the failure mode that never reaches [processBatch]
     */
    var beforePrepare: ((DbBatchTaskContext) -> Unit)? = null

    private var recordOrdinal = 0

    override fun getType(): String = type

    override fun prepare(ctx: DbBatchTaskContext): Long? {
        beforePrepare?.invoke(ctx)
        prepareCalls.incrementAndGet()
        return totalEstimate
    }

    override fun processBatch(ctx: DbBatchTaskContext, batch: DbBatchTaskBatch) {
        beforeBatch?.invoke(ctx, batch)
        seenBatches.add(batch)
        val attempt = attempts.incrementAndGet()
        if (attempt <= failBatchesUntilAttempt) {
            error("injected batch failure on attempt $attempt")
        }
        repeat(recordsPerBatch) {
            val ordinal = recordOrdinal++
            if (failRecordIndexes.contains(ordinal)) {
                ctx.failed++
            } else if (skipRecordIndexes.contains(ordinal)) {
                ctx.skipped++
            } else {
                ctx.processed++
            }
        }
    }

    override fun onFinish(ctx: DbBatchTaskContext) {
        beforeFinish?.invoke(ctx)
        finishCalls.incrementAndGet()
    }

    override fun onCancel(ctx: DbBatchTaskContext) {
        cancelCalls.incrementAndGet()
    }
}
