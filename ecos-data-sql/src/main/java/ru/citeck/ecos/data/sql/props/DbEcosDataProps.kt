package ru.citeck.ecos.data.sql.props

import java.time.Duration

/**
 * Global ecos-data properties. In webapp these properties
 * are loaded from the 'ecos.webapp.data' configuration section.
 */
class DbEcosDataProps(
    val assocs: AssocsProps = AssocsProps(),
    val columns: ColumnsProps = ColumnsProps(),
    val batch: BatchProps = BatchProps(),
    /**
     * The one switch for every background thread this library starts:
     * [ru.citeck.ecos.data.sql.batch.DbBatchTaskEngine]'s drain and
     * [ru.citeck.ecos.data.sql.modelchange.DbModelChangeQueue]'s reconcile tick, both registered by
     * [ru.citeck.ecos.data.sql.domain.DbDomainFactory] when the application becomes ready.
     *
     * **Turning it off stops the feature, not just the load.** A type whose model changed is then
     * reconciled only by the next mutation of one of its records - the lazy behaviour this whole
     * feature exists to replace - and a column migration already queued in `ed_batch_task` never
     * runs at all, so the values in its backup column are never carried across. It is meant for a
     * test that builds a factory and does not want a scheduler mutating its schema underneath it,
     * not as a way to relieve a loaded database; for the latter,
     * [ColumnsProps.reconcileMaxTablesPerTick] and [BatchProps.batchPause] are the knobs that keep
     * the work happening.
     *
     * Kept at the top level rather than inside [ColumnsProps] or [BatchProps] deliberately: a test
     * that replaces one of those sections wholesale must not silently lose the switch as well.
     */
    val backgroundTasksEnabled: Boolean = true
) {
    companion object {
        @JvmField
        val DEFAULT = DbEcosDataProps()
    }

    class AssocsProps(
        /**
         * Max count of association values which may be changed by providing
         * full values list in mutation. When record has more values than this
         * limit, then att_add_... and att_rem_... operations should be used.
         */
        val maxCountToEditByFullValuesList: Int = 150
    )

    class ColumnsProps(
        /**
         * Largest table, in rows, whose column may be converted in place.
         *
         * An in-place conversion rewrites every row of the table under an exclusive lock, and it
         * happens inside whichever mutation triggers it. Below this size that costs seconds; above
         * it, the column is left for the background migration instead. `RENAME` and `ADD COLUMN`,
         * which the background path uses, are metadata-only and do not care about table size.
         */
        val inPlaceAlterMaxRows: Long = 100_000,
        /**
         * How long a save waits for the distributed schema migration lock on a table before
         * giving up.
         *
         * Has to outlast [inPlaceAlterMaxRows]: a table just under that row count can take well
         * over the old hard-coded 10 seconds to `ALTER ... USING` with its index rebuilds, and
         * every concurrent save on every other instance then fails to acquire the lock while the
         * legitimate migration is still running. Configured alongside the threshold it has to
         * outlast, not as an unrelated constant, so raising one is a reminder to check the other.
         */
        val schemaMigrationLockTimeout: Duration = Duration.ofSeconds(10),
        /**
         * How often [ru.citeck.ecos.data.sql.modelchange.DbModelChangeQueue] turns the model
         * changes it has collected into schema reconciliations.
         *
         * This is the whole observable delay of the feature: between a type being saved and its
         * table following, at most one of these passes. During it the lazy path and the tolerant
         * read path still hold, so the worst visible effect is an attribute read back empty.
         * Aligned with [BatchProps.drainInterval] because the two ticks do the two halves of the
         * same job - the schema change and the values that follow it.
         */
        val reconcileInterval: Duration = Duration.ofSeconds(10),
        /**
         * Ceiling on how many tables one tick reconciles; the rest wait for the next one.
         *
         * Exists for the start of an instance, not for the steady state: the registry fires an
         * event per type while it loads (and the startup sweep queues every known DAO on top),
         * so without a ceiling the first tick after a restart of an installation with hundreds of
         * types would try to reconcile all of them in one pass on one scheduler thread. Nothing is
         * dropped when the ceiling is hit - what does not fit is carried into the next tick.
         */
        val reconcileMaxTablesPerTick: Int = 50
    ) {
        init {
            require(!reconcileInterval.isNegative && !reconcileInterval.isZero) {
                "columns.reconcileInterval must be positive, got $reconcileInterval"
            }
            // Zero would be the quiet failure here: the queue would keep collecting model changes,
            // defer every one of them for ever and never reconcile a single table, which looks
            // exactly like the feature being off while every tick still runs.
            require(reconcileMaxTablesPerTick > 0) {
                "columns.reconcileMaxTablesPerTick must be positive, got $reconcileMaxTablesPerTick - " +
                    "at 0 every tick would defer all of its work to the next one and no table " +
                    "would ever be reconciled"
            }
        }
    }

    class BatchProps(
        /**
         * Records per batch. One batch is one transaction, so this is also the unit of rollback.
         */
        val batchSize: Int = 500,
        /**
         * Pause between two *successful* batches - the throttle, so a background
         * migration does not saturate the database. Not to be confused with [retryInitialDelay],
         * which is the wait after a *failed* batch and is two orders of magnitude larger.
         */
        val batchPause: Duration = Duration.ofMillis(100),
        /**
         * How many consecutive failed batches end the task in
         * [ru.citeck.ecos.data.sql.batch.DbBatchTaskStatus.FAILED]. **Zero means never**, which is
         * the default: most causes of a failed batch clear themselves, and a half-migrated column
         * waiting for someone to notice is worse than a task that keeps trying. Set a positive
         * value only if a terminal state to alert on is wanted.
         */
        val maxAttempts: Int = 0,
        /**
         * First retry delay after a failed batch; doubles per consecutive failure up to
         * [retryMaxDelay]. Defaulted to the drain interval because a shorter value cannot be
         * honoured anyway - the drain is what wakes a waiting task.
         */
        val retryInitialDelay: Duration = Duration.ofSeconds(10),
        /**
         * Ceiling of the exponential backoff. At the default a stuck task costs one failed batch every 10 minutes.
         */
        val retryMaxDelay: Duration = Duration.ofMinutes(10),
        /**
         * Consecutive failures after which the retry log line goes from WARN to ERROR. With
         * unlimited retries this is the signal monitoring alerts on, so it is what replaces the
         * `FAILED` status as the "a human should look at this" marker.
         */
        val retryErrorEscalationAfter: Int = 5,
        /**
         * How often the queue is drained.
         */
        val drainInterval: Duration = Duration.ofSeconds(10),
        /**
         * How many passes over the target table one run of a task may make before it yields the
         * table's distributed lock and lets the next drain tick continue it.
         *
         * A pass ends when the cursor reaches the table's maximum id, and rows inserted while the
         * pass ran land past that id - so a table under steady insert load always has one more pass
         * waiting. Without a ceiling such a task never returns: it holds the table lock, occupies a
         * scheduler thread, and because a drain tick walks a schema's tasks sequentially, every
         * other batch task of that data source is starved for as long as it runs. Yielding costs
         * nothing - the task stays `RUNNING` with its cursor where it is and the next tick picks it
         * straight back up - and it is what keeps one busy table from monopolising the queue.
         */
        val maxPassesPerRun: Int = 10
    ) {
        init {
            // batchSize used to be a hard-coded constant; exposing it to configuration means a
            // typo or a misguided "0 means unlimited" reading (true of maxAttempts, not this) can
            // now reach the engine. At 0 the window `runOneBatch` builds is `(cursor, cursor]` -
            // empty, so `fresh.cursor >= maxId` never becomes true and the engine re-issues the
            // same empty window forever, once per batchPause, holding the table's distributed lock
            // the whole time and logging nothing. Negative walks the cursor backwards. Both are
            // caught here instead of at the first wedged table.
            require(batchSize > 0) {
                "batch.batchSize must be positive, got $batchSize - a value of 0 or less leaves " +
                    "the id window empty and spins the engine forever instead of processing anything"
            }
            require(!batchPause.isNegative) {
                "batch.batchPause must not be negative, got $batchPause"
            }
            // Unlike batchSize, 0 is a legitimate, documented value here - "never give up" - so
            // only negative is rejected.
            require(maxAttempts >= 0) {
                "batch.maxAttempts must not be negative, got $maxAttempts - use 0 for unlimited retries"
            }
            require(!retryInitialDelay.isNegative && !retryInitialDelay.isZero) {
                "batch.retryInitialDelay must be positive, got $retryInitialDelay"
            }
            require(!retryMaxDelay.isNegative && !retryMaxDelay.isZero) {
                "batch.retryMaxDelay must be positive, got $retryMaxDelay"
            }
            require(retryMaxDelay >= retryInitialDelay) {
                "batch.retryMaxDelay ($retryMaxDelay) must be at least batch.retryInitialDelay " +
                    "($retryInitialDelay), or the exponential backoff would shrink instead of grow"
            }
            require(retryErrorEscalationAfter > 0) {
                "batch.retryErrorEscalationAfter must be positive, got $retryErrorEscalationAfter"
            }
            require(!drainInterval.isNegative && !drainInterval.isZero) {
                "batch.drainInterval must be positive, got $drainInterval"
            }
            require(maxPassesPerRun > 0) {
                "batch.maxPassesPerRun must be positive, got $maxPassesPerRun - a run has to make " +
                    "at least one pass over the table to do any work at all"
            }
        }
    }
}
