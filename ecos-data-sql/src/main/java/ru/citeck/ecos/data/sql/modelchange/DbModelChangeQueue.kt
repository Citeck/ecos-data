package ru.citeck.ecos.data.sql.modelchange

import io.github.oshai.kotlinlogging.KotlinLogging
import ru.citeck.ecos.commons.task.schedule.Schedules
import ru.citeck.ecos.data.sql.context.DbDataSourceContext
import ru.citeck.ecos.data.sql.records.DbRecordsDao
import ru.citeck.ecos.data.sql.records.utils.DbReadToleranceLog
import ru.citeck.ecos.model.lib.type.repo.TypesRepo
import ru.citeck.ecos.webapp.api.task.scheduler.EcosScheduledTask
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference

/**
 * Turns a stream of "this type changed" notifications into a bounded number of schema
 * reconciliations, on a thread of the scheduler's rather than on whichever thread announced the
 * change.
 *
 * **The coalescing is load-bearing, not an optimisation.** The three places a change arrives from
 * make that so, and each of them on its own would be enough:
 *  - while a passive registry loads from ZooKeeper it publishes one event per type of the
 *    installation, on the application's startup thread - a storm, every single start;
 *  - in `emodel` the event is fired *inside the open transaction that saves the type*, so anything
 *    done synchronously here would put DDL and a distributed lock into a user's save;
 *  - everywhere else it arrives on the ZooKeeper watcher thread, where the registry calls its
 *    listeners one after another and swallows what they throw.
 *
 * So [onTypeChanged] does one thing - it adds a string to a set - and everything else happens on
 * [drainOnce]. The storm collapses into a set, no transaction is ever open around the work, and the
 * watcher thread returns immediately.
 */
class DbModelChangeQueue @JvmOverloads constructor(
    private val dataSourceCtx: DbDataSourceContext,
    /**
     * The unit of work. A parameter rather than a hardcoded instance so that a test can substitute
     * one that fails and assert the queue carries on - see [DbSchemaReconciler]'s own doc for why
     * that class is open.
     */
    private val reconciler: DbSchemaReconciler = DbSchemaReconciler()
) {

    companion object {

        private val log = KotlinLogging.logger {}

        const val RECONCILE_TASK_ID_PREFIX = "ecos-data-model-change-reconcile"

        /**
         * Makes every queue's scheduled task id unique within this JVM, the same way
         * [ru.citeck.ecos.data.sql.batch.DbBatchTaskEngine]'s drain id is and for the same reason:
         * `DbDomainFactory.withDataSource` builds one data source context - and therefore one queue
         * - per data source, and they all schedule onto the one shared scheduler, which rejects a
         * task id that is already registered and active. A constant id would make the second data
         * source of an application fail to start its tick at all.
         */
        private val reconcileTaskCounter = AtomicInteger()
    }

    private val index get() = dataSourceCtx.recordsDaoIndex

    /**
     * The coalescing set itself. Guarded by its own monitor rather than concurrent, because
     * [drainOnce] has to take the whole of it away in one step: a copy-then-remove over a
     * concurrent set would drop a notification that arrived between the two.
     */
    private val pending = LinkedHashSet<String>()

    /**
     * What the previous tick ran out of room for. Kept as DAOs rather than as type ids: the ids
     * were already resolved once, and re-resolving them next tick would redo the walk to the
     * storage boundary for nothing.
     */
    private val deferred = LinkedHashSet<DbRecordsDao>()

    private val fullSweepRequested = AtomicBoolean()

    /**
     * "This table was last reconciled against this model".
     *
     * Without it a frozen column (two types of one table declaring one attribute with different
     * types) asks for the migration lock on every tick for ever: freezing means the
     * registry keeps describing the old type by definition, so the cheap precheck answers "yes"
     * every single time and the migration that follows correctly does nothing.
     *
     * Keyed by records source *and* table, so that a DAO rebuilt over a different table does not
     * inherit the previous one's verdict. In memory only, and therefore empty after a restart -
     * which costs nothing, because the startup sweep wants every table looked at once anyway.
     */
    private val reconciledModels = ConcurrentHashMap<String, Set<DbSchemaReconciler.ExpectedColumn>>()

    /**
     * Resolved from the first registered DAO rather than injected - see [scope].
     */
    private val scopeRef = AtomicReference<DbTypeChangeScope>()

    private val started = AtomicBoolean(false)

    /**
     * One subscription attempt per data source - see [subscribeToTypeChanges].
     */
    private val subscriptionAttempted = AtomicBoolean(false)

    private var scheduledTask: EcosScheduledTask? = null

    /**
     * Unique per queue instance - see [reconcileTaskCounter].
     */
    private val reconcileTaskId = "$RECONCILE_TASK_ID_PREFIX-${reconcileTaskCounter.incrementAndGet()}"

    /**
     * Attaches [onTypeChanged] to the application's type registry, once per data source.
     *
     * Called from [ru.citeck.ecos.data.sql.records.DbRecordsDao.setRecordsServiceFactory] rather
     * than from a composition root for the same reason as the DAO index itself: that
     * method is the one path both production and the tests of this repository take, while
     * `DbDomainFactory` is built by no test in this repository at all. The first DAO to register
     * brings the [TypesRepo] with it - every DAO over one data source is given the same
     * `ModelServiceFactory`, so which one arrives first does not matter.
     *
     * Events published before the first DAO registers are lost by construction, and lose nothing:
     * a type with no DAO over this data source has no table here to reconcile, and the start-up
     * sweep looks at every table once anyway.
     *
     * Never throws. A repository that fails while being subscribed to must not take the
     * registration of a records DAO - and therefore the start of the application - down with it;
     * the schema then follows the model the way it did before this feature existed.
     */
    fun subscribeToTypeChanges(typesRepo: TypesRepo) {
        if (!subscriptionAttempted.compareAndSet(false, true)) {
            return
        }
        try {
            // the definitions both sides of the change carry are deliberately dropped: what has to
            // be repaired is a table, and the answer to "does it match the model" is the model
            // against the column registry as they are when the tick runs, not as they were when the
            // change was announced
            typesRepo.listenTypeChanges { typeId, _, _ -> onTypeChanged(typeId) }
        } catch (e: InterruptedException) {
            Thread.currentThread().interrupt()
            throw e
        } catch (e: Throwable) {
            DbReadToleranceLog.warnOnce(log, "model-change-listen-failed:${typesRepo.javaClass.name}", e) {
                "Subscription to type changes of ${typesRepo.javaClass.name} failed. The schema of " +
                    "this data source will only be reconciled on start-up and on mutations."
            }
            return
        }
        log.debug { "Subscribed to type changes of ${typesRepo.javaClass.name}" }
    }

    /**
     * The whole of what happens on the thread that announced the change.
     *
     * Not "reconcile the tables of this type", not even "resolve which tables those are": see this
     * class's own doc for the three callers that make anything more than this unsafe.
     */
    fun onTypeChanged(typeId: String) {
        if (typeId.isEmpty()) {
            return
        }
        synchronized(pending) {
            pending.add(typeId)
        }
    }

    /**
     * Asks the next tick to look at every DAO the index knows, not only at the ones a change was
     * announced for - the start-up sweep, and the safety net for a model change that
     * happened while this instance was down.
     *
     * Idempotent: a flag, not a queue, so a composition root that requests it twice still costs one
     * sweep. The sweep is not free but it is cheap - one precheck SELECT per table - and the
     * fingerprint above keeps a second sweep from repeating even that for a table already seen.
     */
    fun requestFullSweep() {
        fullSweepRequested.set(true)
    }

    /**
     * Registers [drainOnce] on a fixed-delay schedule of
     * [ru.citeck.ecos.data.sql.props.DbEcosDataProps.ColumnsProps.reconcileInterval].
     *
     * **Never call this from a constructor**, for the reason the batch engine's own `start` spells
     * out: the test mock hands back a real thread-pool-backed scheduler, so a self-starting queue would
     * put a background thread into every test of this repository and change schemas underneath
     * assertions that never asked for it. `DbDomainFactory` starts it when the application is ready;
     * [drainOnce] stays public and synchronous so a test can drive one tick deterministically.
     *
     * Idempotent, so a composition root that runs its startup twice does not end up with two
     * schedules racing the same interval.
     */
    fun start() {
        if (!started.compareAndSet(false, true)) {
            return
        }
        val interval = dataSourceCtx.props.columns.reconcileInterval
        log.info { "Starting the model change reconciliation tick every $interval" }
        scheduledTask = dataSourceCtx.webAppApiForBackgroundTasks.getTasksApi()
            // The MAIN scheduler, for the same reason the batch drain uses it: a scheduler key of
            // this library's own invention would have to be declared in every consuming
            // application's configuration, and start() would throw in every one that forgot.
            .getMainScheduler()
            .schedule(reconcileTaskId, Schedules.fixedDelay(interval)) {
                drainOnce()
            }
    }

    /**
     * Cancels the schedule [start] registered. A no-op if [start] was never called or [stop] has
     * already run.
     */
    fun stop() {
        if (!started.compareAndSet(true, false)) {
            return
        }
        scheduledTask?.cancel()
        scheduledTask = null
    }

    /**
     * One pass: whatever the previous tick left over, plus a full sweep if one was asked for, plus
     * the tables of every type announced since the last pass, minus the ones whose model has not
     * moved, capped at
     * [ru.citeck.ecos.data.sql.props.DbEcosDataProps.ColumnsProps.reconcileMaxTablesPerTick].
     *
     * The order of those steps carries one requirement that is easy to lose: the pending set is
     * taken away **before** the type ids in it are expanded into DAOs. Expanding first and clearing
     * afterwards would throw away every notification that arrived while the expansion ran, and a
     * type changed at exactly that moment would be reconciled only by the next change to it.
     *
     * What comes back and what does not is worth stating exactly, because "nothing is ever
     * dropped" would be too strong. The per-tick ceiling defers what it did not reach, and an
     * interrupt defers what the pass had left; both are retried by the next tick unconditionally.
     * A reconciliation that **fails** is not deferred: [reconcileOne] records the failure and the
     * pass carries on, so that table waits for the next change announced for it or for the next
     * full sweep. That is deliberate - a table failing every tick would otherwise hold the
     * ceiling's worth of the queue for ever - and it is also the reason the fingerprint is stored
     * only for a reconciliation that produced one: a failed table is not remembered as up to date,
     * so the next time it is looked at, it is looked at properly.
     *
     * @return how many DAOs this pass handed to the reconciler. Attempted, not necessarily
     *         changed - a DAO whose table does not exist yet is counted here and does nothing.
     */
    fun drainOnce(): Int {

        val candidates = LinkedHashSet<DbRecordsDao>()

        synchronized(deferred) {
            candidates.addAll(deferred)
            deferred.clear()
        }
        if (fullSweepRequested.compareAndSet(true, false)) {
            candidates.addAll(index.getAll())
        }
        // the snapshot, and only then the expansion - see this method's doc
        val changedTypes = takePending()
        if (changedTypes.isNotEmpty()) {
            val scope = scope()
            if (scope == null) {
                // Not a single DAO has registered over this data source yet, so there is no table
                // any of these types could live in. Dropping them loses nothing: whatever registers
                // later is covered by the start-up sweep.
                log.debug { "No records DAO is registered yet, ${changedTypes.size} model change(s) are dropped" }
            } else {
                changedTypes.forEach { candidates.addAll(daosForTypeChange(scope, it)) }
            }
        }
        if (candidates.isEmpty()) {
            return 0
        }

        val toReconcile = ArrayList<DbRecordsDao>(candidates.size)
        for (dao in candidates) {
            if (!isModelUnchangedSinceLastReconcile(dao)) {
                toReconcile.add(dao)
            }
        }
        val maxPerTick = dataSourceCtx.props.columns.reconcileMaxTablesPerTick
        if (toReconcile.size > maxPerTick) {
            val leftOver = toReconcile.subList(maxPerTick, toReconcile.size)
            log.debug { "Deferring ${leftOver.size} table(s) of this tick to the next one" }
            defer(leftOver)
            leftOver.clear()
        }
        var done = 0
        try {
            while (done < toReconcile.size) {
                reconcileOne(toReconcile[done])
                done++
            }
        } finally {
            // Only an interrupt gets here with work left - reconcileOne swallows everything else -
            // and an interrupt is the shutdown of the tick, not of the queue: the schedule may well
            // run again, and what this pass did not reach has to still be waiting for it.
            if (done < toReconcile.size) {
                defer(toReconcile.subList(done, toReconcile.size))
            }
        }
        return done
    }

    private fun defer(daos: Collection<DbRecordsDao>) {
        synchronized(deferred) {
            deferred.addAll(daos)
        }
    }

    /**
     * Resolving one announced type, with the same rule the reconciliation itself follows: one type
     * that cannot be resolved must not take the rest of the tick's types with it.
     */
    private fun daosForTypeChange(scope: DbTypeChangeScope, typeId: String): List<DbRecordsDao> {
        return try {
            scope.daosForTypeChange(typeId)
        } catch (e: InterruptedException) {
            Thread.currentThread().interrupt()
            throw e
        } catch (e: Throwable) {
            DbReadToleranceLog.warnOnce(log, "model-change-scope-failed:$typeId", e) {
                "The tables storing type '$typeId' could not be resolved, so the change announced " +
                    "for it is skipped. Its table will be repaired on the next mutation of one of " +
                    "its records."
            }
            emptyList()
        }
    }

    private fun takePending(): List<String> {
        return synchronized(pending) {
            if (pending.isEmpty()) {
                emptyList()
            } else {
                val snapshot = ArrayList(pending)
                pending.clear()
                snapshot
            }
        }
    }

    private fun isModelUnchangedSinceLastReconcile(dao: DbRecordsDao): Boolean {
        val lastReconciled = reconciledModels[modelKey(dao)] ?: return false
        return reconciler.getModelFingerprint(dao) == lastReconciled
    }

    private fun reconcileOne(dao: DbRecordsDao) {
        val result = try {
            reconciler.reconcile(dao)
        } catch (e: InterruptedException) {
            Thread.currentThread().interrupt()
            throw e
        } catch (e: Throwable) {
            // DbSchemaReconciler promises not to throw; this catch is about the queue not depending
            // on that promise. One table must not stop the ones queued behind it, and the tick runs
            // on a shared scheduler thread where an escaping exception is logged once and the
            // schedule silently keeps its place.
            DbReadToleranceLog.warnOnce(log, "model-change-reconcile-failed:${dao.getId()}", e) {
                "Reconciliation of records source '${dao.getId()}' failed and was skipped. " +
                    "The remaining tables of this tick are unaffected."
            }
            null
        }
        val fingerprint = result?.fingerprint ?: return
        reconciledModels[modelKey(dao)] = fingerprint
    }

    private fun modelKey(dao: DbRecordsDao): String {
        return dao.getId() + "|" + dao.getTableRef().fullName
    }

    /**
     * The walk from a type id to the DAOs that store it, built from the first registered DAO's
     * model service.
     *
     * It is taken from a DAO rather than passed in because the data source context has no
     * `ModelServiceFactory` of its own - each DAO builds a `DbEcosModelService` over the factory it
     * was given, and `DbDomainFactory` hands the same factory to every DAO it builds over one data
     * source (`withDataSource` copies it), so any registered DAO answers for all of them.
     */
    private fun scope(): DbTypeChangeScope? {
        scopeRef.get()?.let { return it }
        val dao = index.getAll().firstOrNull() ?: return null
        val created = DbTypeChangeScope(dao.getRecordsDaoCtx().ecosTypeService, index)
        scopeRef.compareAndSet(null, created)
        return scopeRef.get()
    }
}
