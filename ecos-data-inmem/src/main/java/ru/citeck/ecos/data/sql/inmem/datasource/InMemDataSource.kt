package ru.citeck.ecos.data.sql.inmem.datasource

import ru.citeck.ecos.data.sql.datasource.DbDataSource
import ru.citeck.ecos.data.sql.inmem.store.InMemStore
import ru.citeck.ecos.txn.lib.TxnContext
import ru.citeck.ecos.txn.lib.resource.CommitPrepareStatus
import ru.citeck.ecos.txn.lib.resource.TransactionResource
import ru.citeck.ecos.txn.lib.transaction.TxnId
import ru.citeck.ecos.txn.lib.transaction.xid.EcosXid
import java.sql.DatabaseMetaData
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.ReentrantLock

/**
 * In-memory [DbDataSource].
 *
 * Unlike [ru.citeck.ecos.data.sql.datasource.DbDataSourceImpl] this data source does not execute
 * SQL. The in-memory schema dao and entity repo mutate the shared [InMemStore] directly via
 * [getStore]; the SQL-string methods ([query], [update], [updateSchema], [withMetaData]) are never
 * invoked on the in-mem path and therefore throw [UnsupportedOperationException].
 *
 * Transactional semantics mirror the PG backend closely enough for the contract tests. There is one
 * shared [store] that every transaction mutates in place; isolation comes from the store's **undo-log**
 * (see [InMemStore]) rather than a snapshot copy:
 *  - a write transaction begins by taking an undo-log [InMemStore.mark];
 *  - normal completion keeps the mutated store — committing forgets the recorded inverses
 *    ([InMemStore.commitTo]);
 *  - an exception replays the inverses back to the mark ([InMemStore.rollbackTo]).
 *
 * This makes a write O(1) instead of O(store size); the previous design deep-copied the whole store on
 * every top-level / `requiresNew` write begin, so bulk creation was O(N^2) (each new EntityRef opens a
 * `requiresNew` id-mapping txn).
 *
 * Read-only transactions never record anything. Nesting follows [DbDataSourceImpl]: reusing an existing
 * transaction of the same read-only mode runs the action directly; a read-only action inside a
 * write transaction is allowed; a write action inside a read-only transaction is forbidden;
 * `requiresNew` starts an independent (separately marked) transaction whose committed writes are made
 * **permanent immediately** ([InMemStore.commitTo]) so they survive a rollback of the parent — exactly
 * what the id-mapping insert needs.
 *
 * **Platform transaction enlistment.** The [withTransaction] brackets give per-call atomicity, but a
 * single `TxnContext.doInTxn { … }` block issues *several* of them and must roll back as a unit when
 * the block throws (mirroring PG, where the managed JDBC connection is committed/rolled back by the
 * platform transaction manager, not by [DbDataSourceImpl] itself). To get that, a durable write bracket
 * running inside a platform transaction does NOT forget its inverses on commit — instead the in-mem
 * source enlists a [TransactionResource] the first time it durably writes, capturing the undo-log mark
 * *before* that first write; on [TransactionResource.rollback] it replays to that mark (undoing every
 * write the doInTxn block made) and on commit it forgets them. The mark is captured at the first
 * **write**, not the first access, so a nested `requiresNew` id-mapping insert (which already made its
 * own writes permanent via [InMemStore.commitTo]) is unaffected by a parent rollback. Outside a platform
 * transaction (no manager / no active txn) a durable write bracket forgets its inverses eagerly.
 *
 * **Threading.** The single shared [store] and its undo-log are mutated in place, and the undo-log's
 * commit/rollback works on absolute positions in one shared stack — so it is correct only while a
 * transaction's entries are an uninterrupted suffix, i.e. while transactions on a data source do not
 * interleave. To guarantee that, transactions are **serialized** by [txnLock]: each [withTransaction]
 * call holds the lock for its duration, and a durable write inside a platform `doInTxn` keeps an extra
 * hold from its first write until the platform manager commits/rolls back (released on the same thread
 * in the [InMemTxnResource] terminal callbacks), so no other thread can splice writes into its undo-log
 * range. Concurrency is therefore safe but not parallel — transactions queue. Lock acquisition is
 * bounded by [LOCK_TIMEOUT_SEC]: a thread that cannot acquire within that window fails fast instead of
 * hanging, so a stuck transaction surfaces as an error rather than a deadlock. (This is a coarse guard,
 * not snapshot isolation: concurrent transactions still see each other's uncommitted in-place writes —
 * fine for the serialized test/mock usage this backend targets.)
 *
 * The schema-command machinery ([watchSchemaCommands]/[withSchemaMock]) replicates
 * [DbDataSourceImpl] so [ru.citeck.ecos.data.sql.service.DbDataServiceImpl]'s migration flow works:
 * the schema dao registers a command for every structural change via [registerSchemaCommand], and
 * [withSchemaMock] suppresses the actual structural change while still recording the command.
 */
class InMemDataSource : DbDataSource {

    private val store = InMemStore()

    /**
     * Serializes transactions on this data source (see the class **Threading** note). Reentrant, so the
     * same thread's nested [withTransaction] calls and the platform-resource's extra hold compose
     * without self-deadlock.
     */
    private val txnLock = ReentrantLock()

    private val currentThreadTxn = ThreadLocal<TxnState>()
    private val thSchemaCommands = ThreadLocal<MutableList<String>>()
    private val thSchemaMock = ThreadLocal.withInitial { false }

    /**
     * The single shared store. Transactions mutate it in place; isolation is provided by the store's
     * undo-log, not by handing out per-transaction copies.
     */
    fun getStore(): InMemStore {
        return store
    }

    fun isSchemaMock(): Boolean {
        return thSchemaMock.get()
    }

    /** Called by the in-mem schema dao for each structural change, mirroring updateSchema. */
    fun registerSchemaCommand(command: String) {
        thSchemaCommands.get()?.add(command)
    }

    override fun updateSchema(query: String) {
        throw UnsupportedOperationException("In-memory data source does not execute SQL schema commands")
    }

    override fun <T> query(query: String, params: List<Any?>, action: (java.sql.ResultSet) -> T): T {
        throw UnsupportedOperationException("In-memory data source does not execute SQL queries")
    }

    override fun update(query: String, params: List<Any?>): List<Long> {
        throw UnsupportedOperationException("In-memory data source does not execute SQL updates")
    }

    override fun <T> withMetaData(action: (DatabaseMetaData) -> T): T {
        throw UnsupportedOperationException("In-memory data source has no JDBC metadata")
    }

    override fun <T> withSchemaMock(action: () -> T): T {
        if (thSchemaMock.get()) {
            return action.invoke()
        }
        thSchemaMock.set(true)
        try {
            return action.invoke()
        } finally {
            thSchemaMock.set(false)
        }
    }

    override fun watchSchemaCommands(action: () -> Unit): List<String> {
        val commandsBefore = thSchemaCommands.get()
        val commandsList = mutableListOf<String>()
        thSchemaCommands.set(commandsList)
        try {
            action.invoke()
        } finally {
            if (commandsBefore == null) {
                thSchemaCommands.remove()
            } else {
                thSchemaCommands.set(commandsBefore)
                commandsBefore.addAll(commandsList)
            }
        }
        return commandsList
    }

    override fun <T> withTransaction(readOnly: Boolean, action: () -> T): T {
        return withTransaction(readOnly, false, action)
    }

    override fun <T> withTransaction(readOnly: Boolean, requiresNew: Boolean, action: () -> T): T {
        // Serialize transactions on this data source (see the class Threading note). tryLock with a
        // bounded wait turns a stuck transaction into a fast error instead of a silent deadlock. The
        // lock is reentrant, so this thread's own nested withTransaction calls don't block.
        if (!txnLock.tryLock(LOCK_TIMEOUT_SEC, TimeUnit.SECONDS)) {
            error(
                "Failed to acquire the in-memory transaction lock within ${LOCK_TIMEOUT_SEC}s. " +
                    "This data source serializes transactions; another thread is holding the lock " +
                    "(a stuck or long-running transaction on the same in-mem data source)."
            )
        }
        try {
            return doWithTransaction(readOnly, requiresNew, action)
        } finally {
            txnLock.unlock()
        }
    }

    private fun <T> doWithTransaction(readOnly: Boolean, requiresNew: Boolean, action: () -> T): T {
        val currentTxn = currentThreadTxn.get()
        if (currentTxn != null && !requiresNew) {
            if (currentTxn.readOnly && !readOnly) {
                error("Write transaction can't be started from readOnly context")
            }
            return if (currentTxn.readOnly == readOnly) {
                // same mode: the action's writes belong to the enclosing transaction's scope
                action.invoke()
            } else {
                // a read-only action inside a write transaction: no undo bracket needed (read-only
                // never mutates), just track the mode so a nested write would be rejected.
                val nested = TxnState(readOnly)
                currentThreadTxn.set(nested)
                try {
                    action.invoke()
                } finally {
                    currentThreadTxn.set(currentTxn)
                }
            }
        }

        // start a new (top-level or requiresNew) transaction
        if (readOnly) {
            val newTxn = TxnState(readOnly = true)
            currentThreadTxn.set(newTxn)
            try {
                return action.invoke()
            } finally {
                currentThreadTxn.set(currentTxn)
            }
        }

        // a write transaction: bracket the action with an undo-log savepoint so it can be rolled back
        val newTxn = TxnState(readOnly = false)
        val savepoint = store.mark()
        currentThreadTxn.set(newTxn)
        try {
            val result = action.invoke()
            commit(currentTxn, requiresNew, savepoint)
            return result
        } catch (originalEx: Throwable) {
            // rollback: replay the inverse of every change this transaction made, restoring the store
            // to exactly its pre-savepoint state. requiresNew children that already committed made
            // their writes permanent (commitTo), so they are not in this range and survive.
            store.rollbackTo(savepoint)
            throw originalEx
        } finally {
            currentThreadTxn.set(currentTxn)
        }
    }

    private fun commit(parentTxn: TxnState?, requiresNew: Boolean, savepoint: Int) {
        val isDurableTopLevel = (parentTxn == null || parentTxn.readOnly) && !requiresNew
        if (isDurableTopLevel) {
            // A durable top-level write. If a platform transaction is active, enlist a resource (once,
            // before the first write's mark) and LEAVE the inverses in the log so the manager can roll
            // the whole doInTxn block back as a unit; the resource forgets them on platform commit.
            // Outside a platform transaction there is nothing that could roll this back, so forget the
            // inverses now (the changes are permanent).
            if (!enlistTxnRollback(savepoint)) {
                store.commitTo(savepoint)
            }
        } else {
            // Either a requiresNew child or a write nested in a write parent that committed: make its
            // writes permanent immediately. For requiresNew this is what lets the id-mapping insert
            // survive a rollback of the parent; the entries are a contiguous suffix, so commitTo only
            // drops this scope's inverses.
            store.commitTo(savepoint)
        }
    }

    /**
     * On the first durable write within a managed platform transaction, register an
     * [InMemTxnResource] capturing the undo-log mark *before* that write. Returns true if a platform
     * transaction is active (so the caller must keep its inverses for the manager-driven rollback),
     * false otherwise. Idempotent per transaction (keyed by this data source).
     */
    private fun enlistTxnRollback(savepoint: Int): Boolean {
        val txn = TxnContext.getTxnOrNull() ?: return false
        txn.getOrAddRes(this) { _, txnId -> InMemTxnResource(txnId, savepoint) }
        return true
    }

    /**
     * The in-mem source's participation in a platform transaction. Holds the undo-log mark as it was
     * before this transaction's first durable write; [rollback] replays the inverses back to it,
     * undoing every write the doInTxn block made. [commitPrepared]/[onePhaseCommit] forget those
     * inverses (the writes are permanent). requiresNew children already made their own writes permanent,
     * so they are outside this range either way.
     *
     * **Lock span.** Construction (on the first durable write, on the doInTxn thread) takes an extra
     * reentrant hold of [txnLock] and the terminal callback releases it, so the lock stays held across
     * the *whole* platform transaction — not just each [withTransaction] call. Without this, another
     * thread could splice its own writes into the shared undo-log between this transaction's brackets,
     * and a later commit/rollback (which acts on absolute log positions) would corrupt them. The manager
     * drives exactly one terminal outcome (commit or rollback) on the same thread, all synchronously; the
     * release is made exactly-once via [lockReleased] and guarded by [java.util.concurrent.locks.ReentrantLock.isHeldByCurrentThread]
     * so it can never throw or double-release, with [dispose] as a backstop.
     */
    private inner class InMemTxnResource(
        private val txnId: TxnId,
        private val savepoint: Int
    ) : TransactionResource {

        private val lockReleased = AtomicBoolean(false)

        init {
            // extra reentrant hold; balanced by releaseLock() in the terminal callback below
            txnLock.lock()
        }

        private fun releaseLock() {
            if (txnLock.isHeldByCurrentThread && lockReleased.compareAndSet(false, true)) {
                txnLock.unlock()
            }
        }

        override fun start() = Unit
        override fun end() = Unit
        override fun getName(): String = RESOURCE_NAME
        override fun getXid(): EcosXid = EcosXid.create(txnId, RESOURCE_BRANCH)
        override fun prepareCommit(): CommitPrepareStatus = CommitPrepareStatus.PREPARED
        override fun commitPrepared() {
            try {
                store.commitTo(savepoint)
            } finally {
                releaseLock()
            }
        }
        override fun onePhaseCommit() {
            try {
                store.commitTo(savepoint)
            } finally {
                releaseLock()
            }
        }
        override fun rollback() {
            try {
                store.rollbackTo(savepoint)
            } finally {
                releaseLock()
            }
        }
        override fun dispose() {
            // backstop: if no terminal callback ran (unexpected), still free the lock
            releaseLock()
        }
    }

    private class TxnState(
        val readOnly: Boolean
    )

    private companion object {
        private const val RESOURCE_NAME = "ecos-data-inmem"
        private val RESOURCE_BRANCH = RESOURCE_NAME.toByteArray()

        /** Max wait for the per-data-source transaction lock before failing fast instead of hanging. */
        private const val LOCK_TIMEOUT_SEC = 10L
    }
}
