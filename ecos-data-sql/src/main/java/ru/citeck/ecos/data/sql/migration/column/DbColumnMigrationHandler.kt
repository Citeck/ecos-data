package ru.citeck.ecos.data.sql.migration.column

import io.github.oshai.kotlinlogging.KotlinLogging
import ru.citeck.ecos.commons.data.ObjectData
import ru.citeck.ecos.context.lib.auth.AuthContext
import ru.citeck.ecos.context.lib.auth.AuthUser
import ru.citeck.ecos.data.sql.batch.DbBatchTaskBatch
import ru.citeck.ecos.data.sql.batch.DbBatchTaskContext
import ru.citeck.ecos.data.sql.batch.DbBatchTaskHandler
import ru.citeck.ecos.data.sql.columnmeta.DbColumnConversions
import ru.citeck.ecos.data.sql.context.DbDataSourceContext
import ru.citeck.ecos.data.sql.context.DbSchemaContext
import ru.citeck.ecos.data.sql.context.DbTableContext
import ru.citeck.ecos.data.sql.dto.DbColumnDef
import ru.citeck.ecos.data.sql.dto.DbColumnIndexDef
import ru.citeck.ecos.data.sql.dto.DbTableRef
import ru.citeck.ecos.data.sql.records.DbRecordsUtils
import ru.citeck.ecos.data.sql.records.assocs.DbAssocBackupDto
import ru.citeck.ecos.data.sql.records.assocs.DbAssocEntity
import ru.citeck.ecos.data.sql.records.assocs.DbAssocRefsDiff
import ru.citeck.ecos.data.sql.records.dao.atts.DbRecord
import ru.citeck.ecos.data.sql.records.dao.mutate.RecMutAssocHandler
import ru.citeck.ecos.data.sql.repo.entity.DbEntity
import ru.citeck.ecos.data.sql.repo.find.DbFindPage
import ru.citeck.ecos.data.sql.repo.find.DbFindSort
import ru.citeck.ecos.data.sql.service.DbDataService
import ru.citeck.ecos.data.sql.service.DbDataServiceConfig
import ru.citeck.ecos.data.sql.service.DbDataServiceImpl
import ru.citeck.ecos.records2.RecordConstants
import ru.citeck.ecos.records2.predicate.model.Predicates
import ru.citeck.ecos.records3.RecordsService
import ru.citeck.ecos.records3.record.atts.schema.ScalarType
import ru.citeck.ecos.webapp.api.entity.EntityRef
import java.util.concurrent.ConcurrentHashMap

/**
 * Carries values from a backup column into the column that replaced it, in id windows.
 *
 * **Writes only where the target cell is still null**, so a value written during the transfer always
 * wins. The accepted consequence: a value *cleared* during the transfer is written over with the old
 * one.
 *
 * **The backup column is not always where the value is.** An attribute leaving the association group
 * kept its real values in `ed_associations` and only the first ten of them in the column, so for
 * those transitions the transfer reads the links [prepare] has just parked - see [isAssocSourced].
 *
 * It also runs the other way round, when the task was queued by a restore: the links parked when the
 * restored column last left the association group go back into `ed_associations` before any value is
 * transferred, so a record that has its original back keeps it and only a record with nothing of its
 * own is filled from the intervening column.
 *
 * Everything here is idempotent, because the engine re-invokes [prepare] on every drain tick, may
 * re-enter [onFinish] after a failure, and never guarantees [onCancel] is reached at all.
 */
class DbColumnMigrationHandler(private val dataSourceCtx: DbDataSourceContext) : DbBatchTaskHandler {

    companion object {
        private val log = KotlinLogging.logger {}

        /**
         * How many target ids the column of a multi-valued assoc-like attribute caches: the same ten
         * `DbRecordsMutateDao` writes, and the same ten `DbRecord` reads as "there may be more than
         * these" before going to `ed_associations` for the full list.
         */
        private const val ASSOC_COLUMN_CACHE_SIZE = 10
    }

    /**
     * One raw service per table, for the same reason [ru.citeck.ecos.data.sql.batch.DbBatchTaskEngine]
     * caches its max-id readers: a [DbDataServiceImpl] caches the table's column list, and building
     * a fresh one per batch would re-read the schema for every id window. [prepare] drops the cache
     * once per run, so a run always starts from the table as it is now.
     */
    private val rawServices = ConcurrentHashMap<DbTableRef, DbDataService<DbEntity>>()

    override fun getType(): String {
        return DbColumnMigrationParams.HANDLER_TYPE
    }

    /**
     * The row estimate, and the place where the attribute's associations leave the association group
     * - once per run rather than once per batch, see [DbAssocGroupDeparture].
     *
     * The engine re-invokes this on every drain tick, so the estimate has to be bounded: the backend
     * answers from its statistics, and falls back to a row count capped well below the deferral
     * threshold when it has none. Never an unbounded `COUNT(*)`, but not always free either.
     */
    override fun prepare(ctx: DbBatchTaskContext): Long? {
        val params = DbColumnMigrationParams.from(ctx.getParams())
        val service = rawService(ctx.schemaCtx, ctx.getTableRef())
        // once per run, before anything reads the columns: another instance - or this one's own
        // synchronous phase - may have altered the table since the last tick
        service.resetColumnsCache()

        if (isSupersededTransition(ctx, params)) {
            // The status is put right so that an administrator sees why nothing happened, but
            // nothing here depends on it landing: `cancel` answers false for a task an
            // administrator has already cancelled, and a restart between this call and the
            // engine's compare-and-set would let the run through. What actually keeps a superseded
            // task from doing anything is that every callback asks the same question - see
            // [processBatch] and [onFinish].
            ctx.schemaCtx.batchTaskService.cancel(ctx.task.id)
            log.info {
                "Batch task ${ctx.task.id} of attribute '${params.attId}' in " +
                    "${ctx.getTableRef().fullName} was queued for a column that has since been " +
                    "replaced, so it is cancelled instead of run"
            }
            return null
        }

        // Before the first write of the run, and that is the whole point of the placement - see
        // [addChildBackReferenceColumns].
        if (params.targetChild) {
            addChildBackReferenceColumns(params, service)
        }

        // When this transition takes the attribute out of the association group, the links are
        // copied into the backup and removed through the ordinary path, with the remote
        // notification: an association may point at an entity in another application and a raw
        // DELETE would leave a dangling back-reference there.
        //
        // Ahead of the backup-column check below on purpose: the links live in `ed_associations`,
        // not in that column, so a DBA who dropped the backup has removed what the value transfer
        // reads - not the links' only chance to reach a backup of their own. Safe to keep that
        // order only because the check above has already ruled out the one case where a missing
        // backup column means the transition itself is gone.
        DbAssocGroupDeparture(service, dataSourceCtx.remoteActionsClient).runIfDeparture(params)

        val tableCtx = service.getTableContext()
        if (tableCtx.getAllPhysicalColumns().none { it.name == params.backupColumn }) {
            // The backup is gone - the only way that happens is a DBA removing it by hand, which is
            // their prerogative. There is nothing left to transfer and nothing to repair.
            log.warn {
                "Backup column '${params.backupColumn}' of ${tableCtx.getTableRef().fullName} no " +
                    "longer exists. Nothing to transfer for attribute '${params.attId}'"
            }
            return null
        }
        if (!DbColumnConversions.isTransferable(params.conversionClass)) {
            // Nothing will be transferred, so there is no total worth estimating. The task still
            // runs to completion, because `onFinish` is where the deferred index is built.
            return null
        }
        // The engine runs prepare inside a platform transaction but not inside a data source one,
        // and a DbSchemaDao call goes straight to a connection - a backend that reads the catalog
        // for its estimate would otherwise fail every prepare with "Transaction is not active".
        val estimate = dataSourceCtx.dataSource.withTransaction(readOnly = true) {
            dataSourceCtx.schemaDao.estimateRowsCount(dataSourceCtx.dataSource, tableCtx.getTableRef())
        }
        return if (estimate < 0) null else estimate
    }

    /**
     * `_parent` and `_parentAtt` are columns of [DbRecord.OPTIONAL_COLUMNS]: they do not exist until
     * the first mutation that gives a record a parent adds them, inside that user's transaction. A
     * table on which no attribute has ever been a child association therefore does not have them,
     * and a transfer into a **child** association is the announcement that they are about to be
     * needed - by [takeChildren], which writes them.
     *
     * **Here, and not where they are written.** [takeChildren] runs after the links, holding the
     * `ed_associations` rows this batch has just created; an `ALTER TABLE` from there asks for an
     * `AccessExclusiveLock` on the table while a user's mutation may be waiting for one of those
     * link rows, and PostgreSQL breaks that cycle by killing one of the two transactions. Measured,
     * on the arrival path:
     *
     * ```
     * Process 84 waits for ShareLock on transaction 499; blocked by process 85.
     * Process 85 waits for AccessExclusiveLock on relation 16543; blocked by process 84.
     * ```
     *
     * The other half of that measurement is why it is not left to the user's mutation either: with
     * the columns missing, whichever of the two needs them first pays, and the batch is holding row
     * locks by then whoever it is.
     *
     * **In `prepare` and not in the transition that queues the task**, for three reasons: the engine
     * calls `prepare` before the first batch of *every* run, so a task queued by an older build, a
     * restarted task and a resumed one are all covered, where the queuing transition covers only
     * tasks queued after this code shipped; `prepare` opens the run's first statement holding
     * nothing, so the `ALTER` can only wait and never deadlock; and the transition runs inside a
     * user's mutation, where the index that comes with `_parent` would be a table scan that user
     * waits for - the very thing [DbShadowColumnTransition] exists to keep out of it.
     *
     * Idempotent and free on the ordinary path: a table that has ever held a child has both columns
     * and nothing is issued. Through `runMigrations` rather than `DbSchemaDao.addColumns` directly,
     * because that is the path which takes the schema migration lock and re-reads the columns under
     * it - so a second instance preparing the same task adds nothing twice.
     *
     * **This covers the migrated table and only it**, which is narrower than "the transfer issues no
     * DDL". A child may live in any other table - `DbSchemaContext.getRecordsService` is a
     * `RecordsService` and not a dao context for exactly that reason - and the `_parent` written on
     * such a child still adds the column to *its* table, from inside the batch's transaction. What
     * would close that is knowing a target's table before the batch runs, and the data layer has no
     * such mapping: `ed_record_ref` holds an `EntityRef` and nothing about storage. The two ways out
     * are a platform change, not a change here - make these columns part of every records table, or
     * give the layer a reference-to-table mapping - and both are written up in the review notes.
     * Where types share the parent's table, which is the common layout, the question does not arise.
     */
    private fun addChildBackReferenceColumns(
        params: DbColumnMigrationParams,
        service: DbDataService<DbEntity>
    ) {
        val tableCtx = service.getTableContext()
        val taken = tableCtx.getAllPhysicalColumns().mapTo(HashSet()) { it.name }
        val missing = DbRecord.OPTIONAL_COLUMNS.filter {
            (it.name == RecordConstants.ATT_PARENT || it.name == RecordConstants.ATT_PARENT_ATT) &&
                !taken.contains(it.name)
        }
        if (missing.isEmpty()) {
            return
        }
        val tableRef = tableCtx.getTableRef()
        // Through the data service and not `DbSchemaDao.addColumns` directly: that is the path that
        // takes the schema migration lock and re-reads the columns under it, so a second instance
        // preparing the same task at the same moment adds nothing twice and does not fail the run
        // with "column already exists". Additive, like every schema reconciliation here - the
        // columns not named are left exactly as they are.
        service.runMigrations(missing, mock = false, diff = true)
        log.info {
            "Attribute '" + params.attId + "' of " + tableRef.fullName + " is becoming a child " +
                "association, so the columns its back-reference lives in were added before the " +
                "transfer wrote anything: " + missing.joinToString(", ") { it.name }
        }
    }

    /**
     * Whether a later transition of the same attribute has replaced this one, which makes
     * everything the task was queued to do wrong rather than merely late.
     *
     * A task is normally re-runnable however often the model has moved since: the params are a
     * snapshot and the job is "carry the values of *that* backup into *that* column". What breaks
     * that is a later transition of the same attribute, which gives the column a different meaning
     * and a backup of its own. A stale task that runs then removes links belonging to whatever the
     * attribute has become, and parks them over the backup that was written for what it used to be.
     *
     * **The newer task is the generation marker, and nothing about the columns is.** A column's
     * name is the attribute's own whatever happens to it. Its type can return to what the task
     * expects through a round trip. Even the registry row of the target column is reused - a
     * restore makes an older row live again under the same id - so an attribute that goes
     * `TEXT -> PERSON -> TEXT` presents the first task with exactly the target row it was queued
     * against. Task ids do not come back: every transition queues one, ids only go up, so a task
     * with a larger id for the same attribute is a later transition and there is nothing older
     * about it that can be true again.
     */
    private fun isSupersededTransition(ctx: DbBatchTaskContext, params: DbColumnMigrationParams): Boolean {
        return ctx.schemaCtx.batchTaskService.findByTable(ctx.task.table).any { other ->
            other.id > ctx.task.id &&
                other.handler == DbColumnMigrationParams.HANDLER_TYPE &&
                attIdOf(other.params) == params.attId
        }
    }

    /**
     * The attribute another task was queued for, or null when its params cannot be read - which is
     * not this task's business to fail over, so an unreadable neighbour simply does not count as a
     * later transition of anything.
     */
    private fun attIdOf(params: ObjectData): String? {
        return try {
            DbColumnMigrationParams.from(params).attId
        } catch (e: Exception) {
            null
        }
    }

    override fun processBatch(ctx: DbBatchTaskContext, batch: DbBatchTaskBatch) {

        val params = DbColumnMigrationParams.from(ctx.getParams())
        if (isSupersededTransition(ctx, params)) {
            // Asked again rather than trusted from `prepare`: the engine decides whether to run a
            // batch from the task's status, and a status is not a lock. An administrator
            // cancelling and restarting the task around `prepare` leaves it PENDING again by the
            // time the engine looks, and this is the callback that would then write into a column
            // that belongs to a later transition.
            return
        }
        val transfersValues = DbColumnConversions.isTransferable(params.conversionClass)
        val restoresAssocs = isAssocRestore(params)
        if (!transfersValues && !restoresAssocs) {
            // Nothing transferable in this pair; the task exists only so that `onFinish` rebuilds
            // the index the transition deferred. Returning before reading the window keeps the
            // counters honest: these rows were never weighed, so they are not counted at all.
            return
        }
        val service = rawService(ctx.schemaCtx, ctx.getTableRef())
        val tableCtx = service.getTableContext()
        val assocSourced = isAssocSourced(params)
        val assocArrival = isAssocArrival(params)

        // The backup column matters to a column-sourced value transfer and to nothing else: the
        // links a restore brings back, and the links an assoc-sourced transfer reads, both live in
        // `ed_associations_backup`, so a DBA who dropped the column has not taken them with it.
        val backupColumnExists = tableCtx.getAllPhysicalColumns().any { it.name == params.backupColumn }
        if (!backupColumnExists && !restoresAssocs && !assocSourced) {
            return
        }

        val rows = service.findRaw(
            Predicates.and(
                Predicates.gt(DbEntity.ID, batch.fromExclusive),
                Predicates.le(DbEntity.ID, batch.toInclusive)
            ),
            listOf(DbFindSort(DbEntity.ID, true)),
            DbFindPage.ALL,
            emptyList(),
            emptyList(),
            emptyList(),
            false
        ).entities

        if (restoresAssocs) {
            // Before the value transfer below, not after: a restored record's column is rewritten
            // with the links that came back, and the transfer only writes where the target column
            // is still null, so the order is what makes "restoration wins over conversion" true.
            restoreAssocsOfWindow(params, service, rows)
        }
        if (!transfersValues || (!backupColumnExists && !assocSourced)) {
            return
        }

        // One query for the whole window rather than one per record - see [isAssocSourced] for why
        // the links and not the column are the source at all.
        val parkedLinks = if (assocSourced) {
            tableCtx.getSchemaCtx().assocBackupService.findByColumnMeta(
                params.backupColumnMetaId,
                rows.mapNotNullTo(LinkedHashSet()) { it[DbEntity.REF_ID] as? Long }
            )
        } else {
            emptyMap()
        }

        for (row in rows) {
            val id = row[DbEntity.ID] as? Long ?: continue
            val source = if (assocSourced) {
                (row[DbEntity.REF_ID] as? Long)?.let { refId ->
                    parkedLinks[refId]?.map { it.targetId }
                }
            } else {
                row[params.backupColumn]
            }
            if (source == null) {
                // Nothing was ever stored here - no value in the backup column, or, for an
                // assoc-sourced transfer, no link of this record in the backup the departure
                // filled. The cell was empty before the type change and it is empty after it, so
                // this row's transfer is done: carrying an empty value across is still carrying it
                // across. `skipped` is for a row the transfer left alone on purpose, and there was
                // no decision to make here.
                ctx.processed++
                continue
            }
            if (row[params.targetColumn] != null) {
                // a value written since the change wins, always - so the transfer leaves this row
                // alone, which is what `skipped` means
                ctx.skipped++
                continue
            }
            val converted = try {
                DbColumnValueConverter.convert(source, params, tableCtx)
            } catch (e: Throwable) {
                if (e is InterruptedException) {
                    Thread.currentThread().interrupt()
                    throw e
                }
                // A value that does not fit is what class LOSSY promised could happen - an expected
                // outcome of changing a column's type, not a failure of the task. The target cell
                // is left as found and the whole original is in the backup, which is what `skipped`
                // says; `failed` is for a row the task could not account for at all.
                ctx.skipped++
                log.warn(e) {
                    "Row $id of ${tableCtx.getTableRef().fullName}, attribute '${params.attId}': " +
                        "the value did not survive the change from ${params.sourceType.asString()} " +
                        "to ${params.targetType.name}, so it was not carried over and the new " +
                        "column is left empty for this row. The original is in " +
                        "'${params.backupColumn}'"
                }
                continue
            }
            if (converted == null) {
                // The one way a non-null source converts to null: a narrowing keeps the first
                // element of an array that has none. Empty in, empty out - counted like the empty
                // source above.
                ctx.processed++
                continue
            }
            // The record is claimed by a write conditional on the target column still being null,
            // so that a value written between the read above and it is not overwritten either. The
            // whole batch shares the engine's transaction, so this commits or rolls back with the
            // cursor.
            //
            // **Where that write goes depends on whether links are involved**, and the difference
            // is not a detail: a transfer into an assoc-like type writes rows of `ed_associations`
            // as well, and the ordinary write path takes those **before** the record's row. Taking
            // them in the other order - claim, then links - closes a cycle with any mutation naming
            // the same reference, and PostgreSQL breaks it by killing one of the two, which can be
            // the user's. So an arrival writes its links first and claims afterwards
            // ([createArrivedAssocs]); a plain value has nothing to order and claims here.
            val arrival = if (assocArrival) {
                createArrivedAssocs(params, service, id, row, converted)
            } else if (
                service.updateByIdIfMatches(
                    id,
                    mapOf(params.targetColumn to null),
                    mapOf(params.targetColumn to converted)
                )
            ) {
                Arrival.WHOLE
            } else {
                Arrival.LOST
            }
            if (arrival == Arrival.LOST) {
                // The user's own value arrived between the read above and the claim, and the
                // conditional update declined to overwrite it: the same deliberate no-op as the
                // "target is not null" check above, reached from the other side of the race.
                // Whatever an arrival had written for this record is out again before this is
                // reached.
                ctx.skipped++
                continue
            }
            if (arrival == Arrival.NONE) {
                // Not one of the references this row named could be linked - for a child arrival
                // that means not one of them could be told it has a parent. So nothing was claimed
                // and nothing was written: the target cell is as the transfer found it and the
                // original is untouched in the backup, which is the same state as a value that
                // does not fit and is counted the same way.
                ctx.skipped++
                log.warn {
                    "Row $id of ${tableCtx.getTableRef().fullName}, attribute '${params.attId}': " +
                        "none of the references its value names could become a link, so nothing " +
                        "was carried over for this row. The original is in " +
                        "'${params.backupColumn}'"
                }
                continue
            }
            if (arrival == Arrival.PART) {
                // The column holds some of what the value named and the backup holds all of it, so
                // this is not a transferred row - and not a failure either. A cell that cannot be
                // carried over whole is one of the expected outcomes of changing a column's type;
                // what protects the administrator is the backup column, not a counter reading zero.
                // What it must not be counted as is `processed`, which would be indistinguishable
                // from a complete transfer - the warning below is what names the row.
                //
                // Not rolled back, unlike the branch above: the column and `ed_associations` were
                // brought into agreement by `createArrivedAssocs`, and undoing that would trade a
                // visible partial transfer for an invisible one.
                ctx.skipped++
                log.warn {
                    "Row $id of ${tableCtx.getTableRef().fullName}, attribute '${params.attId}': " +
                        "only some of the references its value names could become links, so the " +
                        "row was not carried over whole. The whole original value is in " +
                        "'${params.backupColumn}'"
                }
                continue
            }
            ctx.processed++
        }
    }

    override fun onFinish(ctx: DbBatchTaskContext) {
        if (isSupersededTransition(ctx, DbColumnMigrationParams.from(ctx.getParams()))) {
            // Same question as [processBatch], for the same reason: this builds an index on the
            // target column, and a superseded task's idea of that column belongs to a transition
            // that no longer exists.
            return
        }
        val params = DbColumnMigrationParams.from(ctx.getParams())
        buildDeferredIndex(ctx, params)
        if (!DbColumnConversions.isTransferable(params.conversionClass)) {
            log.info {
                "Column migration of '${params.attId}' in ${ctx.getTableRef().fullName} carried no " +
                    "value across: ${params.sourceType.asString()} -> ${params.targetType.name} has " +
                    "nothing transferable. The old values are kept in " +
                    "'${params.backupColumn}'"
            }
            return
        }
        log.info {
            "Column migration of '${params.attId}' in ${ctx.getTableRef().fullName} finished: " +
                "${ctx.processed} rows carried across, ${ctx.skipped} rows left as they were. " +
                "Changing the type of a column is a dangerous operation and some cells are " +
                "expected not to survive it: a value the new type cannot hold, or references that " +
                "cannot become links, are deliberately left where they were, and each one of those " +
                "is named in a warning of its own above. Every original is still in " +
                "'${params.backupColumn}' - that column, and not this count, is what makes the " +
                "change reversible"
        }
    }

    /**
     * Cancelling costs nothing on the **value transfer**: converted values stay, the backup column
     * is untouched, nothing has to be undone. The engine does not guarantee this is reached at all,
     * and that is safe for the same reason.
     *
     * **The restore half is different.** A task queued by [DbColumnRestore] carries a
     * [DbColumnMigrationParams.restoredColumnMetaId], and the links under it are out of
     * `ed_associations` and out of the column cache - `ed_associations_backup` is the record's only
     * copy. Cancelling leaves the attribute reading empty (or, below ten links, reading a stale
     * column cache that no journal filter matches) until something puts them back. Two things do:
     * `DbBatchTaskService.restart`, and the attribute returning to the type the links left.
     *
     * "Returning to the type" is literal: a backup is matched on `(attType, multiple)` exactly, and
     * `DbDataServiceImpl.writeColumnMeta` can relabel a live row's type before it departs, so the
     * links come back under the type their row wore when it left - not always the one they were
     * created under. An `ASSOC`-era set parked under a row since relabelled `PERSON[]` comes back on
     * a return to `PERSON[]`. What no cancel can do is put them out of reach.
     */
    override fun onCancel(ctx: DbBatchTaskContext) {
        val params = DbColumnMigrationParams.from(ctx.getParams())
        if (isAssocRestore(params)) {
            log.warn {
                "Column migration of '${params.attId}' in ${ctx.getTableRef().fullName} was " +
                    "cancelled after ${ctx.processed} rows while it was restoring associations. " +
                    "The links it had not reached yet stay in the association backup under column " +
                    "meta ${params.restoredColumnMetaId} and the attribute reads without them " +
                    "until this task is restarted or the attribute returns to this type again"
            }
            return
        }
        log.info {
            "Column migration of '${params.attId}' in ${ctx.getTableRef().fullName} was cancelled " +
                "after ${ctx.processed} rows. Converted values stay, originals stay in " +
                "'${params.backupColumn}'"
        }
    }

    /**
     * Builds the index the transition deliberately did not build.
     *
     * [DbShadowColumnTransition] hands `addColumns` a definition with the index disabled, because a
     * backend that honours it builds the btree right there - a full heap scan inside the user's
     * mutation, on the very branch whose purpose is to keep long operations out of it. Nothing else
     * will ever build it: after the transition the column matches the model, so it never appears
     * among the missing columns again, and an indexed attribute that changed type would lose its
     * index permanently - a silent query regression on exactly the large tables the branch exists
     * for.
     *
     * Here is the right place: the column is full, and this runs in the background rather than in a
     * user's mutation. It is also the only irreversible thing this handler does, so it is gated
     * twice - on the model asking for an index at all, and on the table not already having one,
     * because `onFinish` can be re-entered after a failed completion write and a second
     * `CREATE INDEX` would leave two identical indexes behind.
     */
    private fun buildDeferredIndex(ctx: DbBatchTaskContext, params: DbColumnMigrationParams) {
        if (!params.targetIndexEnabled) {
            return
        }
        val service = rawService(ctx.schemaCtx, ctx.getTableRef())
        val column = service.getTableContext().getColumnByName(params.targetColumn)
        if (column == null) {
            // the column this task was filling is not there any more - a later transition moved it
            // aside in its turn, and that transition's own task carries the index intention now
            log.info {
                "Column '${params.targetColumn}' of ${ctx.getTableRef().fullName} no longer " +
                    "exists, so the index deferred for attribute '${params.attId}' is not built"
            }
            return
        }
        dataSourceCtx.dataSource.withTransaction(readOnly = false) {
            dataSourceCtx.schemaDao.createColumnIndexIfMissing(
                dataSourceCtx.dataSource,
                ctx.getTableRef(),
                DbColumnDef.Builder(column).withIndex(DbColumnIndexDef(true)).build()
            )
        }
    }

    /**
     * Whether the value this transfer carries has to be read from the **links** rather than from the
     * backup column.
     *
     * A multi-valued assoc-like column is a cache of the first ten target ids and nothing more.
     * A transfer that read it would hand a record with fifteen links ten of them and count the row
     * `processed` - a silent loss reported as success.
     *
     * The links are read from `ed_associations_backup` and not from the live table, because by the
     * time any batch runs the live rows are gone: [prepare] performs the departure once per run.
     * The backup is also what survives a retry or a resumed task.
     *
     * Two conditions, each excluding a real transition:
     *
     *  - **a departure** ([DbAssocGroupDeparture.isDeparture]). An attribute that stays assoc-like
     *    keeps its links live and nothing was parked to read. An `ENTITY_REF` source is excluded by
     *    the same condition: its column is the value, not a cache of it, and it has no rows in
     *    `ed_associations` at all.
     *  - **a multi-valued source**. A single-valued assoc column caches one id, so it is not
     *    truncated and it is exactly what the attribute answered before the change. Should
     *    `ed_associations` still hold several links for it - what an earlier narrowing leaves behind
     *    - the column holds the first by `__index`, which is the one value the attribute ever
     *    answered with.
     *
     * The target's multiplicity is deliberately not a third condition: both sources agree on which
     * element a narrowing keeps, since both are ordered by `__index`, and excluding it would tie
     * this branch to the converter's narrowing rule staying "the first element".
     */
    private fun isAssocSourced(params: DbColumnMigrationParams): Boolean {
        return DbAssocGroupDeparture.isDeparture(params) && params.sourceMultiple
    }

    /**
     * Whether this task has to bring an attribute's associations back out of the backup.
     *
     * Both halves are needed: a transition that restored nothing carries no snapshot to read, and a
     * target that does not keep its values in `ed_associations` has nothing to put back there.
     */
    private fun isAssocRestore(params: DbColumnMigrationParams): Boolean {
        return params.restoredColumnMetaId != DbColumnMigrationParams.NO_RESTORED_COLUMN &&
            DbRecordsUtils.isStoredInAssocsTable(params.targetType)
    }

    /**
     * Whether this transfer has to put the values it converts into `ed_associations` as well as into
     * the column - a text or reference value becoming an association.
     *
     * **The target alone is not the condition**, and the source half is what keeps this from
     * corrupting data. [DbAssocGroupDeparture.isAssocLike] asks of the *source* exactly what a
     * departure asks, so the two are mirror images: a transition either takes an attribute out of
     * `ed_associations` or brings it in, never both.
     *
     *  - **an assoc-like source already has its links.** Only an arity change reaches a task, and it
     *    leaves the live rows untouched. Creating them again would be worse than wasted work:
     *    `createAssocs` matches an existing link on `(source, attribute, target, child)`, so a child
     *    assoc becoming `PERSON[]` would match nothing and insert a second row per link with the
     *    opposite `__child` - a duplicated association rather than a missing one.
     *  - **a `CONTENT` or `RAW` source** is not assoc-like either. A raw column's bytes were never
     *    accounted for under this attribute, so whatever the converter makes of them is a genuine
     *    arrival; `CONTENT` never reaches a transfer at all.
     *
     * A restore is not a third case. Its own links come back through [restoreAssocsOfWindow], which
     * runs first and leaves the record's column non-null, so the value transfer skips that record
     * before it gets here. What is left for this branch on a restore is the rows the restored column
     * had nothing for, filled from the intervening column.
     */
    private fun isAssocArrival(params: DbColumnMigrationParams): Boolean {
        return DbRecordsUtils.isStoredInAssocsTable(params.targetType) &&
            !DbAssocGroupDeparture.isAssocLike(params.sourceType)
    }

    /**
     * For one record whose converted value names references: the ids become rows in
     * `ed_associations` and **then** the record's row is claimed, holding what `DbRecordsMutateDao`
     * would have left in the column.
     *
     * **Through `createAssocs` and not a raw insert**, for the reason the departure removes links
     * the same way: an association may point at an entity owned by another application which keeps a
     * back-reference, so the peers are told ([notifyAssocsCreated]).
     *
     * **The column holds what `ed_associations` holds, not the converted array**, because the two
     * differ in three ways: `createAssocs` deduplicates, so a value naming one reference twice
     * becomes one link and a column that still said two; the column caches only the first ten of a
     * multi-valued attribute where the converted array may hold any number; and the order has to be
     * `__index` order, which is what the attribute answers in. Re-reading is also the only form of
     * this that stays right if the record already had links of its own.
     *
     * **The links go in before the claim**, which is the order every ordinary mutation uses
     * (`DbRecordsMutateDao` writes a record's links and saves the record afterwards). Claiming
     * first would take the same two things the other way round, and a user naming the same
     * reference - which is what somebody re-entering a value they can no longer see does - would
     * close a cycle on them that PostgreSQL breaks by killing one of the two transactions. The
     * price of the ordinary order is an undo when the claim does not hold
     * ([undoLinksWrittenForRecord]), and it is the cheaper price by far.
     *
     * Idempotent: a re-processed window finds the column non-null and skips the record long before
     * this is reached, and `createAssocs` would skip every link the record already has anyway.
     */
    private fun createArrivedAssocs(
        params: DbColumnMigrationParams,
        service: DbDataService<DbEntity>,
        id: Long,
        row: Map<String, Any?>,
        converted: Any
    ): Arrival {
        val tableCtx = service.getTableContext()
        val sourceId = row[DbEntity.REF_ID] as? Long
        if (sourceId == null) {
            // Every row of a domain table has one - it is written on insert and never cleared - so
            // this is a repair case rather than a shape of the data. What is missing is the key an
            // association is written under, so the converted value is all this row can be given.
            log.warn {
                "Record $id of ${tableCtx.getTableRef().fullName} has no '${DbEntity.REF_ID}', so " +
                    "the '${params.attId}' links its converted value names could not be created"
            }
            return claimWithConvertedValue(params, service, id, row, converted)
        }
        val targetIds = when (converted) {
            is Collection<*> -> converted.mapNotNull { (it as? Number)?.toLong() }
            is Number -> listOf(converted.toLong())
            else -> emptyList()
        }
        if (targetIds.isEmpty()) {
            // An empty array converted into an empty array: there is nothing to link, and the
            // column is all there is to write.
            return claimWithConvertedValue(params, service, id, row, converted)
        }
        val schemaCtx = tableCtx.getSchemaCtx()
        // A child link is two facts, and the second one can refuse. Anything it refuses must not
        // become a link either, or the record ends up with a child that does not know it - so which
        // of them may be taken is established first, by reading. The taking itself waits until the
        // links are in, because a child's row is the third thing a user's mutation takes and it
        // takes it last ([takeChildren]).
        val adoptable = if (params.targetChild) {
            childrenThisRecordMayTake(params, tableCtx, sourceId, targetIds)
        } else {
            null
        }
        val linkableIds = if (params.targetChild) {
            adoptable?.linkable ?: emptyList()
        } else {
            targetIds
        }
        if (linkableIds.isEmpty()) {
            // Nothing is written at all in this case, so there is nothing to claim and nothing to
            // take back - not even the `_parent` of a child, since none was adoptable.
            return Arrival.NONE
        }
        val created = schemaCtx.assocsService.createAssocs(
            sourceId,
            params.attId,
            // from the model, through the params - `ed_column_meta` records an AttributeType and
            // nothing about `child`, and a guess here is a link the model calls a child stored as
            // one that is not
            params.targetChild,
            linkableIds,
            migrationCreatorRefId(schemaCtx)
        )
        // The second fact of a child link, now that the first one is written.
        val adoptedChildren = adoptable?.let { takeChildren(params, it) }
        val refused = adoptedChildren?.refused ?: emptyList()
        var added = created
        var linkedIds = linkableIds
        if (refused.isNotEmpty()) {
            // A child that refused its back-reference is not a child link either, and its link was
            // created a moment ago rather than found, so taking it out puts the record back where
            // the ordinary write path would have left it.
            val refusedIds = refused.toHashSet()
            val toRemove = created.filter { it in refusedIds }
            if (toRemove.isNotEmpty()) {
                schemaCtx.assocsService.removeAssocs(sourceId, params.attId, toRemove, true)
            }
            added = created.filter { it !in refusedIds }
            linkedIds = linkableIds.filter { it !in refusedIds }
        }
        if (linkedIds.isEmpty()) {
            // Every one of them refused, so this record ends where it would have ended had none
            // been adoptable: no link, no claim and the original still in the backup.
            return Arrival.NONE
        }
        // The claim, and the record's first and only write of this transfer: `converted` is never
        // put in the column on its own, because what belongs there is what `ed_associations` now
        // holds - deduplicated, capped at the cache size and in `__index` order - and one write is
        // one lock rather than two.
        if (!claimTheRecord(params, service, id, row, sourceId)) {
            undoLinksWrittenForRecord(
                params,
                tableCtx,
                sourceId,
                mapOf(params.targetChild to added),
                adoptedChildren
            )
            return Arrival.LOST
        }
        // After the claim: a peer must not be told about a link that is about to be taken out again.
        notifyAssocsCreated(params, tableCtx, sourceId, mapOf(params.targetChild to added))
        // Measured on distinct targets, because `createAssocs` is keyed on the target and a value
        // naming the same reference twice is one link and not half a transfer.
        return if (linkedIds.toHashSet().size < targetIds.toHashSet().size) {
            Arrival.PART
        } else {
            Arrival.WHOLE
        }
    }

    /**
     * The claim for an arrival that turned out to have no links to write - an unresolvable record,
     * or a value naming nothing. There is no `ed_associations` row to take first, so the record's
     * row is the only thing to take, and the converted value is what goes in it.
     */
    private fun claimWithConvertedValue(
        params: DbColumnMigrationParams,
        service: DbDataService<DbEntity>,
        id: Long,
        row: Map<String, Any?>,
        converted: Any
    ): Arrival {
        val claimed = service.updateByIdIfMatches(
            id,
            mapOf(params.targetColumn to row[params.targetColumn]),
            mapOf(params.targetColumn to converted)
        )
        return if (claimed) {
            Arrival.WHOLE
        } else {
            Arrival.LOST
        }
    }

    /**
     * How much of one row's value became links - a distinction the counters alone cannot make,
     * because [WHOLE] and [PART] both end with a value in the column and only one of them is a
     * transferred row.
     *
     * [PART] exists so that a row whose value named three references of which one became a link is
     * not reported as `processed`: its whole original is in the backup and only a third of it is in
     * the column, and `processed` would be indistinguishable from a complete transfer. Such a row is
     * counted `skipped` and named in a warning instead. It is deliberately **not** a failure - a
     * cell that cannot be carried over whole is an expected outcome of changing a column's type, and
     * what protects the administrator is the backup column rather than a counter reading zero.
     */
    private enum class Arrival {
        /**
         * Every reference the value named is a link now, or there was nothing to link.
         */
        WHOLE,

        /**
         * Somebody else wrote this record between the moment the window was read and the claim, so
         * the transfer left it alone. Anything the arrival had already written for it - links, and
         * the `_parent` of a child - is out again by the time this is returned.
         */
        LOST,

        /**
         * Some became links and some were refused. The column agrees with `ed_associations`.
         */
        PART,

        /**
         * None could be linked. The caller rolls the column back.
         */
        NONE
    }

    /**
     * The read half of a child association's second fact: which of the targets this record may take
     * as its children, and which of those still have to be told. **Nothing is written here**, and
     * that is the whole point of the split - the writing is [takeChildren], and it runs after the
     * links.
     *
     * A link with `__child = true` whose target has no `_parent` makes three answers come out
     * wrong: a deleted child leaves the parent holding a link to it, a `_parent` predicate reads the
     * `__parent` column and finds nothing, and a child of a restricted parent becomes world-readable
     * because its `_parent` is empty. So a target that cannot be taken must not become a link
     * either - and deciding that needs a read and not a write, which is what lets the write wait.
     *
     * @return the targets that may become links, or **null** when the back-reference cannot be
     *         written at all - not the same answer as "none of them qualified". A caller that gets
     *         null must create no links and leave what it was reading from where it is; an empty
     *         list means every target was refused on its merits.
     *
     * Targets are left out for the reasons the ordinary write path also refuses them:
     *
     *  - **it already has a different parent.** A record has one `_parent`, so claiming it would
     *    replace one, and the previous parent would then lose the link from its child attribute - a
     *    migration silently deleting a link a user made.
     *  - **it is already ours under a different attribute.** `_parent` and `_parentAtt` are one
     *    column each, so a record is a child under exactly one attribute. Linking it under a second
     *    one leaves, after the child is deleted, a link to a record that no longer exists.
     *  - **it is already ours under this very attribute.** No mutation at all, so a re-processed
     *    window does not churn - such a target is linkable but not one [takeChildren] writes.
     *  - **the record itself**, which the platform refuses as a recursive parent link.
     *  - **one the platform cannot address.** `EntityRef.valueOf` is subtractive enough that
     *    `user@example.com` parses as the non-existent source `user`; the ordinary write path
     *    refuses it by throwing, so linking it anyway would manufacture the undeletable record all
     *    over again.
     *
     * The per-target `catch` covers the records layer refusing to answer for a target, which is the
     * last shape above. `Exception` rather than `Throwable`, because an `OutOfMemoryError` is not a
     * row that did not fit.
     */
    private fun childrenThisRecordMayTake(
        params: DbColumnMigrationParams,
        tableCtx: DbTableContext,
        sourceId: Long,
        targetIds: List<Long>
    ): AdoptableChildren? {
        val schemaCtx = tableCtx.getSchemaCtx()
        val table = tableCtx.getTableRef().table
        val recordsService = schemaCtx.getRecordsService(table)
        if (recordsService == null) {
            // A drain reaching a table whose records dao this process has not wired yet: the engine
            // hangs its drain on the scheduler and nothing orders that after the daos are up, so
            // this is reachable rather than theoretical.
            //
            // **No links are created here.** Creating them without the back-reference is precisely
            // the defect this function exists to prevent - a child that does not know it has a
            // parent leaves the parent holding a link to it after it is deleted, answers nothing to
            // a `_parent` predicate and reads as world-readable - and it is irreversible, where
            // deferring the row is not. Because this half is a read, the caller learns it here,
            // before it has written anything at all: it leaves the row alone with its original in
            // the backup, or leaves a parked link set parked; either way the work is still there to
            // do.
            log.warn {
                "No records service is registered for ${tableCtx.getTableRef().fullName}, so the " +
                    "'${params.attId}' child links cannot be given their " +
                    "'${RecordConstants.ATT_PARENT}' back-reference and are not created at all"
            }
            return null
        }
        val parentRef = schemaCtx.recordRefService.getEntityRefById(sourceId)
        val refsById = schemaCtx.recordRefService.getEntityRefsByIdsMap(targetIds)
        val result = ArrayList<Long>(targetIds.size)
        // the ones that do not name this record as their parent yet, as opposed to the ones that do
        // already: only these are written, and only these have to be given back if the pass is undone
        val toTell = LinkedHashMap<Long, EntityRef>()
        for (targetId in targetIds) {
            val childRef = refsById[targetId] ?: continue
            if (EntityRef.isEmpty(childRef)) {
                continue
            }
            if (childRef == parentRef) {
                // `processParentAfterMutation` raises "Recursive parent link for record" for this,
                // so the ordinary write path does not hold it either.
                log.warn {
                    "$parentRef names itself under '${params.attId}', which cannot be a child " +
                        "association. The link is not created - the original value stays in " +
                        "'${params.backupColumn}'"
                }
                continue
            }
            try {
                val currentParent = EntityRef.valueOf(
                    recordsService.getAtt(childRef, RecordConstants.ATT_PARENT + ScalarType.ID.schema).asText()
                )
                if (currentParent == parentRef) {
                    val currentParentAtt = recordsService.getAtt(
                        childRef,
                        RecordConstants.ATT_PARENT_ATT
                    ).asText()
                    if (currentParentAtt == params.attId) {
                        result.add(targetId)
                        continue
                    }
                    log.warn {
                        "$childRef is already a child of $parentRef under '$currentParentAtt', and " +
                            "a record has one '${RecordConstants.ATT_PARENT_ATT}', so it cannot " +
                            "also be its child under '${params.attId}'. The link is not created - " +
                            "the original value stays in '${params.backupColumn}'"
                    }
                    continue
                }
                if (EntityRef.isNotEmpty(currentParent)) {
                    log.warn {
                        "$childRef is already a child of $currentParent, so $parentRef does not " +
                            "take it over through the '${params.attId}' migration. The link is not " +
                            "created either - the original value stays in '${params.backupColumn}'"
                    }
                    continue
                }
                result.add(targetId)
                toTell[targetId] = childRef
            } catch (e: Exception) {
                if (e is InterruptedException) {
                    Thread.currentThread().interrupt()
                    throw e
                }
                log.warn(e) {
                    "It could not be established whether $childRef may become a child of " +
                        "$parentRef through the '${params.attId}' migration, so it does not become " +
                        "a child link. The original value stays in '${params.backupColumn}'"
                }
            }
        }
        return AdoptableChildren(recordsService, parentRef, result, toTell)
    }

    /**
     * The write half: `_parent` and `_parentAtt` on every child that does not name this record yet,
     * written through the very
     * [ru.citeck.ecos.data.sql.records.dao.mutate.RecMutAssocHandler.updateParentRefOfChildren]
     * that `DbRecordsMutateDao` writes them with, so the two cannot drift.
     *
     * **After the links, not before**, and this is the ordering the whole split exists for. A child
     * association puts a **third** row in play - the child's - and a user adding the same child
     * takes the three in the order `link -> record's row -> child's row`: `setMutationAtts` creates
     * the link, `dataService.save` writes the record, and `processAssocsAfterMutation` writes the
     * child's `_parent` last. Writing `_parent` before the link would take the first two of those
     * the other way round and close a cycle PostgreSQL breaks by killing one of the two
     * transactions - and the killed one can be the user's. Taking the link first leaves the link
     * itself as the first thing both want, and it orders them before either touches a child's row.
     *
     * The price is that a refusal now arrives with the link already created, so a refused target is
     * reported back ([AdoptedChildren.refused]) and its caller takes that link out again.
     *
     * A failure raised by the **database** has already aborted the batch's transaction - there are
     * no savepoints - so carrying on changes nothing: the remaining statements fail too, the batch
     * rolls back whole and the error is booked on the task row. What this must not do is swallow
     * such a failure into one more counted row and report `DONE`, and it cannot: the counters live
     * in the transaction that rolled back. `Exception` rather than `Throwable`, because an
     * `OutOfMemoryError` is not a row that did not fit.
     */
    private fun takeChildren(
        params: DbColumnMigrationParams,
        adoptable: AdoptableChildren
    ): AdoptedChildren {
        val written = LinkedHashMap<Long, EntityRef>()
        val refused = ArrayList<Long>()
        for ((targetId, childRef) in adoptable.toTell) {
            try {
                RecMutAssocHandler.updateParentRefOfChildren(
                    adoptable.recordsService,
                    adoptable.parentRef,
                    params.attId,
                    listOf(childRef),
                    add = true,
                    // A background transfer, not a user edit. Without these the child's `_modified`
                    // is rewritten and a change event emitted for every child of every row - a
                    // million of each on a million-row table, from a task nobody started.
                    disableEvents = true,
                    disableAudit = true
                )
                written[targetId] = childRef
            } catch (e: Exception) {
                if (e is InterruptedException) {
                    Thread.currentThread().interrupt()
                    throw e
                }
                refused.add(targetId)
                log.warn(e) {
                    "$childRef could not be told that ${adoptable.parentRef} is its parent through " +
                        "the '${params.attId}' migration, so it does not stay a child link either " +
                        "- the link just created for it is taken out again and the original value " +
                        "stays in '${params.backupColumn}'"
                }
            }
        }
        return AdoptedChildren(adoptable.parentRef, written, refused)
    }

    /**
     * What [childrenThisRecordMayTake] found: every target that may become a child link, and -
     * separately - the ones that do not name this record as their parent yet, which are the only
     * ones [takeChildren] writes and the only ones an undo has to give back. A child that already
     * named this record as its parent keeps it either way.
     */
    private class AdoptableChildren(
        val recordsService: RecordsService,
        val parentRef: EntityRef,
        val linkable: List<Long>,
        val toTell: Map<Long, EntityRef>
    )

    /**
     * What [takeChildren] wrote, and what it could not: a child whose row refused the back-reference
     * is not a child link either, so the link its caller has already created for it has to go.
     */
    private class AdoptedChildren(
        val parentRef: EntityRef,
        /** by target id, because an undo has to ask whether the record still links to each one */
        val written: Map<Long, EntityRef>,
        val refused: List<Long>
    )

    /**
     * The `ed_record_ref` id stamped on a link this handler creates, resolved the same way
     * [ru.citeck.ecos.data.sql.records.dao.DbRecordsDaoCtx.getOrCreateUserRefId] resolves it.
     *
     * A background drain runs as the system user: whoever changed the type is not knowable here. A
     * restore has a better answer and uses it - the links it puts back carry their original creator
     * out of the backup. An arrival has no original to carry, the values having been text until now.
     */
    private fun migrationCreatorRefId(schemaCtx: DbSchemaContext): Long {
        val user = AuthContext.getCurrentUser().ifBlank { AuthUser.ANONYMOUS }
        return schemaCtx.recordRefService.getOrCreateIdByEntityRef(schemaCtx.authoritiesApi.getPersonRef(user))
    }

    /**
     * For the records of one id window: the links that left with this column when it last went into
     * a backup are put back into `ed_associations` - with the `__index`, `__created` and `__creator`
     * they left with - the backup of each record that got them back is spent, and the column's cache
     * of the first values is rewritten from what `ed_associations` now holds.
     *
     * Driven from the table's rows rather than from the parked links, because a record **deleted**
     * while the attribute was of the other type has nothing to attach links to. Its rows never
     * appear in any window, so it is passed over by construction and its backup left where it is.
     *
     * Idempotent: `createAssocs` skips a link the record already has, and `consume` leaves nothing
     * for a second pass to find.
     */
    private fun restoreAssocsOfWindow(
        params: DbColumnMigrationParams,
        service: DbDataService<DbEntity>,
        rows: List<Map<String, Any?>>
    ) {
        val tableCtx = service.getTableContext()
        val schemaCtx = tableCtx.getSchemaCtx()
        var restoredRecords = 0
        var restoredLinks = 0
        for (row in rows) {
            val id = row[DbEntity.ID] as? Long ?: continue
            val sourceId = row[DbEntity.REF_ID] as? Long ?: continue
            val parked = schemaCtx.assocBackupService.findByColumnMeta(params.restoredColumnMetaId, sourceId)
            if (parked.isEmpty()) {
                continue
            }
            // Write only where the attribute has nothing, the rule the value transfer follows one
            // method below. A restore runs in the background, and between the moment the column
            // came back and the moment this window reaches the record, the user can have given the
            // attribute a value of their own - through the ordinary mutation path, with every
            // notification that implies. Laying the parked snapshot on top of it would add links
            // the record no longer has, and the user would have no way to tell where they came from.
            //
            // A read cannot establish that on its own, though: the user can be writing *right now*,
            // their rows are invisible to this transaction until they commit, and their link and a
            // parked one for another target collide on no unique key.
            //
            // So this read is **not** what makes the decision safe - [claimTheRecord] is, and it
            // runs after the links are written rather than before. This one only spares the work
            // for a record that plainly has a value already, which is the common case and the one
            // worth not doing the work for.
            if (schemaCtx.assocsService.getTargetAssocs(sourceId, params.attId, DbFindPage.FIRST)
                    .entities.isNotEmpty()
            ) {
                // Spent, not kept. A snapshot is offered exactly once - by the restore of the
                // column it was parked under - and here the offer is declined because the record
                // has a value of its own. Leaving the rows would key a stale generation to a
                // registry row that departs again on the next type change, and the restore after
                // that would hand the record both: the link it chose and the one it replaced a
                // round trip ago. A snapshot the restore never reached - the task an administrator
                // cancelled - is untouched by this and stays waiting, which is what keeps it
                // reachable by every later return to the type.
                schemaCtx.assocBackupService.consume(params.restoredColumnMetaId, sourceId)
                log.info {
                    "Record $sourceId of ${tableCtx.getTableRef().fullName} already has " +
                        "'${params.attId}' links of its own, so the ${parked.size} link(s) that " +
                        "were waiting for it in the association backup are dropped rather than " +
                        "restored on top of them"
                }
                continue
            }
            // The ordinary path, not a raw insert, for the same reason the departure used it: an
            // association may point at an entity owned by another application which keeps a
            // back-reference, and a link appearing without that application hearing about it is as
            // inconsistent as one disappearing without it. Grouped by the child flag because
            // `createAssocs` takes one per call.
            //
            // What that path cannot carry is the rest of the link: it stamps every row with
            // `Instant.now()`, the creator id it is handed and an `__index` of `max + 1`. All three
            // have to survive - `__index` is the user's own ordering - so the rows actually created
            // are corrected from the snapshot below, in the same transaction and before the column
            // cache is rewritten, because that cache is read in `__index` order.
            //
            // The links coming back with `__child = true` are checked against their
            // `_parent`/`_parentAtt` and given them where they have none. A departure leaves the two
            // columns alone, so the usual answer here is "this record is already its parent, under
            // this very attribute" and nothing is written - but two shapes need the write: a child
            // released by the departure of `1.73.0`, whose columns that version cleared and which no
            // upgrade puts back, and a child re-parented while the links were parked, which is
            // refused rather than taken. Which children may be taken is **read** before the first
            // `createAssocs` of this record, so a child the ordinary write path would refuse never
            // becomes a link at all; the `_parent` itself is written after them ([takeChildren]),
            // because that is the order a user's mutation takes the same three rows in.
            val parkedChildren = parked.filter { it.child }
            var adoptableChildren: AdoptableChildren? = null
            val adopted = if (parkedChildren.isEmpty()) {
                emptySet()
            } else {
                val adoptable = childrenThisRecordMayTake(
                    params,
                    tableCtx,
                    sourceId,
                    parkedChildren.map { it.targetId }
                )
                if (adoptable == null) {
                    // The back-reference cannot be written at all - this table's records dao is not
                    // up yet. The parked links are this record's only copy, so nothing is created
                    // and nothing is consumed: the next tick, with the dao wired, finds them where
                    // they are. Skipping the record whole rather than restoring its plain links
                    // alone keeps that retry a repeat rather than a half-done job.
                    log.warn {
                        "Record $sourceId of ${tableCtx.getTableRef().fullName} has " +
                            "${parkedChildren.size} '${params.attId}' child links waiting in the " +
                            "association backup, but they cannot be given their " +
                            "'${RecordConstants.ATT_PARENT}' back-reference yet, so the restore of " +
                            "this record is left for a later tick"
                    }
                    continue
                }
                adoptableChildren = adoptable
                adoptable.linkable.toHashSet()
            }
            if (adopted.size < parkedChildren.size) {
                log.warn {
                    "Record $sourceId of ${tableCtx.getTableRef().fullName} got " +
                        "${adopted.size} of its ${parkedChildren.size} '${params.attId}' child " +
                        "links back from the association backup: the rest name records that have " +
                        "since become children of somebody else, and taking them over would delete " +
                        "a link their present parent holds. Those links are not restored"
                }
            }
            val addedByChild = LinkedHashMap<Boolean, List<Long>>()
            val recreated = ArrayList<DbAssocBackupDto>(parked.size)
            for ((child, links) in parked.groupBy { it.child }) {
                val linkable = if (child) {
                    links.filter { it.targetId in adopted }
                } else {
                    links
                }
                if (linkable.isEmpty()) {
                    continue
                }
                val added = schemaCtx.assocsService.createAssocs(
                    sourceId,
                    params.attId,
                    child,
                    linkable.map { it.targetId },
                    // provisional, and corrected by restoreAssocsMeta below along with the index
                    // and the creation time; one link's author is a better guess than the system
                    // user for the moment in between
                    linkable.first().creator
                )
                addedByChild[child] = added
                val addedTargets = added.toHashSet()
                linkable.filterTo(recreated) { it.targetId in addedTargets }
            }
            // The second fact of a child link, after the links themselves: a user putting the same
            // child back names the link first and the child's row last, and a restore writing the
            // child's row first would hold one end of that pair each.
            val adoptedChildren = adoptableChildren?.let { takeChildren(params, it) }
            val refused = adoptedChildren?.refused ?: emptyList()
            if (refused.isNotEmpty()) {
                // A child that refused its back-reference is not a child link either. Its link was
                // created by this pass a moment ago, so taking it out leaves the record exactly
                // where a refusal during the read would have left it - and the snapshot, spent
                // below, keeps the whole original for the administrator either way.
                val refusedIds = refused.toHashSet()
                val children = addedByChild[true] ?: emptyList()
                val toRemove = children.filter { it in refusedIds }
                if (toRemove.isNotEmpty()) {
                    schemaCtx.assocsService.removeAssocs(sourceId, params.attId, toRemove, true)
                    addedByChild[true] = children.filter { it !in refusedIds }
                    recreated.removeIf { it.child && it.targetId in refusedIds }
                }
            }
            val corrected = schemaCtx.assocBackupService.restoreAssocsMeta(sourceId, recreated)
            if (corrected < recreated.size) {
                // Not worth failing the task for - the links themselves are back - but a link that
                // kept the migration's own `Instant.now()`, system creator and `max + 1` index has
                // quietly lost its history, and no later pass would find it: the backup it came
                // from is consumed once the record is claimed.
                log.warn {
                    "Record $sourceId of ${tableCtx.getTableRef().fullName} got ${recreated.size} " +
                        "'${params.attId}' links back from the association backup, but only " +
                        "$corrected of them could be given their original index, creation time and " +
                        "author back - the rest carry this migration's own metadata"
                }
            }
            // Now, and not before the links: this is where the record is taken from anyone else
            // writing it, and the order is what keeps the two out of each other's way.
            if (!claimTheRecord(params, service, id, row, sourceId)) {
                undoLinksWrittenForRecord(params, tableCtx, sourceId, addedByChild, adoptedChildren)
                log.info {
                    "Record $sourceId of ${tableCtx.getTableRef().fullName} was written by somebody " +
                        "else while its '${params.attId}' links were being restored, so the " +
                        "${parked.size} link(s) that came back were taken out again and the " +
                        "association backup keeps them for the next pass"
                }
                continue
            }
            notifyAssocsCreated(params, tableCtx, sourceId, addedByChild)
            // The links are back in the table that owns them and the record is claimed, so the
            // snapshot is spent. Keeping it would let a later restore of the same registry row
            // resurrect links this record has since moved on from.
            schemaCtx.assocBackupService.consume(params.restoredColumnMetaId, sourceId)
            restoredRecords++
            restoredLinks += parked.size
        }
        if (restoredRecords > 0) {
            log.info {
                "Attribute '${params.attId}' of ${tableCtx.getTableRef().fullName} got $restoredLinks " +
                    "links of $restoredRecords records back from the association backup under column " +
                    "meta ${params.restoredColumnMetaId}"
            }
        }
    }

    /**
     * Tells the applications holding a back-reference that the links exist - the mirror image of the
     * notification [DbAssocGroupDeparture] sends when they leave. Both callers, a restore and an
     * arrival, create links some other application may hold the other end of.
     *
     * A peer that cannot be told must not fail the task: the links are in `ed_associations` either
     * way, a restore's backup is already spent, an arrival's column already claimed, and a later
     * tick has nothing left to re-notify from - so recording the failure where an administrator
     * sees it is all that is left.
     */
    private fun notifyAssocsCreated(
        params: DbColumnMigrationParams,
        tableCtx: DbTableContext,
        sourceId: Long,
        addedByChild: Map<Boolean, List<Long>>
    ) {
        val remoteActionsClient = dataSourceCtx.remoteActionsClient ?: return
        val refService = tableCtx.getSchemaCtx().recordRefService
        val diff = addedByChild.mapNotNull { (child, targetIds) ->
            if (targetIds.isEmpty()) {
                null
            } else {
                DbAssocRefsDiff(params.attId, refService.getEntityRefsByIds(targetIds), emptyList(), child)
            }
        }
        if (diff.isEmpty()) {
            return
        }
        // Resolved before the try, so that a dangling reference id - which throws from here and is a
        // bug in this schema, not in any peer - is not reported as an unreachable application.
        val sourceRef = refService.getEntityRefById(sourceId)
        val creator = AuthContext.getCurrentUser()
        try {
            remoteActionsClient.updateRemoteAssocs(
                currentCtx = tableCtx,
                sourceRef = sourceRef,
                creator = creator,
                assocsDiff = diff
            )
        } catch (e: Exception) {
            if (e is InterruptedException) {
                Thread.currentThread().interrupt()
                throw e
            }
            // "Remote" is the usual case, not the only one: when a target belongs to this
            // application but to another schema context, DbRecordsRemoteActionsClientImpl executes
            // the update in process and writes to that schema's `ed_associations` itself.
            log.error(e) {
                "Record $sourceId of ${tableCtx.getTableRef().fullName} got '${params.attId}' " +
                    "links in '${DbAssocEntity.MAIN_TABLE}', but the back-references of the " +
                    "applications holding them could not be updated - either the application was " +
                    "unreachable or the update itself failed. Their back-references are now stale " +
                    "and need to be reconciled by hand"
            }
        }
    }

    /**
     * The one step that takes the record for a restore: the column of an assoc-like attribute
     * caches the first target ids, and rewriting that cache from what `ed_associations` now holds is
     * both the last thing the restore owes the record **and** the thing that claims it. Returns
     * whether the claim held.
     *
     * **It is a conditional write, and the condition is the record's own column as this batch read
     * it.** The transfer one method above claims its rows the same way and for the same reason: a
     * read cannot say that nothing will be written between it and the write it authorises. Anyone
     * who gave this attribute a value since the window was read has changed this column, and the
     * claim fails; anyone about to has still to write the record's row, so they wait here and then
     * find their own version stale and take their links back with their transaction.
     *
     * The one value a user's write can leave this column at is the one it already held, and that
     * takes naming the same references in the same order - in which case their link and a parked one
     * are the same row of `ed_associations`, and its unique index has decided the matter before this
     * is reached: the loser's batch takes a duplicate key, rolls back and tries again, and the retry
     * finds the link there and declines the snapshot the ordinary way.
     *
     * **After the links and not before.** The ordinary write path creates the links of a record and
     * saves the record afterwards (`DbRecordsMutateDao`), and a restore that claimed the record
     * first would be taking the same two things in the opposite order: with both wanting the same
     * link - a user re-entering the value they could no longer see - the two transactions close a
     * cycle on the record's row and that link's unique index entry, and PostgreSQL breaks it by
     * killing one of them. Taking them in the same order as everybody else costs an undo when the
     * claim fails ([undoLinksWrittenForRecord]) and cannot deadlock.
     *
     * A single-valued target keeps one id, a multi-valued one the first
     * [ASSOC_COLUMN_CACHE_SIZE] in `__index` order - which is why the meta is corrected first.
     *
     * The other caller is an **arrival**, which has claimed the row before it created any link and
     * calls this only for the cache; there the answer is known in advance and ignored.
     */
    private fun claimTheRecord(
        params: DbColumnMigrationParams,
        service: DbDataService<DbEntity>,
        id: Long,
        row: Map<String, Any?>,
        sourceId: Long
    ): Boolean {
        val maxCachedValues = if (params.targetMultiple) {
            ASSOC_COLUMN_CACHE_SIZE
        } else {
            1
        }
        val targetIds = service.getTableContext().getSchemaCtx().assocsService.getTargetAssocs(
            sourceId,
            params.attId,
            DbFindPage(0, maxCachedValues)
        ).entities.map { it.targetId }
        val newValue: Any? = if (params.targetMultiple) {
            targetIds
        } else {
            targetIds.firstOrNull()
        }
        return service.updateByIdIfMatches(
            id,
            mapOf(params.targetColumn to row[params.targetColumn]),
            mapOf(params.targetColumn to newValue)
        )
    }

    /**
     * Puts the record back the way this pass found it, for a record whose claim did not hold. Both
     * halves of the transfer that write links use it - a restore putting parked links back, and an
     * arrival turning text into links.
     *
     * Only what this pass wrote is taken out: the links it created - by id, so a link the record
     * held before is not touched - and the `_parent` back-references it wrote, but not the ones a
     * child already had. Deleted outright rather than moved to the deleted-associations table,
     * because these links never existed as far as anyone outside this transaction is concerned: the
     * peers holding the other end have not been told about them either, `notifyAssocsCreated` being
     * the next thing after the claim.
     *
     * Nothing else is undone because nothing else was written: the claim is the record's only write
     * and it did not fire. A restore's snapshot therefore stays parked and an arrival's original
     * stays in the backup column, so the next tick tries again against whatever the record has
     * become by then - and finds a value of its own there, most likely, and leaves it alone the
     * ordinary way.
     *
     * One thing is not given back: `createAssocs` clears the `ed_associations_deleted` row of a link
     * it re-creates, and this does not put it back. That table is written and cleaned by
     * `DbAssocsService` and read by nothing in the platform, so what is lost is a record of a removal
     * and not anything a caller can observe - and re-creating it would mean teaching `createAssocs`
     * to report what it cleaned, for a table nobody asks.
     */
    private fun undoLinksWrittenForRecord(
        params: DbColumnMigrationParams,
        tableCtx: DbTableContext,
        sourceId: Long,
        addedByChild: Map<Boolean, List<Long>>,
        adoptedChildren: AdoptedChildren?
    ) {
        val schemaCtx = tableCtx.getSchemaCtx()
        val added = addedByChild.values.flatten()
        if (added.isNotEmpty()) {
            schemaCtx.assocsService.removeAssocs(sourceId, params.attId, added, true)
        }
        if (adoptedChildren == null || adoptedChildren.written.isEmpty()) {
            return
        }
        // Only the children the record no longer holds a link to. Writing a back-reference this pass
        // did not need to write is possible - the deciding read happens before the links, and
        // somebody else can make the same child this record's between the two - and for such a child
        // the link that justifies the back-reference is *theirs* and still there. Clearing it then
        // would leave exactly what this whole undo exists to prevent, only from the other side: a
        // record holding a link to a child that does not know it.
        val stillLinked = schemaCtx.assocsService.getTargetAssocs(
            sourceId,
            params.attId,
            DbFindPage.ALL
        ).entities.mapTo(HashSet()) { it.targetId }
        val toRelease = adoptedChildren.written.filterKeys { it !in stillLinked }.values.toList()
        if (toRelease.isEmpty()) {
            return
        }
        val recordsService = schemaCtx.getRecordsService(tableCtx.getTableRef().table) ?: return
        try {
            RecMutAssocHandler.releaseChildren(
                recordsService,
                toRelease,
                disableEvents = true,
                disableAudit = true
            )
        } catch (e: Exception) {
            if (e is InterruptedException) {
                Thread.currentThread().interrupt()
                throw e
            }
            // The links are gone either way, so the child is no longer anyone's through this
            // attribute and the parent holds nothing pointing at it. What is left is a `_parent`
            // naming a record that does not claim it, which an administrator can see and clear -
            // and failing the whole batch over it would undo the windows that did succeed.
            log.warn(e) {
                "The '${RecordConstants.ATT_PARENT}' this restore wrote on " +
                    "${toRelease.size} record(s) could not be taken back after the " +
                    "restore of ${adoptedChildren.parentRef} was undone, so they still name it as " +
                    "their parent while holding no '${params.attId}' link from it"
            }
        }
    }

    /**
     * A service over the same table that can see backup columns. Every other service in the process
     * must not, which is why this one is built here rather than taken from the schema context.
     */
    private fun rawService(schemaCtx: DbSchemaContext, tableRef: DbTableRef): DbDataService<DbEntity> {
        return rawServices.computeIfAbsent(tableRef) {
            DbDataServiceImpl(
                DbEntity::class.java,
                DbDataServiceConfig.create {
                    withTable(tableRef.table)
                    withIncludeBackupColumns(true)
                },
                schemaCtx
            )
        }
    }
}
