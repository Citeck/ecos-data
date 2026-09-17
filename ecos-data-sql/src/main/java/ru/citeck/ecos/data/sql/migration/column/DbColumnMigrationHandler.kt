package ru.citeck.ecos.data.sql.migration.column

import io.github.oshai.kotlinlogging.KotlinLogging
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
import ru.citeck.ecos.data.sql.records.dao.mutate.RecMutAssocHandler
import ru.citeck.ecos.data.sql.repo.entity.DbEntity
import ru.citeck.ecos.data.sql.repo.find.DbFindPage
import ru.citeck.ecos.data.sql.repo.find.DbFindSort
import ru.citeck.ecos.data.sql.service.DbDataService
import ru.citeck.ecos.data.sql.service.DbDataServiceConfig
import ru.citeck.ecos.data.sql.service.DbDataServiceImpl
import ru.citeck.ecos.records2.RecordConstants
import ru.citeck.ecos.records2.predicate.model.Predicates
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

        // When this transition takes the attribute out of the association group, the links are
        // copied into the backup and removed through the ordinary path, with the remote
        // notification: an association may point at an entity in another application and a raw
        // DELETE would leave a dangling back-reference there.
        //
        // Ahead of the backup-column check below on purpose: the links live in `ed_associations`,
        // not in that column, so a DBA who dropped the backup has removed what the value transfer
        // reads - not the links' only chance to reach a backup of their own.
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

    override fun processBatch(ctx: DbBatchTaskContext, batch: DbBatchTaskBatch) {

        val params = DbColumnMigrationParams.from(ctx.getParams())
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
            // Conditional on the target still being null, so that a value written between the read
            // above and this write is not overwritten either. The whole batch shares the engine's
            // transaction, so this commits or rolls back with the cursor.
            val updated = service.updateByIdIfMatches(
                id,
                mapOf(params.targetColumn to null),
                mapOf(params.targetColumn to converted)
            )
            if (!updated) {
                // The user's own value arrived between the read above and this write, and the
                // conditional update declined to overwrite it: the same deliberate no-op as the
                // "target is not null" check above, reached from the other side of the race.
                ctx.skipped++
                continue
            }
            // After the write and only if it won: the conditional update is what claims the
            // record, so a value arriving between the read above and this point takes the row and
            // its links with it, instead of having ours added on top of it.
            val arrival = if (assocArrival) {
                createArrivedAssocs(params, service, id, row, converted)
            } else {
                Arrival.WHOLE
            }
            if (arrival == Arrival.NONE) {
                // Not one of the references this row named could be linked - for a child arrival
                // that means not one of them could be told it has a parent. Leaving the column
                // holding ids no association backs is the disagreement this branch exists to
                // remove, so the row is put back the way it was. That leaves the target cell as the
                // transfer found it and the original untouched in the backup, which is the same
                // state as a value that does not fit and is counted the same way.
                service.updateByIdIfMatches(
                    id,
                    mapOf(params.targetColumn to converted),
                    mapOf(params.targetColumn to null)
                )
                ctx.skipped++
                log.warn {
                    "Row $id of ${tableCtx.getTableRef().fullName}, attribute '${params.attId}': " +
                        "none of the references its value names could become a link, so the " +
                        "converted value was rolled back and nothing was carried over for this " +
                        "row. The original is in '${params.backupColumn}'"
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
     * For one record whose converted value has just been written into the column: the ids become
     * rows in `ed_associations`, and the column is then rewritten to hold what `DbRecordsMutateDao`
     * would have left there.
     *
     * **Through `createAssocs` and not a raw insert**, for the reason the departure removes links
     * the same way: an association may point at an entity owned by another application which keeps a
     * back-reference, so the peers are told ([notifyAssocsCreated]).
     *
     * **The column is rewritten rather than left holding the converted array**, because the two
     * differ in three ways: `createAssocs` deduplicates, so a value naming one reference twice
     * becomes one link and a column that still said two; the column caches only the first ten of a
     * multi-valued attribute where the converted array may hold any number; and the order has to be
     * `__index` order, which is what the attribute answers in. Re-reading is also the only form of
     * this that stays right if the record already had links of its own.
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
            // this is a repair case rather than a shape of the data. The converted value is in the
            // column either way; what is missing is the key an association is written under.
            log.warn {
                "Record $id of ${tableCtx.getTableRef().fullName} has no '${DbEntity.REF_ID}', so " +
                    "the '${params.attId}' links its converted value names could not be created"
            }
            return Arrival.WHOLE
        }
        val targetIds = when (converted) {
            is Collection<*> -> converted.mapNotNull { (it as? Number)?.toLong() }
            is Number -> listOf(converted.toLong())
            else -> emptyList()
        }
        if (targetIds.isEmpty()) {
            // An empty array converted into an empty array: the column and `ed_associations` agree
            // already, and there is nothing to link.
            return Arrival.WHOLE
        }
        val schemaCtx = tableCtx.getSchemaCtx()
        // A child link is two facts, and the second one can refuse. Anything it refuses must not
        // become a link either, or the record ends up with a child that does not know it.
        val linkableIds = if (params.targetChild) {
            adoptArrivedChildren(params, tableCtx, sourceId, targetIds) ?: emptyList()
        } else {
            targetIds
        }
        if (linkableIds.isEmpty()) {
            return Arrival.NONE
        }
        val added = schemaCtx.assocsService.createAssocs(
            sourceId,
            params.attId,
            // from the model, through the params - `ed_column_meta` records an AttributeType and
            // nothing about `child`, and a guess here is a link the model calls a child stored as
            // one that is not
            params.targetChild,
            linkableIds,
            migrationCreatorRefId(schemaCtx)
        )
        notifyAssocsCreated(params, tableCtx, sourceId, mapOf(params.targetChild to added))
        rewriteAssocColumnCache(
            params,
            service,
            id,
            // the value this transfer has just written is what the rewrite has to find in the
            // column for its own conditional update to fire
            row + (params.targetColumn to converted),
            sourceId
        )
        // Measured on distinct targets, because `createAssocs` is keyed on the target and a value
        // naming the same reference twice is one link and not half a transfer.
        return if (linkableIds.toHashSet().size < targetIds.toHashSet().size) {
            Arrival.PART
        } else {
            Arrival.WHOLE
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
         * Some became links and some were refused. The column agrees with `ed_associations`.
         */
        PART,

        /**
         * None could be linked. The caller rolls the column back.
         */
        NONE
    }

    /**
     * The other half of a child association: `_parent` and `_parentAtt` on each target, written
     * through the very
     * [ru.citeck.ecos.data.sql.records.dao.mutate.RecMutAssocHandler.updateParentRefOfChildren]
     * that `DbRecordsMutateDao` writes them with, so the two cannot drift.
     *
     * A link with `__child = true` whose target has no `_parent` makes three answers come out
     * wrong: a deleted child leaves the parent holding a link to it, a `_parent` predicate reads the
     * `__parent` column and finds nothing, and a child of a restricted parent becomes world-readable
     * because its `_parent` is empty.
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
     *    window does not churn.
     *  - **the record itself**, which the platform refuses as a recursive parent link.
     *  - **one the platform cannot address.** `EntityRef.valueOf` is subtractive enough that
     *    `user@example.com` parses as the non-existent source `user`; the ordinary write path
     *    refuses it by throwing, so linking it anyway would manufacture the undeletable record all
     *    over again.
     *
     * The per-target `catch` covers the records layer refusing a target, which is the shape above.
     * A failure raised by the **database** has already aborted the batch's transaction - there are
     * no savepoints - so carrying on changes nothing: the remaining statements fail too, the batch
     * rolls back whole and the error is booked on the task row. What this must not do is swallow
     * such a failure into one more counted row and report `DONE`, and it cannot: the counters live
     * in the transaction that rolled back. `Exception` rather than `Throwable`, because an
     * `OutOfMemoryError` is not a row that did not fit.
     */
    private fun adoptArrivedChildren(
        params: DbColumnMigrationParams,
        tableCtx: DbTableContext,
        sourceId: Long,
        targetIds: List<Long>
    ): List<Long>? {
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
            // deferring the row is not. The caller leaves the row alone with its original in the
            // backup, or leaves a parked link set parked; either way the work is still there to do.
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
                RecMutAssocHandler.updateParentRefOfChildren(
                    recordsService,
                    parentRef,
                    params.attId,
                    listOf(childRef),
                    add = true,
                    // A background transfer, not a user edit. Without these the child's `_modified`
                    // is rewritten and a change event emitted for every child of every row - a
                    // million of each on a million-row table, from a task nobody started.
                    disableEvents = true,
                    disableAudit = true
                )
                result.add(targetId)
            } catch (e: Exception) {
                if (e is InterruptedException) {
                    Thread.currentThread().interrupt()
                    throw e
                }
                log.warn(e) {
                    "$childRef could not be told that $parentRef is its parent through the " +
                        "'${params.attId}' migration, so it does not become a child link either. " +
                        "The original value stays in '${params.backupColumn}'"
                }
            }
        }
        return result
    }

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
            // The links coming back with `__child = true` also need their `_parent`/`_parentAtt`,
            // because the departure cleared exactly those two columns. Restoring only the
            // `ed_associations` rows would put the record back into the undeletable state from the
            // other side. Adopted before the first `createAssocs` of this record, so a child the
            // ordinary write path would refuse never becomes a link at all.
            val parkedChildren = parked.filter { it.child }
            val adopted = if (parkedChildren.isEmpty()) {
                emptySet()
            } else {
                val adoptable = adoptArrivedChildren(
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
                adoptable.toHashSet()
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
            val corrected = schemaCtx.assocBackupService.restoreAssocsMeta(sourceId, recreated)
            if (corrected < recreated.size) {
                // Not worth failing the task for - the links themselves are back - but a link that
                // kept the migration's own `Instant.now()`, system creator and `max + 1` index has
                // quietly lost its history, and no later pass would find it: the backup it came
                // from is consumed on the next line.
                log.warn {
                    "Record $sourceId of ${tableCtx.getTableRef().fullName} got ${recreated.size} " +
                        "'${params.attId}' links back from the association backup, but only " +
                        "$corrected of them could be given their original index, creation time and " +
                        "author back - the rest carry this migration's own metadata"
                }
            }
            notifyAssocsCreated(params, tableCtx, sourceId, addedByChild)
            // The links are back in the table that owns them, so the snapshot is spent. Keeping it
            // would let a later restore of the same registry row resurrect links this record has
            // since moved on from.
            schemaCtx.assocBackupService.consume(params.restoredColumnMetaId, sourceId)
            rewriteAssocColumnCache(params, service, id, row, sourceId)
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
     * The column of an assoc-like attribute caches only the first values, and after a restore that
     * cache has to say what `ed_associations` says.
     *
     * Ten for a multi-valued attribute and one for a single-valued one, the same numbers
     * `DbRecordsMutateDao` writes - ten being what `DbRecord` treats as "there may be more, go and
     * read them all".
     *
     * Conditional on the column still holding what this batch read, so a value written between the
     * read and this write is not overwritten: that mutation has already written a cache of its own.
     */
    private fun rewriteAssocColumnCache(
        params: DbColumnMigrationParams,
        service: DbDataService<DbEntity>,
        id: Long,
        row: Map<String, Any?>,
        sourceId: Long
    ) {
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
        service.updateByIdIfMatches(
            id,
            mapOf(params.targetColumn to row[params.targetColumn]),
            mapOf(params.targetColumn to newValue)
        )
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
