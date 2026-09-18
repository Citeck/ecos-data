package ru.citeck.ecos.data.sql.migration.column

import io.github.oshai.kotlinlogging.KotlinLogging
import ru.citeck.ecos.data.sql.columnmeta.DbBackupColumnNames
import ru.citeck.ecos.data.sql.columnmeta.DbColumnConversions
import ru.citeck.ecos.data.sql.columnmeta.DbColumnMetaDto
import ru.citeck.ecos.data.sql.columnmeta.DbColumnSemanticType
import ru.citeck.ecos.data.sql.context.DbTableContext
import ru.citeck.ecos.data.sql.dto.DbColumnDef
import ru.citeck.ecos.data.sql.dto.DbColumnIndexDef
import ru.citeck.ecos.data.sql.repo.entity.DbEntity
import ru.citeck.ecos.data.sql.service.DbDataService
import ru.citeck.ecos.data.sql.service.DbDataServiceConfig
import ru.citeck.ecos.data.sql.service.DbDataServiceImpl
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType
import java.time.Instant

/**
 * The synchronous half of a column type migration.
 *
 * Renames the old column into a backup, adds a fresh column of the target type, records both in the
 * column registry and queues the background transfer. Every step is O(1) metadata work, because this
 * runs inside a user's mutation - which is the whole reason it replaces `ALTER ... TYPE ... USING`,
 * a statement that rewrites the table under an exclusive lock while that user waits.
 *
 * One branch is not O(1): cancelling a still-undrained task whose attribute was leaving the
 * association group performs that departure here, so the backup holds the links as they were at the
 * moment the attribute left. See [cancelActiveTasksFor] for why that boundary cannot be deferred.
 *
 * **The old column is never dropped** - that is the user's plan B, and [DbColumnRestore] is it being
 * cashed in: when the attribute returns to a type one of its backups already holds, [tryRestore]
 * puts that backup back instead of adding an empty column, and the column it replaces becomes a
 * backup in its turn. Everything after that point is the same for both shapes.
 *
 * [tryRestore] is a step of its own rather than a branch inside [moveAside], because a matching
 * backup wins whether or not the shadow path is taken: a column the backend could cast where it
 * stands has to ask the same question first, or a round trip with a castable return leg would answer
 * from the lossy leg while the value sat in a backup of this very table.
 *
 * @param tableCtx the table being migrated, read for its physical column list. Must be the current
 *                 one: [moveAside] derives the backup name from the names already taken.
 * @param invalidateTableColumnsCache drops the cached column list of *this table's own*
 *                 `DbDataService`, which the schema context cannot reach - see [moveAside] - so the
 *                 caller, which is that data service, hands the reset in.
 */
class DbShadowColumnTransition(
    private val tableCtx: DbTableContext,
    private val invalidateTableColumnsCache: () -> Unit
) {

    companion object {
        private val log = KotlinLogging.logger {}
    }

    private val schemaCtx = tableCtx.getSchemaCtx()
    private val dataSourceCtx = schemaCtx.dataSourceCtx
    private val tableRef = tableCtx.getTableRef()

    /**
     * Only [cancelActiveTasksFor]'s departure uses this, and only on the rare branch where a task is
     * cancelled before it ran, so it is built on demand rather than with the transition. Backup
     * columns are deliberately not included: the departure reads `__ref_id` and nothing else.
     */
    private val tableDataService: DbDataService<DbEntity> by lazy {
        DbDataServiceImpl(
            DbEntity::class.java,
            DbDataServiceConfig.create {
                withTable(tableRef.table)
            },
            schemaCtx
        )
    }

    /**
     * The attribute is going back to a type one of its backups already holds, so the
     * values come out of that backup instead of out of a fresh empty column. Everything downstream
     * - the caches, the cancellation, the task - is the same as an ordinary transition; only the
     * params differ.
     *
     * **Asked before the strategy is chosen, not inside one of the strategies.** A matching backup
     * wins whichever strategy the pair would otherwise take. Asked only on the shadow path, it
     * would miss every round trip whose return leg is a pure cast - `DATE -> DATETIME`,
     * `NUMBER -> G_STR`, `X -> X[]` - and those are the common ones, because the *forward* leg of
     * such a pair is exactly what makes it lossy enough to have left a backup in the first place.
     *
     * Returns false having touched nothing when there is no backup to return to, or when the one
     * there is cannot be used ([DbColumnRestore.restore] decides every reason before the first
     * `ALTER`), so the caller can carry straight on with the ordinary ladder.
     *
     * Never called on a mock/preview run: a restore is performed as two renames plus two registry
     * rewrites, and the rewrites are exactly what a preview may not do - reporting the renames
     * while leaving the registry saying the opposite would describe a table that cannot be read.
     */
    fun tryRestore(
        expectedColumn: DbColumnDef,
        targetType: AttributeType,
        targetChild: Boolean,
        creator: String
    ): Boolean {
        val restoreParams = DbColumnRestore(tableCtx)
            .restoreLatest(expectedColumn, targetType, targetChild, creator)
            ?: return false
        invalidateColumnCaches()
        afterTransition(restoreParams)
        return true
    }

    /**
     * @param currentColumn the column as it physically is now.
     * @param expectedColumn the column definition the model asks for.
     * @param sourceType what the registry says the current column holds.
     * @param targetType the attribute type the model asks for.
     * @param mock a preview run (`runMigrations(mock = true)`). The DDL below is still issued -
     *        `dataSource.withSchemaMock` records it instead of executing it, and the mock
     *        rule is precisely "report the commands that would have been executed" - but nothing is
     *        written to `ed_column_meta` and no task is queued.
     */
    fun moveAside(
        currentColumn: DbColumnDef,
        expectedColumn: DbColumnDef,
        sourceType: DbColumnSemanticType,
        targetType: AttributeType,
        targetChild: Boolean,
        creator: String,
        mock: Boolean
    ) {

        val attId = expectedColumn.name
        val taken = tableCtx.getAllPhysicalColumns().mapTo(HashSet()) { it.name }
        val backupName = DbBackupColumnNames.build(
            attId = attId,
            attType = sourceType,
            multiple = currentColumn.multiple,
            maxBytes = dataSourceCtx.schemaDao.getMaxColumnNameBytes(),
            taken = taken
        )

        // 1. The old column moves aside. renameColumn also drops the single-column indexes that
        //    came with it: they are useless on a column nothing queries and they would
        //    slow every insert down for the rest of the table's life.
        dataSourceCtx.schemaDao.renameColumn(
            dataSourceCtx.dataSource,
            tableRef,
            currentColumn.name,
            backupName
        )
        // 2. A fresh column of the target type takes its place, empty - and deliberately without
        //    the model's index. A backend that honours `DbColumnDef.index` builds the btree right
        //    here (`DbSchemaDaoPg.addColumnsInSync`), and building one means a full heap scan of a
        //    table that may hold millions of rows, inside the user's mutation: exactly the
        //    table-length operation this whole transition exists to keep out of it, and on the
        //    "table too large" branch it would defeat the branch's entire purpose. There is nothing
        //    to index before the transfer runs anyway, the column being empty.
        //
        //    The other half of this decision is owned by the `column-migration` handler
        //    ([DbColumnMigrationParams.HANDLER_TYPE]): it creates the index in its `onFinish`, once
        //    the column is filled, gated on the attribute's own `index.enabled`. Without that step
        //    an indexed attribute would lose its index for good the first time it changed type -
        //    after this transition the column matches the model, so the ordinary "missing column"
        //    path that builds indexes never looks at it again.
        dataSourceCtx.schemaDao.addColumns(
            dataSourceCtx.dataSource,
            tableRef,
            listOf(DbColumnDef.Builder(expectedColumn).withIndex(DbColumnIndexDef.EMPTY).build())
        )

        val conversionClass = DbColumnConversions.classify(
            sourceType,
            currentColumn.multiple,
            targetType,
            expectedColumn.multiple
        )
        if (mock) {
            // a preview must reach and report the same decision a real run would, but it
            // writes no registry rows and queues no task. Nothing physically moved either, so the
            // caches below are not stale and must not be dropped.
            return
        }

        invalidateColumnCaches()

        // 3. Describe both, **each row staying with the data it describes** - the same convention
        //    [DbColumnRestore] follows, and the reason it has to be the same one is
        //    `ed_associations_backup.__column_meta_id`. That key names a registry row,
        //    and it is asked two questions at two different times: a departure parks a column's
        //    links under the row that describes that column, and a restore raises the links of the
        //    row it restored. Both answers are only ever about the same links if a row never stops
        //    describing the column whose data it describes.
        //
        //    So the row that described the live column is rewritten to describe that same column
        //    under its new backup name, and the **new** row is the one minted, for the new and
        //    empty column. Doing it the other way round - recycling the live row to describe the
        //    new column and minting a row for the old data - re-keys every link the recycled row
        //    owns, and the next transition of the attribute recycles it further away from them
        //    until no restore can find them again.
        //
        //    The order of the two writes is load-bearing in the same way [DbColumnRestore]'s
        //    `saveAll` is: the rewrite below frees `attId` under the unique index on
        //    `(table, columnName)` before the insert claims it.
        val now = Instant.now()
        val actualMeta = schemaCtx.columnMetaService.getByTableAndColumn(tableRef.table, attId)
        val backupMeta = schemaCtx.columnMetaService.save(
            actualMeta?.copy(
                columnName = backupName,
                // restated rather than inherited from the row, so that this rewrite describes the
                // column exactly as the minted row used to: the two are the same values - the
                // registry's arity is written from a DbColumnDef and `sourceType` is read from this
                // very row - and saying so here keeps the change to the row's *identity* alone
                attType = sourceType,
                multiple = currentColumn.multiple,
                backup = true,
                created = now,
                creator = creator
            ) ?: DbColumnMetaDto(
                // **Unreachable today**, and kept rather than turned into an `error(...)` because a
                // throw here aborts every record's save on the table. `moveColumnAside` returns
                // early when this table's registry map has no row for the column, that map is read
                // after `seedColumnMeta` has run, and `DbColumnMetaService` has no delete - so
                // `getByTableAndColumn` cannot answer null on any path that gets this far. If some
                // later change makes it reachable, minting a row is the safe answer: there is no
                // row to keep with the data, and a column no row described never had links parked
                // under an id.
                id = DbColumnMetaDto.NEW_REC_ID,
                table = tableRef.table,
                columnName = backupName,
                attId = attId,
                attType = sourceType,
                multiple = currentColumn.multiple,
                backup = true,
                created = now,
                creator = creator
            )
        )
        schemaCtx.columnMetaService.save(
            DbColumnMetaDto(
                id = DbColumnMetaDto.NEW_REC_ID,
                table = tableRef.table,
                columnName = attId,
                attId = attId,
                attType = DbColumnSemanticType.Model(targetType),
                multiple = expectedColumn.multiple,
                backup = false,
                created = now,
                creator = creator
            )
        )

        // A task is queued for **every** transition, class NONE included, and the two things it
        // carries are the reason:
        //  - the index step 2 stripped off the new column. Nothing else will ever build it: after
        //    this transition the column matches the model, so it never appears among the missing
        //    columns again. A class-NONE type change of an indexed attribute would otherwise lose
        //    that index permanently - a silent query regression, on a change that looks like the
        //    cheapest of them all.
        //  - the departure from the association group, which the handler's `prepare`
        //    performs. `ASSOC -> NUMBER` is class NONE and is exactly a departure.
        // A class-NONE task transfers no value - `processBatch` returns at once - so the cost of
        // queuing it is one row in `ed_batch_task` and one drain tick.
        val params = DbColumnMigrationParams(
            attId = attId,
            backupColumn = backupName,
            targetColumn = attId,
            sourceType = sourceType,
            sourceMultiple = currentColumn.multiple,
            targetType = targetType,
            targetMultiple = expectedColumn.multiple,
            targetChild = targetChild,
            // what step 2 above stripped off the new column, for the handler to put back once the
            // column is full
            targetIndexEnabled = expectedColumn.index.enabled,
            conversionClass = conversionClass,
            backupColumnMetaId = backupMeta.id
        )
        afterTransition(params)
        log.info {
            "Column '${currentColumn.name}' of ${tableRef.fullName} was moved to '$backupName' and a " +
                "$conversionClass transfer to ${targetType.name} was queued"
        }
    }

    /**
     * Both caches, and both for the same reason: the transition's DDL went straight through
     * DbSchemaDao, so nothing that caches a column list has heard about it. The schema-level reset
     * covers the schema's own system services; it does **not** cover this domain table's
     * DbDataService, which normally only learns about a foreign DDL change through the reactive
     * "column ... does not exist" handler - and that handler is not going to fire here,
     * because the very next read finds the pre-rename column list perfectly usable and silently
     * reads the wrong column set.
     */
    private fun invalidateColumnCaches() {
        schemaCtx.resetColumnsCache()
        invalidateTableColumnsCache()
    }

    /**
     * What both shapes of this transition - the ordinary one and a restore - do once
     * the columns have moved and the registry has been rewritten: cancel what the previous
     * transition of this attribute left running, and queue the transfer that finishes this one.
     *
     * The order is the contract. Cancelling first means this transition's own task is never a
     * candidate for its own cancellation.
     */
    private fun afterTransition(params: DbColumnMigrationParams) {
        // A repeated type change before the transfer finished cancels the active task.
        // Its `targetColumn` is by now a different physical column of a different type - it would
        // either fail row by row or, where the two types share a physical type, write values the
        // second transition deliberately left out. The partially transferred values it did write
        // are not lost by this: they went into the column that has just become this transition's
        // backup.
        cancelActiveTasksFor(params)
        schemaCtx.batchTaskService.queue(
            DbColumnMigrationParams.HANDLER_TYPE,
            tableRef.table,
            params.toObjectData()
        )
    }

    /**
     * Cancels every still-runnable `column-migration` task of this table that was queued for
     * [attId]: the column such a task was filling is no longer the column the model describes.
     *
     * Runs before the new task is queued, so this transition's own task is never a candidate.
     *
     * **A cancelled task's departure from the association group is performed here first** - unless
     * this transition is putting the attribute **back** into the group. The removal normally lives
     * in the handler's `prepare`, so cancelling before any drain tick reached it would leave the
     * links of an attribute that is no longer assoc-like alive in `ed_associations`, and leave the
     * backup registry row owning nothing for a later restore to find. It runs *before* the cancel:
     * should it throw, the task is still runnable and its own `prepare` is the second chance.
     *
     * **The exception.** When [newParams] restores the very row the cancelled task was going to park
     * under, those links are the attribute's live value again the moment this commits - they were
     * never removed, and nothing read them meanwhile. Parking them would make the task queued a line
     * below put them straight back, and until it ran the attribute would read empty for no reason.
     *
     * **"The attribute is assoc-like again" would be too coarse a condition.** A transition into a
     * *different* assoc-like type with no backup of its own - `ASSOC[] -> TEXT[]` undrained, then
     * `PERSON[]` - is an ordinary [moveAside]: the `bigint[]` column that arrives is empty, and the
     * old links are not its value in any sense. Skipping the departure there leaves `ed_associations`
     * holding a set nothing reads and every predicate still finds, and strands it: the next departure
     * parks it under whatever type the attribute wears by then, and a restore matches on
     * `(attType, multiple)` exactly. Hence the equality below rather than a test on the target type.
     *
     * **Why the copy may not be deferred onto a later background task.** `ed_associations_backup`
     * under a registry row has to hold the links *as they were when the attribute left the group*,
     * because that is the set a restore gives back. Deferring would move that boundary to whenever
     * the drain got round to it, and this sequence would corrupt it: `ASSOC -> TEXT` (queued, never
     * drained) -> `TEXT -> ASSOC` (that task cancelled) -> the user edits the links -> `ASSOC -> TEXT`
     * again. The deferred copy would write the second era's links into the first era's backup. The
     * price is a walk of the attribute's associations inside a user's mutation, on this branch only.
     *
     * Two facts a reader cannot derive from the code:
     *
     *  - **the two callers are guarded by different distributed locks** - a drain tick holds the
     *    batch task lock, a user's mutation the schema migration lock - so `prepare` and this
     *    cancellation can run the departure for the same `backupColumnMetaId` at once. Neither sees
     *    the other's uncommitted inserts, so the unique index aborts whichever commits second.
     *    Nothing is lost, but it is a constraint violation someone will see in a log.
     *  - **an administrator can cancel a task instead**, through `DbBatchTaskCancelAction`, and that
     *    path does *not* run the departure: a task cancelled before it was drained leaves its links
     *    live. The way back is `DbBatchTaskService.restart`, which revives the task so that `prepare`
     *    runs the departure with the original params.
     *
     * A task whose params cannot be read is left alone rather than guessed at: cancelling on a failed
     * parse would silently stop an unrelated transfer.
     */
    private fun cancelActiveTasksFor(newParams: DbColumnMigrationParams) {
        val attId = newParams.attId
        val batchTaskService = schemaCtx.batchTaskService
        for (task in batchTaskService.findByTable(tableRef.table)) {
            if (task.handler != DbColumnMigrationParams.HANDLER_TYPE || task.status.isFinal()) {
                continue
            }
            val taskParams = try {
                DbColumnMigrationParams.from(task.params)
            } catch (e: Exception) {
                log.error(e) {
                    "Batch task ${task.id} of ${tableRef.fullName} has unreadable column-migration " +
                        "params and was left as it is while '$attId' moved aside"
                }
                continue
            }
            if (taskParams.attId != attId) {
                continue
            }
            // the one case in which the links this task would have parked are the live value of
            // the column this transition is putting back - see the KDoc
            val restoringTheRowThisTaskWouldParkUnder =
                newParams.restoredColumnMetaId != DbColumnMigrationParams.NO_RESTORED_COLUMN &&
                    newParams.restoredColumnMetaId == taskParams.backupColumnMetaId
            if (!restoringTheRowThisTaskWouldParkUnder) {
                DbAssocGroupDeparture(tableDataService, dataSourceCtx.remoteActionsClient)
                    .runIfDeparture(taskParams)
            }
            if (batchTaskService.cancel(task.id)) {
                log.info {
                    "Batch task ${task.id} was cancelled: attribute '$attId' of ${tableRef.fullName} " +
                        "changed type again before its transfer finished. The values it " +
                        "had already written are in the backup this transition just created"
                }
            }
        }
    }
}
