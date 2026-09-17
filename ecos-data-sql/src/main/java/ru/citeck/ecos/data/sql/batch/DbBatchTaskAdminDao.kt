package ru.citeck.ecos.data.sql.batch

import ru.citeck.ecos.context.lib.auth.AuthContext
import ru.citeck.ecos.data.sql.repo.find.DbFindPage
import ru.citeck.ecos.data.sql.repo.find.DbFindSort
import ru.citeck.ecos.records2.predicate.PredicateUtils
import ru.citeck.ecos.records2.predicate.model.AttributePredicate
import ru.citeck.ecos.records2.predicate.model.EmptyPredicate
import ru.citeck.ecos.records2.predicate.model.Predicate
import ru.citeck.ecos.records2.predicate.model.Predicates
import ru.citeck.ecos.records3.record.dao.AbstractRecordsDao
import ru.citeck.ecos.records3.record.dao.atts.RecordAttsDao
import ru.citeck.ecos.records3.record.dao.query.RecordsQueryDao
import ru.citeck.ecos.records3.record.dao.query.dto.query.RecordsQuery
import ru.citeck.ecos.records3.record.dao.query.dto.query.SortBy
import ru.citeck.ecos.records3.record.dao.query.dto.res.RecsQueryRes
import java.time.Instant

/**
 * The administrator view of one schema's `ed_batch_task` queue: a plain query-and-atts read
 * over [DbBatchTaskService.find], with `id`, `handler`, `table`, `status`, `processed`/`total`,
 * `error`, `errorCount` and `nextAttemptAt` exposed as record attributes.
 *
 * Deliberately query and atts only. The platform convention for a list with row actions
 * (`ApplyEcosPatchAction`, `CancelActivityRecordsDao`) is that each action is its own tiny
 * `AbstractRecordsDao` + `ValueMutateDao` with its own `sourceId`, rather than one class doing both
 * the listing and the mutation - see [DbBatchTaskCancelAction] and [DbBatchTaskRestartAction], which
 * are the only things that write, and which do so purely by delegating to [DbBatchTaskService.cancel]
 * and [DbBatchTaskService.restart]. Those two already carry and enforce the lifecycle rules;
 * this class does not re-derive any of them.
 *
 * One instance per schema, mirroring [DbBatchTaskService] itself: `sourceId` is supplied by the
 * caller so the Spring registration (see `DbRecordsVersionAutoConfig` for the
 * template) can give every schema's admin view its own source, the way it already does for
 * `record-version-$schema`.
 */
class DbBatchTaskAdminDao(
    private val sourceIdValue: String,
    private val batchTaskService: DbBatchTaskService
) : AbstractRecordsDao(),
    RecordsQueryDao,
    RecordAttsDao {

    companion object {
        const val ID = "batch-task"

        /**
         * The published record attribute names, mapped onto the physical column they read from.
         * Without this, a predicate or a sort written against the names this DAO itself publishes
         * (`status`, `errorCount`, ...) would be handed straight to [DbBatchTaskService.find] as-is,
         * which only recognises the raw `__`-prefixed [DbBatchTaskEntity] column names - so filtering
         * or sorting by `status` would silently match/order by nothing rather than fail loudly.
         */
        private val ATT_TO_COLUMN: Map<String, String> = mapOf(
            "id" to DbBatchTaskEntity.ID,
            "handler" to DbBatchTaskEntity.HANDLER,
            "table" to DbBatchTaskEntity.TABLE_ID,
            "status" to DbBatchTaskEntity.STATUS,
            "cursor" to DbBatchTaskEntity.CURSOR,
            "total" to DbBatchTaskEntity.TOTAL,
            "processed" to DbBatchTaskEntity.PROCESSED,
            "skipped" to DbBatchTaskEntity.SKIPPED,
            "failed" to DbBatchTaskEntity.FAILED,
            "error" to DbBatchTaskEntity.ERROR,
            "errorCount" to DbBatchTaskEntity.ERROR_COUNT,
            "nextAttemptAt" to DbBatchTaskEntity.NEXT_ATTEMPT_AT,
            "created" to DbBatchTaskEntity.CREATED,
            "creator" to DbBatchTaskEntity.CREATOR,
            "started" to DbBatchTaskEntity.STARTED,
            "finished" to DbBatchTaskEntity.FINISHED
        )

        /**
         * A clause against an attribute this DAO does not publish, expressed so that the **database**
         * decides it rather than the predicate optimizer. `id` is sequence-generated and positive on
         * every persisted row ([DbBatchTaskEntity.NEW_REC_ID] exists only in memory, before the
         * insert), so [NEVER_MATCHES] is false on every row and [ALWAYS_MATCHES] is true on every row.
         */
        private val NEVER_MATCHES: Predicate = Predicates.lt(DbBatchTaskEntity.ID, 0)

        /**
         * The dual of [NEVER_MATCHES] - see its doc.
         */
        private val ALWAYS_MATCHES: Predicate = Predicates.ge(DbBatchTaskEntity.ID, 0)

        /**
         * Renames every attribute-level clause of [predicate] onto its column, degrading instead of
         * failing when an attribute cannot be resolved - the same "one bad clause must not break the
         * whole query" rule the platform uses elsewhere.
         *
         * The degrade states the literal truth about an attribute this DAO does not publish: **no
         * row has it.** So a value comparison against it is false on every row and an `is empty`
         * check is true on every row, and `or(eq(status, "FAILED"), empty(unknownAtt))` listing the
         * whole queue is the right answer rather than a widening bug.
         *
         * **Both halves are real value predicates on purpose.** [Predicates.alwaysTrue] and
         * [Predicates.alwaysFalse] are the obvious spelling and are folded correctly by
         * [ru.citeck.ecos.records2.predicate.PredicateUtils.mapAttributePredicates] - but that fold
         * has a hole: it does not pass its own `filterEmptyComposite` down its recursive calls, so a
         * composite all of whose branches fold away returns `null` before reaching the
         * identity-constant branch, and `null` is then dropped by the enclosing composite. Measured:
         * `or(eq(unknownA, "x"), eq(unknownB, "y"))` listed the entire queue, and the same `OR`
         * nested in an `AND` vanished from it. A value predicate is invisible to that optimizer, so
         * it simply composes.
         *
         * Two "hardenings" that are worse, both measured: a uniform always-false folds
         * `and(eq(status, "FAILED"), empty(unknownAtt))` to "match nothing", and a uniform
         * always-true makes a flat `AND` match everything. All four shapes are pinned in
         * `DbBatchTaskAdminDaoTest`.
         */
        private fun mapPredicateToColumns(predicate: Predicate): Predicate {
            return PredicateUtils.mapAttributePredicates(predicate) { pred: AttributePredicate ->
                val column = ATT_TO_COLUMN[pred.getAttribute()]
                if (column == null) {
                    if (pred is EmptyPredicate) ALWAYS_MATCHES else NEVER_MATCHES
                } else {
                    val mapped = pred.copy<AttributePredicate>()
                    mapped.setAttribute(column)
                    mapped
                }
            } ?: Predicates.alwaysTrue()
        }

        /**
         * [RecordsQuery.sortBy] entries that name an unpublished attribute are dropped rather than
         * failing the query (the same "degrade, don't break" rule as [mapPredicateToColumns]); if
         * none are left, falls back to [DbBatchTaskService.find]'s own newest-first default so a
         * caller that names no sort - or only unresolvable ones - still gets a stable order rather
         * than an arbitrary one.
         */
        private fun mapSortToColumns(sortBy: List<SortBy>): List<DbFindSort> {
            val mapped = sortBy.mapNotNull { by ->
                ATT_TO_COLUMN[by.attribute]?.let { DbFindSort(it, by.ascending) }
            }
            return mapped.ifEmpty { listOf(DbFindSort(DbBatchTaskEntity.ID, false)) }
        }
    }

    constructor(batchTaskService: DbBatchTaskService) : this(ID, batchTaskService)

    override fun getId(): String {
        return sourceIdValue
    }

    override fun queryRecords(recsQuery: RecordsQuery): Any? {
        checkReadPermissions()
        val found = batchTaskService.find(
            predicate = mapPredicateToColumns(recsQuery.getPredicate()),
            sort = mapSortToColumns(recsQuery.sortBy),
            page = DbFindPage(recsQuery.page.skipCount, recsQuery.page.maxItems)
        )

        val result = RecsQueryRes<TaskRecord>()
        result.setRecords(found.entities.map { TaskRecord(it) })
        result.setTotalCount(found.totalCount)
        return result
    }

    override fun getRecordAtts(recordId: String): Any? {
        checkReadPermissions()
        val id = recordId.toLongOrNull() ?: return null
        return batchTaskService.getById(id)?.let { TaskRecord(it) }
    }

    /**
     * The read half of the guard [DbBatchTaskCancelAction] and [DbBatchTaskRestartAction] already
     * apply to the write half. This is not a harmless list: `handler` and `table` describe the
     * internal schema, and `error` is free text lifted straight out of a database exception, so it
     * routinely carries SQL fragments and column names. None of that belongs to a regular user who
     * can reach `records.query`.
     */
    private fun checkReadPermissions() {
        if (!AuthContext.isRunAsSystemOrAdmin()) {
            error("Permission denied")
        }
    }

    /**
     * The record attributes of one `ed_batch_task` row. A plain getter-per-attribute wrapper rather
     * than a generic JSON dump: the point of this view is that `errorCount` and `nextAttemptAt` are
     * named attributes with no other display anywhere,
     * so they are spelled out here rather than left to fall out of a dump of [DbBatchTaskDto].
     */
    class TaskRecord(private val dto: DbBatchTaskDto) {

        fun getId(): String = dto.id.toString()
        fun getHandler(): String = dto.handler
        fun getTable(): String = dto.table
        fun getStatus(): String = dto.status.name
        fun getCursor(): Long = dto.cursor
        fun getTotal(): Long = dto.total
        fun getProcessed(): Long = dto.processed
        fun getSkipped(): Long = dto.skipped
        fun getFailed(): Long = dto.failed
        fun getError(): String = dto.error
        fun getErrorCount(): Int = dto.errorCount
        fun getNextAttemptAt(): Instant = dto.nextAttemptAt
        fun getCreated(): Instant = dto.created
        fun getCreator(): String = dto.creator
        fun getStarted(): Instant = dto.started
        fun getFinished(): Instant = dto.finished
    }
}
