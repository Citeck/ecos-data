package ru.citeck.ecos.data.sql.batch

import io.github.oshai.kotlinlogging.KotlinLogging
import ru.citeck.ecos.commons.data.ObjectData
import ru.citeck.ecos.commons.json.Json
import ru.citeck.ecos.context.lib.auth.AuthContext
import ru.citeck.ecos.context.lib.auth.AuthUser
import ru.citeck.ecos.data.sql.context.DbSchemaContext
import ru.citeck.ecos.data.sql.repo.find.DbFindPage
import ru.citeck.ecos.data.sql.repo.find.DbFindQuery
import ru.citeck.ecos.data.sql.repo.find.DbFindRes
import ru.citeck.ecos.data.sql.repo.find.DbFindSort
import ru.citeck.ecos.data.sql.service.DbDataService
import ru.citeck.ecos.data.sql.service.DbDataServiceConfig
import ru.citeck.ecos.data.sql.service.DbDataServiceImpl
import ru.citeck.ecos.records2.predicate.model.Predicate
import ru.citeck.ecos.records2.predicate.model.Predicates
import ru.citeck.ecos.txn.lib.TxnContext
import java.time.Instant

/**
 * The background job queue of one schema.
 *
 * Deliberately dumb: it stores and reads rows and enforces the lifecycle rules that belong to a
 * row rather than to a run (you cannot cancel a finished task, a restart forgives attempts but
 * keeps the cursor). Everything about *running* a task lives in
 * [ru.citeck.ecos.data.sql.batch.DbBatchTaskEngine].
 */
/**
 * `open` so that a test can subclass it as the seam for injecting a write into the exact gap
 * [ru.citeck.ecos.data.sql.batch.DbBatchTaskEngine.runOneBatch] closes with the compare-and-set in
 * [saveProgressIfStatusMatches] - see `DbBatchTaskEngineTest`'s `CancellingBatchTaskService`. No
 * production code subclasses this.
 */
open class DbBatchTaskService(schemaCtx: DbSchemaContext) {

    companion object {
        private val log = KotlinLogging.logger {}

        /**
         * Bound on the compare-and-set retry of [cancel] and [restart], mirroring
         * `DbBatchTaskEngine.MAX_PROGRESS_WRITE_ATTEMPTS` and justified the same way: the only
         * writers a status-conditioned write can lose to are the engine and another administrator,
         * and a final status never moves again, so a retry conditioned on a freshly re-read status
         * lands on its very next attempt. The ceiling only guards a state this design does not
         * produce - the row changing status over and over - and therefore fails loudly instead of
         * spinning for ever.
         */
        private const val MAX_CONDITIONAL_WRITE_ATTEMPTS = 5
    }

    private val dataService: DbDataService<DbBatchTaskEntity> = DbDataServiceImpl(
        DbBatchTaskEntity::class.java,
        DbDataServiceConfig.create {
            withTable(DbBatchTaskEntity.TABLE)
        },
        schemaCtx
    )

    fun createTableIfNotExists() {
        TxnContext.doInTxn {
            dataService.runMigrations(mock = false, diff = true)
        }
    }

    fun queue(handler: String, table: String, params: ObjectData): DbBatchTaskDto {
        return save(
            DbBatchTaskDto(
                handler = handler,
                table = table,
                params = params,
                status = DbBatchTaskStatus.PENDING,
                created = Instant.now(),
                // the established idiom in this repository - see how DbDataServiceImpl seeds
                // ed_column_meta rows. The column is NOT NULL and a blank creator is not a real
                // answer: a task queued by the migration machinery genuinely belongs to the system.
                creator = AuthContext.getCurrentUser().ifBlank { AuthUser.SYSTEM }
            )
        )
    }

    /**
     * `open` for the same test-seam reason as [saveProgressIfStatusMatches]: [cancel] and [restart]
     * read the row through this method and then write it conditionally, and a test needs a way to
     * commit something real into that gap. See `DbBatchTaskEngineTest`'s seam subclasses.
     */
    open fun getById(id: Long): DbBatchTaskDto? {
        return TxnContext.doInTxn(readOnly = true) {
            dataService.findById(id)?.let { toDto(it) }
        }
    }

    /**
     * Drainable tasks, oldest first. [DbBatchTaskStatus.RUNNING] is included on purpose: it means
     * "some instance was working on this", and that instance may be gone.
     *
     * Tasks still inside their retry backoff window are excluded here rather than skipped by the
     * engine, so that one backing-off task does not hide the tasks queued behind it.
     *
     * @param now injected rather than read from the clock inside, so a test can move time without
     *            sleeping through a ten-minute backoff.
     */
    fun findActive(now: Instant = Instant.now()): List<DbBatchTaskDto> {
        return TxnContext.doInTxn(readOnly = true) {
            dataService.findAll(
                Predicates.and(
                    Predicates.inVals(
                        DbBatchTaskEntity.STATUS,
                        listOf(DbBatchTaskStatus.PENDING.name, DbBatchTaskStatus.RUNNING.name)
                    ),
                    Predicates.le(DbBatchTaskEntity.NEXT_ATTEMPT_AT, now)
                ),
                listOf(DbFindSort(DbBatchTaskEntity.ID, true))
            ).map { toDto(it) }
        }
    }

    fun findByTable(table: String): List<DbBatchTaskDto> {
        return TxnContext.doInTxn(readOnly = true) {
            dataService.findAll(
                Predicates.eq(DbBatchTaskEntity.TABLE_ID, table),
                listOf(DbFindSort(DbBatchTaskEntity.ID, true))
            ).map { toDto(it) }
        }
    }

    /**
     * The administrator's view of the queue: every task regardless of status, not just
     * the drainable subset [findActive] returns. A [DbBatchTaskStatus.FAILED] task sitting inside its
     * retry backoff window - the exact one an administrator would want to `restart` - is invisible to
     * [findActive] by design, so that method cannot be reused here.
     *
     * The converse matters just as much: this method does **not** apply [findActive]'s retry-backoff
     * filter and does not exclude final tasks, so it must never be used to feed the drain - a caller
     * that did would pick up a task that is still inside its backoff window, or one that is already
     * `DONE`/`CANCELLED`/`FAILED` for good. [findActive] is the only method the engine may drain from.
     *
     * @param sort defaults to newest first: with no priority and no lease owner, creation order is
     *             the only order the queue itself has, and an administrator most wants to see what
     *             was queued most recently.
     */
    fun find(
        predicate: Predicate = Predicates.alwaysTrue(),
        sort: List<DbFindSort> = listOf(DbFindSort(DbBatchTaskEntity.ID, false)),
        page: DbFindPage = DbFindPage.ALL
    ): DbFindRes<DbBatchTaskDto> {
        return TxnContext.doInTxn(readOnly = true) {
            dataService.find(
                DbFindQuery.create {
                    withPredicate(predicate)
                    withSortBy(sort)
                },
                page,
                true
            ).mapEntities { toDto(it) }
        }
    }

    /**
     * Whole-row write, with no condition of any kind. **Production code may use it only for the
     * insert [queue] performs.** Every *update* of an existing row must go through
     * [saveIfStatusMatches] or [saveProgressIfStatusMatches]: `ed_batch_task` carries no
     * `__upd_version`, so this method writes every column from whatever snapshot it is handed, and
     * an administrator's `cancel` committed between a read and such a write is silently overwritten
     * - which is the bug both conditional writers exist to make unreachable.
     */
    fun save(dto: DbBatchTaskDto): DbBatchTaskDto {
        return TxnContext.doInTxn {
            toDto(dataService.save(toEntity(dto)))
        }
    }

    /**
     * Atomic compare-and-set of the fields a completed batch writes, keyed on id and conditioned on
     * [expectedStatus] rather than on the whole row: `ed_batch_task` carries neither an ext id nor
     * `__upd_version`, so plain [save] writes the whole row unconditionally and cannot express "only
     * if the status is still what I read". That gap is exactly how a `cancel` committed between the
     * engine's read and its write used to get silently reverted to [DbBatchTaskStatus.RUNNING] - the
     * whole-row write carried the pre-cancel status right back over it.
     *
     * `status` itself is deliberately not part of [newValues]: this call only ever wants to leave the
     * status exactly as it found it and let the write fail rather than touch it, so there is no way
     * to ask it to also change the status.
     *
     * @return true when the row still had [expectedStatus] and was updated; false means some other
     *         writer (almost always [cancel]) got to the row first, and the caller must re-read it
     *         rather than assume its own view of processed/skipped/failed became durable.
     */
    open fun saveProgressIfStatusMatches(
        id: Long,
        expectedStatus: DbBatchTaskStatus,
        cursor: Long,
        processed: Long,
        skipped: Long,
        failed: Long
    ): Boolean {
        return TxnContext.doInTxn {
            dataService.updateByIdIfMatches(
                id,
                mapOf(DbBatchTaskEntity.STATUS to expectedStatus.name),
                mapOf(
                    DbBatchTaskEntity.CURSOR to cursor,
                    DbBatchTaskEntity.PROCESSED to processed,
                    DbBatchTaskEntity.SKIPPED to skipped,
                    DbBatchTaskEntity.FAILED to failed,
                    // as in the plain save() paths this mirrors: a batch that got through clears the
                    // consecutive-failure bookkeeping so the task isn't excluded from findActive by a
                    // backoff window it no longer needs
                    DbBatchTaskEntity.ERROR_COUNT to 0,
                    DbBatchTaskEntity.NEXT_ATTEMPT_AT to Instant.EPOCH
                )
            )
        }
    }

    /**
     * The general status-conditioned write every `ed_batch_task` writer goes through.
     *
     * The table carries no `__upd_version`, so [save] writes the **whole row** from whatever
     * snapshot the caller read earlier - and between that read and the write there is always a gap
     * (a handler callback, an `onFinish`, a `prepare`) in which an administrator's `CANCELLED` can
     * commit and be silently carried away. This lands only if the row still carries
     * [expectedStatus], and touches only the columns named in [newValues], so `__cursor` and the
     * counters cannot be rewound by a stale snapshot either.
     *
     * **Not the same method as [saveProgressIfStatusMatches], and must not be merged with it.** That
     * one deliberately cannot express a status change, because it serves the single writer - a
     * committed batch's progress - that must leave the status as it found it. This one serves the
     * writers whose purpose *is* a transition, so [DbBatchTaskEntity.STATUS] is allowed in
     * [newValues]. Unifying them would hand the progress writer a way to change the status it is
     * conditioned on.
     *
     * @param newValues raw `ed_batch_task` column names ([DbBatchTaskEntity]'s constants) to values,
     *                  not DTO property names.
     * @return true when the row still had [expectedStatus] and was updated; false means another
     *         writer got there first and the caller must re-read the row and act on its real status
     *         - never fall back to an unconditional [save].
     */
    open fun saveIfStatusMatches(
        id: Long,
        expectedStatus: DbBatchTaskStatus,
        newValues: Map<String, Any?>
    ): Boolean {
        return TxnContext.doInTxn {
            dataService.updateByIdIfMatches(
                id,
                mapOf(DbBatchTaskEntity.STATUS to expectedStatus.name),
                newValues
            )
        }
    }

    /**
     * @return true when this call is what moved the task to [DbBatchTaskStatus.CANCELLED]. False
     *         means it was already final, which is not an error - two administrators pressing
     *         cancel is normal.
     *
     * Writes **only** `__status` and `__finished`, through [saveIfStatusMatches]. It deliberately
     * writes neither `__cursor` nor `__processed`/`__skipped`/`__failed`/`__total`/`__started`: a
     * batch that commits between the read below and the write would otherwise be written back out
     * of existence by a whole-row [save] built from the pre-batch snapshot. That would rewind the
     * cursor over rows the handler has already converted - and [restart] deliberately keeps the
     * cursor, so an administrator could not get them back without a manual `UPDATE`.
     */
    fun cancel(id: Long): Boolean {
        return TxnContext.doInTxn {
            var current = getById(id) ?: return@doInTxn false
            var attempt = 0
            while (attempt < MAX_CONDITIONAL_WRITE_ATTEMPTS) {
                attempt++
                if (current.status.isFinal()) {
                    return@doInTxn false
                }
                if (saveIfStatusMatches(
                        id,
                        current.status,
                        mapOf(
                            DbBatchTaskEntity.STATUS to DbBatchTaskStatus.CANCELLED.name,
                            DbBatchTaskEntity.FINISHED to Instant.now()
                        )
                    )
                ) {
                    return@doInTxn true
                }
                current = getById(id) ?: return@doInTxn false
            }
            error(
                "Batch task $id: compare-and-set on cancel did not land after $attempt attempts - " +
                    "the row's status kept changing underneath it"
            )
        }
    }

    /**
     * Puts a finished task back in the queue. The cursor is kept: a restart resumes, it does not
     * rewind. Rewinding would re-run the handler over rows it has already converted, and the SPI
     * only promises idempotency inside one batch.
     *
     * Same write shape as [cancel]: only `__status`, `__error`, `__error_count`,
     * `__next_attempt_at` and `__finished` are named, so the cursor and the counters are untouched
     * by construction rather than by a copy that happens to carry the right values.
     */
    fun restart(id: Long): Boolean {
        return TxnContext.doInTxn {
            var current = getById(id) ?: return@doInTxn false
            var attempt = 0
            while (attempt < MAX_CONDITIONAL_WRITE_ATTEMPTS) {
                attempt++
                if (!current.status.isFinal()) {
                    return@doInTxn false
                }
                if (saveIfStatusMatches(
                        id,
                        current.status,
                        mapOf(
                            DbBatchTaskEntity.STATUS to DbBatchTaskStatus.PENDING.name,
                            DbBatchTaskEntity.ERROR to "",
                            DbBatchTaskEntity.ERROR_COUNT to 0,
                            // an administrator pressing restart is an explicit "try now"; making
                            // them wait out a backoff window they did not set would be surprising
                            DbBatchTaskEntity.NEXT_ATTEMPT_AT to Instant.EPOCH,
                            DbBatchTaskEntity.FINISHED to Instant.EPOCH
                        )
                    )
                ) {
                    return@doInTxn true
                }
                current = getById(id) ?: return@doInTxn false
            }
            error(
                "Batch task $id: compare-and-set on restart did not land after $attempt attempts - " +
                    "the row's status kept changing underneath it"
            )
        }
    }

    fun resetColumnsCache() {
        dataService.resetColumnsCache()
    }

    private fun toDto(entity: DbBatchTaskEntity): DbBatchTaskDto {
        return DbBatchTaskDto(
            id = entity.id,
            handler = entity.handler,
            table = entity.table,
            params = parseParams(entity.id, entity.params),
            status = DbBatchTaskStatus.parse(entity.status),
            cursor = entity.cursor,
            total = entity.total,
            processed = entity.processed,
            skipped = entity.skipped,
            failed = entity.failed,
            error = entity.error,
            errorCount = entity.errorCount,
            nextAttemptAt = entity.nextAttemptAt,
            created = entity.created,
            creator = entity.creator,
            started = entity.started,
            finished = entity.finished
        )
    }

    /**
     * Unparseable `__params` degrade to empty rather than throwing, because [toDto] also feeds the
     * administrator's list and a single corrupt row must still render - but the degrade is loud.
     * Silently handing a handler an empty [ObjectData] is how a column migration ends up running
     * against the wrong attribute and reporting `DONE`.
     */
    private fun parseParams(id: Long, raw: String): ObjectData {
        val parsed = try {
            Json.mapper.read(raw, ObjectData::class.java)
        } catch (e: Exception) {
            log.error(e) {
                "Batch task $id has unreadable __params and will be seen with empty params: '$raw'"
            }
            return ObjectData.create()
        }
        if (parsed == null) {
            log.error {
                "Batch task $id has unreadable __params and will be seen with empty params: '$raw'"
            }
            return ObjectData.create()
        }
        return parsed
    }

    private fun toEntity(dto: DbBatchTaskDto): DbBatchTaskEntity {
        val entity = DbBatchTaskEntity()
        entity.id = dto.id
        entity.handler = dto.handler
        entity.table = dto.table
        entity.params = dto.params.toString()
        entity.status = dto.status.name
        entity.cursor = dto.cursor
        entity.total = dto.total
        entity.processed = dto.processed
        entity.skipped = dto.skipped
        entity.failed = dto.failed
        entity.error = dto.error
        entity.errorCount = dto.errorCount
        entity.nextAttemptAt = dto.nextAttemptAt
        entity.created = dto.created
        entity.creator = dto.creator
        entity.started = dto.started
        entity.finished = dto.finished
        return entity
    }
}
