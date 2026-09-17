package ru.citeck.ecos.data.sql.batch

import ru.citeck.ecos.data.sql.dto.DbColumnConstraint
import ru.citeck.ecos.data.sql.dto.DbColumnType
import ru.citeck.ecos.data.sql.repo.entity.annotation.ColumnType
import ru.citeck.ecos.data.sql.repo.entity.annotation.Constraints
import ru.citeck.ecos.data.sql.repo.entity.annotation.Index
import ru.citeck.ecos.data.sql.repo.entity.annotation.Indexes
import java.time.Instant

/**
 * One row per background job. Schema-level, like [ru.citeck.ecos.data.sql.columnmeta.DbColumnMetaEntity]:
 * a single table holds the jobs of every domain table of the schema, which is why [TABLE_ID] is
 * part of the lookups.
 *
 * There is deliberately no lease owner and no lease expiry. Mutual exclusion is
 * [ru.citeck.ecos.webapp.api.lock.EcosLockApi]'s job and it releases when an instance dies;
 * resumability is the cursor's job. There is no priority either - tasks are taken in
 * creation order.
 *
 * The index is on [STATUS] alone rather than on `(status, id)`: the drain reads the active tasks of
 * a schema, which is a handful of rows, and then orders them by the primary key it already has.
 */
@Indexes(
    Index(columns = [DbBatchTaskEntity.STATUS]),
    Index(columns = [DbBatchTaskEntity.TABLE_ID])
)
class DbBatchTaskEntity {

    companion object {
        const val TABLE = "ed_batch_task"

        const val NEW_REC_ID = -1L

        const val ID = "id"
        const val HANDLER = "__handler"
        const val TABLE_ID = "__table"
        const val PARAMS = "__params"
        const val STATUS = "__status"
        const val CURSOR = "__cursor"
        const val TOTAL = "__total"
        const val PROCESSED = "__processed"
        const val SKIPPED = "__skipped"
        const val FAILED = "__failed"
        const val ERROR = "__error"
        const val ERROR_COUNT = "__error_count"
        const val NEXT_ATTEMPT_AT = "__next_attempt_at"
        const val CREATED = "__created"
        const val CREATOR = "__creator"
        const val STARTED = "__started"
        const val FINISHED = "__finished"
    }

    @Constraints(DbColumnConstraint.PRIMARY_KEY)
    var id: Long = NEW_REC_ID

    @Constraints(DbColumnConstraint.NOT_NULL)
    var handler: String = ""

    @Constraints(DbColumnConstraint.NOT_NULL)
    var table: String = ""

    @Constraints(DbColumnConstraint.NOT_NULL)
    @ColumnType(DbColumnType.JSON)
    var params: String = "{}"

    @Constraints(DbColumnConstraint.NOT_NULL)
    var status: String = DbBatchTaskStatus.PENDING.name

    @Constraints(DbColumnConstraint.NOT_NULL)
    var cursor: Long = 0

    @Constraints(DbColumnConstraint.NOT_NULL)
    var total: Long = -1

    @Constraints(DbColumnConstraint.NOT_NULL)
    var processed: Long = 0

    @Constraints(DbColumnConstraint.NOT_NULL)
    var skipped: Long = 0

    @Constraints(DbColumnConstraint.NOT_NULL)
    var failed: Long = 0

    @Constraints(DbColumnConstraint.NOT_NULL)
    var error: String = ""

    @Constraints(DbColumnConstraint.NOT_NULL)
    var errorCount: Int = 0

    /**
     * Earliest time this task may be picked up again. [Instant.EPOCH] means "right now" and is what
     * a task that has never failed carries, so the drain's filter needs no special case for it.
     */
    @Constraints(DbColumnConstraint.NOT_NULL)
    var nextAttemptAt: Instant = Instant.EPOCH

    @Constraints(DbColumnConstraint.NOT_NULL)
    var created: Instant = Instant.EPOCH

    @Constraints(DbColumnConstraint.NOT_NULL)
    var creator: String = ""

    @Constraints(DbColumnConstraint.NOT_NULL)
    var started: Instant = Instant.EPOCH

    @Constraints(DbColumnConstraint.NOT_NULL)
    var finished: Instant = Instant.EPOCH
}
