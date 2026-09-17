package ru.citeck.ecos.data.sql.batch

import ru.citeck.ecos.context.lib.auth.AuthContext
import ru.citeck.ecos.records3.record.dao.AbstractRecordsDao
import ru.citeck.ecos.records3.record.dao.mutate.ValueMutateDao
import ru.citeck.ecos.webapp.api.entity.EntityRef

/**
 * The `restart` row action of the batch task admin view. See [DbBatchTaskCancelAction] for why this is
 * a separate tiny DAO rather than a method on [DbBatchTaskAdminDao], and for the authorization,
 * not-found and cross-schema-ref checks this mirrors.
 *
 * [DbBatchTaskService.restart] already carries the rule this delegates to: a still-active
 * task cannot be restarted, and a restart forgives attempts but keeps the cursor, because it resumes
 * rather than rewinds. A refusal is surfaced as a thrown error rather than a silently accepted
 * no-op mutation, for the same reason as the cancel action.
 */
class DbBatchTaskRestartAction(
    private val sourceIdValue: String,
    private val batchTaskService: DbBatchTaskService,
    /**
     * See [DbBatchTaskCancelAction.listSourceId].
     */
    private val listSourceId: String = DbBatchTaskAdminDao.ID
) : AbstractRecordsDao(),
    ValueMutateDao<DbBatchTaskRestartAction.ActionDto> {

    companion object {
        const val ID = "batch-task-restart"
    }

    constructor(batchTaskService: DbBatchTaskService) : this(ID, batchTaskService)

    override fun getId(): String {
        return sourceIdValue
    }

    override fun mutate(value: ActionDto): Any? {
        if (!AuthContext.isRunAsSystemOrAdmin()) {
            error("Permission denied")
        }
        val ref = value.recordRef
        if (ref.getSourceId() != listSourceId) {
            error("Not a valid batch task id: '$ref'")
        }
        val id = ref.getLocalId().toLongOrNull()
            ?: error("Not a valid batch task id: '$ref'")
        // See DbBatchTaskCancelAction.mutate for why "not found" is looked up separately from the
        // restart call, rather than trusting a `false` from restart() to mean "still active".
        batchTaskService.getById(id) ?: error("Batch task '$id' not found")
        if (!batchTaskService.restart(id)) {
            error("Batch task '$id' can't be restarted: it is still active")
        }
        return value.recordRef
    }

    class ActionDto(
        val recordRef: EntityRef = EntityRef.EMPTY
    )
}
