package ru.citeck.ecos.data.sql.batch

import ru.citeck.ecos.context.lib.auth.AuthContext
import ru.citeck.ecos.records3.record.dao.AbstractRecordsDao
import ru.citeck.ecos.records3.record.dao.mutate.ValueMutateDao
import ru.citeck.ecos.webapp.api.entity.EntityRef

/**
 * The `cancel` row action of the batch task admin view. Its own tiny DAO with its own `sourceId`, per
 * the platform convention ([ru.citeck.ecos.apps.domain.patch.api.records.ApplyEcosPatchAction] et
 * al.) of keeping a mutating action separate from the [DbBatchTaskAdminDao] that lists the rows.
 *
 * All of the lifecycle rule - you cannot cancel an already-final task - belongs to
 * [DbBatchTaskService.cancel] (already tested there). A refusal is surfaced as a thrown
 * error rather than a silently accepted no-op mutation, because an administrator pressing "cancel"
 * on a task that turns out to already be `DONE` must see that nothing happened, not a fake success.
 */
class DbBatchTaskCancelAction(
    private val sourceIdValue: String,
    private val batchTaskService: DbBatchTaskService,
    /**
     * The `sourceId` of the [DbBatchTaskAdminDao] this action is paired with. Ids in `ed_batch_task`
     * are bare `Long`s with no schema tag of their own, so once one action is registered per schema
     *, a ref built against a different schema's list would otherwise still resolve to a
     * same-numbered row here and cancel the wrong task - see [ActionDto.recordRef]'s check in
     * [mutate]. Defaults to [DbBatchTaskAdminDao.ID], matching the single-schema default the two
     * classes share when neither is given an explicit `sourceId`.
     */
    private val listSourceId: String = DbBatchTaskAdminDao.ID
) : AbstractRecordsDao(),
    ValueMutateDao<DbBatchTaskCancelAction.ActionDto> {

    companion object {
        const val ID = "batch-task-cancel"
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
        // Looked up separately from the cancel call below so the three outcomes an administrator
        // can hit - not found, refused because already final, cancelled - stay distinguishable.
        // DbBatchTaskService.cancel()/.restart() collapse "not found" and "refused" into the same
        // `false` (its own KDoc: two administrators pressing cancel is normal, and that is the
        // right contract at that layer), but a message that tells a stale UI link "it is already
        // in a final state" about a task that was never there would be false.
        batchTaskService.getById(id) ?: error("Batch task '$id' not found")
        if (!batchTaskService.cancel(id)) {
            error("Batch task '$id' can't be cancelled: it is already in a final state")
        }
        return value.recordRef
    }

    class ActionDto(
        val recordRef: EntityRef = EntityRef.EMPTY
    )
}
