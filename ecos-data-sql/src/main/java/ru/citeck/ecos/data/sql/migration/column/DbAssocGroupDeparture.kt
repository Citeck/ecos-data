package ru.citeck.ecos.data.sql.migration.column

import io.github.oshai.kotlinlogging.KotlinLogging
import ru.citeck.ecos.context.lib.auth.AuthContext
import ru.citeck.ecos.data.sql.columnmeta.DbColumnSemanticType
import ru.citeck.ecos.data.sql.records.DbRecordsUtils
import ru.citeck.ecos.data.sql.records.assocs.DbAssocBackupEntity
import ru.citeck.ecos.data.sql.records.assocs.DbAssocEntity
import ru.citeck.ecos.data.sql.records.assocs.DbAssocRefsDiff
import ru.citeck.ecos.data.sql.remote.DbRecordsRemoteActionsClient
import ru.citeck.ecos.data.sql.repo.entity.DbEntity
import ru.citeck.ecos.data.sql.repo.find.DbFindPage
import ru.citeck.ecos.data.sql.service.DbDataService
import ru.citeck.ecos.records2.predicate.model.Predicates
import ru.citeck.ecos.records2.predicate.model.ValuePredicate

/**
 * Takes an attribute's associations out of `ed_associations` and into the backup
 * keyed by the transition's registry row, when the attribute stops being assoc-like.
 *
 * While an attribute is assoc-like its real values live in `ed_associations` and the column only
 * caches the first ten. Once it is not, those rows go inert - nothing updates them and
 * nothing reads them - so leaving them there would resurrect a stale set of links the day the
 * attribute becomes an association again.
 *
 * **A class of its own rather than a private method of [DbColumnMigrationHandler]**, because the
 * departure must not depend on a task surviving long enough to be drained. Two callers run it, and
 * between them they cover every way a transition can end:
 *  - [DbColumnMigrationHandler.prepare], once per run of the task;
 *  - [DbShadowColumnTransition]'s cancellation path, when a second type change cancels the task
 *    before any drain tick reached it.
 *
 * Both are safe because this is idempotent: the copy skips a link the backup already holds, and once
 * the live rows are gone there is nothing left to find.
 *
 * **What leaves is the links, not the parentage.** `_parent`/`_parentAtt` of a child are left
 * exactly as the user's own mutation wrote them, and every operation that the missing links would
 * otherwise break is taught the state instead: a deleted child forgets itself from the snapshot
 * (`RecMutAssocHandler.forgetParkedChildLinks`), and a deleted parent takes its parked children with
 * it (`DbRecordsDeleteDao`). Clearing them - which is what `1.73.0` did - orphans every child for as
 * long as an administrator leaves the attribute as text: nothing names it, no `_parent` predicate
 * finds it, no interface leads to it, and `DefaultDbPermsComponent` reads a record with no `_parent`
 * as readable by everybody. It also cannot be done at all for a child this application does not own,
 * the two columns being in a database it cannot write, so the old release was both harmful and
 * partial - the shape of a single-schema assumption in a platform where a child may live in any
 * application.
 *
 * @param tableService a service over the table being migrated. Only its `__ref_id` column and its
 *        [DbDataService.getTableContext] are used, so a service with or without backup columns does
 *        equally well.
 * @param remoteActionsClient the peer-notification client, or null where the application has none.
 */
class DbAssocGroupDeparture(
    private val tableService: DbDataService<DbEntity>,
    private val remoteActionsClient: DbRecordsRemoteActionsClient?
) {

    companion object {
        private val log = KotlinLogging.logger {}

        /**
         * How many `ed_associations` rows [run] reads at a time while it looks for the source
         * records of the table being migrated. The same 100 `DbAssocsService`'s own iteration uses,
         * and for the same reason: an attribute of a large installation can own more links than fit
         * in memory at once.
         */
        private const val SOURCE_SCAN_BATCH = 100

        /**
         * Whether this transition takes an attribute out of the association group - the only case
         * there is anything to do for.
         */
        @JvmStatic
        fun isDeparture(params: DbColumnMigrationParams): Boolean {
            return isAssocLike(params.sourceType) &&
                !isAssocLike(DbColumnSemanticType.Model(params.targetType))
        }

        /**
         * Whether a column of this semantic type keeps its real values in `ed_associations` rather
         * than in the column itself.
         *
         * Delegates to [DbRecordsUtils.isStoredInAssocsTable] rather than restating the set, because
         * that function *is* the definition of "lives in `ed_associations`" - ENTITY_REF, which
         * `AttributeType.isAssocLike` includes and that function excludes, keeps its reference in
         * the column itself and has no rows to account for.
         *
         * A [DbColumnSemanticType.Raw] source is never assoc-like: the registry never recorded what
         * those bytes mean, so there is no attribute to account for links under.
         *
         * Public because the *arrival* is decided by the same question asked of the other end -
         * see [DbColumnMigrationHandler.isAssocArrival]. The two conditions are mirror images and
         * must not drift apart: a transition either takes the links out of `ed_associations` or
         * puts them in, and a pair that did both, or neither where one was due, is exactly the
         * disagreement between the two tables this class exists to prevent.
         */
        @JvmStatic
        fun isAssocLike(type: DbColumnSemanticType): Boolean {
            return type is DbColumnSemanticType.Model && DbRecordsUtils.isStoredInAssocsTable(type.attType)
        }
    }

    /**
     * [run], but only when [isDeparture] says there is one.
     */
    fun runIfDeparture(params: DbColumnMigrationParams) {
        if (isDeparture(params)) {
            run(params)
        }
    }

    /**
     * Copies this attribute's live associations into the backup and then removes them.
     *
     * The copy happens first and in the caller's transaction, so no committed state ever has the
     * links gone from one table without having arrived in the other.
     *
     * Driven from `ed_associations` rather than from the table's rows: only the records that
     * actually have links are worth touching, and the source ids found there are then narrowed to
     * the records of **this** table - `ed_associations` is schema-wide, and two tables may each
     * define an attribute of the same name, of which only one is migrating.
     *
     * `DbAssocsService.forEachAssoc` is used to enumerate those source ids and for nothing else:
     * the copy itself reads the raw rows, because a `DbAssocDto` has no `__index`, `__created` or
     * `__creator` to carry across.
     */
    fun run(params: DbColumnMigrationParams) {

        val tableCtx = tableService.getTableContext()
        val tableRef = tableCtx.getTableRef()
        val schemaCtx = tableCtx.getSchemaCtx()
        val assocsService = schemaCtx.assocsService
        val attributeId = assocsService.getIdForAtt(params.attId)
        if (attributeId == -1L) {
            // the attribute has no id in ed_attributes, so nothing was ever linked through it
            return
        }
        var copied = 0
        var sources = 0
        assocsService.forEachAssoc(
            Predicates.eq(DbAssocEntity.ATTRIBUTE, attributeId),
            SOURCE_SCAN_BATCH
        ) { assocs ->
            val ownSourceIds = sourceIdsOfThisTable(assocs.mapTo(LinkedHashSet()) { it.sourceId })
            if (ownSourceIds.isNotEmpty()) {
                copied += schemaCtx.assocBackupService.backupAssocsOf(
                    params.backupColumnMetaId,
                    params.attId,
                    ownSourceIds
                )
                sources += ownSourceIds.size
                ownSourceIds.forEach { removeAndNotify(params, it) }
            }
            // The rows just removed are the ones this page walked past, and the walk continues from
            // the largest id it saw, so the pages left to it are the ones it has not seen yet.
            false
        }
        if (copied > 0) {
            log.info {
                "Attribute '${params.attId}' of ${tableRef.fullName} left the association group: " +
                    "$copied links of $sources records were copied into " +
                    "'${DbAssocBackupEntity.TABLE}' under column meta ${params.backupColumnMetaId} " +
                    "and removed from '${DbAssocEntity.MAIN_TABLE}'"
            }
        }
    }

    /**
     * Which of [refIds] are references to records of the table being migrated. One query over
     * `__ref_id`, which carries a unique index.
     */
    private fun sourceIdsOfThisTable(refIds: Set<Long>): List<Long> {
        if (refIds.isEmpty()) {
            return emptyList()
        }
        val ownRefIds = tableService.findAll(
            ValuePredicate(DbEntity.REF_ID, ValuePredicate.Type.IN, refIds)
        ).mapTo(HashSet()) { it.refId }
        return refIds.filter { it in ownRefIds }
    }

    /**
     * Removes one record's links to the attribute and tells the other applications about it.
     *
     * `force = true` because the copy is already ours: the unforced path writes a second copy into
     * `ed_associations_deleted`, and that must not happen - the trash can holds the links
     * the user deleted on purpose, and a restore that could not tell the two apart would bring those
     * back too.
     *
     * The notification is a separate call from the removal, as it is in `DbRecordsMutateDao`: an
     * association may point at an entity owned by another application, which keeps its own
     * back-reference, and a removal it never hears about leaves that reference dangling.
     */
    private fun removeAndNotify(params: DbColumnMigrationParams, sourceId: Long) {

        val tableCtx = tableService.getTableContext()
        val schemaCtx = tableCtx.getSchemaCtx()
        val assocs = schemaCtx.assocsService.getTargetAssocs(sourceId, params.attId, DbFindPage.ALL).entities
        if (assocs.isEmpty()) {
            return
        }
        val removed = schemaCtx.assocsService.removeAssocs(
            sourceId,
            params.attId,
            assocs.map { it.targetId },
            force = true
        ).toHashSet()
        if (removed.isEmpty() || remoteActionsClient == null) {
            return
        }
        val refService = schemaCtx.recordRefService
        val diff = assocs.asSequence()
            .filter { it.targetId in removed }
            .groupBy({ it.child }, { refService.getEntityRefById(it.targetId) })
            .map { (child, refs) -> DbAssocRefsDiff(params.attId, emptyList(), refs, child) }

        // Resolved before the try, both of them: a source id with no row in `ed_record_ref` throws
        // "Ref doesn't found for id ..." from here, and that is a broken reference in this schema,
        // not a peer that could not be reached - reporting it as one would send an administrator
        // looking at the wrong application.
        val sourceRef = refService.getEntityRefById(sourceId)
        val creator = AuthContext.getCurrentUser()

        // A peer that is down must not fail this. DbRecordsRemoteActionsClientImpl throws
        // "App is not available" for an unreachable application, and this runs inside the batch
        // engine's prepare and inside a user's mutation - one throw there would be booked as a task
        // failure with backoff and, after maxAttempts, would mark the whole column migration FAILED,
        // value transfer included, because some other application was restarting. The links are gone
        // locally either way and the next tick has nothing left to re-notify from, so the only
        // honest thing to do with the failure is to record it where an administrator sees it.
        //
        // "Remote" is the usual case and not the only one: when a target reference belongs to this
        // application but to a different schema context, DbRecordsRemoteActionsClientImpl executes
        // the update in process and writes to that schema's `ed_associations` itself, so a failure
        // here can just as well be a local write that did not happen.
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
            log.error(e) {
                "Record $sourceId of ${tableCtx.getTableRef().fullName} lost its " +
                    "'${params.attId}' links to the " +
                    "association backup, but the back-references of the applications holding them " +
                    "could not be updated - either the application was unreachable or the update " +
                    "itself failed. Their back-references are now stale and need to be reconciled " +
                    "by hand. The links themselves are safe in '${DbAssocBackupEntity.TABLE}'"
            }
        }
    }
}
