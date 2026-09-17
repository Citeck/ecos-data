package ru.citeck.ecos.data.sql.migration.column

import io.github.oshai.kotlinlogging.KotlinLogging
import ru.citeck.ecos.context.lib.auth.AuthContext
import ru.citeck.ecos.data.sql.columnmeta.DbColumnSemanticType
import ru.citeck.ecos.data.sql.context.DbSchemaContext
import ru.citeck.ecos.data.sql.records.DbRecordsDao
import ru.citeck.ecos.data.sql.records.DbRecordsUtils
import ru.citeck.ecos.data.sql.records.assocs.DbAssocBackupEntity
import ru.citeck.ecos.data.sql.records.assocs.DbAssocEntity
import ru.citeck.ecos.data.sql.records.assocs.DbAssocRefsDiff
import ru.citeck.ecos.data.sql.remote.DbRecordsRemoteActionsClient
import ru.citeck.ecos.data.sql.repo.entity.DbEntity
import ru.citeck.ecos.data.sql.repo.find.DbFindPage
import ru.citeck.ecos.data.sql.service.DbDataService
import ru.citeck.ecos.data.sql.service.DbDataServiceConfig
import ru.citeck.ecos.data.sql.service.DbDataServiceImpl
import ru.citeck.ecos.records2.RecordConstants
import ru.citeck.ecos.records2.predicate.model.Predicates
import ru.citeck.ecos.records2.predicate.model.ValuePredicate
import ru.citeck.ecos.records3.RecordsService
import ru.citeck.ecos.webapp.api.entity.EntityRef

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
     * The raw data service over the table a child lives in, keyed by the source id of its reference.
     *
     * One departure run releases the children of many parent records, and they cluster into a
     * handful of tables; building a [DbDataServiceImpl] per child would re-read a column list per
     * child. Holds nulls too - a source id this application cannot resolve to a `DbRecordsDao` is
     * asked once and remembered as unreachable.
     */
    private val childServices = HashMap<String, ChildTable?>()

    /**
     * Releases the children of one record from the attribute that has just left the association
     * group: `_parent` and `_parentAtt` are cleared, together, by a direct write to the two columns.
     *
     * **Why the two columns and not a record mutation.** A child association is two facts - the
     * `ed_associations` row with `__child = true`, and the pair of columns on the child - and the
     * removal above has just taken the first away. Leaving the second is fatal, not untidy:
     * `DbRecordsDeleteDao` removes a deleted child from its parent by mutating the parent's child
     * attribute, `RecMutAssocHandler.validateChildAssocs` refuses that mutation because the
     * attribute is no longer a child association, and the record becomes **undeletable**.
     *
     * Neither route through record mutation can do it.
     * `RecMutAssocHandler.updateParentRefOfChildren(add = false)` does not detach a child, it
     * **deletes** it - recursively, stripping the links of every unrelated record that pointed at
     * any of them, none of which any return of the type could put back. An ordinary mutation setting
     * `_parent = null` throws the very exception this is fixing.
     *
     * **Both columns or neither**, because `DbRecordsDeleteDao` gates its parent notification on
     * both being set; clearing one would leave the parent holding a link to a deleted child just as
     * silently. **Only this attribute's children, and only this parent's**: the conditional update
     * names both old values, so a child re-parented meanwhile is left exactly as it is.
     *
     * **The cost.** Until the attribute becomes a child association again, a released child has no
     * `_parent`, and [ru.citeck.ecos.data.sql.records.perms.DefaultDbPermsComponent] treats a record
     * with none as readable by everybody. Accepted because it is **reversible** - the restore puts
     * the columns back with the links - whereas deleting the record is reversible by nothing.
     *
     * **The boundary: a child this application cannot reach.** Its `_parent` lives in another
     * application's database, so there is no write of two columns to be had. Such a child is
     * reported as an error naming the record, for an administrator to reconcile by hand; the links
     * are safe in the backup either way.
     */
    private fun releaseChildren(
        params: DbColumnMigrationParams,
        sourceId: Long,
        childIds: List<Long>
    ) {
        if (childIds.isEmpty()) {
            return
        }
        val tableCtx = tableService.getTableContext()
        val schemaCtx = tableCtx.getSchemaCtx()
        val recordsService = schemaCtx.getRecordsService(tableCtx.getTableRef().table)
        val parentRef = schemaCtx.recordRefService.getEntityRefById(sourceId)
        val refsById = schemaCtx.recordRefService.getEntityRefsByIdsMap(childIds)

        val idsByTable = LinkedHashMap<ChildTable, MutableList<Long>>()
        val unreachable = ArrayList<EntityRef>()
        for (childId in childIds) {
            val childRef = refsById[childId]
            if (childRef == null || EntityRef.isEmpty(childRef)) {
                continue
            }
            val childTable = recordsService?.let { childTableOf(it, schemaCtx, childRef) }
            if (childTable == null) {
                unreachable.add(childRef)
            } else {
                idsByTable.computeIfAbsent(childTable) { ArrayList() }.add(childId)
            }
        }
        // A child's write permissions are not the question here: the model of the *parent's* table
        // changed, and the release is part of that change. The same reasoning DbRecordsDeleteDao
        // states for the source assocs it removes in the system context.
        AuthContext.runAsSystem {
            idsByTable.forEach { (childTable, ids) ->
                childTable.clearParentOf(parentRef, params.attId, ids)
            }
        }
        if (unreachable.isNotEmpty()) {
            log.error {
                "Records ${unreachable.joinToString(limit = 20)} lost their '${params.attId}' link " +
                    "to $parentRef to the association backup, but this application cannot reach " +
                    "the table they live in, so their '${RecordConstants.ATT_PARENT}' back-reference " +
                    "still names $parentRef and has to be cleared by hand. Until it is, those " +
                    "records cannot be deleted or re-parented. The links themselves are safe in " +
                    "'${DbAssocBackupEntity.TABLE}'"
            }
        }
    }

    /**
     * The table a child lives in, or null when this application cannot say.
     *
     * Null covers three shapes and they are all the same answer for this purpose: a reference to
     * another application, a source id with no records dao behind it, and a dao that is not a
     * [DbRecordsDao] and therefore has no columns of ours to write.
     */
    private fun childTableOf(
        recordsService: RecordsService,
        schemaCtx: DbSchemaContext,
        childRef: EntityRef
    ): ChildTable? {
        val appName = childRef.getAppName()
        if (appName.isNotBlank() && appName != schemaCtx.dataSourceCtx.appName) {
            return null
        }
        val sourceId = childRef.getSourceId()
        if (sourceId.isBlank()) {
            return null
        }
        if (childServices.containsKey(sourceId)) {
            return childServices[sourceId]
        }
        val dao = try {
            recordsService.getRecordsDao(sourceId)
        } catch (e: Exception) {
            log.warn(e) { "Records dao '$sourceId' could not be resolved while releasing children" }
            null
        }
        val result = if (dao !is DbRecordsDao) {
            null
        } else {
            val childTableCtx = dao.getRecordsDaoCtx().tableCtx
            ChildTable(
                DbDataServiceImpl(
                    DbEntity::class.java,
                    DbDataServiceConfig.create { withTable(childTableCtx.getTableRef().table) },
                    childTableCtx.getSchemaCtx()
                ),
                childTableCtx.getSchemaCtx()
            )
        }
        childServices[sourceId] = result
        return result
    }

    /**
     * One table holding children, and the two conditional updates it can answer.
     *
     * Carries the child's **own** schema context rather than the migrating table's, because
     * `_parent` is a `ed_record_ref` id and `_parentAtt` an `ed_attributes` id, and both id spaces
     * are per schema. A child in another schema of the same application does not hold this schema's
     * numbers, and comparing them would clear the wrong rows or, far more likely, none.
     */
    private class ChildTable(
        val service: DbDataService<DbEntity>,
        val schemaCtx: DbSchemaContext
    ) {
        fun clearParentOf(
            parentRef: EntityRef,
            attId: String,
            childIds: Collection<Long>
        ) {
            // Resolved in the child's own schema rather than taken from the caller: for the usual
            // case - the child in a table of the same schema - these are the very numbers the
            // caller holds, and for a child in another schema of this application they are the only
            // correct ones. Neither is created here: a reference or an attribute name this schema
            // has never registered cannot be what any of its rows point at.
            val parentRefId = schemaCtx.recordRefService.getIdByEntityRef(parentRef)
            if (parentRefId == -1L) {
                return
            }
            val attributeId = schemaCtx.assocsService.getIdForAtt(attId)
            if (attributeId == -1L) {
                return
            }
            val entities = service.findAll(
                ValuePredicate(DbEntity.REF_ID, ValuePredicate.Type.IN, childIds)
            )
            for (entity in entities) {
                if (entity.attributes[RecordConstants.ATT_PARENT] as? Long != parentRefId) {
                    continue
                }
                if (entity.attributes[RecordConstants.ATT_PARENT_ATT] as? Long != attributeId) {
                    continue
                }
                service.updateByIdIfMatches(
                    entity.id,
                    mapOf(
                        RecordConstants.ATT_PARENT to parentRefId,
                        RecordConstants.ATT_PARENT_ATT to attributeId
                    ),
                    mapOf(
                        RecordConstants.ATT_PARENT to null,
                        RecordConstants.ATT_PARENT_ATT to null
                    )
                )
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
        // The other half of every child link just removed. Before the notification and in the same
        // transaction as the removal, because the two columns and the row are one fact.
        releaseChildren(
            params,
            sourceId,
            assocs.mapNotNull { if (it.child && it.targetId in removed) it.targetId else null }
        )
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
