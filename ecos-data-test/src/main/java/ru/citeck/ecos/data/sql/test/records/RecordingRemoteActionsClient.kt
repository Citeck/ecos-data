package ru.citeck.ecos.data.sql.test.records

import ru.citeck.ecos.data.sql.context.DbTableContext
import ru.citeck.ecos.data.sql.records.assocs.DbAssocRefsDiff
import ru.citeck.ecos.data.sql.remote.DbRecordsRemoteActionsClient
import ru.citeck.ecos.webapp.api.entity.EntityRef
import java.util.concurrent.CopyOnWriteArrayList

/**
 * A [DbRecordsRemoteActionsClient] that records what it was told and does nothing else.
 *
 * The production client resolves each reference to the application that owns it and sends an HTTP
 * request there; none of that is in reach of a test, and none of it is what a caller of
 * `updateRemoteAssocs` is responsible for. What the caller *is* responsible for - which references
 * it reports as removed, under which attribute, with which `child` flag, and for which source
 * record - is exactly what this records, so it can be asserted.
 *
 * Installed for every test rather than only where it is asserted, because a null client is a
 * silently skipped call: a test that meant to assert on a notification would have passed while the
 * call site was never reached at all.
 */
class RecordingRemoteActionsClient : DbRecordsRemoteActionsClient {

    /**
     * Every `updateRemoteAssocs` call since the last [clear], oldest first.
     */
    val assocUpdates = CopyOnWriteArrayList<AssocsUpdate>()

    /**
     * Makes `updateRemoteAssocs` throw for any call carrying a diff for this attribute, the way the
     * production client does for an application it cannot reach -
     * `DbRecordsRemoteActionsClientImpl` answers `error("App is not available: ...")` for an
     * unavailable peer. The call is still recorded first, so a test can tell "threw" from "was never
     * reached".
     *
     * Per attribute rather than a blanket switch because **every** mutation notifies, most of them
     * with an empty diff: a blanket switch would break the unrelated mutation a test uses to
     * reconcile the schema instead of the call it meant to fail.
     */
    var failAssocUpdatesFor: String? = null

    fun clear() {
        assocUpdates.clear()
    }

    /**
     * The recorded calls that carry a diff for [assocId] - the ordinary way to find the one meant.
     */
    fun assocUpdatesFor(assocId: String): List<AssocsUpdate> {
        return assocUpdates.filter { call -> call.assocsDiff.any { it.assocId == assocId } }
    }

    override fun migrateRemoteRef(targetApp: String, fromRef: EntityRef, toRef: EntityRef, migratedBy: String) {
        // nothing to record: no test asserts on it yet, and the production behaviour is an HTTP call
    }

    override fun updateRemoteAssocs(
        currentCtx: DbTableContext,
        sourceRef: EntityRef,
        creator: String,
        assocsDiff: List<DbAssocRefsDiff>
    ) {
        assocUpdates.add(AssocsUpdate(sourceRef, creator, assocsDiff))
        val failing = failAssocUpdatesFor
        if (failing != null && assocsDiff.any { it.assocId == failing }) {
            error("App is not available: some-other-app")
        }
    }

    override fun deleteRemoteAssocs(currentCtx: DbTableContext, sourceRef: EntityRef, force: Boolean) {
        // as above
    }

    class AssocsUpdate(
        val sourceRef: EntityRef,
        val creator: String,
        val assocsDiff: List<DbAssocRefsDiff>
    )
}
