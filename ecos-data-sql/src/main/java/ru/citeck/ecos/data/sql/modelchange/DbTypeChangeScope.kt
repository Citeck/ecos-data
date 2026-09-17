package ru.citeck.ecos.data.sql.modelchange

import io.github.oshai.kotlinlogging.KotlinLogging
import ru.citeck.ecos.data.sql.ecostype.DbEcosModelService
import ru.citeck.ecos.data.sql.records.DbRecordsDao

/**
 * "This type changed -> these are the tables that may need re-checking because of it."
 *
 * A model change is announced per type, but what has to be repaired is a table, and the two are not
 * one-to-one: a descendant with DEFAULT storage and no explicit `sourceId` inherits its parent's,
 * gets no records DAO of its own, and its records live in the ancestor's table. Resolving the
 * change against the changed type alone would find nothing for exactly those types, and their table
 * would never be reconciled by the trigger at all - it would fall back to being repaired lazily, on
 * the first mutation, which is the defect this whole feature exists to remove.
 *
 * Hence the step *up*, to the boundary of the storage the type belongs to
 * ([DbEcosModelService.getStorageBoundaryTypeIds]), and a lookup for *every* id on the way, not just
 * for the top one: a DAO may be registered by hand at any level of the chain
 * (`ecos-integrations` passes an arbitrary `recordsTypeRef`), and types under the bare abstract
 * roots resolve to a blank `sourceId` and climb all the way to `base`, which has no DAO.
 *
 * There is deliberately no walk *down* the subtree: emodel re-publishes every descendant of a
 * changed type by itself, so each affected type arrives here as its own notification.
 */
class DbTypeChangeScope(
    private val modelService: DbEcosModelService,
    private val daoIndex: DbRecordsDaoIndex
) {

    companion object {
        private val log = KotlinLogging.logger {}
    }

    /**
     * The live DAOs whose tables can hold records of [typeId], nearest first, without duplicates.
     *
     * An unknown type is an empty list and not an error: a type can be deleted between the moment
     * its change was announced and the moment the queue gets to it, and there is nothing left to
     * reconcile in that case anyway.
     */
    fun daosForTypeChange(typeId: String): List<DbRecordsDao> {
        val typeInfo = modelService.getTypeInfo(typeId)
        if (typeInfo == null) {
            log.debug { "Type '$typeId' is not found, so there is nothing to reconcile for it" }
            return emptyList()
        }
        // LinkedHashSet over DbRecordsDao, which does not override equals: identity is exactly the
        // right notion here - two different DAOs over one table are two things to reconcile, and one
        // DAO reachable through two ids of the chain (a cycle in the type tree) is one.
        val result = LinkedHashSet<DbRecordsDao>()
        for (boundaryTypeId in modelService.getStorageBoundaryTypeIds(typeInfo)) {
            result.addAll(daoIndex.getByTypeId(boundaryTypeId))
        }
        return result.toList()
    }
}
