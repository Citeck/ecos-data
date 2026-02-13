package ru.citeck.ecos.data.sql.trashcan

import ru.citeck.ecos.data.sql.repo.entity.DbEntity
import ru.citeck.ecos.data.sql.trashcan.entity.DbTrashcanEntity
import ru.citeck.ecos.records2.predicate.model.Predicate

interface DbTrashcanService {

    /**
     * Save a snapshot of the entity into the trashcan before it is physically deleted
     * from the source table. The snapshot includes all entity fields, content references
     * and deletion context (who deleted, trace/txn ids).
     *
     * @param entity         entity being deleted
     * @param sourceTable    name of the table the entity is deleted from
     * @param contentIds     IDs of content entries associated with the entity
     * @param deletedById    ref-ID of the user who initiated the deletion
     * @param deletedAsId    ref-ID of the user on whose behalf the deletion is performed (runAs)
     */
    fun moveToTrashcan(
        entity: DbEntity,
        sourceTable: String,
        contentIds: List<Long>,
        deletedById: Long,
        deletedAsId: Long
    )

    /**
     * Return all trashcan entries for the given record ref-ID,
     * ordered by [DbTrashcanEntity.DELETED_AT] descending (most recent first).
     */
    fun findAllByRefId(refId: Long): List<DbTrashcanEntity>

    /**
     * Return the most recently deleted trashcan entry for the given record ref-ID,
     * or `null` if no entry exists.
     */
    fun findLatestByRefId(refId: Long): DbTrashcanEntity?

    /**
     * Query trashcan entries using an arbitrary predicate with pagination.
     * Results are ordered by [DbTrashcanEntity.DELETED_AT] descending.
     *
     * @param predicate  filter predicate (field names correspond to [DbTrashcanEntity] constants)
     * @param skipCount  number of entries to skip
     * @param maxItems   maximum number of entries to return
     */
    fun findAll(predicate: Predicate, skipCount: Int, maxItems: Int): List<DbTrashcanEntity>

    /**
     * Delete a single trashcan entry by its primary key.
     * Associated content is **not** removed — use [deleteAllByRefId] for full cleanup.
     */
    fun deleteById(id: Long)

    /**
     * Delete all trashcan entries for the given record ref-ID
     * and permanently remove their associated content.
     */
    fun deleteAllByRefId(refId: Long)

    /**
     * Ensure the trashcan table exists in the current schema.
     */
    fun createTableIfNotExists()

    /**
     * Reset internal column metadata cache (called after schema changes).
     */
    fun resetColumnsCache()
}
