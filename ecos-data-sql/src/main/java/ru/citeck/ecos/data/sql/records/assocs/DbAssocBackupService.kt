package ru.citeck.ecos.data.sql.records.assocs

import ru.citeck.ecos.data.sql.context.DbSchemaContext
import ru.citeck.ecos.data.sql.repo.find.DbFindSort
import ru.citeck.ecos.data.sql.service.DbDataService
import ru.citeck.ecos.data.sql.service.DbDataServiceConfig
import ru.citeck.ecos.data.sql.service.DbDataServiceImpl
import ru.citeck.ecos.records2.predicate.model.Predicates
import ru.citeck.ecos.records2.predicate.model.ValuePredicate
import ru.citeck.ecos.txn.lib.TxnContext

/**
 * The permanent home of the associations an attribute owned while it was assoc-like.
 *
 * Reads [DbAssocEntity.MAIN_TABLE] through a data service of its own rather than through
 * [DbAssocsService]: the copy has to be of the **raw rows**. [DbAssocDto] carries only
 * `(sourceId, attribute, targetId, child)`, and the association's own `__index`,
 * `__created` and `__creator` to survive - the original values, not the moment of the backup.
 *
 * Every write joins the caller's transaction ([TxnContext.doInTxn] has REQUIRED semantics), which is
 * what makes the copy and the removal that follows it one atomic step: there is never a committed
 * state in which the links have left `ed_associations` without having arrived here.
 */
class DbAssocBackupService(private val schemaCtx: DbSchemaContext) {

    private val dataService: DbDataService<DbAssocBackupEntity> = DbDataServiceImpl(
        DbAssocBackupEntity::class.java,
        DbDataServiceConfig.create {
            withTable(DbAssocBackupEntity.TABLE)
        },
        schemaCtx
    )

    private val assocsDataService: DbDataService<DbAssocEntity> = DbDataServiceImpl(
        DbAssocEntity::class.java,
        DbDataServiceConfig.create {
            withTable(DbAssocEntity.MAIN_TABLE)
        },
        schemaCtx
    )

    private val attsService = schemaCtx.attributesService

    fun createTableIfNotExists() {
        TxnContext.doInTxn {
            dataService.runMigrations(mock = false, diff = true)
        }
    }

    /**
     * Gives an already existing table the index on `__source_id` that a table created today gets
     * from [DbAssocBackupEntity]'s own `@Indexes`.
     *
     * Needed as a step of its own because schema reconciliation adds indexes for **new columns**
     * (`DbDataServiceImpl.addIndexesAndConstraintsForNewColumns` returns at once when there are
     * none), so an index added to an entity whose table already exists reaches no installation that
     * has one. Asks the catalog rather than `CREATE INDEX IF NOT EXISTS`, because indexes built
     * from an entity carry no name for that to compare against.
     */
    fun createSourceIndexIfMissing() {
        TxnContext.doInTxn {
            dataService.runMigrations(mock = false, diff = true)
            val tableCtx = dataService.getTableContext()
            val column = tableCtx.getColumnByName(DbAssocBackupEntity.SOURCE_ID)
                ?: error("Column '${DbAssocBackupEntity.SOURCE_ID}' is not found in ${tableCtx.getTableRef().fullName}")
            schemaCtx.dataSourceCtx.schemaDao.createColumnIndexIfMissing(
                schemaCtx.dataSourceCtx.dataSource,
                tableCtx.getTableRef(),
                column
            )
        }
    }

    /**
     * Copies every live association of [attribute] whose source is one of [sourceIds] into the
     * backup keyed by [columnMetaId]. The live rows are left alone - removing them is the caller's
     * job, and it has to go through [DbAssocsService.removeAssocs] so that the other applications
     * holding a back-reference hear about it.
     *
     * @return how many rows were copied.
     */
    fun backupAssocsOf(columnMetaId: Long, attribute: String, sourceIds: Collection<Long>): Int {
        if (sourceIds.isEmpty()) {
            return 0
        }
        val attributeId = attsService.getIdsForAtts(listOf(attribute), false)[attribute] ?: -1L
        if (attributeId == -1L) {
            // the attribute has no id in ed_attributes, so nothing was ever linked through it
            return 0
        }
        return TxnContext.doInTxn {
            val assocs = assocsDataService.findAll(
                Predicates.and(
                    ValuePredicate(DbAssocEntity.SOURCE_ID, ValuePredicate.Type.IN, sourceIds),
                    Predicates.eq(DbAssocEntity.ATTRIBUTE, attributeId)
                ),
                listOf(DbFindSort(DbAssocEntity.INDEX, true))
            )
            if (assocs.isEmpty()) {
                return@doInTxn 0
            }
            // A link this backup already holds is skipped rather than inserted again. The caller
            // removes the live rows in the same transaction, so a second call normally finds
            // nothing left to copy at all - but a duplicate would violate the unique index, and a
            // constraint violation aborts the whole transaction rather than one row, which would
            // wedge the task that called this on every drain tick from then on.
            //
            // Rows left under the same key by an earlier departure are not this method's problem:
            // a snapshot is spent by the restore that reads it, either by being given back or by
            // being consumed when the record turned out to have a value of its own.
            val alreadyBacked = dataService.findAll(
                Predicates.and(
                    Predicates.eq(DbAssocBackupEntity.COLUMN_META_ID, columnMetaId),
                    ValuePredicate(DbAssocBackupEntity.SOURCE_ID, ValuePredicate.Type.IN, sourceIds)
                )
            ).mapTo(HashSet()) { it.sourceId to it.targetId }

            val toSave = assocs.filterNot { (it.sourceId to it.targetId) in alreadyBacked }
                .map { toBackupEntity(columnMetaId, it) }
            if (toSave.isEmpty()) {
                return@doInTxn 0
            }
            dataService.save(toSave).size
        }
    }

    /**
     * The links of one record that left with one departure of the attribute, **in their original
     * order**: a multi-valued association's order is part of its value, and `__index` is the only
     * place it is recorded.
     */
    fun findByColumnMeta(columnMetaId: Long, sourceId: Long): List<DbAssocBackupDto> {
        return TxnContext.doInTxn(readOnly = true) {
            dataService.findAll(
                Predicates.and(
                    Predicates.eq(DbAssocBackupEntity.COLUMN_META_ID, columnMetaId),
                    Predicates.eq(DbAssocBackupEntity.SOURCE_ID, sourceId)
                ),
                listOf(DbFindSort(DbAssocBackupEntity.INDEX, true))
            ).map { toDto(it) }
        }
    }

    /**
     * The same answer as [findByColumnMeta] for a whole window of records, in **one** query.
     *
     * The column migration's value transfer needs this for every row of every batch it walks
     * (the source of an assoc-like column's value is the links, not the
     * column), and calling the single-record form per row would turn one batch into one query per
     * record. The index this table carries is `(__column_meta_id, __source_id)`, which is exactly
     * what an `IN` over the window uses.
     *
     * Sorted by `__index` alone and then grouped: `groupBy` keeps the order it met the elements in,
     * so each record's list comes out in its own `__index` order without asking a backend for a
     * two-column sort. Records with nothing parked are absent from the map rather than present with
     * an empty list - "this record had no links" and "this record was not asked about" are the same
     * answer to the caller, and neither is a value to write.
     */
    fun findByColumnMeta(columnMetaId: Long, sourceIds: Collection<Long>): Map<Long, List<DbAssocBackupDto>> {
        if (sourceIds.isEmpty()) {
            return emptyMap()
        }
        return TxnContext.doInTxn(readOnly = true) {
            dataService.findAll(
                Predicates.and(
                    Predicates.eq(DbAssocBackupEntity.COLUMN_META_ID, columnMetaId),
                    ValuePredicate(DbAssocBackupEntity.SOURCE_ID, ValuePredicate.Type.IN, sourceIds)
                ),
                listOf(DbFindSort(DbAssocBackupEntity.INDEX, true))
            ).map { toDto(it) }.groupBy { it.sourceId }
        }
    }

    /**
     * Puts back the three fields a parked link keeps and [DbAssocsService.createAssocs] cannot.
     *
     * That function is the right way to re-create a link - it is the path other applications hear
     * about, and it refuses a duplicate - but it stamps every row with `Instant.now()`, the caller's
     * creator id and an `__index` re-derived from `max + 1`. Two of those are the user's data:
     * `__index` **is** the order of a multi-valued attribute, and a link re-created with today's
     * timestamp is a different link. So the rows are corrected immediately afterwards, in the same
     * transaction, from the snapshot they came out of.
     *
     * [backups] must be the links actually created, not everything the snapshot held: a link the
     * record already has belongs to its present. Rows are matched on `(attributeId, targetId)` and
     * not on the target alone, so a link of a *different* attribute to the same target is untouched.
     *
     * **`__index` is restored as it was, and may tie** with a link the user added in the interim,
     * which was numbered `max + 1` over a different set. Nothing renumbers either, and the unique
     * constraint is on `(source, attribute, target)` rather than on the index, so a tie costs only
     * the relative order of the two tied links - which no read depends on. Renumbering would be the
     * lossy choice: it would discard the ordering this method exists to keep.
     *
     * @return how many rows were corrected.
     */
    fun restoreAssocsMeta(sourceId: Long, backups: List<DbAssocBackupDto>): Int {
        if (backups.isEmpty()) {
            return 0
        }
        return TxnContext.doInTxn {
            val backupByAssoc = backups.associateBy { it.attributeId to it.targetId }
            val live = assocsDataService.findAll(
                Predicates.and(
                    Predicates.eq(DbAssocEntity.SOURCE_ID, sourceId),
                    ValuePredicate(
                        DbAssocEntity.TARGET_ID,
                        ValuePredicate.Type.IN,
                        backups.mapTo(LinkedHashSet()) { it.targetId }
                    )
                )
            )
            val toSave = live.mapNotNull { entity ->
                val backup = backupByAssoc[entity.attributeId to entity.targetId]
                if (backup == null) {
                    null
                } else {
                    entity.index = backup.index
                    entity.created = backup.created
                    entity.creator = backup.creator
                    entity
                }
            }
            if (toSave.isEmpty()) {
                0
            } else {
                assocsDataService.save(toSave).size
            }
        }
    }

    /**
     * Drops the backup of one record under one departure, for a restore that has already put the
     * links back: keeping them would let a later restore of the same registry row
     * resurrect links the record has since moved on from.
     *
     * Deletes by the explicit ids read first, never by predicate: `delete(predicate)` refuses a
     * predicate that normalises to always-true, and rightly - a mistyped key there empties the one
     * table that is supposed to be the user's last copy.
     */
    fun consume(columnMetaId: Long, sourceId: Long) {
        TxnContext.doInTxn {
            val entities = dataService.findAll(
                Predicates.and(
                    Predicates.eq(DbAssocBackupEntity.COLUMN_META_ID, columnMetaId),
                    Predicates.eq(DbAssocBackupEntity.SOURCE_ID, sourceId)
                )
            )
            dataService.delete(entities)
        }
    }

    /**
     * Forgets one record's parked links to [targetIds] under [attributeId] - whichever departure
     * parked them.
     *
     * The case is a child deleted while the attribute that held it is **not** an association: the
     * links are in this table and the deletion reaches the parent as an ordinary
     * `att_remove` notification, which has no attribute to remove anything from. Without this the
     * snapshot would go on naming a record that no longer exists, and the day the attribute became
     * an association again the restore would create a link to it.
     *
     * Not keyed by the departure, because the notification does not know which one parked the link
     * and does not need to: a target that is gone is gone under every departure of that attribute.
     *
     * Deletes by the explicit ids read first, never by predicate, for the reason [consume] states.
     *
     * @return how many rows were dropped - zero meaning the record had nothing parked for these
     *         targets, which is what tells a caller the notification was not a parked one.
     */
    fun forgetTargets(sourceId: Long, attributeId: Long, targetIds: Collection<Long>): Int {
        if (targetIds.isEmpty()) {
            return 0
        }
        return TxnContext.doInTxn {
            val entities = dataService.findAll(
                Predicates.and(
                    Predicates.eq(DbAssocBackupEntity.SOURCE_ID, sourceId),
                    Predicates.eq(DbAssocBackupEntity.ATTRIBUTE, attributeId),
                    ValuePredicate(DbAssocBackupEntity.TARGET_ID, ValuePredicate.Type.IN, targetIds)
                )
            )
            if (entities.isEmpty()) {
                0
            } else {
                dataService.delete(entities)
                entities.size
            }
        }
    }

    /**
     * Drops everything one record has parked, under every departure, and answers the targets of the
     * **child** links among them.
     *
     * One read for both answers, because both are asked at the same moment and by the same caller:
     * `DbRecordsDeleteDao` deletes a record, and needs its parked children (the cascade it performs
     * from `ed_associations` cannot see them - they are here) and needs this record's rows gone,
     * there being no record left for a restore to put anything back into. Asking separately would
     * read the table twice on **every** deletion of **every** record, where the answer is almost
     * always "nothing".
     *
     * Deletes by the explicit ids read first, never by predicate, for the reason [consume] states.
     */
    fun takeParkedLinksOf(sourceId: Long): List<Long> {
        return TxnContext.doInTxn {
            val entities = dataService.findAll(
                Predicates.eq(DbAssocBackupEntity.SOURCE_ID, sourceId)
            )
            if (entities.isEmpty()) {
                emptyList()
            } else {
                dataService.delete(entities)
                entities.asSequence()
                    .filter { it.child }
                    .mapTo(LinkedHashSet()) { it.targetId }
                    .toList()
            }
        }
    }

    fun resetColumnsCache() {
        dataService.resetColumnsCache()
        assocsDataService.resetColumnsCache()
    }

    private fun toBackupEntity(columnMetaId: Long, assoc: DbAssocEntity): DbAssocBackupEntity {
        val entity = DbAssocBackupEntity()
        entity.id = DbAssocBackupEntity.NEW_REC_ID
        entity.columnMetaId = columnMetaId
        entity.sourceId = assoc.sourceId
        entity.targetId = assoc.targetId
        entity.attributeId = assoc.attributeId
        entity.child = assoc.child
        entity.index = assoc.index
        entity.created = assoc.created
        entity.creator = assoc.creator
        return entity
    }

    private fun toDto(entity: DbAssocBackupEntity): DbAssocBackupDto {
        return DbAssocBackupDto(
            sourceId = entity.sourceId,
            targetId = entity.targetId,
            attributeId = entity.attributeId,
            child = entity.child,
            index = entity.index,
            created = entity.created,
            creator = entity.creator
        )
    }
}
