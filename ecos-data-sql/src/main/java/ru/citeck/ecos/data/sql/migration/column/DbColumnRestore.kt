package ru.citeck.ecos.data.sql.migration.column

import io.github.oshai.kotlinlogging.KotlinLogging
import ru.citeck.ecos.data.sql.columnmeta.DbBackupColumnNames
import ru.citeck.ecos.data.sql.columnmeta.DbColumnConversions
import ru.citeck.ecos.data.sql.columnmeta.DbColumnMetaDto
import ru.citeck.ecos.data.sql.columnmeta.DbColumnSemanticType
import ru.citeck.ecos.data.sql.context.DbTableContext
import ru.citeck.ecos.data.sql.dto.DbColumnDef
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType
import java.time.Instant

/**
 * The attribute is going back to a type it already had.
 *
 * The backup column is renamed back into place and the column that occupied it meanwhile becomes a
 * backup of its own, so the values typed under the "wrong" type are not lost either. What the
 * restored column does **not** have is the rows created while the other type was in force; those are
 * filled in afterwards by the ordinary transfer, under its usual `IS NULL` rule.
 *
 * **Two registry rows are rewritten and no third is created**, because
 * [ru.citeck.ecos.data.sql.columnmeta.DbColumnMetaService] cannot delete a row. The two columns swap
 * names, so each row is rewritten to follow its own data.
 *
 * **[DbShadowColumnTransition.moveAside] follows the same convention, and has to.**
 * `ed_associations_backup.__column_meta_id` names a registry row: a departure parks a column's links
 * under the row describing that column, and a restore raises the links of the row it restored. Those
 * are the same links only while a row never stops describing the column whose data it describes.
 * Recycling the live row for the new, empty column and minting a fresh row for the old data would
 * re-key every link that row owns, and no later restore would look at it again.
 *
 * **What re-using the rows costs.** A restore whose task is cancelled before it was drained leaves
 * the links it was going to give back parked where they are - the record's only copy. Until
 * something drains them the attribute reads empty, and below ten links it reads its stale column
 * cache instead, which is quieter and worse to notice. The way back is `DbBatchTaskService.restart`,
 * or the attribute returning to the type the links left.
 *
 * **How far the invariant reaches.** A registry row never stops describing the column whose data it
 * describes; that part is total. Which row a *return* chooses is a different question, decided by
 * `(attType, multiple)` plus recency, and both can be moved by something other than a departure:
 *
 *  - the match is exact, and `DbDataServiceImpl.writeColumnMeta` can relabel a **live** row's type
 *    before it departs. An `ASSOC`-era link set can end up under `__backup_att_person_multiple`, and
 *    a return to `ASSOC[]` will not look there - returning to `PERSON[]` gives it back. Nothing is
 *    lost, but "a later restore always finds them again" is not true as a sentence;
 *  - recency is `__created` and not the row id, precisely because a restore and a relabel can put
 *    the two out of step - see
 *    [ru.citeck.ecos.data.sql.columnmeta.DbColumnMetaService.findBackups].
 */
class DbColumnRestore(private val tableCtx: DbTableContext) {

    companion object {
        private val log = KotlinLogging.logger {}
    }

    private val schemaCtx = tableCtx.getSchemaCtx()
    private val dataSourceCtx = schemaCtx.dataSourceCtx

    /**
     * The freshest backup of this attribute whose recorded type is
     * the one being returned to, restored - and, if that one cannot be used, the next freshest, and
     * so on.
     *
     * **The fallback is not a nicety.** A registry row can outlive the column it names: a DBA may
     * drop a backup column by hand, and a third-row transition shape leaves a phantom behind. One
     * such row at the head of the list would otherwise hide every older matching backup, and the
     * attribute would take an ordinary transition - a fresh empty column - while the data it was
     * asking for sat in a backup one place further down the list. That sits directly on the
     * guarantee this whole class exists for, and [restore] is already written to decline having
     * touched nothing, which is what makes trying the next candidate safe rather than
     * half-performed.
     */
    fun restoreLatest(
        expectedColumn: DbColumnDef,
        targetType: AttributeType,
        targetChild: Boolean,
        creator: String
    ): DbColumnMigrationParams? {
        return findRestorableBackups(expectedColumn.name, targetType, expectedColumn.multiple)
            .firstNotNullOfOrNull { restore(it, expectedColumn, targetChild, creator) }
    }

    /**
     * The backups of this attribute whose recorded type is the one being returned
     * to, **newest first**. `findBackups` returns them in that order - by `__created`, the moment
     * each row became a backup, and **not** by row id, which a restore and a registry-only relabel
     * can both put out of step with recency - and the order is load-bearing.
     *
     * [targetMultiple] is as much a part of the match as the type: a backup of the other arity is a
     * column of a different physical type - `text` against `text[]` - and restoring it would leave
     * the registry describing the attribute in a way the model contradicts on the very next diff,
     * which would move the column aside again, and again.
     */
    fun findRestorableBackups(
        attId: String,
        targetType: AttributeType,
        targetMultiple: Boolean
    ): List<DbColumnMetaDto> {
        return schemaCtx.columnMetaService.findBackups(tableRef(), attId).filter {
            it.attType == DbColumnSemanticType.Model(targetType) && it.multiple == targetMultiple
        }
    }

    /**
     * [findRestorableBackups]'s first candidate.
     */
    fun findRestorableBackup(
        attId: String,
        targetType: AttributeType,
        targetMultiple: Boolean
    ): DbColumnMetaDto? {
        return findRestorableBackups(attId, targetType, targetMultiple).firstOrNull()
    }

    /**
     * Returns the params of the transfer that still has to run - filling the rows
     * the restored column has nothing for - or null when the restore did not happen at all, in which
     * case the caller carries on with the ordinary transition.
     *
     * **Every reason to answer null is decided before the first `ALTER`**, so a null return always
     * means the table is untouched and the fallback is safe.
     *
     * Three renames, in this order, so that no moment exists in which the attribute's own name is
     * unclaimed by a column holding data:
     *  1. the current column moves to a backup name of its own;
     *  2. the chosen backup takes the attribute's name;
     *  3. both registry rows are rewritten to say so.
     *
     * The description of the column moving aside is read from the registry rather than taken as a
     * parameter, because the registry is the source of truth about a column's
     * semantic type and arity - and it is the same row [DbShadowColumnTransition.moveAside] is
     * handed its `sourceType` from.
     */
    fun restore(
        backup: DbColumnMetaDto,
        expectedColumn: DbColumnDef,
        targetChild: Boolean,
        creator: String
    ): DbColumnMigrationParams? {

        val attId = expectedColumn.name
        val tableRef = tableCtx.getTableRef()
        val targetType = (backup.attType as? DbColumnSemanticType.Model)?.attType ?: return null
        val currentMeta = schemaCtx.columnMetaService.getByTableAndColumn(tableRef.table, attId) ?: run {
            // No registry row for the column being replaced, so there is nothing to describe the
            // backup this restore would make of it - and a backup column the registry cannot
            // describe is a column nothing will ever restore in its turn.
            log.warn {
                "Column '$attId' of ${tableRef.fullName} has no column registry row, so backup " +
                    "${backup.id} is not restored and the ordinary transition is used instead"
            }
            return null
        }
        val allColumns = tableCtx.getAllPhysicalColumns()
        val backupColumn = allColumns.find { it.name == backup.columnName }
        if (backupColumn == null ||
            backupColumn.type != expectedColumn.type ||
            backupColumn.multiple != expectedColumn.multiple
        ) {
            // The registry says this backup holds what the model is asking for, but the column
            // itself is shaped differently - or a DBA has removed it. Restoring it would put a
            // column the model contradicts into the attribute's place, and the next diff would move
            // it aside again. The ordinary transition builds a column that does match.
            log.warn {
                "Backup column '${backup.columnName}' of ${tableRef.fullName} is " +
                    (backupColumn?.let { "${it.type}/${it.multiple}" } ?: "missing") +
                    " where attribute '$attId' expects ${expectedColumn.type}/${expectedColumn.multiple}, " +
                    "so it is not restored and the ordinary transition is used instead"
            }
            return null
        }

        val asideName = DbBackupColumnNames.build(
            attId = attId,
            attType = currentMeta.attType,
            multiple = currentMeta.multiple,
            maxBytes = dataSourceCtx.schemaDao.getMaxColumnNameBytes(),
            taken = allColumns.mapTo(HashSet()) { it.name }
        )
        // 1. and 2. Both renames also drop the single-column indexes of the column being renamed
        //   . For the column moving aside that is the point; for the column coming back
        //    it means the restored column arrives without the model's index, which is why this
        //    restore queues a task exactly as the ordinary transition does - `onFinish` is what
        //    builds the index again.
        dataSourceCtx.schemaDao.renameColumn(dataSourceCtx.dataSource, tableRef, attId, asideName)
        dataSourceCtx.schemaDao.renameColumn(dataSourceCtx.dataSource, tableRef, backup.columnName, attId)

        // 3. The two rows swap roles, each staying with the data it describes.
        //
        //    The order of the two is load-bearing, not cosmetic: `ed_column_meta` carries a unique
        //    index on `(__table, __column_name)`, and the row that frees `attId` has to be written
        //    before the row that claims it. The same ordering rule applies in
        //    [DbShadowColumnTransition.moveAside].
        val now = Instant.now()
        schemaCtx.columnMetaService.saveAll(
            listOf(
                currentMeta.copy(columnName = asideName, backup = true, created = now, creator = creator),
                backup.copy(columnName = attId, backup = false, created = now, creator = creator)
            )
        )
        log.info {
            "Attribute '$attId' of ${tableRef.fullName} returned to " +
                "${targetType.name}${if (expectedColumn.multiple) "[]" else ""}, so backup column " +
                "'${backup.columnName}' was restored into place and the " +
                "${currentMeta.attType.asString()} column it replaced was moved to '$asideName'"
        }
        return DbColumnMigrationParams(
            attId = attId,
            backupColumn = asideName,
            targetColumn = attId,
            sourceType = currentMeta.attType,
            sourceMultiple = currentMeta.multiple,
            targetType = targetType,
            targetMultiple = expectedColumn.multiple,
            // from the model the attribute is returning to, not from the registry row being
            // restored: `ed_column_meta` records an AttributeType and nothing about `child`, and
            // the links that appear on a restore are created by the same transfer that
            // creates them for an ordinary rule-11 change
            targetChild = targetChild,
            targetIndexEnabled = expectedColumn.index.enabled,
            conversionClass = DbColumnConversions.classify(
                currentMeta.attType,
                currentMeta.multiple,
                targetType,
                expectedColumn.multiple
            ),
            backupColumnMetaId = currentMeta.id,
            // the links parked when this attribute last left the association group under the very
            // column that has just come back
            restoredColumnMetaId = backup.id
        )
    }

    private fun tableRef(): String = tableCtx.getTableRef().table
}
