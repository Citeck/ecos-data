package ru.citeck.ecos.data.sql.modelchange

import io.github.oshai.kotlinlogging.KotlinLogging
import ru.citeck.ecos.context.lib.auth.AuthContext
import ru.citeck.ecos.data.sql.columnmeta.DbColumnMetaDto
import ru.citeck.ecos.data.sql.columnmeta.DbColumnSemanticType
import ru.citeck.ecos.data.sql.records.DbRecordsDao
import ru.citeck.ecos.data.sql.records.utils.DbReadToleranceLog
import ru.citeck.ecos.model.lib.type.dto.TypeInfo
import ru.citeck.ecos.model.lib.utils.ModelUtils
import ru.citeck.ecos.webapp.api.entity.EntityRef

/**
 * Reconciling the schema of **one** table's records DAO, in the form a scheduler tick can call:
 * under the system user, without a transaction of its own, and without an exception ever leaving it.
 *
 * Open so that the queue's own tests can substitute a reconciler that fails; production builds one
 * instance and reuses it, because it holds no state - everything it needs comes from the DAO.
 */
open class DbSchemaReconciler {

    companion object {

        private val log = KotlinLogging.logger {}

        /**
         * The cheap answer to "is there any point in migrating this table": one SELECT from
         * `ed_column_meta` against the type model, and no distributed lock.
         *
         * [DbDataServiceImpl.isSchemaChangeRequired][ru.citeck.ecos.data.sql.service.DbDataServiceImpl]
         * cannot serve here even though it looks like the same question: it is answered from this
         * instance's caches, **both empty on a freshly started instance**, and `runMigrations` resets
         * them before every call - so it would say "yes" on every tick of every table for ever. A
         * restart with hundreds of types would take hundreds of locks and enter hundreds of no-op
         * migrations, which is the storm this precheck exists to prevent.
         *
         * The comparison is exactly as strong as the reconciliation that follows it, so "no
         * difference" really means "those migrations would change nothing". Direction of error is
         * asserted by tests: a wrong "yes" costs one idle lock, a wrong "no" is unreachable for a
         * model-vs-registry difference. A registry-vs-physical difference - an administrator who
         * altered a column by hand - is invisible here on purpose; the lazy path still repairs it.
         *
         * An empty registry means "never seeded", not "nothing to do": seeding is itself something
         * only a migration performs.
         */
        fun isReconcileRequired(
            expected: Set<ExpectedColumn>,
            registryByColumn: Map<String, DbColumnMetaDto>
        ): Boolean {
            if (registryByColumn.isEmpty()) {
                return true
            }
            for (column in expected) {
                val meta = registryByColumn[column.name] ?: return true
                if (meta.attType != column.attType || meta.multiple != column.multiple) {
                    return true
                }
            }
            return false
        }

        private fun expectedColumns(dao: DbRecordsDao, typeInfo: TypeInfo): Set<ExpectedColumn> {
            return dao.getRecordsDaoCtx().ecosTypeService
                .getColumnsForTypes(listOf(typeInfo))
                .mapTo(LinkedHashSet()) {
                    ExpectedColumn(
                        it.column.name,
                        DbColumnSemanticType.Model(it.attribute.type),
                        it.column.multiple
                    )
                }
        }

        /**
         * What each type sharing [dao]'s table expects of that table, keyed by type id, the type
         * owning the table first.
         *
         * Per type and not merged, because the two things this class does with the answer need it
         * split: a migration is always driven by one type - that is what a mutation does, and a
         * merged set would have no type to run it for - and two types that disagree about a column
         * have no merged answer at all (`getColumnsForTypes` refuses such a list outright; the
         * disagreement is a frozen column, not a value to pick between).
         */
        private fun expectedColumnsByType(
            dao: DbRecordsDao,
            typeInfo: TypeInfo
        ): Map<String, Set<ExpectedColumn>> {
            val modelService = dao.getRecordsDaoCtx().ecosTypeService
            val result = LinkedHashMap<String, Set<ExpectedColumn>>()
            result[typeInfo.id] = expectedColumns(dao, typeInfo)
            for (type in modelService.getStorageTypes(typeInfo)) {
                if (type.id != typeInfo.id) {
                    result[type.id] = expectedColumns(dao, type)
                }
            }
            return result
        }
    }

    /**
     * The set [reconcile] would compare the registry against right now, or null if it cannot be
     * worked out at all - an unknown type, or anything thrown on the way.
     *
     * Exists for [DbModelChangeQueue]: the queue remembers the fingerprint a
     * reconciliation ended with and needs the current one to tell "the model has not moved since"
     * from "it has". Deliberately the very same computation [reconcile] performs, so that a DAO
     * skipped here is provably a DAO [reconcile] would have found up to date - a fingerprint made
     * of anything coarser would let a model change go unnoticed until the next restart.
     *
     * Null is "do not skip", never "skip": a table whose fingerprint cannot be computed is left for
     * [reconcile] to look at and report on.
     */
    open fun getModelFingerprint(dao: DbRecordsDao): Set<ExpectedColumn>? {
        return try {
            AuthContext.runAsSystem {
                val typeId = dao.getTypeRef().getLocalId()
                val typeInfo = dao.getRecordsDaoCtx().ecosTypeService.getTypeInfo(typeId)
                typeInfo?.let { fingerprintOf(expectedColumnsByType(dao, it)) }
            }
        } catch (e: InterruptedException) {
            Thread.currentThread().interrupt()
            throw e
        } catch (e: Throwable) {
            log.debug(e) { "Model fingerprint of records source '${dao.getId()}' can't be computed" }
            null
        }
    }

    /**
     * Brings [dao]'s table in line with the model of every type stored in it, or explains why it
     * did not.
     *
     * "Every type stored in it" and not only [dao]'s own: a descendant with `DEFAULT` storage has
     * no records DAO of its own and its columns live in this table, so a change announced for it
     * arrives here as this DAO (`DbTypeChangeScope`) and would otherwise find nothing to do.
     *
     * Never throws, with the single exception of [InterruptedException], which belongs to whoever
     * is shutting the tick down rather than to the table. One table's failure must not stop the
     * queue behind it, and an exception thrown out of the event path would be swallowed by
     * `EcosRegistryImpl.fireEvent` anyway - silently, and without the deduplicated warning this
     * produces instead.
     */
    open fun reconcile(dao: DbRecordsDao): Result {
        return try {
            // the trigger has no user of its own, and the migration writes to ed_column_meta,
            // ed_batch_task and the table itself
            AuthContext.runAsSystem {
                reconcileImpl(dao)
            }
        } catch (e: InterruptedException) {
            Thread.currentThread().interrupt()
            throw e
        } catch (e: Throwable) {
            DbReadToleranceLog.warnOnce(log, "schema-reconcile-failed:${dao.getTableRef().fullName}", e) {
                "Schema reconciliation failed for table ${dao.getTableRef().fullName} " +
                    "(records source '${dao.getId()}'). The table keeps its current schema and will " +
                    "be repaired on the next mutation of one of its records."
            }
            Result(Status.FAILED, null, 0)
        }
    }

    private fun reconcileImpl(dao: DbRecordsDao): Result {

        val tableRef = dao.getTableRef()
        val schemaCtx = dao.getSchemaCtx()

        // Tables are created lazily by the first record written to them. Reconciling a table that
        // does not exist would create one per type of the installation on the first tick after a
        // restart - a cost nobody asked for, and rows nobody will ever read.
        val tableExists = schemaCtx.doInNewRoTxn { schemaCtx.isTableExists(tableRef) }
        if (!tableExists) {
            log.debug { "Table ${tableRef.fullName} doesn't exist yet, so there is nothing to reconcile" }
            return Result(Status.TABLE_NOT_EXISTS, null, 0)
        }

        val typeId = dao.getTypeRef().getLocalId()
        val typeInfo = dao.getRecordsDaoCtx().ecosTypeService.getTypeInfo(typeId)
        if (typeInfo == null) {
            // a type can be deleted between the moment its change is announced and the moment the
            // queue reaches it, and its DAO stays registered until something unregisters it
            log.debug {
                "Type '$typeId' of records source '${dao.getId()}' is not found, " +
                    "so table ${tableRef.fullName} is skipped"
            }
            return Result(Status.TYPE_NOT_FOUND, null, 0)
        }

        val columnsByType = expectedColumnsByType(dao, typeInfo)
        val fingerprint = fingerprintOf(columnsByType)
        val registry = schemaCtx.columnMetaService.getLiveByTable(tableRef.table)
        val expected = expectedColumnsToCompare(columnsByType, typeInfo.id, registry)
        if (!isReconcileRequired(expected, registry)) {
            return Result(Status.UP_TO_DATE, fingerprint, 0)
        }

        // One migration per type that needs one, and none for a type that does not: each of them
        // opens its own transaction and takes the migration lock inside it, which is the cost the
        // precheck above exists to avoid paying for nothing. This loop reaches at least one type
        // whenever the precheck said yes - the difference it found is either in the table's own
        // type, which `isReconcileRequired` sees below, or in a column of another type, which by
        // construction of `expected` is in the registry and therefore differs there too.
        //
        // EntityRef.EMPTY rather than the type's own ref for the table's own type: it makes the DAO
        // resolve its configured type, which is the call every other caller of runMigrations makes.
        // There is deliberately nothing transactional wrapped around any of this.
        var commandsCount = 0
        for ((otherTypeId, columns) in columnsByType) {
            val isOwnType = otherTypeId == typeInfo.id
            val migrationNeeded = if (isOwnType) {
                isReconcileRequired(columns, registry)
            } else {
                isMigrationNeededFor(columns, registry)
            }
            if (migrationNeeded) {
                val typeRef = if (isOwnType) EntityRef.EMPTY else ModelUtils.getTypeRef(otherTypeId)
                commandsCount += dao.runMigrations(typeRef, mock = false, diff = true).size
            }
        }
        log.debug { "Table ${tableRef.fullName} reconciled with $commandsCount schema command(s)" }
        return Result(Status.RECONCILED, fingerprint, commandsCount)
    }

    /**
     * Every column every type of the storage declares, disagreements included.
     *
     * This is the fingerprint the queue stores, and deliberately **not** the set the precheck
     * compares: it is derived from the model alone, so "the fingerprint did not move" means "no
     * type of this table changed" and nothing else. Narrowing it the way
     * [expectedColumnsToCompare] narrows the comparison would make a skip depend on the column
     * registry, and a table would then have to be read before it could be skipped - which is the
     * one thing the fingerprint exists to avoid.
     */
    private fun fingerprintOf(columnsByType: Map<String, Set<ExpectedColumn>>): Set<ExpectedColumn> {
        val result = LinkedHashSet<ExpectedColumn>()
        columnsByType.values.forEach { result.addAll(it) }
        return result
    }

    /**
     * The set the precheck compares against the registry: everything the table's own type declares,
     * plus - from the other types sharing the table - only the columns the registry already knows.
     *
     * The narrowing is what makes reconciling a shared table safe to act on, and it answers two
     * different hazards with one rule:
     *  - a descendant's column that does not exist yet must not be **created** here. Columns of a
     *    type with no records in this table appear when its first record is written, and bringing
     *    that forward would add columns nobody has written to on every table of the installation;
     *  - the "types sharing a table" grouping is over-broad when the resolved `sourceId` is blank
     *    (see `DbEcosModelService.getTableAttTypes`), so the list can legitimately contain types
     *    that share nothing with this table at all. Over-detection there is documented as harmless
     *    *because it only ever freezes a column* - it stops being harmless the moment something
     *    migrates because of it, and this narrowing is what keeps that true: an unrelated type can
     *    only be considered when it declares a column this table already has, which is the frozen
     *    case and therefore a migration that does nothing.
     */
    private fun expectedColumnsToCompare(
        columnsByType: Map<String, Set<ExpectedColumn>>,
        ownTypeId: String,
        registryByColumn: Map<String, DbColumnMetaDto>
    ): Set<ExpectedColumn> {
        val result = LinkedHashSet<ExpectedColumn>()
        for ((typeId, columns) in columnsByType) {
            if (typeId == ownTypeId) {
                result.addAll(columns)
            } else {
                columns.filterTo(result) { registryByColumn.containsKey(it.name) }
            }
        }
        return result
    }

    /**
     * Whether a type other than the table's own is worth running a migration for: it is, exactly
     * when a column it declares already exists in the registry with a different shape.
     *
     * A missing column is deliberately not a reason - see [expectedColumnsToCompare].
     */
    private fun isMigrationNeededFor(
        columns: Set<ExpectedColumn>,
        registryByColumn: Map<String, DbColumnMetaDto>
    ): Boolean {
        return columns.any { column ->
            val meta = registryByColumn[column.name] ?: return@any false
            meta.attType != column.attType || meta.multiple != column.multiple
        }
    }

    /**
     * One column as the type model expects it to be. Also the unit of the model fingerprint
     * the queue remembers the fingerprint a reconciliation ended with and skips the
     * DAO until the model moves, which is what keeps a frozen column from asking for the
     * lock on every tick forever - the registry keeps describing the old type there by definition,
     * so the precheck alone would answer "yes" every time.
     */
    data class ExpectedColumn(
        val name: String,
        val attType: DbColumnSemanticType,
        val multiple: Boolean
    )

    /**
     * [fingerprint] is null exactly when no comparison was made - i.e. for every skipped outcome -
     * so that the queue cannot mistake "checked, and the model expects nothing" for "never checked".
     */
    data class Result(
        val status: Status,
        val fingerprint: Set<ExpectedColumn>?,
        val commandsCount: Int
    )

    enum class Status {

        /**
         * The migration ran. It may still have emitted no command - a semantic-only change does.
         */
        RECONCILED,

        /**
         * The precheck found the registry already describing what the model asks for.
         */
        UP_TO_DATE,

        /**
         * The table has not been created yet, and reconciliation is not what creates it.
         */
        TABLE_NOT_EXISTS,

        /**
         * The DAO's type is gone from the model.
         */
        TYPE_NOT_FOUND,

        /**
         * Something threw. The table is unchanged and the failure was reported once.
         */
        FAILED
    }
}
