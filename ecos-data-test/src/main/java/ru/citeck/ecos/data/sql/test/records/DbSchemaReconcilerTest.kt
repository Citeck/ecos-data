package ru.citeck.ecos.data.sql.test.records

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import ru.citeck.ecos.data.sql.columnmeta.DbColumnMetaEntity
import ru.citeck.ecos.data.sql.columnmeta.DbColumnSemanticType
import ru.citeck.ecos.data.sql.dto.DbColumnType
import ru.citeck.ecos.data.sql.modelchange.DbSchemaReconciler
import ru.citeck.ecos.data.sql.records.DbRecordsDao
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType
import ru.citeck.ecos.model.lib.type.dto.TypeInfo
import ru.citeck.ecos.model.lib.type.dto.TypeModelDef
import java.io.ByteArrayOutputStream
import java.io.PrintStream

/**
 * The unit of reconciliation: what one table costs when nothing changed, what it does when
 * something did, and what it does instead of throwing when it cannot.
 *
 * Everything here is asserted through [DbSchemaReconciler.reconcile] rather than against the
 * comparison function directly - the property that matters is not "the comparison said no" but
 * "nothing was migrated and no distributed lock was taken because of it", and only the whole call
 * shows that.
 */
class DbSchemaReconcilerTest : DbRecordsTestBase() {

    private val reconciler = DbSchemaReconciler()

    private fun isTableExists(dao: DbRecordsDao): Boolean {
        val schemaCtx = dao.getSchemaCtx()
        return schemaCtx.doInNewRoTxn { schemaCtx.isTableExists(dao.getTableRef()) }
    }

    private fun textAtt(id: String) = AttributeDef.create { withId(id) }

    private fun att(id: String, type: AttributeType, multiple: Boolean = false) = AttributeDef.create {
        withId(id)
        withType(type)
        withMultiple(multiple)
    }

    /**
     * slf4j-simple (the test binding) writes to stderr and this project has no in-JVM log capture
     * utility - the same approach [DbShadowColumnTransitionTest] uses.
     */
    private fun captureStderr(action: () -> Unit): String {
        val original = System.err
        val captured = ByteArrayOutputStream()
        System.setErr(PrintStream(captured, true))
        try {
            action()
        } finally {
            System.setErr(original)
        }
        return captured.toString()
    }

    // ---------------------------------------------------------------------------------------
    // The cheap precheck: what it costs to answer "nothing to do here".
    // ---------------------------------------------------------------------------------------

    /**
     * The whole point of the precheck, and the one assertion that measures it: an ordinary restart
     * must cost one SELECT per table, not one distributed lock per table.
     */
    @Test
    fun aConsistentTableIsNotReconciledAndTakesNoLockTest() {

        registerAtts(listOf(textAtt("someAtt")))
        createRecord("someAtt" to "value-0")

        val locksBefore = schemaMigrationLockCallCount()
        val result = reconciler.reconcile(mainCtx.dao)

        assertThat(schemaMigrationLockCallCount())
            .describedAs("the precheck must answer without the migration lock, not despite it")
            .isEqualTo(locksBefore)
        assertThat(result.status).isEqualTo(DbSchemaReconciler.Status.UP_TO_DATE)
        assertThat(result.commandsCount).isEqualTo(0)
    }

    @Test
    fun anAttributeTypeChangeIsSeenByThePrecheckTest() {

        registerAtts(listOf(textAtt("someAtt")))
        createRecord("someAtt" to "value-0")

        registerAtts(listOf(att("someAtt", AttributeType.NUMBER)))

        assertThat(reconciler.reconcile(mainCtx.dao).status)
            .isEqualTo(DbSchemaReconciler.Status.RECONCILED)
    }

    /**
     * Nothing about the attribute changes except its multiplicity. The
     * registry row carries `multiple`, so the precheck sees it - this is exactly the change
     * `isColumnSchemaUpdateRequired` historically ignored.
     */
    @Test
    fun aMultiplicityOnlyChangeIsSeenByThePrecheckTest() {

        registerAtts(listOf(att("someAtt", AttributeType.TEXT, multiple = true)))
        createRecord("someAtt" to listOf("value-0"))

        registerAtts(listOf(att("someAtt", AttributeType.TEXT, multiple = false)))

        assertThat(reconciler.reconcile(mainCtx.dao).status)
            .isEqualTo(DbSchemaReconciler.Status.RECONCILED)
    }

    @Test
    fun aNewAttributeIsSeenByThePrecheckTest() {

        registerAtts(listOf(textAtt("someAtt")))
        createRecord("someAtt" to "value-0")

        registerAtts(listOf(textAtt("someAtt"), textAtt("newAtt")))

        assertThat(reconciler.reconcile(mainCtx.dao).status)
            .isEqualTo(DbSchemaReconciler.Status.RECONCILED)
        assertThat(getColumns().map { it.name })
            .describedAs("and the column is really added, without a single mutation")
            .contains("newAtt")
    }

    /**
     * The upgrade case: a table whose registry rows are missing has to be reconciled even
     * if nothing else says so, because seeding the registry is itself something only a migration
     * does.
     */
    @Test
    fun aTableWithNoRegistryRowsIsAlwaysReconciledTest() {

        assumeRawSqlSupported()

        registerAtts(listOf(textAtt("someAtt")))
        createRecord("someAtt" to "value-0")

        val metaTable = tableRef.withTable(DbColumnMetaEntity.TABLE).fullName
        sqlUpdate("DELETE FROM $metaTable WHERE \"${DbColumnMetaEntity.TABLE_ID}\" = '${tableRef.table}'")

        assertThat(reconciler.reconcile(mainCtx.dao).status)
            .isEqualTo(DbSchemaReconciler.Status.RECONCILED)
    }

    /**
     * The error direction of the precheck, asserted rather than argued: "yes" must not stick. If it
     * did, every tick would take the lock for a table that was repaired on the first one.
     */
    @Test
    fun aReconciledTableIsNotReconciledAgainTest() {

        registerAtts(listOf(textAtt("someAtt")))
        createRecord("someAtt" to "value-0")
        registerAtts(listOf(att("someAtt", AttributeType.NUMBER)))

        assertThat(reconciler.reconcile(mainCtx.dao).status)
            .isEqualTo(DbSchemaReconciler.Status.RECONCILED)

        val locksBefore = schemaMigrationLockCallCount()
        val second = reconciler.reconcile(mainCtx.dao)

        assertThat(schemaMigrationLockCallCount()).isEqualTo(locksBefore)
        assertThat(second.status).isEqualTo(DbSchemaReconciler.Status.UP_TO_DATE)
        assertThat(second.commandsCount).isEqualTo(0)
    }

    // ---------------------------------------------------------------------------------------
    // The unit of reconciliation.
    // ---------------------------------------------------------------------------------------

    /**
     * The feature in one test: the model changed, nobody wrote anything, and the schema followed
     * anyway. TEXT -> NUMBER is LOSSY, so the old column is kept under its backup name and the
     * user's data is still there to be carried across by the background task.
     */
    @Test
    fun aTypeChangeIsAppliedWithoutAnyMutationTest() {

        registerAtts(listOf(textAtt("someAtt")))
        val rec = createRecord("someAtt" to "value-0")
        val modifiedBefore = records.getAtt(rec, "_modified").asText()

        registerAtts(listOf(att("someAtt", AttributeType.NUMBER)))

        val result = reconciler.reconcile(mainCtx.dao)

        assertThat(result.status).isEqualTo(DbSchemaReconciler.Status.RECONCILED)
        assertThat(getColumns().first { it.name == "someAtt" }.type)
            .describedAs("the column now has the type the model asks for")
            .isEqualTo(DbColumnType.DOUBLE)
        assertThat(getTableCtx().getAllPhysicalColumns().map { it.name })
            .describedAs("and the guarantee of the whole feature - the old values are still there")
            .contains("__backup_someAtt_text")
        assertThat(records.query(baseQuery).getRecords()).hasSize(1)
        assertThat(records.getAtt(rec, "_modified").asText())
            .describedAs("no record was mutated to get here")
            .isEqualTo(modifiedBefore)
    }

    /**
     * Tables are created lazily by the first write. A reconciliation that created them would
     * materialize a table for every type of the installation on the first tick after a restart.
     */
    @Test
    fun aDaoWhoseTableDoesNotExistIsSkippedTest() {

        assertThat(isTableExists(tempCtx.dao)).isFalse()

        val result = reconciler.reconcile(tempCtx.dao)

        assertThat(result.status).isEqualTo(DbSchemaReconciler.Status.TABLE_NOT_EXISTS)
        assertThat(result.commandsCount).isEqualTo(0)
        assertThat(isTableExists(tempCtx.dao))
            .describedAs("reconciliation must not be what creates a table")
            .isFalse()
    }

    /**
     * A type can be deleted between the moment its change is announced and the moment the tick gets
     * to it, so a missing type is a normal outcome and not an error.
     */
    @Test
    fun aDaoWhoseTypeIsGoneIsSkippedTest() {

        registerAtts(listOf(textAtt("someAtt")))
        createRecord("someAtt" to "value-0")

        unregisterType(REC_TEST_TYPE_ID)

        val result = reconciler.reconcile(mainCtx.dao)

        assertThat(result.status).isEqualTo(DbSchemaReconciler.Status.TYPE_NOT_FOUND)
        assertThat(result.commandsCount).isEqualTo(0)
    }

    /**
     * One broken table must not stop the queue. The failure used here is the column-name guard,
     * which every backend reports at the same limit, and it is thrown from inside the migration -
     * i.e. from the part the reconciler delegates to and cannot validate up front.
     */
    @Test
    fun aFailingMigrationIsSwallowedTest() {

        registerAtts(listOf(textAtt("someAtt")))
        createRecord("someAtt" to "value-0")

        val tooLongAtt = "a".repeat(dbSchemaDao.getMaxColumnNameBytes() + 1)
        registerAtts(listOf(textAtt("someAtt"), textAtt(tooLongAtt)))

        val result = reconciler.reconcile(mainCtx.dao)

        assertThat(result.status).isEqualTo(DbSchemaReconciler.Status.FAILED)
        assertThat(result.commandsCount).isEqualTo(0)
        assertThat(getColumns().map { it.name })
            .describedAs("the table is left exactly as it was")
            .doesNotContain(tooLongAtt)
    }

    /**
     * The bearing case. Two types sharing one table disagree about `someAtt`, so the column freezes
     * the column - and the trigger path has to respect that freeze exactly as the mutation path
     * does. It does so by construction, because it calls the same `runMigrations`; this is the
     * assertion that turns "by construction" into a fact.
     */
    @Test
    fun aFrozenColumnStaysFrozenWhenReconciledTest() {

        registerAtts(listOf(att("frozenAtt", AttributeType.NUMBER)))
        val rec = createRecord("frozenAtt" to 42)
        registerType(
            TypeInfo.create {
                withId("child-type")
                withParentRef(REC_TEST_TYPE_REF)
                withModel(
                    TypeModelDef.create()
                        .withAttributes(listOf(att("frozenAtt", AttributeType.TEXT)))
                        .build()
                )
            }
        )
        // the parent now moves the attribute somewhere the child does not follow, so the two
        // declarations disagree and there is no answer the migration could act on
        registerAtts(listOf(att("frozenAtt", AttributeType.BOOLEAN)))

        val log = captureStderr {
            assertThat(reconciler.reconcile(mainCtx.dao).status)
                .isEqualTo(DbSchemaReconciler.Status.RECONCILED)
        }

        assertThat(log)
            .describedAs("the freeze must be a decision the migration reached, and it has to be visible")
            .contains("declare attribute 'frozenAtt' with different types")
        assertThat(getColumns().first { it.name == "frozenAtt" }.type)
            .describedAs("the column must not move while two types disagree about it")
            .isEqualTo(DbColumnType.DOUBLE)
        assertThat(getTableCtx().getAllPhysicalColumns().map { it.name })
            .describedAs("freezing means nothing happens at all - not even a backup")
            .doesNotContain("__backup_frozenAtt_double")
        assertThat(records.getAtt(rec, "frozenAtt").asDouble()).isEqualTo(42.0)
        assertThat(
            getTableCtx().getSchemaCtx().columnMetaService
                .getLiveByTable(tableRef.table)
                .getValue("frozenAtt").attType
        ).isEqualTo(DbColumnSemanticType.Model(AttributeType.NUMBER))
    }

    // ---------------------------------------------------------------------------------------
    // Types that share the table: a descendant with DEFAULT storage has no DAO of its own, so
    // its columns can only ever be reconciled through the DAO of the type that owns the table.
    // ---------------------------------------------------------------------------------------

    /**
     * The gap this class would otherwise leave wide open. The descendant's records live in the
     * ancestor's table, the change is announced for the descendant and resolved to the ancestor's
     * DAO - and a reconciliation that compared only the ancestor's own model would answer
     * "up to date" and hand the column back to the lazy mutation path for ever.
     */
    @Test
    fun aColumnDeclaredByADescendantIsReconciledTest() {

        registerAtts(listOf(textAtt("someAtt")))
        // the ancestor's own column has to exist before the descendant's change, or the migration
        // of the ancestor's type would be needed anyway - for creating it - and the lock count
        // below would prove nothing
        createRecord("someAtt" to "value-0")
        registerType(
            TypeInfo.create {
                withId("reconciler-child")
                withParentRef(REC_TEST_TYPE_REF)
                withModel(
                    TypeModelDef.create().withAttributes(listOf(textAtt("childAtt"))).build()
                )
            }
        )
        createRecord("_type" to "reconciler-child", "childAtt" to "value-0")

        updateType("reconciler-child") { type ->
            type.withModel(
                TypeModelDef.create()
                    .withAttributes(listOf(att("childAtt", AttributeType.NUMBER)))
                    .build()
            )
        }

        val locksBefore = schemaMigrationLockCallCount()
        assertThat(reconciler.reconcile(mainCtx.dao).status)
            .describedAs("the precheck has to see a change the ancestor's own model does not carry")
            .isEqualTo(DbSchemaReconciler.Status.RECONCILED)
        assertThat(getColumns().first { it.name == "childAtt" }.type).isEqualTo(DbColumnType.DOUBLE)
        assertThat(schemaMigrationLockCallCount() - locksBefore)
            .describedAs(
                "one type needed a migration, so one lock - the type owning the table declares " +
                    "nothing that moved and must not be migrated for company"
            )
            .isEqualTo(1)
    }

    /**
     * The other side of the same rule, and the reason it is written as a narrowing rather than as
     * "reconcile every type of the storage": a type whose records have never been written to this
     * table must not get columns here. They appear when its first record does, the way they always
     * have - reconciliation repairs what exists, it does not pre-create what might.
     *
     * The same narrowing is what keeps the deliberately over-broad grouping of a blank resolved
     * `sourceId` harmless: without it, two unrelated types could add columns to each other's tables.
     */
    @Test
    fun aColumnOfADescendantWithNoRecordsIsNotCreatedTest() {

        registerAtts(listOf(textAtt("someAtt")))
        createRecord("someAtt" to "value-0")
        registerType(
            TypeInfo.create {
                withId("reconciler-child-without-records")
                withParentRef(REC_TEST_TYPE_REF)
                withModel(
                    TypeModelDef.create()
                        .withAttributes(listOf(textAtt("neverWrittenAtt")))
                        .build()
                )
            }
        )

        assertThat(reconciler.reconcile(mainCtx.dao).status)
            .describedAs("nothing the ancestor declares has moved")
            .isEqualTo(DbSchemaReconciler.Status.UP_TO_DATE)
        assertThat(getTableCtx().getAllPhysicalColumns().map { it.name })
            .doesNotContain("neverWrittenAtt")

        // and the same holds when something else does make the table reconcile
        registerAtts(listOf(textAtt("someAtt"), textAtt("anotherAtt")))
        assertThat(reconciler.reconcile(mainCtx.dao).status)
            .isEqualTo(DbSchemaReconciler.Status.RECONCILED)
        assertThat(getTableCtx().getAllPhysicalColumns().map { it.name })
            .describedAs("a reconciliation of this table is not an invitation to build another type's columns")
            .contains("anotherAtt")
            .doesNotContain("neverWrittenAtt")
    }
}
