package ru.citeck.ecos.data.sql.test.records

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import ru.citeck.ecos.data.sql.batch.DbBatchTaskStatus
import ru.citeck.ecos.data.sql.columnmeta.DbConversionClass
import ru.citeck.ecos.data.sql.migration.column.DbColumnMigrationParams
import ru.citeck.ecos.data.sql.migration.column.DbColumnRestore
import ru.citeck.ecos.model.lib.attributes.dto.AttIndexDef
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType
import ru.citeck.ecos.model.lib.type.dto.TypeInfo
import ru.citeck.ecos.model.lib.type.dto.TypeModelDef
import ru.citeck.ecos.txn.lib.TxnContext
import ru.citeck.ecos.webapp.api.entity.EntityRef
import java.io.ByteArrayOutputStream
import java.io.PrintStream

/**
 * What the synchronous phase does when it cannot convert a column in place.
 */
class DbShadowColumnTransitionTest : DbRecordsTestBase() {

    private fun queuedTasks() = TxnContext.doInTxn {
        getTableCtx().getSchemaCtx().batchTaskService.findByTable(tableRef.table)
    }

    private fun params() = DbColumnMigrationParams.from(queuedTasks().single().params)

    /**
     * Captures stderr around a block of code and returns what was logged to it. slf4j-simple (the
     * test binding) writes there by default, and this project has no in-JVM log-capture utility -
     * the same approach [DbLargeTableMigrationTest] uses.
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

    /**
     * Two types sharing one table that disagree about `someAtt`, built the way
     * [DbColumnTypeConflictTest] builds its fixture: the parent holds the attribute as text and a
     * record's value is already in the column, while the child - whose records live in the very
     * same table - declares it as an assoc. Mutating a child record is what makes the migration
     * reach a decision about the column, so the caller does that and the freeze is what must
     * happen instead of a transition.
     *
     * @return the parent's record, whose value must survive untouched.
     */
    private fun prepareConflictingTypes(): EntityRef {
        registerAtts(listOf(AttributeDef.create { withId("someAtt") }))
        val parentRec = createRecord("someAtt" to "value-0")
        registerType(
            TypeInfo.create {
                withId("child-type")
                withParentRef(REC_TEST_TYPE_REF)
                withModel(
                    TypeModelDef.create()
                        .withAttributes(
                            listOf(
                                AttributeDef.create()
                                    .withId("someAtt")
                                    .withType(AttributeType.ASSOC)
                                    .build()
                            )
                        )
                        .build()
                )
            }
        )
        return parentRec
    }

    @Test
    fun anUnconvertibleTypeChangeMovesTheColumnAsideInsteadOfRefusingTest() {

        registerAtts(listOf(AttributeDef.create { withId("someAtt") }))
        val rec = createRecord("someAtt" to "value-0")

        // TEXT -> ASSOC: lossy, and no backend can cast it
        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("someAtt")
                    withType(AttributeType.ASSOC)
                }
            )
        )
        createRecord("someAtt" to null)

        val ctx = getTableCtx()
        assertThat(ctx.getAllPhysicalColumns().map { it.name })
            .describedAs("the old column is still there under its backup name, with the user's data in it")
            .contains("__backup_someAtt_text")
        val target = ctx.getColumns().first { it.name == "someAtt" }
        assertThat(target.type.name)
            .describedAs("and a fresh column of the target's physical type stands in its place")
            .isEqualTo("LONG")
        assertThat(records.getAtt(rec, "someAtt").asText())
            .describedAs("the new column starts empty - filling it is the background task's job")
            .isEmpty()
    }

    @Test
    fun theTransitionQueuesExactlyOneTaskDescribingItselfFullyTest() {

        registerAtts(listOf(AttributeDef.create { withId("someAtt") }))
        createRecord("someAtt" to "value-0")
        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("someAtt")
                    withType(AttributeType.ASSOC)
                }
            )
        )
        createRecord("someAtt" to null)

        assertThat(queuedTasks()).hasSize(1)
        assertThat(queuedTasks().single().handler).isEqualTo(DbColumnMigrationParams.HANDLER_TYPE)
        val p = params()
        assertThat(p.attId).isEqualTo("someAtt")
        assertThat(p.backupColumn).isEqualTo("__backup_someAtt_text")
        assertThat(p.targetColumn).isEqualTo("someAtt")
        assertThat(p.targetType).isEqualTo(AttributeType.ASSOC)
        assertThat(p.conversionClass).isEqualTo(DbConversionClass.LOSSY)
        assertThat(p.backupColumnMetaId)
            .describedAs("the associations backed up for this transition are keyed by this row")
            .isGreaterThan(0)
    }

    @Test
    fun bothColumnsAreDescribedInTheRegistryTest() {

        registerAtts(listOf(AttributeDef.create { withId("someAtt") }))
        createRecord("someAtt" to "value-0")
        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("someAtt")
                    withType(AttributeType.ASSOC)
                }
            )
        )
        createRecord("someAtt" to null)

        val schemaCtx = getTableCtx().getSchemaCtx()
        val backup = TxnContext.doInTxn {
            schemaCtx.columnMetaService.getByTableAndColumn(tableRef.table, "__backup_someAtt_text")
        }!!
        assertThat(backup.backup).isTrue()
        assertThat(backup.attId)
            .describedAs("a backup keeps the id of the attribute it belonged to, so 8.5 can find it")
            .isEqualTo("someAtt")
        assertThat(backup.attType.asString()).isEqualTo("text")

        val actual = TxnContext.doInTxn {
            schemaCtx.columnMetaService.getByTableAndColumn(tableRef.table, "someAtt")
        }!!
        assertThat(actual.backup).isFalse()
        assertThat(actual.attType.asString()).isEqualTo("assoc")
    }

    @Test
    fun aSemanticOnlyChangeIsNoticedEvenThoughThePhysicalTypeIsUnchangedTest() {

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("someAtt")
                    withType(AttributeType.ASSOC)
                }
            )
        )
        createRecord("someAtt" to null)

        // ASSOC -> ENTITY_REF: lossy. Both are physically LONG, so a diff that compares
        // column definitions sees nothing at all - and the values live in different places.
        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("someAtt")
                    withType(AttributeType.ENTITY_REF)
                }
            )
        )
        createRecord("someAtt" to null)

        assertThat(queuedTasks())
            .describedAs("the registry is what makes this change visible; without it the data silently lies")
            .hasSize(1)
        assertThat(params().backupColumn).isEqualTo("__backup_someAtt_assoc")
    }

    @Test
    fun aSafeInPlaceConversionStillGoesInPlaceAndQueuesNothingTest() {

        registerAtts(listOf(AttributeDef.create { withId("someAtt") }))
        val rec = createRecord("someAtt" to "value-0")

        // TEXT -> TEXT[]: a widening, a pure cast the backend can do, table far below the threshold
        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("someAtt")
                    withMultiple(true)
                }
            )
        )
        createRecord("someAtt" to listOf("a", "b"))

        assertThat(queuedTasks())
            .describedAs("an in-place conversion needs no background work and must not queue any")
            .isEmpty()
        assertThat(getTableCtx().getAllPhysicalColumns().map { it.name })
            .describedAs("and it leaves no backup behind, because the column never moved")
            .doesNotContain("__backup_someAtt_text")
        assertThat(records.getAtt(rec, "someAtt[]").asStrList()).containsExactly("value-0")
    }

    @Test
    fun aSemanticChangeThatMovesNoBytesIsRecordedWithoutMovingTheColumnTest() {

        registerAtts(
            listOf(
                AttributeDef.create { withId("someAtt") },
                AttributeDef.create { withId("otherAtt") }
            )
        )
        val rec = createRecord("someAtt" to "value-0")

        // TEXT -> OPTIONS: the same bytes with the same meaning and the same search
        // behaviour. Nothing has to move, so nothing may - a backup here would make the value
        // disappear until a background task copied it back onto itself.
        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("someAtt")
                    withType(AttributeType.OPTIONS)
                },
                AttributeDef.create { withId("otherAtt") }
            )
        )
        // the migration decision is reached from any mutation of this table; writing someAtt itself
        // would only add an options-value validation this test has no interest in
        createRecord("otherAtt" to "trigger")

        assertThat(getTableCtx().getAllPhysicalColumns().map { it.name })
            .describedAs("a free rename of the type must not cost a backup column")
            .doesNotContain("__backup_someAtt_text")
        assertThat(queuedTasks())
            .describedAs("nor a background task")
            .isEmpty()
        assertThat(records.getAtt(rec, "someAtt").asText())
            .describedAs("and the value stays readable throughout")
            .isEqualTo("value-0")
        assertThat(
            TxnContext.doInTxn {
                getTableCtx().getSchemaCtx().columnMetaService
                    .getByTableAndColumn(tableRef.table, "someAtt")
            }!!.attType.asString()
        )
            .describedAs("the registry follows the model, because the bytes really do mean that now")
            .isEqualTo("options")
    }

    @Test
    fun narrowingAnArrayIsNeverJustARegistryCorrectionTest() {

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("someAtt")
                    withMultiple(true)
                },
                AttributeDef.create { withId("otherAtt") }
            )
        )
        val rec = createRecord("someAtt" to listOf("a", "b"))

        // TEXT[] -> OPTIONS: the type half of this change is free - the same bare strings - but
        // the multiplicity half is not: multiple -> single is lossy, the first element is taken and
        // the rest stays in the backup. A change that is free in one half and lossy in the other is
        // lossy, because the two halves combine by the more cautious answer.
        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("someAtt")
                    withType(AttributeType.OPTIONS)
                },
                AttributeDef.create { withId("otherAtt") }
            )
        )
        createRecord("otherAtt" to "trigger")

        val ctx = getTableCtx()
        assertThat(ctx.getAllPhysicalColumns().first { it.name == "__backup_someAtt_text_multiple" }.multiple)
            .describedAs("the array the model no longer wants is moved aside, whole")
            .isTrue()
        assertThat(ctx.getColumns().first { it.name == "someAtt" }.multiple)
            .describedAs("and the column standing in its place is the scalar the model asked for")
            .isFalse()

        val p = params()
        assertThat(p.sourceMultiple).isTrue()
        assertThat(p.targetMultiple).isFalse()
        assertThat(p.conversionClass)
            .describedAs("a registry correction would have claimed this is free; it is not")
            .isEqualTo(DbConversionClass.LOSSY)

        val backupMeta = TxnContext.doInTxn {
            ctx.getSchemaCtx().columnMetaService
                .getByTableAndColumn(tableRef.table, "__backup_someAtt_text_multiple")
        }!!
        assertThat(backupMeta.multiple)
            .describedAs("both values are still readable from the backup, which is described as the array it is")
            .isTrue()
        assertThat(backupMeta.attType.asString()).isEqualTo("text")
        assertThat(records.getAtt(rec, "someAtt[]").asStrList())
            .describedAs("the new column starts empty - taking the first element is the transfer's job")
            .isEmpty()
    }

    @Test
    fun narrowingAnArrayWithoutChangingItsTypeStillMigratesTest() {

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("someAtt")
                    withMultiple(true)
                }
            )
        )
        val rec = createRecord("someAtt" to listOf("a", "b"))

        // TEXT[] -> TEXT: the attribute type does not move at all, and the physical diff ignores an
        // array narrowed to a scalar by long-standing design - so without the registry's own record
        // of multiplicity neither half of the diff would fire and the column would stay an array
        // for ever while the model said scalar - the "multiple -> single is silently ignored"
        // divergence. It is lossy: the first element is taken, the rest stays in the backup.
        registerAtts(listOf(AttributeDef.create { withId("someAtt") }))
        createRecord("someAtt" to "c")

        val ctx = getTableCtx()
        assertThat(ctx.getAllPhysicalColumns().first { it.name == "__backup_someAtt_text_multiple" }.multiple)
            .describedAs("the array is moved aside whole - the values beyond the first live only there")
            .isTrue()
        assertThat(ctx.getColumns().first { it.name == "someAtt" }.multiple)
            .describedAs("and the model finally gets the scalar column it has been asking for")
            .isFalse()

        val p = params()
        assertThat(p.sourceMultiple).isTrue()
        assertThat(p.targetMultiple).isFalse()
        assertThat(p.conversionClass)
            .describedAs("narrowing loses everything after the first element, whatever the type does")
            .isEqualTo(DbConversionClass.LOSSY)

        val backupMeta = TxnContext.doInTxn {
            ctx.getSchemaCtx().columnMetaService
                .getByTableAndColumn(tableRef.table, "__backup_someAtt_text_multiple")
        }!!
        assertThat(backupMeta.multiple).isTrue()
        assertThat(backupMeta.attType.asString())
            .describedAs("the type never changed; only the multiplicity did, and the registry says so")
            .isEqualTo("text")
        assertThat(records.getAtt(rec, "someAtt[]").asStrList())
            .describedAs("the new column starts empty - taking the first element is the transfer's job")
            .isEmpty()
    }

    @Test
    fun aSafeChangeThatStillNeedsTheValuesRewrittenGoesThroughTheBackupTest() {

        registerAtts(listOf(AttributeDef.create { withId("someAtt") }))
        createRecord("someAtt" to "value-0")

        // TEXT -> MLTEXT: safe, but not free. The column type does not move, yet a bare
        // string is not an MLText object and EQ search stops finding it, so the values
        // have to be rewritten. An in-place ALTER cannot do that: there is no type change to ask
        // it to perform.
        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("someAtt")
                    withType(AttributeType.MLTEXT)
                }
            )
        )
        createRecord("someAtt" to "value-1")

        assertThat(getTableCtx().getAllPhysicalColumns().map { it.name })
            .describedAs("the bare strings are kept where the transfer can still read them")
            .contains("__backup_someAtt_text")
        val p = params()
        assertThat(p.targetType).isEqualTo(AttributeType.MLTEXT)
        assertThat(p.conversionClass)
            .describedAs("safe, and still worth a background transfer: every value survives it")
            .isEqualTo(DbConversionClass.SAFE)
    }

    @Test
    fun aConversionWithNothingToTransferStillMovesTheColumnAsideTest() {

        registerAtts(listOf(AttributeDef.create { withId("someAtt") }))
        createRecord("someAtt" to "value-0")

        // TEXT -> CONTENT: class NONE. Nothing is transferable, but the data must still survive.
        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("someAtt")
                    withType(AttributeType.CONTENT)
                }
            )
        )
        createRecord("someAtt" to null)

        assertThat(getTableCtx().getAllPhysicalColumns().map { it.name })
            .describedAs("class NONE is not a reason to destroy the old values")
            .contains("__backup_someAtt_text")
        assertThat(queuedTasks())
            .describedAs(
                "and a task is queued even so. It carries no value across - its class says so - but " +
                    "it carries the two things this transition cannot do itself: the index step 2 " +
                    "stripped off the new column, which nothing else would ever build, and the " +
                    "departure from the association group"
            )
            .hasSize(1)
        assertThat(params().conversionClass)
            .describedAs("the class is recorded honestly; it is what makes processBatch return at once")
            .isEqualTo(DbConversionClass.NONE)
    }

    @Test
    fun aFrozenConflictingColumnIsStillNeverMovedAsideTest() {

        // a column two types disagree about is frozen, and that decision outranks this one
        val parentRec = prepareConflictingTypes()

        // the child's model asks for an assoc where the column holds text - the decision this
        // migration reaches must be the freeze, not a transition
        val log = captureStderr {
            records.create(RECS_DAO_ID, mapOf("_type" to "child-type"))
        }

        assertThat(log)
            .describedAs(
                "the freeze has to be a decision this migration reached, not an accident of never " +
                    "reaching the decision point: only the logged reason tells those two apart"
            )
            .contains("declare attribute 'someAtt' with different types")

        assertThat(getTableCtx().getAllPhysicalColumns().map { it.name })
            .describedAs("freezing means nothing happens at all - not even a backup")
            .doesNotContain("__backup_someAtt_text")
        assertThat(queuedTasks()).isEmpty()
        assertThat(getTableCtx().getColumns().first { it.name == "someAtt" }.type.name)
            .describedAs("the column stays exactly as the frozen type left it")
            .isEqualTo("TEXT")
        assertThat(records.getAtt(parentRec, "someAtt").asText()).isEqualTo("value-0")
    }

    @Test
    fun theTransitionBuildsNoIndexOnTheColumnItAddsTest() {

        val indexedAtt = { type: AttributeType ->
            AttributeDef.create {
                withId("someAtt")
                withType(type)
                withIndex(AttIndexDef.create().withEnabled(true).build())
            }
        }
        registerAtts(listOf(indexedAtt(AttributeType.TEXT)))
        createRecord("someAtt" to "value-0")
        registerAtts(listOf(indexedAtt(AttributeType.ASSOC)))

        val dataSource = getTableCtx().getSchemaCtx().dataSourceCtx.dataSource
        val commands = dataSource.watchSchemaCommands {
            createRecord("someAtt" to null)
        }

        assertThat(getTableCtx().getAllPhysicalColumns().map { it.name })
            .describedAs("sanity: this mutation really did carry the transition out")
            .contains("__backup_someAtt_text")
        assertThat(commands.filter { it.contains("index", ignoreCase = true) })
            .describedAs(
                "building a btree over the attribute's column means a full heap scan, inside the " +
                    "user's mutation - the exact table-length operation this transition exists to " +
                    "keep out of it. The index is rebuilt after the transfer instead"
            )
            .noneMatch { it.contains("someAtt") }
    }

    /**
     * The return to TEXT is also a *restore* (see [DbColumnRestore]): the text backup this
     * attribute left behind holds what the model is asking for again, so it is renamed back into
     * place instead of an empty column being added, and the assoc column takes its turn in a
     * backup. That is why only one backup column is left at the end rather than two.
     */
    @Test
    fun aSecondTransitionCancelsTheTaskTheFirstOneQueuedTest() {

        registerAtts(listOf(AttributeDef.create { withId("someAtt") }))
        val rec = createRecord("someAtt" to "value-0")

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("someAtt")
                    withType(AttributeType.ASSOC)
                }
            )
        )
        createRecord("someAtt" to null)

        // the model changes its mind again before the first transfer was ever drained:
        // the active task has to be cancelled - its target column is by now a different column of
        // a different type, so letting it run would write values into rows the second transfer
        // deliberately left empty.
        registerAtts(listOf(AttributeDef.create { withId("someAtt") }))
        createRecord("someAtt" to "value-1")

        val tasks = queuedTasks()
        assertThat(tasks).hasSize(2)
        assertThat(tasks[0].status)
            .describedAs("the task queued for the type the model has since abandoned must not run")
            .isEqualTo(DbBatchTaskStatus.CANCELLED)
        assertThat(DbColumnMigrationParams.from(tasks[0].params).targetType).isEqualTo(AttributeType.ASSOC)
        assertThat(tasks[1].status.isFinal())
            .describedAs("while the task of the transition that just happened is left to run")
            .isFalse()
        assertThat(DbColumnMigrationParams.from(tasks[1].params).targetType).isEqualTo(AttributeType.TEXT)

        // what makes the cancel safe: neither generation of values was destroyed by it
        val schemaCtx = getTableCtx().getSchemaCtx()
        assertThat(getTableCtx().getAllPhysicalColumns().map { it.name })
            .describedAs("whatever the cancelled transfer had already written is kept in a backup")
            .contains("__backup_someAtt_assoc")
        assertThat(
            TxnContext.doInTxn {
                schemaCtx.columnMetaService.getByTableAndColumn(tableRef.table, "__backup_someAtt_assoc")
            }!!.attType.asString()
        )
            .isEqualTo("assoc")
        assertThat(
            TxnContext.doInTxn {
                schemaCtx.columnMetaService.getByTableAndColumn(tableRef.table, "someAtt")
            }!!.attType.asString()
        )
            .describedAs("and the original strings are back in the attribute's own column")
            .isEqualTo("text")
        assertThat(records.getAtt(rec, "someAtt").asText())
            .describedAs("not in a backup column somebody has to go and look for: back where the user reads it")
            .isEqualTo("value-0")
    }

    @Test
    fun aPreviewReportsTheTransitionWithoutPerformingItTest() {

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("someAtt")
                    withType(AttributeType.ASSOC)
                }
            )
        )
        createRecord("someAtt" to null)
        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("someAtt")
                    withType(AttributeType.ENTITY_REF)
                }
            )
        )

        val commands = recordsDao.runMigrations(REC_TEST_TYPE_REF, mock = true, diff = true)

        assertThat(commands)
            .describedAs("a preview reports the commands a real run would execute")
            .anyMatch { it.contains("__backup_someAtt_assoc") }
        assertThat(getTableCtx().getAllPhysicalColumns().map { it.name })
            .describedAs("but nothing is actually moved")
            .doesNotContain("__backup_someAtt_assoc")
        val schemaCtx = getTableCtx().getSchemaCtx()
        assertThat(
            TxnContext.doInTxn {
                schemaCtx.columnMetaService.getByTableAndColumn(tableRef.table, "__backup_someAtt_assoc")
            }
        )
            .describedAs("a preview writes nothing to the registry")
            .isNull()
        assertThat(
            TxnContext.doInTxn {
                schemaCtx.columnMetaService.getByTableAndColumn(tableRef.table, "someAtt")
            }!!.attType.asString()
        )
            .describedAs("and the registry still describes what the column really holds")
            .isEqualTo("assoc")
        assertThat(queuedTasks())
            .describedAs("and queues nothing")
            .isEmpty()
    }
}
