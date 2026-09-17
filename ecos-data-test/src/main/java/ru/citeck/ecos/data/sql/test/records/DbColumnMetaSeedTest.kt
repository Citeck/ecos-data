package ru.citeck.ecos.data.sql.test.records

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import ru.citeck.ecos.data.sql.columnmeta.DbColumnMetaDto
import ru.citeck.ecos.data.sql.columnmeta.DbColumnSemanticType
import ru.citeck.ecos.data.sql.dto.DbColumnDef
import ru.citeck.ecos.data.sql.dto.DbColumnType
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType
import ru.citeck.ecos.model.lib.type.dto.TypeInfo
import ru.citeck.ecos.model.lib.type.dto.TypeModelDef
import ru.citeck.ecos.txn.lib.TxnContext

/**
 * The registry has to describe what the columns really are, including on databases that existed
 * long before it did. Everything here goes through the ordinary mutation path, because that is what
 * an upgraded installation actually does first.
 */
class DbColumnMetaSeedTest : DbRecordsTestBase() {

    private fun metaOf(column: String): DbColumnMetaDto? {
        return getTableCtx().getSchemaCtx().columnMetaService
            .getByTable(tableRef.table)
            .find { it.columnName == column }
    }

    /**
     * This one only exercises [ru.citeck.ecos.data.sql.service.DbDataServiceImpl.writeColumnMeta]
     * on the create-table branch of `ensureColumnsExistImpl` - the table does not exist yet, so
     * `seedColumnMeta`'s "column already exists without a registry row" branch is never reached.
     * [aPreRegistryColumnThatMatchesTheModelIsSeededWithItsModelTypeTest] below covers that branch,
     * which is the one the upgrade scenario actually depends on.
     */
    @Test
    fun healthyColumnIsSeededWithItsModelTypeOnTableCreationTest() {

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("textAtt")
                    withType(AttributeType.TEXT)
                },
                AttributeDef.create {
                    withId("assocAtt")
                    withType(AttributeType.ASSOC)
                },
                AttributeDef.create {
                    withId("multiTextAtt")
                    withType(AttributeType.TEXT)
                    withMultiple(true)
                }
            )
        )
        createRecord("textAtt" to "value")

        assertThat(metaOf("textAtt")).isNotNull
        assertThat(metaOf("textAtt")!!.attType)
            .isEqualTo(DbColumnSemanticType.Model(AttributeType.TEXT))
        assertThat(metaOf("textAtt")!!.backup).isFalse()
        assertThat(metaOf("textAtt")!!.multiple).isFalse()
        assertThat(metaOf("textAtt")!!.attId).isEqualTo("textAtt")

        // ASSOC and TEXT share one physical type - the registry is the only thing that tells them apart
        assertThat(metaOf("assocAtt")!!.attType)
            .isEqualTo(DbColumnSemanticType.Model(AttributeType.ASSOC))

        assertThat(metaOf("multiTextAtt")!!.multiple).isTrue()
    }

    @Test
    fun systemColumnsAreNotSeededTest() {

        registerAtts(listOf(AttributeDef.create { withId("textAtt") }))
        createRecord("textAtt" to "value")

        // the registry describes attribute columns; __ext_id, __created and friends are not attributes
        assertThat(
            getTableCtx().getSchemaCtx().columnMetaService.getByTable(tableRef.table)
                .map { it.columnName }
        ).containsExactly("textAtt")
    }

    @Test
    fun aColumnThatContradictsTheModelIsSeededAsAnUnknownSourceTypeTest() {

        // the mismatch is set up with raw DDL: it reproduces a column created by an older platform
        // version, which only a real SQL backend lets us fabricate
        assumeRawSqlSupported()

        registerAtts(listOf(AttributeDef.create { withId("textAtt") }))
        createRecord("textAtt" to "value")

        // a column that exists physically as VARCHAR and has no registry row, exactly like every
        // column of an installation upgraded from before the registry existed
        sqlUpdate("ALTER TABLE ${tableRef.fullName} ADD COLUMN \"brokenAtt\" VARCHAR")

        // this instance's cache still doesn't know "brokenAtt" exists - reset it so the next access
        // reads the real schema, exactly like a process that is touching this table for the first
        // time after an upgrade (which is the scenario this test fabricates)
        mainCtx.dataService.resetColumnsCache()

        // ... while the model says it is an association, i.e. BIGINT
        registerAtts(
            listOf(
                AttributeDef.create { withId("textAtt") },
                AttributeDef.create {
                    withId("brokenAtt")
                    withType(AttributeType.ASSOC)
                }
            )
        )

        // the mutation must succeed: an impossible conversion is no longer allowed to freeze the
        // table for every record in it
        val record = createRecord("textAtt" to "other-value")
        assertThat(records.getAtt(record, "textAtt").asText()).isEqualTo("other-value")

        // seeding is what named the column's contents in the first place, and the shadow-column
        // transition then carried that name onto the backup it created: the seeded description is
        // therefore read off the backup row, which is where those bytes now live.
        val backupMeta = metaOf("__backup_brokenAtt_raw_text")
        assertThat(backupMeta).describedAs("the broken column must be registered, not ignored").isNotNull
        assertThat(backupMeta!!.attType)
            .describedAs("no AttributeType is known for it, only its physical shape")
            .isEqualTo(DbColumnSemanticType.Raw(DbColumnType.TEXT))
        assertThat(backupMeta.attId).isEqualTo("brokenAtt")
        assertThat(backupMeta.backup).isTrue()

        // the values themselves are untouched - an impossible conversion moves them aside, it never
        // rewrites or drops them
        assertThat(getTableCtx().getAllPhysicalColumns().first { it.name == "__backup_brokenAtt_raw_text" }.type)
            .isEqualTo(DbColumnType.TEXT)

        val meta = metaOf("brokenAtt")
        assertThat(meta!!.attType)
            .describedAs("and the fresh column standing in the attribute's place is the assoc the model asked for")
            .isEqualTo(DbColumnSemanticType.Model(AttributeType.ASSOC))
        assertThat(meta.backup).isFalse()
        assertThat(getColumns().first { it.name == "brokenAtt" }.type).isEqualTo(DbColumnType.LONG)
    }

    @Test
    fun aPreRegistryColumnThatMatchesTheModelIsSeededWithItsModelTypeTest() {

        // fabricate exactly what an installation looks like the moment it is upgraded to this
        // version: columns exist physically, with no registry row, because they were created
        // through the *old* runMigrations overload that carries no attribute types
        // (DbExpectedAttTypes.EMPTY) - writeColumnMeta returns early on an empty attTypes map, so
        // nothing is written. No raw SQL involved, so this runs on both backends.
        //
        // TxnContext.doInTxn, not a bare dataService call: the records test harness runs on a
        // managed datasource, where DbDataSourceImpl commits only when !isManaged(). Without an
        // enclosing TxnContext transaction the connection is never enlisted and the DDL is silently
        // rolled back when it returns to the pool (see DbSchemaDaoPrimitivesTest).
        TxnContext.doInTxn {
            mainCtx.dataService.runMigrations(
                listOf(
                    DbColumnDef.create {
                        withName("textAtt")
                        withType(DbColumnType.TEXT)
                    },
                    // ASSOC's physical shape is LONG, same as PERSON/ENTITY_REF/CONTENT/etc. - this
                    // is exactly the case where Model vs Raw is actually distinguishable, unlike a
                    // column whose physical type uniquely identifies its attribute type
                    DbColumnDef.create {
                        withName("assocAtt")
                        withType(DbColumnType.LONG)
                    }
                ),
                mock = false,
                diff = true
            )
        }

        assertThat(metaOf("textAtt"))
            .describedAs("no type model was involved in creating the table, so no registry row exists yet")
            .isNull()
        assertThat(metaOf("assocAtt")).isNull()

        // the model now catches up with what already physically exists - this is the first
        // migration that has a type model behind it, exactly like the first mutation of an upgraded
        // installation's table
        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("textAtt")
                    withType(AttributeType.TEXT)
                },
                AttributeDef.create {
                    withId("assocAtt")
                    withType(AttributeType.ASSOC)
                }
            )
        )
        createRecord("textAtt" to "value")

        assertThat(metaOf("textAtt")!!.attType)
            .describedAs("the physical column already is what the model says, so it is recorded as such")
            .isEqualTo(DbColumnSemanticType.Model(AttributeType.TEXT))
        assertThat(metaOf("assocAtt")!!.attType)
            .describedAs("LONG matches ASSOC too - the registry is the only thing that can tell them apart")
            .isEqualTo(DbColumnSemanticType.Model(AttributeType.ASSOC))
    }

    @Test
    fun aSupportedConversionUpdatesTheRegistryToTheNewTypeTest() {

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("numAtt")
                    withType(AttributeType.NUMBER)
                }
            )
        )
        createRecord("numAtt" to 42)
        assertThat(metaOf("numAtt")!!.attType)
            .isEqualTo(DbColumnSemanticType.Model(AttributeType.NUMBER))

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("numAtt")
                    withType(AttributeType.TEXT)
                }
            )
        )
        createRecord("numAtt" to "text-now")

        assertThat(getColumns().first { it.name == "numAtt" }.type).isEqualTo(DbColumnType.TEXT)
        assertThat(metaOf("numAtt")!!.attType)
            .describedAs("the registry follows the column, and the column was really converted")
            .isEqualTo(DbColumnSemanticType.Model(AttributeType.TEXT))
    }

    /**
     * A semantic-only change - ASSOC -> ENTITY_REF, one physical column type, two very different
     * places for the values to live - is carried by the shadow-column path, so
     * the registry ends up describing two columns instead of one. The principle is unchanged and is
     * what this test pins: **every row says what its column really holds**, never what the model
     * wishes. The old values keep their ASSOC description under the backup name, and the fresh,
     * empty column is described as the ENTITY_REF it genuinely is.
     */
    @Test
    fun aSemanticOnlyChangeLeavesTheOldValuesDescribedUnderTheirBackupNameTest() {

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("refAtt")
                    withType(AttributeType.ASSOC)
                }
            )
        )
        val target = createRecord()
        createRecord("refAtt" to target)
        assertThat(metaOf("refAtt")!!.attType)
            .isEqualTo(DbColumnSemanticType.Model(AttributeType.ASSOC))

        // ASSOC -> ENTITY_REF does not change the physical column at all, so nothing was converted
        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("refAtt")
                    withType(AttributeType.ENTITY_REF)
                }
            )
        )
        createRecord("refAtt" to target)

        assertThat(metaOf("refAtt")!!.attType)
            .describedAs("the column standing in the attribute's place is new, empty and really is an entity ref")
            .isEqualTo(DbColumnSemanticType.Model(AttributeType.ENTITY_REF))

        val backup = metaOf("__backup_refAtt_assoc")
        assertThat(backup)
            .describedAs("the values that were there are not lost, and not left undescribed either")
            .isNotNull
        assertThat(backup!!.attType)
            .describedAs("the registry records what the backup column holds - assocs, as it always did")
            .isEqualTo(DbColumnSemanticType.Model(AttributeType.ASSOC))
        assertThat(backup.backup).isTrue()
        assertThat(backup.attId)
            .describedAs("and which attribute they belonged to, which is how a restore finds them again")
            .isEqualTo("refAtt")
    }

    /**
     * Pins the half of `isSchemaChangeRequired`'s containment check that no other test reaches: a
     * `(name, type)` pair the reconciled set lacks has to trigger a migration, even when nothing
     * physical changed - exactly the `ASSOC -> ENTITY_REF` case above, but observed through the
     * lock rather than through the registry. [aSemanticOnlyChangeLeavesTheOldValuesDescribedUnderTheirBackupNameTest]
     * asserts on what the registry ends up saying, which is a statement about the shadow-column
     * transition the migration performs - not about the migration having been *entered*, which is
     * the decision this test pins and which has to keep holding whatever that transition later
     * becomes. Only the lock call count can show that: the registry trigger depends on the
     * migration actually running for cases like this.
     */
    @Test
    fun aPairTheReconciledSetLacksTriggersAMigrationTest() {

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("refAtt")
                    withType(AttributeType.ASSOC)
                }
            )
        )
        val target = createRecord()
        createRecord("refAtt" to target)

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("refAtt")
                    withType(AttributeType.ENTITY_REF)
                }
            )
        )

        val locksBefore = schemaMigrationLockCallCount()
        createRecord("refAtt" to target)

        assertThat(schemaMigrationLockCallCount())
            .describedAs(
                "(refAtt, Model(ENTITY_REF)) is not in the reconciled set - only " +
                    "(refAtt, Model(ASSOC)) is - so this save must take the migration lock even " +
                    "though ASSOC and ENTITY_REF share one physical column type"
            )
            .isGreaterThan(locksBefore)
    }

    /**
     * The fast path in `isSchemaChangeRequired` skips the migration once the physical columns and
     * the semantic map both look reconciled. A column can be physically present and already shaped
     * exactly like the model wants - so the physical checks alone see no diff - while still having
     * no registry row, because the row is written by the migration itself and nothing else. That
     * state is reached by a plain **read** (not a migration) populating the column cache after a
     * migration elsewhere reset it to null: exactly what happens right after an upgraded
     * installation's very first migration of a table, before its very first ordinary read-then-save.
     * If the fast path trusted the physical columns alone, that first save would never seed the
     * registry, and the background migration this plan builds towards would have nothing to act on.
     *
     * No raw SQL needed - runs on both backends.
     */
    @Test
    fun aModelChangeWithNoPhysicalTraceStillReachesTheRegistryTest() {

        registerAtts(listOf(AttributeDef.create { withId("textAtt") }))
        // cache = {textAtt}, registry = {textAtt: Model(TEXT)}
        createRecord("textAtt" to "value")

        // a column that exists physically, with no type model behind it at all - the old
        // (pre-registry) migration overload, exactly like an installation upgraded to this version
        // finds its pre-existing columns. writeColumnMeta/seedColumnMeta both return early on an
        // empty attTypes map, so no registry row is written for "extraAtt" here.
        //
        // TxnContext.doInTxn: the records test harness runs on a managed datasource, where
        // DbDataSourceImpl commits only when !isManaged() - runMigrations needs an enclosing
        // transaction to actually commit (see DbSchemaDaoPrimitivesTest).
        TxnContext.doInTxn {
            mainCtx.dataService.runMigrations(
                listOf(
                    DbColumnDef.create {
                        withName("textAtt")
                        withType(DbColumnType.TEXT)
                    },
                    DbColumnDef.create {
                        withName("extraAtt")
                        withType(DbColumnType.TEXT)
                    }
                ),
                mock = false,
                diff = true
            )
        }
        // the public runMigrations() overload resets both the column cache and the reconciled set
        // unconditionally on return - this instance now knows neither the columns nor the model

        // load-bearing mechanism, not a specific line: the very next read of any kind - metaOf
        // below included, since it goes through getTableCtx() - repopulates the column cache
        // (now {textAtt, extraAtt}) via a plain read, not a migration, which never touches the
        // reconciled set. The reconciled set therefore stays null even though the cache ends up
        // fresh and complete - the exact combination an upgraded installation's first
        // read-then-save produces. Deleting every read between here and the next migration would
        // leave the cache null too, short-circuiting isSchemaChangeRequired before the semantic
        // gate this test is pinning is ever reached - which is why this test proves nothing on its
        // own without *some* read happening first, whichever one it is.
        assertThat(metaOf("extraAtt"))
            .describedAs("no type model was involved in adding the column, so no registry row exists yet")
            .isNull()

        // the model now catches up with what already physically exists - physically a no-op, since
        // both columns already match
        registerAtts(
            listOf(
                AttributeDef.create { withId("textAtt") },
                AttributeDef.create {
                    withId("extraAtt")
                    withType(AttributeType.TEXT)
                }
            )
        )
        createRecord("textAtt" to "another")

        assertThat(metaOf("extraAtt"))
            .describedAs(
                "the reconciled set was null, so the migration had to run even though the physical " +
                    "columns alone showed no diff at all - otherwise this row would never be seeded"
            )
            .isNotNull
        assertThat(metaOf("extraAtt")!!.attType)
            .isEqualTo(DbColumnSemanticType.Model(AttributeType.TEXT))
    }

    /**
     * `seedColumnMeta` writes registry rows inside the same transaction as the mutation that
     * triggered the migration, and a seed-only migration (nothing to alter or add) produces no
     * schema commands - so [ru.citeck.ecos.data.sql.service.DbDataServiceImpl]'s own
     * commands-not-empty rollback hook never fires for it. If the surrounding mutation then fails
     * later in the same transaction - here, a mandatory attribute left empty, checked by
     * `DbIntegrityCheckListener` as a `BEFORE_COMMIT` action - the whole transaction rolls back,
     * registry rows included, and the reconciled state must roll back with it. Otherwise every
     * later save on this table would wrongly believe the registry already reflects rows that were
     * never actually committed.
     */
    @Test
    fun aSeedThatRollsBackIsReSeededOnTheNextSuccessfulSaveTest() {

        registerAtts(listOf(AttributeDef.create { withId("textAtt") }))
        val first = createRecord("textAtt" to "value")

        // "extraAtt" and "mandatoryAtt" both exist physically already, with no type model behind
        // either of them yet - so the migration below has nothing to alter or add, only rows to
        // seed - and a read (not a migration) puts the column cache back in a state that already
        // matches the model that follows, exactly like
        // aModelChangeWithNoPhysicalTraceStillReachesTheRegistryTest above
        TxnContext.doInTxn {
            mainCtx.dataService.runMigrations(
                listOf(
                    DbColumnDef.create {
                        withName("textAtt")
                        withType(DbColumnType.TEXT)
                    },
                    DbColumnDef.create {
                        withName("extraAtt")
                        withType(DbColumnType.TEXT)
                    },
                    DbColumnDef.create {
                        withName("mandatoryAtt")
                        withType(DbColumnType.TEXT)
                    }
                ),
                mock = false,
                diff = true
            )
        }
        records.getAtt(first, "textAtt")

        registerAtts(
            listOf(
                AttributeDef.create { withId("textAtt") },
                AttributeDef.create {
                    withId("extraAtt")
                    withType(AttributeType.TEXT)
                },
                AttributeDef.create {
                    withId("mandatoryAtt")
                    withType(AttributeType.TEXT)
                    withMandatory(true)
                }
            )
        )

        // the migration runs - physically a no-op, every column already matches - and
        // seedColumnMeta writes rows for "extraAtt" and "mandatoryAtt". The mutation itself then
        // fails the mandatory check, so the whole transaction, seeded rows included, rolls back.
        assertThrows<Exception> {
            createRecord("textAtt" to "another")
        }
        assertThat(metaOf("extraAtt"))
            .describedAs("the seeding transaction rolled back, so the row must not have survived it")
            .isNull()

        // a later, successful save must still produce the row: the failed attempt's optimistic
        // reconciliation must not have stuck around to claim the registry already reflects it
        createRecord("textAtt" to "yet-another", "mandatoryAtt" to "value")
        assertThat(metaOf("extraAtt")!!.attType)
            .describedAs("the registry must be re-seeded on the next successful save")
            .isEqualTo(DbColumnSemanticType.Model(AttributeType.TEXT))
    }

    /**
     * The reconciled set is an [java.util.concurrent.atomic.AtomicReference], not a plain field,
     * because a rollback's `.set(null)` and another thread's merge can interleave: thread A
     * migrates, seeds registry rows in A's still-open transaction, and publishes its
     * reconciliation; thread B then takes the lock and reads the field while computing its own
     * merge; A's mutation then fails and A's hook fires. If B's write lands after A's hook with a
     * merge computed from the pre-rollback value, B resurrects A's rolled-back entries - forever,
     * since only A's own hook was ever going to retract them. `updateAndGet`'s compare-and-swap
     * loop closes that gap (see [ru.citeck.ecos.data.sql.service.DbDataServiceImpl.reconciledAttTypeEntries]),
     * but the exact interleaving needs A's hook to land in the narrow window between B's read and
     * B's write - not reproducible with real threads without a flaky timing test, so this is not
     * that test.
     *
     * What *is* honestly testable, deterministically, in a single thread: that a rollback's `null`
     * is not lost merely because a *different*, successful migration ran afterwards. This drives
     * two sequential mutations - the first seeds and rolls back, exactly like the test above, the
     * second is a different type's successful migration - and asserts that a third, later mutation
     * of the first type still re-seeds "extraAtt" rather than wrongly finding it already
     * reconciled. It does not exercise the compare-and-swap itself (nothing races here), but it
     * does pin that the rollback's effect is not something a later, unrelated, successful migration
     * can undo by publishing over it - the part of the fix a sequential test can actually reach.
     */
    @Test
    fun aRollbackStaysTheLastWordEvenWhenAnotherTypesMigrationRunsInBetweenTest() {

        registerAtts(listOf(AttributeDef.create { withId("textAtt") }))
        val first = createRecord("textAtt" to "value")

        // every column both the failing mutation and the sibling type below will need already
        // exists physically, with no registry row and no reconciled state yet - same seed-only
        // setup as aSeedThatRollsBackIsReSeededOnTheNextSuccessfulSaveTest above
        TxnContext.doInTxn {
            mainCtx.dataService.runMigrations(
                listOf(
                    DbColumnDef.create {
                        withName("textAtt")
                        withType(DbColumnType.TEXT)
                    },
                    DbColumnDef.create {
                        withName("extraAtt")
                        withType(DbColumnType.TEXT)
                    },
                    DbColumnDef.create {
                        withName("mandatoryAtt")
                        withType(DbColumnType.TEXT)
                    },
                    DbColumnDef.create {
                        withName("siblingAtt")
                        withType(DbColumnType.TEXT)
                    }
                ),
                mock = false,
                diff = true
            )
        }
        records.getAtt(first, "textAtt")

        registerAtts(
            listOf(
                AttributeDef.create { withId("textAtt") },
                AttributeDef.create {
                    withId("extraAtt")
                    withType(AttributeType.TEXT)
                },
                AttributeDef.create {
                    withId("mandatoryAtt")
                    withType(AttributeType.TEXT)
                    withMandatory(true)
                }
            )
        )

        // seeds {extraAtt, mandatoryAtt}, then fails the mandatory check - the whole transaction,
        // seeded rows included, rolls back, and the hook nulls the reconciled set
        assertThrows<Exception> {
            createRecord("textAtt" to "another")
        }
        assertThat(metaOf("extraAtt")).isNull()

        // a *different* type sharing the table migrates successfully in between - this must not
        // make the rolled-back "extraAtt" reconciliation reappear
        registerType(
            TypeInfo.create {
                withId("sibling-type")
                withParentRef(REC_TEST_TYPE_REF)
                withModel(
                    TypeModelDef.create()
                        .withAttributes(
                            listOf(
                                AttributeDef.create()
                                    .withId("siblingAtt")
                                    .build()
                            )
                        )
                        .build()
                )
            }
        )
        records.create(RECS_DAO_ID, mapOf("_type" to "sibling-type", "siblingAtt" to "s1"))

        // the retry: must still re-seed "extraAtt" rather than wrongly find it already reconciled
        createRecord("textAtt" to "yet-another", "mandatoryAtt" to "value")
        assertThat(metaOf("extraAtt")!!.attType)
            .describedAs(
                "the rollback must stay the last word for extraAtt, regardless of the sibling " +
                    "type's successful migration in between"
            )
            .isEqualTo(DbColumnSemanticType.Model(AttributeType.TEXT))
    }

    private fun registerChildInSameTable(childAttId: String) {
        registerType(
            TypeInfo.create {
                withId("child-type")
                withParentRef(REC_TEST_TYPE_REF)
                withModel(
                    TypeModelDef.create()
                        .withAttributes(
                            listOf(
                                AttributeDef.create()
                                    .withId(childAttId)
                                    .build()
                            )
                        )
                        .build()
                )
            }
        )
    }

    /**
     * The reconciled set has to be a genuine accumulation across types sharing a table, not just
     * the last migration's map with a different name. Two sibling child types, each adding an
     * attribute the other does not declare, are the case that actually exercises it: after both
     * have saved once, a second save of the first child's type is a re-save of a map that is a
     * *subset* of what the two migrations together have already reconciled - base attribute plus
     * its own - even though it is not a subset of either migration's map taken alone. If the set
     * only ever remembered the most recent migration, this save would wrongly need the lock again.
     *
     * Every column both child types need already exists physically before either model catches up
     * to it - the same pre-registry setup as
     * [aModelChangeWithNoPhysicalTraceStillReachesTheRegistryTest] above - so that neither child's
     * first migration issues any DDL. That is deliberate, not incidental: an in-lock migration that
     * *does* change the schema resets the column cache via [runMigrationsInLock]'s own
     * commands-not-empty path, and that reset also drops the reconciled set (see
     * [reconciledAttTypeEntries] - safe, but it means this specific accumulation would not survive
     * a real DDL change in between, and this test is pinning the accumulation, not that already-
     * accepted cost).
     *
     * There is no direct way to assert "no migration ran" from outside `DbDataServiceImpl` - the
     * decision is private and produces no observable side effect when it says no. The honest proxy
     * is the schema migration lock: [DataMockFactory.schemaMigrationLockCallCount] counts requests
     * for this table's exact key, so "the count did not move" is precisely "no migration was even
     * attempted", not a weaker stand-in for it.
     */
    @Test
    fun aSubsetOfAnAlreadyReconciledMapNeedsNoNewMigrationTest() {

        registerAtts(listOf(AttributeDef.create { withId("baseAtt") }))
        registerChildInSameTable("child1Att")
        registerType(
            TypeInfo.create {
                withId("child2-type")
                withParentRef(REC_TEST_TYPE_REF)
                withModel(
                    TypeModelDef.create()
                        .withAttributes(
                            listOf(
                                AttributeDef.create()
                                    .withId("child2Att")
                                    .build()
                            )
                        )
                        .build()
                )
            }
        )

        // fabricate the table with every column both child types will need, through the old
        // (pre-registry) migration overload - physically present, no registry row, no reconciled
        // state - exactly like the upgrade scenario aModelChangeWithNoPhysicalTraceStillReachesTheRegistryTest
        // fabricates above
        TxnContext.doInTxn {
            mainCtx.dataService.runMigrations(
                listOf(
                    DbColumnDef.create {
                        withName("baseAtt")
                        withType(DbColumnType.TEXT)
                    },
                    DbColumnDef.create {
                        withName("child1Att")
                        withType(DbColumnType.TEXT)
                    },
                    DbColumnDef.create {
                        withName("child2Att")
                        withType(DbColumnType.TEXT)
                    }
                ),
                mock = false,
                diff = true
            )
        }
        // a plain read, not a migration: repopulates the column cache without touching the
        // reconciled set
        getTableCtx()

        // first child type's catch-up: {baseAtt, child1Att} is not contained in the (empty)
        // reconciled set, so this takes the lock - physically a no-op, since every column already
        // matches, so no DDL resets the reconciled set along the way
        val child1Rec = records.create(RECS_DAO_ID, mapOf("_type" to "child-type", "child1Att" to "c1"))

        // a second, sibling child type declaring a *different* attribute - {baseAtt, child2Att} is
        // not contained in {baseAtt, child1Att} either, so this also takes the lock - also
        // physically a no-op
        records.create(RECS_DAO_ID, mapOf("_type" to "child2-type", "child2Att" to "c2"))

        val locksBefore = schemaMigrationLockCallCount()

        // a second record of the *first* child type: {baseAtt, child1Att} is a subset of what the
        // two migrations above together have already reconciled - {baseAtt, child1Att, child2Att} -
        // even though it is not a subset of the second migration's map taken alone. This must not
        // take the lock again.
        records.create(RECS_DAO_ID, mapOf("_type" to "child-type", "child1Att" to "c1-again"))

        assertThat(schemaMigrationLockCallCount())
            .describedAs(
                "the accumulated set must recognise this save's map as already fully reconciled - " +
                    "a set that forgot child1's reconciliation when child2 migrated would wrongly " +
                    "take the lock again here"
            )
            .isEqualTo(locksBefore)

        assertThat(records.getAtt(child1Rec, "child1Att").asText()).isEqualTo("c1")
    }
}
