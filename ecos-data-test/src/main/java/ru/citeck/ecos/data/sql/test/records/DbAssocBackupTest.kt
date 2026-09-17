package ru.citeck.ecos.data.sql.test.records

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import ru.citeck.ecos.commons.data.ObjectData
import ru.citeck.ecos.data.sql.batch.DbBatchTaskEngine
import ru.citeck.ecos.data.sql.batch.DbBatchTaskStatus
import ru.citeck.ecos.data.sql.columnmeta.DbColumnSemanticType
import ru.citeck.ecos.data.sql.repo.find.DbFindPage
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType
import ru.citeck.ecos.txn.lib.TxnContext
import ru.citeck.ecos.webapp.api.entity.EntityRef

/**
 * An attribute leaving the association group must take its links with it into a
 * backup of their own - not leave them in `ed_associations` to go stale, and not mix them with the
 * trash can, which holds links the user deleted deliberately.
 */
class DbAssocBackupTest : DbRecordsTestBase() {

    private fun drain() {
        val schemaCtx = getTableCtx().getSchemaCtx()
        DbBatchTaskEngine(schemaCtx.dataSourceCtx).drainSchemaOnce(schemaCtx)
    }

    private fun backupMetaId(): Long {
        val schemaCtx = getTableCtx().getSchemaCtx()
        return TxnContext.doInTxn {
            schemaCtx.columnMetaService.findBackups(tableRef.table, "links").first().id
        }
    }

    /**
     * Resolves a record's reference to its `ed_record_ref` id, which is what an association keys on.
     */
    private fun getIdByRef(ref: EntityRef): Long {
        return dbRecordRefService.getIdByEntityRef(ref)
    }

    @Test
    fun everyLinkSurvivesTheDepartureFromTheAssociationGroupTest() {

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("links")
                    withType(AttributeType.ASSOC)
                    withMultiple(true)
                }
            )
        )
        val targets = (0 until 15).map { createRecord("someKey" to "target-$it") }
        val source = createRecord("links" to targets.map { it.toString() })

        // ASSOC -> TEXT is lossy, and the source of truth is ed_associations, not the
        // column, which only ever cached the first ten
        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("links")
                    withType(AttributeType.TEXT)
                    withMultiple(true)
                }
            )
        )
        createRecord("someKey" to "unrelated")
        drain()

        val schemaCtx = getTableCtx().getSchemaCtx()
        val sourceId = TxnContext.doInTxn { getIdByRef(source) }
        val backed = TxnContext.doInTxn {
            schemaCtx.assocBackupService.findByColumnMeta(backupMetaId(), sourceId)
        }
        assertThat(backed)
            .describedAs("all fifteen, not the ten the column cached - this is the whole point of 8.7")
            .hasSize(15)
        assertThat(backed.map { it.targetId }).doesNotHaveDuplicates()
        assertThat(backed).allMatch { it.created.epochSecond > 0 }
        assertThat(backed.map { it.index })
            .describedAs("order is part of what a multi-valued association means")
            .containsExactlyElementsOf(0 until 15)
    }

    @Test
    fun theLinksAreRemovedFromTheLiveTableAfterwardsTest() {

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("links")
                    withType(AttributeType.ASSOC)
                    withMultiple(true)
                }
            )
        )
        val target = createRecord("someKey" to "target")
        val source = createRecord("links" to listOf(target.toString()))

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("links")
                    withType(AttributeType.TEXT)
                    withMultiple(true)
                }
            )
        )
        createRecord("someKey" to "unrelated")
        drain()

        val schemaCtx = getTableCtx().getSchemaCtx()
        val sourceId = TxnContext.doInTxn { getIdByRef(source) }
        val live = TxnContext.doInTxn {
            schemaCtx.assocsService.getTargetAssocs(sourceId, "links", DbFindPage.ALL)
        }
        assertThat(live.entities)
            .describedAs("inert rows left behind come back to life at the wrong moment")
            .isEmpty()
    }

    @Test
    fun aLinkTheUserDeletedBeforeTheTypeChangeIsNotBackedUpTest() {

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("links")
                    withType(AttributeType.ASSOC)
                    withMultiple(true)
                }
            )
        )
        val keep = createRecord("someKey" to "keep")
        val drop = createRecord("someKey" to "drop")
        val source = createRecord("links" to listOf(keep.toString(), drop.toString()))
        updateRecord(source, "links" to listOf(keep.toString()))

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("links")
                    withType(AttributeType.TEXT)
                    withMultiple(true)
                }
            )
        )
        createRecord("someKey" to "unrelated")
        drain()

        val schemaCtx = getTableCtx().getSchemaCtx()
        val sourceId = TxnContext.doInTxn { getIdByRef(source) }
        val backed = TxnContext.doInTxn {
            schemaCtx.assocBackupService.findByColumnMeta(backupMetaId(), sourceId)
        }
        assertThat(backed)
            .describedAs(
                "ed_associations_deleted and ed_associations_backup must never mix, or " +
                    "restoring would resurrect links the user removed on purpose"
            )
            .hasSize(1)
    }

    /**
     * A departure whose values are not transferable at all is still a departure.
     *
     * `ASSOC -> NUMBER` is class NONE - there is nothing to carry from a reference id to a double
     * - so the transition used to queue no background task, and with no task there was no `prepare`
     * and no departure: the links stayed live in `ed_associations` under an attribute that is no
     * longer assoc-like, which is the resurrection a departure exists to prevent. The transition now
     * queues a task for every type change.
     */
    @Test
    fun aDepartureWithNothingToTransferStillTakesTheLinksWithItTest() {

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("links")
                    withType(AttributeType.ASSOC)
                    withMultiple(true)
                }
            )
        )
        val target = createRecord("someKey" to "target")
        val source = createRecord("links" to listOf(target.toString()))

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("links")
                    withType(AttributeType.NUMBER)
                    withMultiple(true)
                }
            )
        )
        createRecord("someKey" to "unrelated")
        drain()

        val schemaCtx = getTableCtx().getSchemaCtx()
        val sourceId = TxnContext.doInTxn { getIdByRef(source) }
        assertThat(TxnContext.doInTxn { schemaCtx.assocBackupService.findByColumnMeta(backupMetaId(), sourceId) })
            .describedAs("class NONE says nothing about the links, only about the column's values")
            .hasSize(1)
        assertThat(
            TxnContext.doInTxn {
                schemaCtx.assocsService.getTargetAssocs(sourceId, "links", DbFindPage.ALL)
            }.entities
        )
            .describedAs("and they must not be left behind to go stale")
            .isEmpty()
    }

    /**
     * The departure must survive the cancellation of the task that was going to perform it.
     *
     * A second type change cancels the first one's still-undrained task, because the column that
     * task was filling is no longer the column the model describes. The departure rides on that
     * task's `prepare`, so cancelling it used to mean the links of an attribute that stopped being
     * assoc-like simply stayed live - and the ASSOC backup registry row then owned nothing, so a
     * later restore would have put nothing back.
     */
    @Test
    fun aSecondTypeChangeBeforeTheFirstDrainStillTakesTheLinksTest() {

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("links")
                    withType(AttributeType.ASSOC)
                    withMultiple(true)
                }
            )
        )
        val target = createRecord("someKey" to "target")
        val source = createRecord("links" to listOf(target.toString()))

        // the attribute leaves the association group, and nothing drains before it changes again
        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("links")
                    withType(AttributeType.TEXT)
                    withMultiple(true)
                }
            )
        )
        createRecord("someKey" to "unrelated")
        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("links")
                    withType(AttributeType.NUMBER)
                    withMultiple(true)
                }
            )
        )
        createRecord("someKey" to "unrelated-2")
        drain()

        val schemaCtx = getTableCtx().getSchemaCtx()
        val sourceId = TxnContext.doInTxn { getIdByRef(source) }
        val assocBackup = TxnContext.doInTxn {
            schemaCtx.columnMetaService.findBackups(tableRef.table, "links")
        }.single { it.attType == DbColumnSemanticType.Model(AttributeType.ASSOC) }

        assertThat(TxnContext.doInTxn { schemaCtx.assocBackupService.findByColumnMeta(assocBackup.id, sourceId) })
            .describedAs("the links belong to the ASSOC backup, whatever happened to the task")
            .hasSize(1)
        assertThat(
            TxnContext.doInTxn {
                schemaCtx.assocsService.getTargetAssocs(sourceId, "links", DbFindPage.ALL)
            }.entities
        )
            .describedAs("a cancelled task must not leave live links behind it")
            .isEmpty()
    }

    /**
     * The other applications have to hear about the removal.
     *
     * An association may point at an entity owned by another application, which keeps its own
     * back-reference to it. The departure removes the links through `removeAssocs`, which knows
     * nothing about those peers, so the notification is a separate call - exactly as it is after an
     * ordinary mutation in `DbRecordsMutateDao`. What this pins is the part the caller owns: which
     * references are reported as removed, under which attribute, with which `child` flag, and for
     * which source record.
     *
     * A **child** association on purpose: `child = false` is what a caller that hardcoded the flag
     * would also produce.
     */
    @Test
    fun theApplicationsHoldingABackReferenceAreToldTheLinksAreGoneTest() {

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("links")
                    withType(AttributeType.ASSOC)
                    withMultiple(true)
                    withConfig(ObjectData.create().set("child", true))
                }
            )
        )
        val targets = (0 until 3).map { createRecord("someKey" to "target-$it") }
        val source = createRecord("links" to targets.map { it.toString() })
        // the mutations above are notifications of their own; only the departure is under test
        remoteActionsClient.clear()

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("links")
                    withType(AttributeType.TEXT)
                    withMultiple(true)
                }
            )
        )
        createRecord("someKey" to "unrelated")
        drain()

        val update = remoteActionsClient.assocUpdatesFor("links").single()
        assertThat(update.sourceRef.getLocalId())
            .describedAs("the notification is about the record whose links went away")
            .isEqualTo(source.getLocalId())

        val diff = update.assocsDiff.single()
        assertThat(diff.removed.map { it.getLocalId() })
            .describedAs("every link that was really removed, resolved back to the reference it was")
            .containsExactlyInAnyOrderElementsOf(targets.map { it.getLocalId() })
        assertThat(diff.added).describedAs("a departure adds nothing").isEmpty()
        assertThat(diff.child)
            .describedAs("carried from the association rows, not assumed")
            .isTrue()
    }

    /**
     * And an application that is down must not be able to fail the migration.
     *
     * `DbRecordsRemoteActionsClientImpl` throws `App is not available` for a peer it cannot reach.
     * That call now happens inside the batch engine's `prepare`, where an escaping exception is
     * booked as a task failure with backoff and, after `maxAttempts`, marks the **whole** column
     * migration FAILED - the value transfer included - because some other application was
     * restarting. The links are gone locally either way and the next tick has nothing left to
     * re-notify from, so the failure is logged and the migration carries on.
     */
    @Test
    fun anApplicationThatCannotBeReachedDoesNotFailTheMigrationTest() {

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("links")
                    withType(AttributeType.ASSOC)
                    withMultiple(true)
                }
            )
        )
        val target = createRecord("someKey" to "target")
        val source = createRecord("links" to listOf(target.toString()))

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("links")
                    withType(AttributeType.TEXT)
                    withMultiple(true)
                }
            )
        )
        remoteActionsClient.clear()
        remoteActionsClient.failAssocUpdatesFor = "links"
        createRecord("someKey" to "unrelated")
        drain()
        remoteActionsClient.failAssocUpdatesFor = null

        val schemaCtx = getTableCtx().getSchemaCtx()
        assertThat(remoteActionsClient.assocUpdatesFor("links"))
            .describedAs("the notification really was attempted - otherwise this proves nothing")
            .hasSize(1)
        assertThat(TxnContext.doInTxn { schemaCtx.batchTaskService.findByTable(tableRef.table) }.map { it.status })
            .describedAs("a peer being down is not this migration's failure")
            .containsExactly(DbBatchTaskStatus.DONE)

        val sourceId = TxnContext.doInTxn { getIdByRef(source) }
        assertThat(TxnContext.doInTxn { schemaCtx.assocBackupService.findByColumnMeta(backupMetaId(), sourceId) })
            .describedAs("and the links are safe in the backup, which is what the notification was about")
            .hasSize(1)
    }

    /**
     * A restore takes a record's links back out of the backup, so `consume` has to remove
     * **that record's** rows and nothing else. Two records share the registry row here precisely
     * because a key that is one column too short would empty the whole snapshot and leave the
     * second record with no copy of its links at all - and the backup is the only copy there is.
     */
    @Test
    fun consumingOneRecordsBackupLeavesEveryOtherRecordsAloneTest() {

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("links")
                    withType(AttributeType.ASSOC)
                    withMultiple(true)
                }
            )
        )
        val target = createRecord("someKey" to "target")
        val consumed = createRecord("links" to listOf(target.toString()))
        val untouched = createRecord("links" to listOf(target.toString()))

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("links")
                    withType(AttributeType.TEXT)
                    withMultiple(true)
                }
            )
        )
        createRecord("someKey" to "unrelated")
        drain()

        val schemaCtx = getTableCtx().getSchemaCtx()
        val metaId = backupMetaId()
        val consumedId = TxnContext.doInTxn { getIdByRef(consumed) }
        val untouchedId = TxnContext.doInTxn { getIdByRef(untouched) }

        TxnContext.doInTxn { schemaCtx.assocBackupService.consume(metaId, consumedId) }

        assertThat(TxnContext.doInTxn { schemaCtx.assocBackupService.findByColumnMeta(metaId, consumedId) })
            .describedAs("the record whose links were put back keeps no copy of them")
            .isEmpty()
        assertThat(TxnContext.doInTxn { schemaCtx.assocBackupService.findByColumnMeta(metaId, untouchedId) })
            .describedAs("the other record's only copy of its links must still be there")
            .hasSize(1)
    }

    /**
     * The two departures have to be departures of **two different columns**, and the arity is what
     * makes them so: a multi-valued assoc attribute that comes back multi-valued reclaims the very
     * column it left (see `DbColumnRestore`), so a later departure of it is a departure of the
     * same column, described by the same registry row - one snapshot, not two, and rightly, because
     * there is only one column's worth of history to keep. Returning as *single*-valued is a
     * different physical column, so it gets a registry row of its own, and that is the case the key
     * is about.
     */
    @Test
    fun eachBackupOfTheSameAttributeKeepsItsOwnSnapshotTest() {

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("links")
                    withType(AttributeType.ASSOC)
                    withMultiple(true)
                }
            )
        )
        val first = createRecord("someKey" to "first")
        val source = createRecord("links" to listOf(first.toString()))
        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("links")
                    withType(AttributeType.TEXT)
                    withMultiple(true)
                }
            )
        )
        createRecord("someKey" to "unrelated")
        drain()

        // back to ASSOC - single-valued this time, so nothing is restored and a second assoc column
        // is built - then away again: a second backup, with a different set of links
        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("links")
                    withType(AttributeType.ASSOC)
                }
            )
        )
        val second = createRecord("someKey" to "second")
        updateRecord(source, "links" to second.toString())
        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("links")
                    withType(AttributeType.TEXT)
                }
            )
        )
        createRecord("someKey" to "unrelated-2")
        drain()

        val schemaCtx = getTableCtx().getSchemaCtx()
        val sourceId = TxnContext.doInTxn { getIdByRef(source) }
        val firstId = TxnContext.doInTxn { getIdByRef(first) }
        val secondId = TxnContext.doInTxn { getIdByRef(second) }
        // The text columns these transitions moved aside are not departures from the association
        // group and own no links. The two that are, are the ones this test is about.
        val metas = TxnContext.doInTxn { schemaCtx.columnMetaService.findBackups(tableRef.table, "links") }
            .filter { it.attType == DbColumnSemanticType.Model(AttributeType.ASSOC) }
        assertThat(metas).describedAs("two departures, two backups").hasSize(2)

        val newest = TxnContext.doInTxn { schemaCtx.assocBackupService.findByColumnMeta(metas[0].id, sourceId) }
        val oldest = TxnContext.doInTxn { schemaCtx.assocBackupService.findByColumnMeta(metas[1].id, sourceId) }
        assertThat(newest.map { it.targetId })
            .describedAs("the second departure took the link the first one never saw")
            .containsExactly(secondId)
        assertThat(oldest.map { it.targetId })
            .describedAs("the key is the registry row precisely so these do not merge")
            .containsExactly(firstId)
    }

    /**
     * The case the arity in the test above deliberately avoids, and the one a restore creates: the
     * **same** column departing twice, keyed both times by the same recycled registry row (see
     * `DbColumnRestore` for why the row is recycled).
     *
     * What keeps the two departures from merging is not a second row - there is none - but the
     * restore in between spending the first snapshot before the second one is written. If it did
     * not, the row would hold the union of two eras and a later restore would hand the user links
     * they had replaced.
     */
    @Test
    fun aRestoredSnapshotIsSpentBeforeTheSameColumnDepartsAgainTest() {

        fun asAssoc() = registerAtts(
            listOf(
                AttributeDef.create {
                    withId("links")
                    withType(AttributeType.ASSOC)
                    withMultiple(true)
                }
            )
        )
        fun asText() = registerAtts(
            listOf(
                AttributeDef.create {
                    withId("links")
                    withType(AttributeType.TEXT)
                    withMultiple(true)
                }
            )
        )

        asAssoc()
        val first = createRecord("someKey" to "first")
        val source = createRecord("links" to listOf(first.toString()))

        asText()
        createRecord("someKey" to "unrelated")
        drain()

        // back to ASSOC: the very column that left comes back, and the drain gives the links back
        // and spends the snapshot they were parked in
        asAssoc()
        createRecord("someKey" to "unrelated-2")
        drain()

        val second = createRecord("someKey" to "second")
        updateRecord(source, "links" to listOf(second.toString()))

        // and away again - the second departure writes under the same registry row as the first
        asText()
        createRecord("someKey" to "unrelated-3")
        drain()

        val schemaCtx = getTableCtx().getSchemaCtx()
        val sourceId = TxnContext.doInTxn { getIdByRef(source) }
        val firstId = TxnContext.doInTxn { getIdByRef(first) }
        val secondId = TxnContext.doInTxn { getIdByRef(second) }
        val metas = TxnContext.doInTxn { schemaCtx.columnMetaService.findBackups(tableRef.table, "links") }
            .filter { it.attType == DbColumnSemanticType.Model(AttributeType.ASSOC) }
        assertThat(metas)
            .describedAs("one column, one registry row - the return reclaimed the column it left")
            .hasSize(1)

        val parked = TxnContext.doInTxn { schemaCtx.assocBackupService.findByColumnMeta(metas[0].id, sourceId) }
        assertThat(parked.map { it.targetId })
            .describedAs("the second departure's links, alone: the first era's were given back and spent")
            .containsExactly(secondId)
        assertThat(parked.map { it.targetId })
            .describedAs("a restore of this row must not resurrect the link the user replaced")
            .doesNotContain(firstId)
    }
}
