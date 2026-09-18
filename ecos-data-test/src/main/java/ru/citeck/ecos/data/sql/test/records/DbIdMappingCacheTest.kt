package ru.citeck.ecos.data.sql.test.records

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import ru.citeck.ecos.data.sql.dto.DbTableRef
import ru.citeck.ecos.data.sql.records.DbRecordsControlAtts
import ru.citeck.ecos.data.sql.records.refs.DbRecordRefEntity
import ru.citeck.ecos.data.sql.repo.entity.DbEntity
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.txn.lib.TxnContext
import ru.citeck.ecos.webapp.api.entity.EntityRef
import java.time.Duration
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference

class DbIdMappingCacheTest : DbRecordsTestBase() {

    companion object {
        /**
         * How long the competing writer of
         * [aCollisionWithAnOpenTransactionWaitsForItRatherThanPollingTest] keeps its transaction
         * open. Longer than a lookup takes and shorter than anybody's patience.
         */
        private val COMPETITOR_HOLD: Duration = Duration.ofSeconds(2)
    }

    /**
     * Two writers asking for the same reference at the same time get **the same id**, and the loser
     * gets it from the winner rather than from a second row.
     *
     * The contract `DbIdMappingService` exists for and the one no test stated: ids are a pool shared
     * by transactions that know nothing about each other, so "not there, insert it" has to survive
     * somebody else inserting it in between.
     *
     * Staged with the storage barrier, because the race has to be *inside* the insert: the loser
     * has already looked and found nothing, which is the only moment the question arises. The
     * competitor still needs a thread of its own for its own connection, so this is skipped on a
     * backend that serializes transactions on one lock.
     */
    @Test
    fun aReferenceCreatedByAnotherWriterInsideTheRaceIsNotCreatedTwiceTest() {

        assumeConcurrentTransactionsSupported()

        val ref = EntityRef.valueOf("test-app/test@raced-ref")
        val winnerId = AtomicLong(-1)

        beforeStorageWrite = armOnce(ref) {
            winnerId.set(TxnContext.doInNewTxn { dbRecordRefService.getOrCreateIdByEntityRef(ref) })
        }
        val id = try {
            TxnContext.doInTxn { dbRecordRefService.getOrCreateIdByEntityRef(ref) }
        } finally {
            beforeStorageWrite = null
        }

        assertThat(winnerId.get())
            .describedAs("the fixture: somebody really did register the reference in between")
            .isGreaterThan(0)
        assertThat(id)
            .describedAs("so the loser takes the id that exists instead of failing or making a second one")
            .isEqualTo(winnerId.get())
    }

    /**
     * A reference registered inside a transaction that then **rolls back** keeps its id.
     *
     * The reason `saveAtomicallyOrGetExistingByExtId` runs in a transaction of its own, and the
     * property any rewrite of it has to keep: an id handed out is a promise to every other
     * transaction, including ones that have already written it into a row of their own. Taking it
     * back with the caller's rollback would leave those pointing at nothing.
     */
    @Test
    fun aReferenceRegisteredByATransactionThatRolledBackKeepsItsIdTest() {

        val ref = EntityRef.valueOf("test-app/test@ref-of-a-doomed-txn")

        val idInsideTxn = AtomicLong(-1)
        val rollback = RuntimeException("rolled back on purpose")
        assertThat(
            runCatching {
                TxnContext.doInNewTxn {
                    idInsideTxn.set(dbRecordRefService.getOrCreateIdByEntityRef(ref))
                    throw rollback
                }
            }.exceptionOrNull()
        ).isNotNull()

        assertThat(idInsideTxn.get()).isGreaterThan(0)
        assertThat(TxnContext.doInTxn { dbRecordRefService.getIdByEntityRef(ref) })
            .describedAs("the id outlives the transaction that asked for it")
            .isEqualTo(idInsideTxn.get())
    }

    /**
     * The loser of the race gets the id the reference **resolves to**, not the id of the row it
     * collided with.
     *
     * `ed_record_ref` holds redirects: moving a record's id leaves a tombstone under the old text
     * whose `__moved_to` names the live row. Every ordinary lookup resolves those - `getIdByExtId`
     * does it through `resolveId` - and the one path that did not was the collision path, which
     * answered with the physical id of whatever row it collided with. A tombstone appearing between
     * the look and the insert is exactly that: the caller writes the id of a redirect into its
     * associations, and every reader of them ends up at the reference's old text.
     */
    @Test
    fun theLoserOfTheRaceGetsTheIdTheReferenceResolvesToTest() {

        assumeConcurrentTransactionsSupported()

        val oldRef = EntityRef.valueOf("test-app/test@ref-moved-inside-the-race")
        val newRef = EntityRef.valueOf("test-app/test@ref-moved-inside-the-race-target")
        val liveId = AtomicLong(-1)

        beforeStorageWrite = armOnce(oldRef) {
            TxnContext.doInNewTxn {
                liveId.set(dbRecordRefService.getOrCreateIdByEntityRef(oldRef))
                dbRecordRefService.migrateRefIfExists(oldRef, newRef, -1L)
            }
        }
        val id = try {
            TxnContext.doInTxn { dbRecordRefService.getOrCreateIdByEntityRef(oldRef) }
        } finally {
            beforeStorageWrite = null
        }

        assertThat(TxnContext.doInTxn { dbRecordRefService.getIdByEntityRef(newRef) })
            .describedAs("the fixture: the reference really was moved while the insert was in flight")
            .isEqualTo(liveId.get())
        assertThat(id)
            .describedAs(
                "so the collision answers with the live row the tombstone points at, not with the " +
                    "tombstone"
            )
            .isEqualTo(liveId.get())
    }

    /**
     * A collision with a writer whose transaction is **still open** is answered by waiting for it,
     * not by looking and giving up.
     *
     * The property the single statement rests on, and the reason the loop of five lookups with a
     * sleep between them could be deleted rather than tightened: a row inserted by a transaction
     * that has not committed is invisible to every reader and cannot be locked either, so "insert,
     * then look" has nothing to wait on and can only poll. `ON CONFLICT DO UPDATE` waits on the
     * conflicting tuple itself and comes back with the id the winner left.
     *
     * The competitor writes the row **through the data service inside a transaction it holds**
     * rather than through this service, which would commit the row at once in a transaction of its
     * own and leave nothing to wait for. It holds it for [COMPETITOR_HOLD], and the call under test
     * is required to take at least half of that: it has to have been blocked, not merely slow.
     */
    @Test
    fun aCollisionWithAnOpenTransactionWaitsForItRatherThanPollingTest() {

        assumeConcurrentTransactionsSupported()

        val ref = EntityRef.valueOf("test-app/test@ref-of-a-slow-winner")
        val extId = dbRecordRefService.getStoredExtId(ref)
        val winnerId = AtomicLong(-1)
        val winnerHoldsTheRow = CountDownLatch(1)
        val failure = AtomicReference<Throwable>()
        val armed = AtomicBoolean(true)

        beforeStorageWrite = { table, entities ->
            if (table.table == DbRecordRefEntity.TABLE &&
                entities.any { it[DbEntity.EXT_ID] == extId } &&
                armed.compareAndSet(true, false)
            ) {
                val competitor = Thread({
                    try {
                        TxnContext.doInNewTxn {
                            val entity = DbRecordRefEntity()
                            entity.extId = extId
                            winnerId.set(dbRecordRefService.save(entity).id)
                            winnerHoldsTheRow.countDown()
                            Thread.sleep(COMPETITOR_HOLD.toMillis())
                        }
                    } catch (e: Throwable) {
                        failure.set(e)
                        winnerHoldsTheRow.countDown()
                    }
                }, "competing-writer-holding-its-txn")
                competitor.start()
                assertThat(winnerHoldsTheRow.await(1, TimeUnit.MINUTES))
                    .describedAs("the competitor never reached its insert, so nothing was raced")
                    .isTrue()
                failure.get()?.let { throw it }
            }
        }
        val startedAt = System.currentTimeMillis()
        val id = try {
            TxnContext.doInTxn { dbRecordRefService.getOrCreateIdByEntityRef(ref) }
        } finally {
            beforeStorageWrite = null
        }
        val elapsed = System.currentTimeMillis() - startedAt

        assertThat(id)
            .describedAs("the waiter gets the id the winner left behind")
            .isEqualTo(winnerId.get())
        assertThat(elapsed)
            .describedAs("and it got there by waiting for that transaction, not by looking and sleeping")
            .isGreaterThanOrEqualTo(COMPETITOR_HOLD.toMillis() / 2)
    }

    private fun armOnce(ref: EntityRef, action: () -> Unit): (DbTableRef, List<Map<String, Any?>>) -> Unit {
        val armed = AtomicBoolean(true)
        val extId = dbRecordRefService.getStoredExtId(ref)
        return { table, entities ->
            if (table.table == DbRecordRefEntity.TABLE &&
                entities.any { it[DbEntity.EXT_ID] == extId } &&
                armed.compareAndSet(true, false)
            ) {
                val failure = AtomicReference<Throwable>()
                val competitor = Thread({
                    try {
                        action()
                    } catch (e: Throwable) {
                        failure.set(e)
                    }
                }, "competing-writer")
                competitor.start()
                competitor.join(Duration.ofMinutes(1).toMillis())
                assertThat(competitor.isAlive)
                    .describedAs("the competing writer is still waiting for something")
                    .isFalse()
                failure.get()?.let { throw it }
            }
        }
    }

    @Test
    fun lookupAndReverseLookupTest() {
        registerType()
            .withAttributes(AttributeDef.create().withId("text"))
            .register()

        val ref = createRecord("text" to "value")

        val id = dbRecordRefService.getIdByEntityRef(ref)
        assertThat(id).isGreaterThan(0)

        // Repeated lookup returns same id
        val id2 = dbRecordRefService.getIdByEntityRef(ref)
        assertThat(id2).isEqualTo(id)

        // Reverse lookup
        val resolvedRef = dbRecordRefService.getEntityRefById(id)
        assertThat(resolvedRef).isEqualTo(ref)
    }

    @Test
    fun cacheConsistencyAfterMoveRefTest() {
        registerType()
            .withAttributes(AttributeDef.create().withId("text"))
            .register()

        val ref = createRecord("text" to "abc")
        val newRef = records.mutateAtt(ref, DbRecordsControlAtts.UPDATE_ID, "new-id")

        // Old ref should not resolve to the record anymore
        assertThat(records.getAtt(ref, "_notExists?bool").asBoolean()).isTrue()

        // New ref should resolve correctly
        assertThat(records.getAtt(newRef, "text").asText()).isEqualTo("abc")

        // Cache: old ref's movedTo should point to the new ref's id
        val newId = dbRecordRefService.getIdByEntityRef(newRef)
        val oldId = dbRecordRefService.getIdByEntityRef(ref)
        assertThat(oldId).isEqualTo(newId)

        // Reverse: newId should resolve to newRef
        val resolvedRef = dbRecordRefService.getEntityRefById(newId)
        assertThat(resolvedRef).isEqualTo(newRef)
    }

    @Test
    fun cacheConsistencyAfterSwapMoveRefTest() {
        registerType()
            .withLocalIdTemplate("\${scope}$\${id}")
            .withAttributes(
                AttributeDef.create().withId("scope")
            )
            .register()

        val initialRef = createRecord("scope" to "uiserv", "id" to "my-id")
        assertThat(initialRef.getLocalId()).isEqualTo("uiserv\$my-id")

        // Populate cache
        val initialId = dbRecordRefService.getIdByEntityRef(initialRef)
        assertThat(initialId).isGreaterThan(0)

        // Move to new localId template
        registerType()
            .withLocalIdTemplate("\${scope}___\${id}")
            .withAttributes(
                AttributeDef.create().withId("scope")
            )
            .register()

        val newRef = records.mutateAtt(initialRef, DbRecordsControlAtts.UPDATE_ID, true)
        assertThat(newRef.getLocalId()).isEqualTo("uiserv___my-id")

        // Move back (triggers swap-ids branch in moveRef)
        registerType()
            .withLocalIdTemplate("\${scope}$\${id}")
            .withAttributes(
                AttributeDef.create().withId("scope")
            )
            .register()

        val swappedRef = records.mutateAtt(newRef, DbRecordsControlAtts.UPDATE_ID, true)
        assertThat(swappedRef).isEqualTo(initialRef)

        // After swap, cache must be consistent
        val idAfterSwap = dbRecordRefService.getIdByEntityRef(initialRef)
        assertThat(idAfterSwap).isEqualTo(initialId)

        val resolvedAfterSwap = dbRecordRefService.getEntityRefById(initialId)
        assertThat(resolvedAfterSwap).isEqualTo(initialRef)

        // newRef should point back to initialRef via movedTo
        val movedToRef = dbRecordRefService.getMovedToRef(newRef)
        assertThat(movedToRef).isEqualTo(initialRef)
    }

    @Test
    fun getOrCreateIdsPreservesOrderTest() {
        registerType()
            .withAttributes(AttributeDef.create().withId("text"))
            .register()

        val ref1 = createRecord("text" to "a")
        val ref2 = createRecord("text" to "b")
        val ref3 = createRecord("text" to "c")

        val refs = listOf(ref3, ref1, ref2)
        val ids = dbRecordRefService.getOrCreateIdByEntityRefs(refs)

        assertThat(ids).hasSize(3)
        // Verify order matches input
        for (i in refs.indices) {
            val expectedId = dbRecordRefService.getIdByEntityRef(refs[i])
            assertThat(ids[i]).isEqualTo(expectedId)
        }
    }

    @Test
    fun batchLookupAndReverseLookupTest() {
        registerType()
            .withAttributes(AttributeDef.create().withId("text"))
            .register()

        val ref1 = createRecord("text" to "a")
        val ref2 = createRecord("text" to "b")

        // Batch lookup
        val ids = dbRecordRefService.getIdByEntityRefs(listOf(ref1, ref2))
        assertThat(ids).hasSize(2)
        assertThat(ids[0]).isGreaterThan(0)
        assertThat(ids[1]).isGreaterThan(0)

        // Single lookups return same ids
        assertThat(dbRecordRefService.getIdByEntityRef(ref1)).isEqualTo(ids[0])
        assertThat(dbRecordRefService.getIdByEntityRef(ref2)).isEqualTo(ids[1])

        // Reverse batch lookup
        val refs = dbRecordRefService.getEntityRefsByIds(ids)
        assertThat(refs).containsExactly(ref1, ref2)
    }

    @Test
    fun nonExistentRefReturnsMinusOneTest() {
        registerType()
            .withAttributes(AttributeDef.create().withId("text"))
            .register()

        val fakeRef = EntityRef.create(APP_NAME, RECS_DAO_ID, "does-not-exist")
        val id = dbRecordRefService.getIdByEntityRef(fakeRef)
        assertThat(id).isEqualTo(-1L)
    }
}
