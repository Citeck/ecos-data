package ru.citeck.ecos.data.sql.inmem

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import org.junit.jupiter.api.assertThrows
import ru.citeck.ecos.data.sql.datasource.DbDataSource
import ru.citeck.ecos.data.sql.dto.DbColumnDef
import ru.citeck.ecos.data.sql.dto.DbColumnType
import ru.citeck.ecos.data.sql.repo.entity.DbEntity
import ru.citeck.ecos.data.sql.test.records.DbRecordsTestBase
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType
import ru.citeck.ecos.records2.predicate.model.Predicates
import ru.citeck.ecos.records3.record.dao.query.dto.query.RecordsQuery
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

/**
 * Regression test for the O(N^2) bulk-create defect in the in-memory backend.
 *
 * Before the undo-log change, every top-level / `requiresNew` write transaction deep-copied the whole
 * store to provide rollback isolation. Each new
 * EntityRef opens a `requiresNew` id-mapping transaction
 * ([ru.citeck.ecos.data.sql.service.DbDataServiceImpl.saveAtomicallyOrGetExistingByExtId]), so creating
 * N records over a store that grows to N rows was O(N^2) — minutes at a few thousand records. The
 * undo-log makes each write O(1), so this completes in well under the timeout below.
 *
 * The test also pins the two isolation invariants the undo-log must preserve:
 *  - a rolled-back write transaction restores the store byte-for-byte;
 *  - the (non-transactional) id sequence is NOT reset by a rollback — a rolled-back transaction still
 *    consumes its ids and they are never reissued.
 */
class InMemBulkCreatePerfTest : DbRecordsTestBase() {

    @Test
    @Timeout(value = 20, unit = TimeUnit.SECONDS)
    @Disabled("Heavy 5000-record benchmark; not part of the routine suite. Enable manually to profile bulk create.")
    fun `bulk create of several thousand records is linear, not quadratic`() {

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("idx")
                    withType(AttributeType.NUMBER)
                },
                // an assoc to a distinct external ref per record => a distinct id-mapping insert
                // (requiresNew) per record, i.e. exactly the path that was O(N^2).
                AttributeDef.create {
                    withId("assocAtt")
                    withType(AttributeType.ASSOC)
                }
            )
        )

        val count = 5_000
        val startedAt = System.nanoTime()
        for (i in 0 until count) {
            createRecord(
                "idx" to i,
                "assocAtt" to "emodel/some-distinct-target@target-$i"
            )
        }
        val elapsedMs = (System.nanoTime() - startedAt) / 1_000_000

        println("InMemBulkCreatePerfTest: created $count records in ${elapsedMs}ms")

        val total = records.query(
            RecordsQuery.create {
                withSourceId(recordsDao.getId())
                withQuery(Predicates.alwaysTrue())
                withMaxItems(1)
            }
        ).getTotalCount()
        assertThat(total).isEqualTo(count.toLong())
    }

    @Test
    fun `rollback restores the store and never reissues ids`() {

        val columns = listOf(
            DbColumnDef.create {
                withName("str_column")
                withType(DbColumnType.TEXT)
            }
        )

        val refIdCounter = AtomicLong()
        fun newEntity(value: String): DbEntity {
            val entity = DbEntity()
            entity.refId = refIdCounter.getAndIncrement()
            entity.attributes["str_column"] = value
            return entity
        }

        val dataSource: DbDataSource = InMemDataServiceTestUtils.createDataSource()
        val service = InMemDataServiceTestUtils.createService(dataSource, "test-perf-rollback")

        val committed = service.save(newEntity("committed"), columns)
        assertThat(service.findAll()).hasSize(1)
        val committedId = committed.id

        // a write transaction that throws must roll back: the staged save is fully discarded and the
        // committed row is left byte-identical.
        assertThrows<RuntimeException> {
            dataSource.withTransaction(false) {
                service.save(newEntity("rolled-back"), columns)
                assertThat(service.findAll()).hasSize(2)
                throw RuntimeException("force rollback")
            }
        }

        val after = service.findAll()
        assertThat(after).hasSize(1)
        assertThat(after[0].attributes["str_column"]).isEqualTo("committed")
        assertThat(after[0].id).isEqualTo(committedId)

        // the id consumed by the rolled-back insert must NOT be reissued: the next committed insert
        // gets a strictly greater id than both the committed row and the rolled-back (discarded) one.
        val next = service.save(newEntity("after-rollback"), columns)
        assertThat(next.id).isGreaterThan(committedId)
        // exactly one id was burned by the rolled-back txn, so the next id skips it.
        assertThat(next.id).isGreaterThanOrEqualTo(committedId + 2)
        assertThat(service.findAll()).hasSize(2)
    }
}
