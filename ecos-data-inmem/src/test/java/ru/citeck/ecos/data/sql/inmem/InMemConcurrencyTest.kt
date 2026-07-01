package ru.citeck.ecos.data.sql.inmem

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import ru.citeck.ecos.data.sql.dto.DbColumnDef
import ru.citeck.ecos.data.sql.dto.DbColumnType
import ru.citeck.ecos.data.sql.repo.entity.DbEntity
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

/**
 * Proves that [ru.citeck.ecos.data.sql.inmem.datasource.InMemDataSource] serializes concurrent
 * transactions instead of corrupting its shared in-place store / undo-log.
 *
 * The store and its undo-log assume one transaction at a time (the undo-log is a single stack indexed
 * by absolute position). Without the data source's transaction lock, threads writing concurrently would
 * race on the backing `LinkedHashMap`s and splice entries into each other's undo-log ranges, surfacing
 * as `ConcurrentModificationException`, lost rows or a wrong count. With the lock they queue, so the
 * outcome is exactly as if the writes had run sequentially.
 */
class InMemConcurrencyTest {

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    fun `concurrent writes on a shared data source are serialized, not corrupted`() {

        val columns = listOf(
            DbColumnDef.create {
                withName("str_column")
                withType(DbColumnType.TEXT)
            }
        )

        val dataSource = InMemDataServiceTestUtils.createDataSource()
        val service = InMemDataServiceTestUtils.createService(dataSource, "test-concurrency")

        val threads = 6
        val perThread = 100
        val refIdSeq = AtomicLong()
        val errors = ConcurrentLinkedQueue<Throwable>()
        val startGate = CountDownLatch(1)
        val executor = Executors.newFixedThreadPool(threads)
        try {
            val futures = (0 until threads).map { t ->
                executor.submit {
                    startGate.await()
                    try {
                        for (i in 0 until perThread) {
                            val entity = DbEntity()
                            entity.refId = refIdSeq.getAndIncrement()
                            entity.attributes["str_column"] = "t$t-i$i"
                            service.save(entity, columns)
                        }
                    } catch (e: Throwable) {
                        errors.add(e)
                    }
                }
            }
            startGate.countDown()
            futures.forEach { it.get(50, TimeUnit.SECONDS) }
        } finally {
            executor.shutdownNow()
        }

        assertThat(errors).isEmpty()

        val all = service.findAll()
        assertThat(all).hasSize(threads * perThread)
        // every record landed with a distinct id and its payload intact (no torn/lost writes)
        assertThat(all.map { it.id }.toSet()).hasSize(threads * perThread)
        assertThat(all.mapNotNull { it.attributes["str_column"] as? String }.toSet())
            .hasSize(threads * perThread)
    }
}
