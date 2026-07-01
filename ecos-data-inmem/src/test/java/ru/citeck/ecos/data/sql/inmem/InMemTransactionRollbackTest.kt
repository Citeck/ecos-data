package ru.citeck.ecos.data.sql.inmem

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import ru.citeck.ecos.data.sql.datasource.DbDataSource
import ru.citeck.ecos.data.sql.dto.DbColumnDef
import ru.citeck.ecos.data.sql.dto.DbColumnType
import ru.citeck.ecos.data.sql.repo.entity.DbEntity
import java.util.concurrent.atomic.AtomicLong

/**
 * Storage-SPI mid-transaction rollback test for the in-memory backend - the one observable behaviour
 * the shared [ru.citeck.ecos.data.sql.test.DbDataServiceContractTest] does NOT cover (it is per-backend,
 * since not every backend participates in the platform transaction manager). The save / find /
 * predicate / sorting / paging / delete / optimistic-locking behaviours are covered by that contract,
 * which [InMemDataServiceContractTest] runs against this backend - so they are intentionally not
 * duplicated here. Driven through [ru.citeck.ecos.data.sql.service.DbDataServiceImpl] over
 * [InMemDataSource].
 */
class InMemTransactionRollbackTest {

    companion object {
        private const val STR_COLUMN = "str_column"

        private val refIdCounter = AtomicLong()
    }

    private fun newEntity(): DbEntity {
        val entity = DbEntity()
        entity.refId = refIdCounter.getAndIncrement()
        return entity
    }

    @Test
    fun testTransactionRollback() {

        val columns = listOf(
            DbColumnDef.create {
                withName(STR_COLUMN)
                withType(DbColumnType.TEXT)
            }
        )

        val dataSource: DbDataSource = InMemDataServiceTestUtils.createDataSource()
        val service = InMemDataServiceTestUtils.createService(dataSource, "test-txn")

        val committed = newEntity()
        committed.attributes[STR_COLUMN] = "committed"
        service.save(committed, columns)
        assertThat(service.findAll()).hasSize(1)

        // a write transaction that throws must roll back: the staged save is discarded
        assertThrows<RuntimeException> {
            dataSource.withTransaction(false) {
                val rolled = newEntity()
                rolled.attributes[STR_COLUMN] = "rolled-back"
                service.save(rolled, columns)
                assertThat(service.findAll()).hasSize(2)
                throw RuntimeException("force rollback")
            }
        }

        val after = service.findAll()
        assertThat(after).hasSize(1)
        assertThat(after[0].attributes[STR_COLUMN]).isEqualTo("committed")
    }
}
