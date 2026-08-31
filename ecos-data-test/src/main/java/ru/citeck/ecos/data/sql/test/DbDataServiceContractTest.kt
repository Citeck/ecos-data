package ru.citeck.ecos.data.sql.test

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import org.junit.jupiter.api.assertThrows
import ru.citeck.ecos.context.lib.ctx.GlobalEcosContext
import ru.citeck.ecos.data.sql.context.DbDataSourceContext
import ru.citeck.ecos.data.sql.datasource.DbDataSource
import ru.citeck.ecos.data.sql.domain.migration.DbMigrationService
import ru.citeck.ecos.data.sql.dto.DbColumnDef
import ru.citeck.ecos.data.sql.dto.DbColumnType
import ru.citeck.ecos.data.sql.repo.entity.DbEntity
import ru.citeck.ecos.data.sql.repo.find.DbFindPage
import ru.citeck.ecos.data.sql.repo.find.DbFindQuery
import ru.citeck.ecos.data.sql.repo.find.DbFindSort
import ru.citeck.ecos.data.sql.service.DbDataService
import ru.citeck.ecos.data.sql.service.DbDataServiceConfig
import ru.citeck.ecos.data.sql.service.DbDataServiceFactory
import ru.citeck.ecos.data.sql.service.DbDataServiceImpl
import ru.citeck.ecos.records2.predicate.model.Predicates
import ru.citeck.ecos.records2.predicate.model.ValuePredicate
import ru.citeck.ecos.test.commons.EcosWebAppApiMock
import ru.citeck.ecos.txn.lib.TxnContext
import ru.citeck.ecos.txn.lib.manager.EcosTxnProps
import ru.citeck.ecos.txn.lib.manager.TransactionManagerImpl
import java.time.Instant
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

/**
 * Backend-AGNOSTIC contract test for the ecos-data storage SPI.
 *
 * It exercises the observable behaviour every [DbEntityRepo][ru.citeck.ecos.data.sql.repo.DbEntityRepo]
 * /[DbSchemaDao][ru.citeck.ecos.data.sql.schema.DbSchemaDao] implementation must provide - save,
 * find-by-id/ext-id, predicate filtering, sorting, paging, total count, multiple-column arrays,
 * optimistic locking and delete - entirely through
 * [DbDataServiceImpl]/[DbDataSourceContext]. The backend is supplied by a subclass through two
 * abstract hooks; nothing else is backend-specific. (Mid-transaction rollback is a storage-SPI
 * behaviour too, but is exercised per-backend - e.g. `InMemTransactionRollbackTest` - rather than
 * here, since not every backend participates in the platform transaction manager.)
 *
 * Concrete subclasses live in each backend module:
 *  - `ecos-data-sql-pg`  -> PG / Testcontainers
 *  - `ecos-data-inmem`   -> the in-memory backend
 *
 * This is the test-module pattern from `ecos-webapp-commons`'s `ecos-webapp-lib-spring-test`: the
 * reusable base lives in `src/main`, consumers depend on it at `scope=test`.
 */
abstract class DbDataServiceContractTest {

    companion object {
        private const val STR_COLUMN = "str_column"
        private const val NUM_COLUMN = "num_column"
        private const val DATE_COLUMN = "date_column"
        private const val V0 = "value_0"
        private const val V1 = "value_1"
        private const val V2 = "value_2"

        private val refIdCounter = AtomicLong()
    }

    /**
     * Create the storage backend factory (the single seam that selects the backend).
     */
    protected abstract fun createDataServiceFactory(): DbDataServiceFactory

    /**
     * Create a fresh data source compatible with the factory above.
     */
    protected abstract fun createDataSource(): DbDataSource

    private fun newEntity(): DbEntity {
        val entity = DbEntity()
        entity.refId = refIdCounter.getAndIncrement()
        return entity
    }

    private fun createService(tableName: String): DbDataService<DbEntity> {

        val webAppApi = EcosWebAppApiMock("test")
        val txnManager = TransactionManagerImpl()
        txnManager.init(webAppApi, EcosTxnProps())
        TxnContext.setManager(txnManager)

        val dsCtx = DbDataSourceContext(
            createDataSource(),
            createDataServiceFactory(),
            DbMigrationService(),
            webAppApi,
            GlobalEcosContext.getContext()
        )
        val schemaCtx = dsCtx.getSchemaContext("ecos-data-contract-test-schema")

        val config = DbDataServiceConfig.create {
            withTable(tableName)
        }
        return DbDataServiceImpl(DbEntity::class.java, config, schemaCtx)
    }

    private fun dateColumns(): List<DbColumnDef> {
        return textColumns() + listOf(
            DbColumnDef.create {
                withName(DATE_COLUMN)
                withType(DbColumnType.DATETIME)
            }
        )
    }

    private fun textColumns(multiple: Boolean = false): List<DbColumnDef> {
        return listOf(
            DbColumnDef.create {
                withName(STR_COLUMN)
                withType(DbColumnType.TEXT)
                withMultiple(multiple)
            }
        )
    }

    @Test
    fun testSaveFindByIdExtIdAndPredicate() {

        val columns = textColumns()
        val service = createService("contract-basic")

        assertThat(service.findAll()).isEmpty()

        var entity = newEntity()
        entity.attributes[STR_COLUMN] = V0
        entity = service.save(entity, columns)

        assertThat(entity.extId).isNotBlank()
        assertThat(entity.deleted).isFalse
        assertThat(entity.status).isEmpty()

        assertThat(service.findAll()).hasSize(1)

        val byExtId = service.findByExtId(entity.extId) ?: error("not found by ext id")
        assertThat(byExtId.attributes[STR_COLUMN]).isEqualTo(V0)

        val byId = service.findById(entity.id) ?: error("not found by id")
        assertThat(byId.attributes[STR_COLUMN]).isEqualTo(V0)

        assertThat(service.findAll(ValuePredicate.eq(STR_COLUMN, V0))).hasSize(1)
        assertThat(service.findAll(ValuePredicate.eq(STR_COLUMN, "other"))).isEmpty()
        assertThat(service.findAll(ValuePredicate.contains(STR_COLUMN, "value"))).hasSize(1)
        assertThat(service.findAll(ValuePredicate.contains(STR_COLUMN, "unknown"))).isEmpty()
    }

    @Test
    fun testMultipleColumnAndOptimisticLock() {

        val columns = textColumns(multiple = true)
        val service = createService("contract-arrays")

        var entity = newEntity()
        entity.attributes[STR_COLUMN] = V0
        entity = service.save(entity, columns)

        assertThat(entity.attributes[STR_COLUMN] as List<*>)
            .containsExactlyInAnyOrderElementsOf(listOf(V0))

        entity.attributes[STR_COLUMN] = listOf(V0, V1, V2)
        entity = service.save(entity, columns)

        assertThat(entity.attributes[STR_COLUMN] as List<*>)
            .containsExactlyInAnyOrderElementsOf(listOf(V0, V1, V2))

        assertThrows<Exception> {
            service.save(entity, columns)
            service.save(entity, columns)
        }
    }

    @Test
    fun testSortPagingAndCount() {

        val columns = listOf(
            DbColumnDef.create {
                withName(NUM_COLUMN)
                withType(DbColumnType.INT)
            }
        )
        val service = createService("contract-sort")

        for (i in listOf(3, 1, 2, 5, 4)) {
            val entity = newEntity()
            entity.attributes[NUM_COLUMN] = i
            service.save(entity, columns)
        }

        val asc = service.findAll(Predicates.alwaysTrue(), listOf(DbFindSort(NUM_COLUMN, true)))
        assertThat(asc.map { it.attributes[NUM_COLUMN] }).containsExactly(1, 2, 3, 4, 5)

        val desc = service.findAll(Predicates.alwaysTrue(), listOf(DbFindSort(NUM_COLUMN, false)))
        assertThat(desc.map { it.attributes[NUM_COLUMN] }).containsExactly(5, 4, 3, 2, 1)

        val page = service.find(
            DbFindQuery.create {
                withPredicate(Predicates.alwaysTrue())
                withSortBy(listOf(DbFindSort(NUM_COLUMN, true)))
            },
            DbFindPage(1, 2),
            true
        )
        assertThat(page.entities.map { it.attributes[NUM_COLUMN] }).containsExactly(2, 3)
        assertThat(page.totalCount).isEqualTo(5)

        assertThat(service.getCount(Predicates.alwaysTrue())).isEqualTo(5)
        assertThat(service.getCount(ValuePredicate.gt(NUM_COLUMN, 3))).isEqualTo(2)
    }

    @Test
    fun testDeleteByPredicateAndId() {

        val columns = listOf(
            DbColumnDef.create {
                withName(NUM_COLUMN)
                withType(DbColumnType.INT)
            }
        )
        val service = createService("contract-delete")

        val saved = (1..5).map { i ->
            val entity = newEntity()
            entity.attributes[NUM_COLUMN] = i
            service.save(entity, columns)
        }

        service.delete(ValuePredicate.gt(NUM_COLUMN, 3))
        assertThat(service.findAll().map { it.attributes[NUM_COLUMN] }).containsExactlyInAnyOrder(1, 2, 3)

        service.delete(saved[0].id)
        assertThat(service.findAll().map { it.attributes[NUM_COLUMN] }).containsExactlyInAnyOrder(2, 3)
    }

    @Test
    fun testConditionalUpdateWithMatchingExpectedValues() {

        val columns = textColumns()
        val service = createService("contract-cond-update-match")

        var entity = newEntity()
        entity.attributes[STR_COLUMN] = V0
        entity = service.save(entity, columns)

        val updated = service.updateByExtIdIfMatches(
            entity.extId,
            mapOf(STR_COLUMN to V0),
            mapOf(STR_COLUMN to V1)
        )

        assertThat(updated).isTrue
        val stored = service.findByExtId(entity.extId) ?: error("not found by ext id")
        assertThat(stored.attributes[STR_COLUMN]).isEqualTo(V1)
        // the update version moves too, so a holder of the pre-update copy can no longer save over it
        assertThat(stored.updVersion).isEqualTo(entity.updVersion + 1)
        assertThrows<Exception> { service.save(entity, columns) }
    }

    @Test
    fun testConditionalUpdateWithNonMatchingExpectedValues() {

        val columns = textColumns()
        val service = createService("contract-cond-update-mismatch")

        var entity = newEntity()
        entity.attributes[STR_COLUMN] = V0
        entity = service.save(entity, columns)

        val updated = service.updateByExtIdIfMatches(
            entity.extId,
            mapOf(STR_COLUMN to V2),
            mapOf(STR_COLUMN to V1)
        )

        assertThat(updated).isFalse
        val stored = service.findByExtId(entity.extId) ?: error("not found by ext id")
        assertThat(stored.attributes[STR_COLUMN]).isEqualTo(V0)
        assertThat(stored.updVersion).isEqualTo(entity.updVersion)
    }

    @Test
    fun testConditionalUpdateOfMissingRow() {

        val columns = textColumns()
        val service = createService("contract-cond-update-missing")

        val entity = newEntity()
        entity.attributes[STR_COLUMN] = V0
        service.save(entity, columns)

        val updated = service.updateByExtIdIfMatches(
            "ext-id-of-nothing",
            mapOf(STR_COLUMN to V0),
            mapOf(STR_COLUMN to V1)
        )

        assertThat(updated).isFalse
        assertThat(service.findAll().map { it.attributes[STR_COLUMN] }).containsExactly(V0)
    }

    @Test
    fun testConditionalUpdateWithInstantAndNullValues() {

        val columns = dateColumns()
        val service = createService("contract-cond-update-instant")

        var entity = newEntity()
        entity.attributes[STR_COLUMN] = V0
        entity = service.save(entity, columns)
        assertThat(service.findByExtId(entity.extId)?.attributes?.get(DATE_COLUMN)).isNull()

        // an expected null is compared with IS NULL, and a bare Instant is not a bindable JDBC
        // parameter - the primitive converts it by column type
        val newDate = Instant.parse("2024-05-06T07:08:09Z")
        val updated = service.updateByExtIdIfMatches(
            entity.extId,
            mapOf(DATE_COLUMN to null),
            mapOf(STR_COLUMN to V1, DATE_COLUMN to newDate)
        )

        assertThat(updated).isTrue
        val stored = service.findByExtId(entity.extId) ?: error("not found by ext id")
        assertThat(stored.attributes[STR_COLUMN]).isEqualTo(V1)
        assertThat(stored.attributes[DATE_COLUMN]).isEqualTo(newDate)

        // the same expected null no longer matches now that the column is set
        assertThat(
            service.updateByExtIdIfMatches(
                entity.extId,
                mapOf(DATE_COLUMN to null),
                mapOf(STR_COLUMN to V2)
            )
        ).isFalse
        assertThat(service.findByExtId(entity.extId)?.attributes?.get(STR_COLUMN)).isEqualTo(V1)

        // and an Instant round-trips as an expected value as well
        assertThat(
            service.updateByExtIdIfMatches(
                entity.extId,
                mapOf(DATE_COLUMN to newDate),
                mapOf(STR_COLUMN to V2)
            )
        ).isTrue
        assertThat(service.findByExtId(entity.extId)?.attributes?.get(STR_COLUMN)).isEqualTo(V2)
    }

    @Test
    fun testConditionalUpdateRejectsInvalidColumnMaps() {

        val columns = dateColumns()
        val service = createService("contract-cond-update-reject")

        var entity = newEntity()
        entity.attributes[STR_COLUMN] = V0
        entity = service.save(entity, columns)
        val extId = entity.extId
        val match = mapOf(STR_COLUMN to V0)
        val set = mapOf(STR_COLUMN to V1)

        // nothing to assign
        assertThrows<Exception> { service.updateByExtIdIfMatches(extId, match, emptyMap()) }
        // nothing to compare - an unconditional overwrite by ext id, never what the caller meant
        assertThrows<Exception> { service.updateByExtIdIfMatches(extId, emptyMap(), set) }

        // a column the table doesn't have: dropping it would lose an assignment, or a condition
        assertThrows<Exception> {
            service.updateByExtIdIfMatches(extId, match, mapOf("no_such_column" to V1))
        }
        assertThrows<Exception> {
            service.updateByExtIdIfMatches(extId, mapOf("no_such_column" to V0), set)
        }

        // reserved columns: the first two identify the row, the last is maintained by the update
        for (reserved in listOf(DbEntity.ID, DbEntity.EXT_ID, DbEntity.UPD_VERSION)) {
            assertThrows<Exception> {
                service.updateByExtIdIfMatches(extId, match, set + mapOf(reserved to 1L))
            }
            assertThrows<Exception> {
                service.updateByExtIdIfMatches(extId, match + mapOf(reserved to 1L), set)
            }
        }

        // every rejection happened before any write
        val stored = service.findByExtId(extId) ?: error("not found by ext id")
        assertThat(stored.attributes[STR_COLUMN]).isEqualTo(V0)
        assertThat(stored.updVersion).isEqualTo(entity.updVersion)
    }

    @Test
    @Timeout(value = 120, unit = TimeUnit.SECONDS)
    fun testConcurrentConditionalUpdatesLetExactlyOneWin() {

        val columns = listOf(
            DbColumnDef.create {
                withName(NUM_COLUMN)
                withType(DbColumnType.INT)
            }
        )
        val service = createService("contract-cond-update-race")

        var entity = newEntity()
        entity.attributes[NUM_COLUMN] = 0
        entity = service.save(entity, columns)
        val extId = entity.extId

        val threads = 6
        val results = ConcurrentLinkedQueue<Boolean>()
        val errors = ConcurrentLinkedQueue<Throwable>()
        val startGate = CountDownLatch(1)
        val executor = Executors.newFixedThreadPool(threads)
        try {
            val futures = (1..threads).map { t ->
                executor.submit {
                    startGate.await()
                    try {
                        results.add(
                            service.updateByExtIdIfMatches(
                                extId,
                                mapOf(NUM_COLUMN to 0),
                                mapOf(NUM_COLUMN to t)
                            )
                        )
                    } catch (e: Throwable) {
                        errors.add(e)
                    }
                }
            }
            startGate.countDown()
            futures.forEach { it.get(100, TimeUnit.SECONDS) }
        } finally {
            executor.shutdownNow()
        }

        // A read-then-check-then-save implementation would either let several threads through or
        // blow up on the optimistic lock; the compare and the write must be a single atomic step.
        // Detection is probabilistic - a green run is evidence, not proof, that no window exists.
        assertThat(errors).isEmpty()
        assertThat(results.count { it }).isEqualTo(1)
        assertThat(results).hasSize(threads)

        val stored = service.findByExtId(extId) ?: error("not found by ext id")
        assertThat(stored.attributes[NUM_COLUMN] as Int).isBetween(1, threads)
    }
}
