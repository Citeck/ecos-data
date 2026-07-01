package ru.citeck.ecos.data.sql.test

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
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
        private const val V0 = "value_0"
        private const val V1 = "value_1"
        private const val V2 = "value_2"

        private val refIdCounter = AtomicLong()
    }

    /** Create the storage backend factory (the single seam that selects the backend). */
    protected abstract fun createDataServiceFactory(): DbDataServiceFactory

    /** Create a fresh data source compatible with the factory above. */
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
}
