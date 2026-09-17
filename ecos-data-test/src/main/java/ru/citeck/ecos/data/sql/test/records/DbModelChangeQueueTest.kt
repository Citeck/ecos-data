package ru.citeck.ecos.data.sql.test.records

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import ru.citeck.ecos.data.sql.dto.DbColumnType
import ru.citeck.ecos.data.sql.modelchange.DbModelChangeQueue
import ru.citeck.ecos.data.sql.modelchange.DbSchemaReconciler
import ru.citeck.ecos.data.sql.props.DbEcosDataProps
import ru.citeck.ecos.data.sql.records.DbRecordsDao
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType
import ru.citeck.ecos.model.lib.type.dto.TypeModelDef
import java.util.concurrent.TimeUnit

/**
 * What a storm of model changes costs and what it does.
 *
 * Everything asserted here is a property of the *queue*, not of the reconciliation it delegates to:
 * how many reconciliations a hundred notifications turn into, what an unknown type costs, what
 * happens to the work one tick had no room for, and what keeps a table that was already reconciled
 * from being reconciled again on every tick for ever.
 */
class DbModelChangeQueueTest : DbRecordsTestBase() {

    private fun queue(reconciler: DbSchemaReconciler = DbSchemaReconciler()): DbModelChangeQueue {
        return DbModelChangeQueue(dataSourceCtx, reconciler)
    }

    private fun textAtt(id: String) = AttributeDef.create { withId(id) }

    private fun att(id: String, type: AttributeType) = AttributeDef.create {
        withId(id)
        withType(type)
    }

    /**
     * `registerAttributes` rebuilds the whole type from its id and model, which would drop the
     * `sourceId` and the parent of a type that has its own - so a type other than the fixture's
     * main one has to be updated in place instead.
     */
    private fun setTypeAtts(typeId: String, vararg atts: AttributeDef) {
        updateType(typeId) { type ->
            type.withModel(TypeModelDef.create().withAttributes(atts.toList()).build())
        }
    }

    /**
     * The headline property: a hundred announcements about one type are one reconciliation, and the
     * tick after them is free. Without coalescing this is a hundred distributed locks, on the
     * thread that announced the change.
     */
    @Test
    fun aHundredNotificationsAboutOneTypeAreOneReconciliationTest() {

        registerAtts(listOf(textAtt("someAtt")))
        createRecord("someAtt" to "value-0")
        registerAtts(listOf(att("someAtt", AttributeType.NUMBER)))

        val queue = queue()
        repeat(100) { queue.onTypeChanged(REC_TEST_TYPE_ID) }

        assertThat(queue.drainOnce()).isEqualTo(1)
        assertThat(queue.drainOnce())
            .describedAs("the set was emptied by the tick, so the next one has nothing to do")
            .isEqualTo(0)
        assertThat(getColumns().first { it.name == "someAtt" }.type).isEqualTo(DbColumnType.DOUBLE)
    }

    /**
     * The other half of the storm: on a real installation most of what a registry publishes is of
     * no interest to this data source at all. Those notifications have to cost an index lookup and
     * nothing else - measured by the lock counter rather than by eye, because "it felt fast" is not
     * a property.
     */
    @Test
    @Timeout(value = 2, unit = TimeUnit.MINUTES)
    fun thousandsOfUnregisteredTypesCostNothingTest() {

        registerAtts(listOf(textAtt("someAtt")))
        createRecord("someAtt" to "value-0")

        val queue = queue()
        val locksBefore = schemaMigrationLockCallCount()
        repeat(5000) { queue.onTypeChanged("type-not-in-this-data-source-$it") }

        assertThat(queue.drainOnce())
            .describedAs("a type no DAO of this data source stores resolves to no table")
            .isEqualTo(0)
        assertThat(schemaMigrationLockCallCount())
            .describedAs("and resolving it must not take the migration lock even once")
            .isEqualTo(locksBefore)
    }

    /**
     * One table's reconciliation throwing must not take the tables queued behind it with it. The
     * substitute here throws rather than returning
     * [DbSchemaReconciler.Status.FAILED] on purpose: the queue has to hold even when the promise
     * that reconciliation never throws is broken.
     */
    @Test
    fun aFailingReconciliationDoesNotStopTheTickTest() {

        registerAtts(listOf(textAtt("someAtt")))
        createRecord("someAtt" to "value-0")
        registerAtts(listOf(att("someAtt", AttributeType.NUMBER)))

        val secondCtx = registerType()
            .asSubTypeWithId("queue-second-type")
            .withSourceId("queue-second-dao")
            .withAttributes(textAtt("secondAtt"))
            .register()
        secondCtx.createRecord("secondAtt" to "value-0")
        setTypeAtts("queue-second-type", att("secondAtt", AttributeType.NUMBER))

        val failing = object : DbSchemaReconciler() {
            override fun reconcile(dao: DbRecordsDao): Result {
                if (dao === mainCtx.dao) {
                    error("reconciliation of ${dao.getId()} is broken on purpose")
                }
                return super.reconcile(dao)
            }
        }
        val queue = queue(failing)
        queue.onTypeChanged(REC_TEST_TYPE_ID)
        queue.onTypeChanged("queue-second-type")

        assertThat(queue.drainOnce())
            .describedAs("both tables were attempted, the broken one included")
            .isEqualTo(2)
        assertThat(secondCtx.getColumns().first { it.name == "secondAtt" }.type)
            .describedAs("the table queued behind the broken one was still reconciled")
            .isEqualTo(DbColumnType.DOUBLE)
        assertThat(getColumns().first { it.name == "someAtt" }.type)
            .describedAs("and the broken one was left exactly as it was")
            .isEqualTo(DbColumnType.TEXT)
    }

    /**
     * The start-up sweep: nobody announced anything, and every table the index knows is
     * still looked at once.
     */
    @Test
    fun aFullSweepQueuesEveryDaoOfTheIndexTest() {

        registerAtts(listOf(textAtt("someAtt")))
        createRecord("someAtt" to "value-0")

        val queue = queue()
        queue.requestFullSweep()

        val daoCount = dataSourceCtx.recordsDaoIndex.getAll().size
        assertThat(daoCount)
            .describedAs("the fixture registers several DAOs, so this is a real sweep")
            .isGreaterThan(1)
        assertThat(queue.drainOnce()).isEqualTo(daoCount)
        assertThat(queue.drainOnce())
            .describedAs("the sweep is a flag, not a queue - one request is one sweep")
            .isEqualTo(0)
    }

    /**
     * A table reconciled against a model that has not moved since must not be
     * reconciled again, however often its type is announced - otherwise a frozen column asks for
     * the migration lock on every tick for the lifetime of the instance.
     *
     * The second notification is what makes this test about the fingerprint rather than about an
     * empty pending set.
     */
    @Test
    fun aTableIsNotReconciledTwiceForOneModelTest() {

        registerAtts(listOf(textAtt("someAtt")))
        createRecord("someAtt" to "value-0")
        registerAtts(listOf(att("someAtt", AttributeType.NUMBER)))

        val queue = queue()
        queue.onTypeChanged(REC_TEST_TYPE_ID)
        assertThat(queue.drainOnce()).isEqualTo(1)

        queue.onTypeChanged(REC_TEST_TYPE_ID)
        assertThat(queue.drainOnce())
            .describedAs("announced again, but the model behind it did not move")
            .isEqualTo(0)

        registerAtts(listOf(att("someAtt", AttributeType.NUMBER), textAtt("anotherAtt")))
        queue.onTypeChanged(REC_TEST_TYPE_ID)
        assertThat(queue.drainOnce())
            .describedAs("and when it does move, the table is reconciled again")
            .isEqualTo(1)
        assertThat(getColumns().map { it.name }).contains("anotherAtt")
    }

    /**
     * The whole reason this class exists: the notification itself does nothing. In `emodel` it
     * arrives inside the open transaction that saves the type, so a schema change made here would
     * be a schema change inside a user's save.
     */
    @Test
    fun aNotificationOnItsOwnChangesNothingTest() {

        registerAtts(listOf(textAtt("someAtt")))
        createRecord("someAtt" to "value-0")
        registerAtts(listOf(att("someAtt", AttributeType.NUMBER)))

        val locksBefore = schemaMigrationLockCallCount()
        queue().onTypeChanged(REC_TEST_TYPE_ID)

        assertThat(getColumns().first { it.name == "someAtt" }.type)
            .describedAs("the sink only remembers the type id; the tick is what acts on it")
            .isEqualTo(DbColumnType.TEXT)
        assertThat(schemaMigrationLockCallCount()).isEqualTo(locksBefore)
    }
}

/**
 * The per-tick ceiling, kept out of [DbModelChangeQueueTest] proper because `dataProps` is read by
 * `DataMockFactory.setUp` while building the data source context - i.e. in `@BeforeEach`, before any
 * test body runs - so a test needing a different value needs a class of its own. The same reason
 * `DbBatchTaskEngineBatchSizePropsTest` is a separate class.
 */
class DbModelChangeQueueTickLimitTest : DbRecordsTestBase() {

    init {
        dataProps = DbEcosDataProps(
            columns = DbEcosDataProps.ColumnsProps(reconcileMaxTablesPerTick = 1)
        )
    }

    /**
     * What one tick has no room for has to come back on the next one. Losing it would be the worst
     * of the failure modes available here: the model and the schema stay apart with nothing left to
     * announce them again, which is exactly the "two tables diverging for ever" the feature exists
     * to make unreachable.
     */
    @Test
    fun workBeyondTheTickLimitIsDeferredAndNotLostTest() {

        registerAtts(listOf(AttributeDef.create { withId("someAtt") }))
        createRecord("someAtt" to "value-0")
        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("someAtt")
                    withType(AttributeType.NUMBER)
                }
            )
        )

        val secondCtx = registerType()
            .asSubTypeWithId("limit-second-type")
            .withSourceId("limit-second-dao")
            .withAttributes(AttributeDef.create { withId("secondAtt") })
            .register()
        secondCtx.createRecord("secondAtt" to "value-0")
        updateType("limit-second-type") { type ->
            type.withModel(
                TypeModelDef.create()
                    .withAttributes(
                        listOf(
                            AttributeDef.create {
                                withId("secondAtt")
                                withType(AttributeType.NUMBER)
                            }
                        )
                    ).build()
            )
        }

        val queue = DbModelChangeQueue(dataSourceCtx)
        queue.onTypeChanged(REC_TEST_TYPE_ID)
        queue.onTypeChanged("limit-second-type")

        assertThat(queue.drainOnce()).describedAs("one table per tick").isEqualTo(1)
        assertThat(queue.drainOnce()).describedAs("the deferred one, on the next tick").isEqualTo(1)
        assertThat(queue.drainOnce()).describedAs("and then nothing is left").isEqualTo(0)

        assertThat(getColumns().first { it.name == "someAtt" }.type).isEqualTo(DbColumnType.DOUBLE)
        assertThat(secondCtx.getColumns().first { it.name == "secondAtt" }.type)
            .isEqualTo(DbColumnType.DOUBLE)
    }
}
