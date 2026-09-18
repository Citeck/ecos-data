package ru.citeck.ecos.data.sql.test.records

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import ru.citeck.ecos.data.sql.batch.DbBatchTaskEngine
import ru.citeck.ecos.data.sql.batch.DbBatchTaskStatus
import ru.citeck.ecos.data.sql.columnmeta.DbBackupColumnNames
import ru.citeck.ecos.data.sql.dto.DbColumnType
import ru.citeck.ecos.data.sql.modelchange.DbModelChangeQueue
import ru.citeck.ecos.data.sql.props.DbEcosDataProps
import ru.citeck.ecos.data.sql.records.DbRecordsDao
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType
import ru.citeck.ecos.model.lib.type.dto.TypeInfo
import ru.citeck.ecos.model.lib.type.dto.TypeModelDef
import ru.citeck.ecos.model.lib.utils.ModelUtils
import ru.citeck.ecos.records3.record.dao.impl.proxy.RecordsDaoProxy
import ru.citeck.ecos.webapp.api.entity.EntityRef
import java.time.Duration
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

/**
 * The promise of the whole migration-triggers plan, asserted end to end: a change of the **model**
 * repairs the **schema**, with nothing mutated and nothing called by hand.
 *
 * What makes these tests different from [DbModelChangeQueueTest] and [DbSchemaReconcilerTest] is
 * what is *not* in them. Neither `onTypeChanged` nor `reconcile` is ever called here: the only
 * thing a test body does is change the type model, exactly as an administrator does, and then let
 * one tick run. The notification travels the production path - `TypesRepo.listenTypeChanges`, the
 * subscription ecos-data makes when its first records DAO registers, the coalescing queue - and a
 * break anywhere on it makes these tests red while every unit test around them stays green.
 *
 * `inPlaceAlterMaxRows` is pinned at zero so that every type change here takes the shadow-column
 * path and leaves a backup behind: on a table of three rows PostgreSQL would convert `TEXT` to
 * `DOUBLE PRECISION` where it stands, and there would be no backup column for these tests to point
 * at. The property being asserted is the trigger, not the width of the conversion.
 */
class DbMigrationTriggerTest : DbRecordsTestBase() {

    init {
        dataProps = DbEcosDataProps(
            columns = DbEcosDataProps.ColumnsProps(inPlaceAlterMaxRows = 0),
            batch = DbEcosDataProps.BatchProps(batchPause = Duration.ZERO)
        )
    }

    private companion object {
        const val PROXY_SRC_ID = "trigger-proxy-src"
        const val PROXY_REPO_ID = "trigger-proxy-repo"
        const val PROXY_TYPE_ID = "trigger-proxy-backed"
        const val PROXY_CHILD_TYPE_ID = "trigger-proxy-backed-child"
        const val ORPHAN_TYPE_ID = "trigger-orphan-type"
    }

    /**
     * The queue the application runs, the one the subscription was made for - never a fresh one.
     */
    private val triggerQueue: DbModelChangeQueue
        get() = dataSourceCtx.modelChangeQueue

    private fun textAtt(id: String) = AttributeDef.create { withId(id) }

    private fun att(id: String, type: AttributeType) = AttributeDef.create {
        withId(id)
        withType(type)
    }

    private fun backupColumnsOf(att: String): List<String> {
        return getTableCtx().getAllPhysicalColumns()
            .map { it.name }
            .filter { it.startsWith(DbBackupColumnNames.PREFIX + att) }
    }

    /**
     * `registerAttributes` rebuilds a type from its id and model alone, which would drop the parent
     * and the sourceId of a type that has them - so a type other than the fixture's main one is
     * updated in place instead.
     */
    private fun setTypeAtts(typeId: String, vararg atts: AttributeDef) {
        updateType(typeId) { type ->
            type.withModel(TypeModelDef.create().withAttributes(atts.toList()).build())
        }
    }

    /**
     * The headline case. Nothing in this body touches the queue or the reconciler: `registerAtts`
     * is the administrator changing the type, and the tick is the scheduler.
     */
    @Test
    fun aTypeChangeAloneRepairsTheSchemaTest() {

        registerAtts(listOf(textAtt("someAtt")))
        createRecord("someAtt" to "value-0")

        registerAtts(listOf(att("someAtt", AttributeType.NUMBER)))
        triggerQueue.drainOnce()

        assertThat(getColumns().first { it.name == "someAtt" }.type)
            .describedAs("the schema followed the model without a single mutation")
            .isEqualTo(DbColumnType.DOUBLE)
        assertThat(backupColumnsOf("someAtt"))
            .describedAs("and the original column was moved aside, not rewritten")
            .hasSize(1)
    }

    /**
     * The step up to the storage boundary, reached through the production path.
     *
     * A descendant with `DEFAULT` storage and no `sourceId` of its own inherits its parent's, gets
     * no records DAO, and its records live in the parent's table - so the change announced for it
     * has to repair a table it is not the type of.
     */
    @Test
    fun aDescendantTypeChangeRepairsTheAncestorsTableTest() {

        registerAtts(listOf(textAtt("someAtt")))
        registerType(
            TypeInfo.create {
                withId("trigger-child-without-storage")
                withParentRef(REC_TEST_TYPE_REF)
                withModel(
                    TypeModelDef.create()
                        .withAttributes(listOf(textAtt("childAtt")))
                        .build()
                )
            }
        )
        createRecord("_type" to "trigger-child-without-storage", "childAtt" to "value-0")
        assertThat(getColumns().map { it.name })
            .describedAs("the descendant has no table of its own - this is the premise")
            .contains("childAtt")

        setTypeAtts("trigger-child-without-storage", att("childAtt", AttributeType.NUMBER))
        triggerQueue.drainOnce()

        assertThat(getColumns().first { it.name == "childAtt" }.type)
            .describedAs("a change announced for the descendant repaired the ancestor's table")
            .isEqualTo(DbColumnType.DOUBLE)
    }

    // ---------------------------------------------------------------------------------------
    // The *moment* a trigger fires, which nothing above tests. "Data is never lost and
    // the administrator always has a plan B" is a property of that moment - of two instances
    // reacting at once, of an instance that died before it reacted, and of a model that moved
    // back while the first move was still being carried out.
    // ---------------------------------------------------------------------------------------

    /**
     * The cluster, as far as one JVM can be one: two queues over the same lock API, both told about
     * the same type, both draining at the same time. Exactly one of them may move the column aside;
     * a second backup would mean two copies of the same original, of which only one is ever
     * restored - the other becomes a column nobody reads and nobody deletes.
     *
     * What makes this hold is not the queue but `runMigrationsInLock`: the loser of the lock
     * re-reads the schema and finds nothing left to do.
     */
    @Test
    @Timeout(value = 2, unit = TimeUnit.MINUTES)
    fun twoInstancesReactingAtOnceLeaveOneBackupTest() {

        registerAtts(listOf(textAtt("someAtt")))
        createRecord("someAtt" to "value-0")
        registerAtts(listOf(att("someAtt", AttributeType.NUMBER)))

        val first = DbModelChangeQueue(dataSourceCtx)
        val second = DbModelChangeQueue(dataSourceCtx)
        first.onTypeChanged(REC_TEST_TYPE_ID)
        second.onTypeChanged(REC_TEST_TYPE_ID)

        val startLine = CountDownLatch(1)
        val pool = Executors.newFixedThreadPool(2)
        try {
            val futures = listOf(first, second).map { queue ->
                pool.submit {
                    startLine.await()
                    queue.drainOnce()
                }
            }
            startLine.countDown()
            futures.forEach { it.get(1, TimeUnit.MINUTES) }
        } finally {
            pool.shutdownNow()
        }

        assertThat(backupColumnsOf("someAtt"))
            .describedAs("two reactions to one model change, one copy of the original")
            .hasSize(1)
        assertThat(getColumns().first { it.name == "someAtt" }.type).isEqualTo(DbColumnType.DOUBLE)
    }

    /**
     * The same property without the threads: a tick that runs twice over one instance. Cheaper to
     * diagnose than the concurrent case and it fails for a different reason - the fingerprint of
     * the model fingerprint and the precheck, rather than the distributed lock.
     */
    @Test
    fun drainingTwiceLeavesOneBackupTest() {

        registerAtts(listOf(textAtt("someAtt")))
        createRecord("someAtt" to "value-0")
        registerAtts(listOf(att("someAtt", AttributeType.NUMBER)))

        triggerQueue.drainOnce()
        triggerQueue.onTypeChanged(REC_TEST_TYPE_ID)
        triggerQueue.drainOnce()

        assertThat(backupColumnsOf("someAtt")).hasSize(1)
    }

    /**
     * An instance told about a change and shut down before its tick ran. The notification is in
     * memory and dies with it - so what has to survive is not the notification but the *difference*,
     * and the sweep of the next instance to come up is what finds it.
     *
     * This is the reason the start-up sweep exists at all: without it a model change made in the
     * seconds before a restart would wait for the next change to the same type.
     */
    @Test
    fun aChangeSurvivesTheInstanceThatWasToldAboutItTest() {

        registerAtts(listOf(textAtt("someAtt")))
        createRecord("someAtt" to "value-0")
        registerAtts(listOf(att("someAtt", AttributeType.NUMBER)))

        // told, and then thrown away without ever draining - the instance died
        DbModelChangeQueue(dataSourceCtx).onTypeChanged(REC_TEST_TYPE_ID)
        assertThat(getColumns().first { it.name == "someAtt" }.type)
            .describedAs("nothing happened, which is the premise")
            .isEqualTo(DbColumnType.TEXT)

        val afterRestart = DbModelChangeQueue(dataSourceCtx)
        afterRestart.requestFullSweep()
        afterRestart.drainOnce()

        assertThat(getColumns().first { it.name == "someAtt" }.type).isEqualTo(DbColumnType.DOUBLE)
    }

    /**
     * A restore reached by the trigger rather than by a mutation: the type is changed by mistake and
     * changed back, and the original values come back with it.
     *
     * The whole promise of the feature in one test - and the one place where the trigger has to
     * cooperate with the batch engine, because the values move in the background after the schema
     * has already changed.
     */
    @Test
    fun aModelChangedByMistakeAndChangedBackGivesTheValuesBackTest() {

        registerAtts(listOf(textAtt("someAtt")))
        val rec = createRecord("someAtt" to "original")

        registerAtts(listOf(att("someAtt", AttributeType.NUMBER)))
        triggerQueue.drainOnce()
        drainBatchTasks()

        assertThat(getColumns().first { it.name == "someAtt" }.type)
            .describedAs("the mistake really was carried out")
            .isEqualTo(DbColumnType.DOUBLE)

        registerAtts(listOf(textAtt("someAtt")))
        triggerQueue.drainOnce()
        drainBatchTasks()

        assertThat(records.getAtt(rec, "someAtt").asText())
            .describedAs("the backup came back, reached without a single mutation of the record")
            .isEqualTo("original")
    }

    /**
     * A second records DAO over the same table, with a column cache warmed before the queue
     * migrated it. Its cache is now wrong, and nothing told it so: the schema-wide reset of
     * `DbShadowColumnTransition` reaches the schema's own system services and the migrating DAO,
     * not a neighbour built over the same table.
     *
     * What is asserted is the recovery, not the absence of a stumble. `execReadOnlyQuery` treats a
     * schema-mismatch SQL state as "drop the cache and let this attempt fail", deliberately - the
     * caller has to see the error rather than a silently wrong answer - so the first read after a
     * foreign migration is allowed to throw. What must not happen is that the neighbour stays
     * broken, or answers with the old column's data.
     */
    @Test
    fun aNeighbourOverTheSameTableRecoversAfterTheQueueMigratesItTest() {

        registerAtts(listOf(textAtt("someAtt")))
        // a value the conversion can actually carry across: an unconvertible one would leave the
        // new column empty by design - an untransferable cell is a normal outcome and the original
        // stays in the backup - and this test would then be measuring that instead
        val rec = createRecord("someAtt" to "42")

        val neighbour = createRecordsDao(
            tableRef,
            REC_TEST_TYPE_REF,
            "trigger-neighbour-dao"
        )
        val neighbourRef = EntityRef.create(APP_NAME, neighbour.dao.getId(), rec.getLocalId())
        assertThat(records.getAtt(neighbourRef, "someAtt").asText())
            .describedAs("the neighbour's column cache is warm now - that is the premise")
            .isEqualTo("42")

        registerAtts(listOf(att("someAtt", AttributeType.NUMBER)))
        triggerQueue.drainOnce()
        drainBatchTasks()

        assertThat(getColumns().first { it.name == "someAtt" }.type)
            .describedAs("the table moved under the neighbour - that is the situation under test")
            .isEqualTo(DbColumnType.DOUBLE)

        // One lost attempt is allowed and no more. The stale cache is dropped by the failure and
        // the caller is shown the error rather than a silently wrong answer - that is the chosen
        // contract - but the *next* read has to work. Before the type-mismatch half of
        // DbDataServiceImpl.isStaleSchemaError existed, this loop failed every time on PostgreSQL:
        // a changed column type does not fail in the database at all, it fails while mapping the
        // row, so no SQL state ever said "your schema is stale" and nothing reset the cache.
        val firstReadFailed = runCatching { records.getAtt(neighbourRef, "someAtt") }.isFailure
        repeat(3) { attempt ->
            assertThat(records.getAtt(neighbourRef, "someAtt").asDouble())
                .describedAs(
                    "read ${attempt + 1} after the migration" +
                        if (firstReadFailed) " (one attempt was lost)" else " (nothing stumbled)"
                )
                .isEqualTo(42.0)
        }
    }

    /**
     * The batch engine that survives the schema change it was queued by - which is the only shape
     * production has, and the one no test had.
     *
     * [drainBatchTasks] builds a fresh [DbBatchTaskEngine] on every call, so its cached per-table
     * max-id reader is always newly born and never meets a table that moved. The engine of a running
     * application is built once and ticks for the lifetime of the instance, and what queues a column
     * migration is a DDL statement - so its first read after one is the likeliest in the whole
     * system to be holding a column list that no longer matches.
     *
     * The sequence below is the one a live installation produced, and nothing shorter reproduces it:
     * the reader has to be warmed **by running a task**, because a drain with no task for the table
     * never reads it at all. So the column goes TEXT -> NUMBER and is drained (the reader now has
     * the table as NUMBER), and only then does it go back to TEXT and get drained again by the same
     * engine.
     *
     * There the restore's value transfer stalled with "Can't convert String to Double" thrown from
     * `DbBatchTaskEngine.readMaxId`, and the task sat with an error recorded against it. The read
     * path recovers from that by itself now, at the cost of one lost attempt and one error on the
     * task; this asserts the cost is not paid, because the engine drops that reader's cache once
     * per run.
     */
    @Test
    fun anEngineThatOutlivesTheSchemaChangeRunsWithoutAnErrorTest() {

        registerAtts(listOf(textAtt("someAtt")))
        createRecord("someAtt" to "42")

        val schemaCtx = getTableCtx().getSchemaCtx()
        val engine = DbBatchTaskEngine(schemaCtx.dataSourceCtx)

        registerAtts(listOf(att("someAtt", AttributeType.NUMBER)))
        triggerQueue.drainOnce()
        // this drain runs a real task, which is what makes the engine's max-id reader cache the
        // table - as NUMBER, the shape it is about to stop having
        engine.drainSchemaOnce(schemaCtx)
        assertThat(schemaCtx.batchTaskService.findByTable(tableRef.table))
            .describedAs("the reader is warm only if there really was a task to run")
            .isNotEmpty()

        registerAtts(listOf(textAtt("someAtt")))
        triggerQueue.drainOnce()
        engine.drainSchemaOnce(schemaCtx)

        val tasks = schemaCtx.batchTaskService.findByTable(tableRef.table)
        assertThat(tasks)
            .describedAs("the return to TEXT queued a transfer of its own")
            .hasSizeGreaterThan(1)
        assertThat(tasks.map { it.errorCount })
            .describedAs("a stale reader costs exactly one failed attempt, and it is recorded here")
            .containsOnly(0)
        assertThat(tasks.map { it.error })
            .describedAs("and nothing is written into the error column")
            .containsOnly("")
        assertThat(tasks.map { it.status })
            .describedAs("one drain each is enough when nothing had to be retried")
            .containsOnly(DbBatchTaskStatus.DONE)
        assertThat(records.getAtt(createRecord("someAtt" to "back-to-text"), "someAtt").asText())
            .describedAs("and the table really is back on the old type")
            .isEqualTo("back-to-text")
    }

    // ---------------------------------------------------------------------------------------
    // A type whose sourceId names something other than the DAO that owns its table. Every
    // proxied domain of the platform is shaped this way, and no test had that shape before.
    // ---------------------------------------------------------------------------------------

    /**
     * The type points at a [RecordsDaoProxy] - `activity` in front of `activity-repo`, `comment` in
     * front of `comment-repo`, `camel-dsl-instance` in front of `camel-dsl-instance-repo` - and the
     * proxy has no table of its own and is not a `DbRecordsDao` at all.
     *
     * The trigger reaches the table behind it because the DAO index is keyed by the type a DAO
     * declares and never by the source id the type points at; in a proxied domain those are two
     * different strings by construction. The premise is asserted first, because a fixture where
     * they happened to be equal would let this pass for the wrong reason.
     */
    @Test
    fun aTypeBehindAProxyIsStillReachedByTheTriggerTest() {

        val repo = createProxiedType(PROXY_TYPE_ID, listOf(textAtt("someAtt")))
        val rec = repo.createRecord("someAtt" to "value-0")

        assertThat(records.getAtt(EntityRef.create(APP_NAME, PROXY_SRC_ID, rec.getLocalId()), "someAtt").asText())
            .describedAs("the type's sourceId really resolves through the proxy - the premise")
            .isEqualTo("value-0")
        assertThat(repo.dao.getId())
            .describedAs("and it really is not the id of the DAO that owns the table")
            .isNotEqualTo(PROXY_SRC_ID)
        assertThat(records.getRecordsDao(PROXY_SRC_ID))
            .describedAs(
                "resolving the type's sourceId through the records service - the obvious way to " +
                    "find the table of a type - hands back the proxy, which has no table at all"
            )
            .isNotInstanceOf(DbRecordsDao::class.java)

        setTypeAtts(PROXY_TYPE_ID, att("someAtt", AttributeType.NUMBER))
        triggerQueue.drainOnce()

        assertThat(repo.getColumns().first { it.name == "someAtt" }.type)
            .describedAs("the change reached the table behind the proxy")
            .isEqualTo(DbColumnType.DOUBLE)
    }

    /**
     * The two hard parts at once: a descendant with no storage of its own **under** a proxy-backed
     * parent, which is what `immediate-activity` and `ecos-camel-dsl-instance-import-data` are on a
     * live installation.
     *
     * The descendant inherits the parent's resolved `sourceId` - the proxy's - so the climb to the
     * storage boundary compares the proxy id with itself and carries on up to the type that has the
     * DAO. A descendant left with a blank `sourceId` instead would stop the climb at its own id and
     * the change would reach nothing, which is why the inherited value is set here explicitly
     * rather than left to the fixture's default.
     */
    @Test
    fun aDescendantOfAProxyBackedTypeRepairsTheAncestorsTableTest() {

        val repo = createProxiedType(PROXY_TYPE_ID, listOf(textAtt("someAtt")))
        registerType(
            TypeInfo.create {
                withId(PROXY_CHILD_TYPE_ID)
                withParentRef(ModelUtils.getTypeRef(PROXY_TYPE_ID))
                // what TypeDefResolver derives for a descendant with DEFAULT storage
                withSourceId(PROXY_SRC_ID)
                withModel(
                    TypeModelDef.create()
                        .withAttributes(listOf(textAtt("childAtt")))
                        .build()
                )
            }
        )
        repo.createRecord("_type" to ModelUtils.getTypeRef(PROXY_CHILD_TYPE_ID), "childAtt" to "value-0")
        assertThat(repo.getColumns().map { it.name })
            .describedAs("the descendant has no table of its own - the premise")
            .contains("childAtt")

        setTypeAtts(PROXY_CHILD_TYPE_ID, att("childAtt", AttributeType.NUMBER))
        triggerQueue.drainOnce()

        assertThat(repo.getColumns().first { it.name == "childAtt" }.type)
            .describedAs("a change announced for the descendant repaired the proxied ancestor's table")
            .isEqualTo(DbColumnType.DOUBLE)
    }

    /**
     * A records DAO built without `withTypeRef` - the shape `ecos-integrations` has today, where
     * `ModelUtils.getTypeRef(TYPE_ID)` is computed inside the builder block and its value dropped.
     *
     * Such a DAO cannot be indexed: the event carries a type id and this DAO claims no type, so
     * there is nothing to key it under. The table is then repaired only on the next mutation. This
     * records that consequence rather than arguing with it - the fix belongs in the application
     * that builds the DAO - so that the blast radius is written down somewhere a reader will find
     * it.
     */
    @Test
    fun aDaoWithNoTypeRefIsNotReachedByTheTriggerTest() {

        val orphan = createRecordsDao(
            tableRef.withTable("trigger_orphan_table"),
            EntityRef.EMPTY,
            "trigger-orphan-dao"
        )
        registerType(
            TypeInfo.create {
                withId(ORPHAN_TYPE_ID)
                withSourceId("trigger-orphan-dao")
                withModel(
                    TypeModelDef.create()
                        .withAttributes(listOf(textAtt("someAtt")))
                        .build()
                )
            }
        )
        orphan.createRecord("_type" to ModelUtils.getTypeRef(ORPHAN_TYPE_ID), "someAtt" to "value-0")

        setTypeAtts(ORPHAN_TYPE_ID, att("someAtt", AttributeType.NUMBER))
        triggerQueue.drainOnce()

        assertThat(orphan.getColumns().first { it.name == "someAtt" }.type)
            .describedAs("a DAO that declares no type is invisible to the trigger")
            .isEqualTo(DbColumnType.TEXT)
    }

    /**
     * The fixture both proxy tests need: a table owned by a DAO under one id, a [RecordsDaoProxy]
     * in front of it under another, and a type pointing at the proxy.
     */
    private fun createProxiedType(typeId: String, atts: List<AttributeDef>): RecordsDaoTestCtx {
        val repo = createRecordsDao(
            tableRef.withTable("trigger_proxy_backed"),
            ModelUtils.getTypeRef(typeId),
            PROXY_REPO_ID
        )
        records.register(RecordsDaoProxy(PROXY_SRC_ID, PROXY_REPO_ID))
        registerType(
            TypeInfo.create {
                withId(typeId)
                withSourceId(PROXY_SRC_ID)
                withModel(TypeModelDef.create().withAttributes(atts).build())
            }
        )
        return repo
    }

    private fun drainBatchTasks() {
        val schemaCtx = getTableCtx().getSchemaCtx()
        DbBatchTaskEngine(schemaCtx.dataSourceCtx).drainSchemaOnce(schemaCtx)
    }

    /**
     * The start-up sweep, for the application whose `TypesRepo` has no events at all: the
     * schema still converges, one sweep later, with nothing ever announced.
     */
    @Test
    fun aStartUpSweepRepairsTheSchemaWithoutAnyEventTest() {

        registerAtts(listOf(textAtt("someAtt")))
        createRecord("someAtt" to "value-0")
        registerAtts(listOf(att("someAtt", AttributeType.NUMBER)))

        // a queue of its own, with an empty pending set: whatever the subscription announced went
        // to the application's queue, so anything this one repairs it repaired because of the sweep
        val coldQueue = DbModelChangeQueue(dataSourceCtx)
        coldQueue.requestFullSweep()
        coldQueue.drainOnce()

        assertThat(getColumns().first { it.name == "someAtt" }.type).isEqualTo(DbColumnType.DOUBLE)
    }
}

/**
 * The start-up storm, kept in a class of its own because `dataProps` is read by
 * `DataMockFactory.setUp` while it builds the data source context - i.e. in `@BeforeEach`, before
 * any test body runs - so a test needing a different value needs a different class. Same reason
 * [DbModelChangeQueueTickLimitTest] is separate.
 */
class DbMigrationTriggerStormTest : DbRecordsTestBase() {

    init {
        dataProps = DbEcosDataProps(
            columns = DbEcosDataProps.ColumnsProps(
                inPlaceAlterMaxRows = 0,
                reconcileMaxTablesPerTick = 1
            )
        )
    }

    private val triggerQueue: DbModelChangeQueue
        get() = dataSourceCtx.modelChangeQueue

    /**
     * What a passive registry does on every single start: one event per type of the installation,
     * on the application's start-up thread. The ceiling has to hold against it - one table per
     * tick, and one migration lock at most - and nothing may be dropped on the way.
     */
    @Test
    fun aStormOfStartUpEventsIsCappedAtOneTablePerTickTest() {

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
            .asSubTypeWithId("storm-second-type")
            .withSourceId("storm-second-dao")
            .withAttributes(AttributeDef.create { withId("secondAtt") })
            .register()
        secondCtx.createRecord("secondAtt" to "value-0")
        updateType("storm-second-type") { type ->
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

        // Everything above was announced through the subscription already - the storm is what the
        // application's own queue is holding by now, and no test body put it there.
        val locksBefore = schemaMigrationLockCallCount()
        assertThat(triggerQueue.drainOnce())
            .describedAs("one table per tick, however many types were announced")
            .isEqualTo(1)
        assertThat(schemaMigrationLockCallCount() - locksBefore)
            .describedAs("and one table is one migration lock")
            .isLessThanOrEqualTo(1)

        assertThat(triggerQueue.drainOnce()).isEqualTo(1)
        assertThat(triggerQueue.drainOnce())
            .describedAs("nothing was dropped and nothing is repeated")
            .isEqualTo(0)

        assertThat(getColumns().first { it.name == "someAtt" }.type).isEqualTo(DbColumnType.DOUBLE)
        assertThat(secondCtx.getColumns().first { it.name == "secondAtt" }.type)
            .isEqualTo(DbColumnType.DOUBLE)
    }
}
