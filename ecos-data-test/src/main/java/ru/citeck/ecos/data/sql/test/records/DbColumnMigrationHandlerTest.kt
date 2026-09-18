package ru.citeck.ecos.data.sql.test.records

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import ru.citeck.ecos.commons.data.ObjectData
import ru.citeck.ecos.context.lib.auth.AuthContext
import ru.citeck.ecos.context.lib.auth.AuthGroup
import ru.citeck.ecos.data.sql.batch.DbBatchTaskContext
import ru.citeck.ecos.data.sql.batch.DbBatchTaskEngine
import ru.citeck.ecos.data.sql.batch.DbBatchTaskStatus
import ru.citeck.ecos.data.sql.migration.column.DbColumnMigrationHandler
import ru.citeck.ecos.data.sql.migration.column.DbColumnMigrationParams
import ru.citeck.ecos.data.sql.props.DbEcosDataProps
import ru.citeck.ecos.data.sql.records.assocs.DbAssocBackupEntity
import ru.citeck.ecos.data.sql.records.assocs.DbAssocEntity
import ru.citeck.ecos.data.sql.repo.entity.DbEntity
import ru.citeck.ecos.data.sql.repo.find.DbFindPage
import ru.citeck.ecos.data.sql.repo.find.DbFindSort
import ru.citeck.ecos.data.sql.service.DbDataServiceConfig
import ru.citeck.ecos.data.sql.service.DbDataServiceImpl
import ru.citeck.ecos.model.lib.attributes.dto.AttIndexDef
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType
import ru.citeck.ecos.records2.RecordConstants
import ru.citeck.ecos.records2.predicate.model.Predicates
import ru.citeck.ecos.txn.lib.TxnContext
import ru.citeck.ecos.webapp.api.entity.EntityRef
import java.time.Duration
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference

/**
 * The background transfer.
 *
 * Every test here needs the synchronous phase to choose the shadow-column path, so the in-place
 * threshold is pinned at zero: a table with any rows at all is "too large to convert inside a
 * user's mutation", which is the branch this handler exists to finish. Without it a pair the
 * backend can express - `NUMBER -> TEXT` is one on PostgreSQL - would be converted in place and
 * there would be no task to drain at all. Which *pairs* convert to what is the conversion matrix's
 * job, not this class's.
 *
 * The batch size is three so that a handful of records is still walked in several id windows.
 */
class DbColumnMigrationHandlerTest : DbRecordsTestBase() {

    companion object {
        /**
         * A `bytea` a `text` column cannot hold. `String(bytes, UTF_8)` turns the `0x00` into
         * `U+0000`, which PostgreSQL rejects **server-side** with
         * `ERROR: invalid byte sequence for encoding "UTF8": 0x00` - not at bind time, which matters
         * because a server-side rejection aborts the whole transaction and is exactly why the value
         * has to be refused before it is bound rather than caught at the write. Invalid UTF-8 is
         * deliberately *not* what this is: those bytes become `U+FFFD` and store perfectly well.
         */
        private val BYTES_WITH_NUL = byteArrayOf('a'.code.toByte(), 0, 'b'.code.toByte())

        /**
         * A `text` value a `jsonb` column cannot hold, and the reason the guard cannot stop at the
         * text-family targets. Nothing about this string is unusual - the `\u0000` is six ordinary
         * ASCII characters, so it stores in a `text` column without a murmur - but the *parsed*
         * string value carries a `U+0000`, and PostgreSQL answers
         * `ERROR: unsupported Unicode escape sequence / cannot be converted to text`.
         *
         * A raw string literal on purpose: Kotlin would turn `"\u0000"` into an actual NUL, which is
         * the other case entirely.
         */
        private val JSON_WITH_ESCAPED_NUL = """{"a":"b\u0000c"}"""

        /**
         * The other thing a `jsonb` column will not take, and the one that can do worse than stall.
         * Jackson accepts a lone high-surrogate escape and re-emits it as the **raw** unpaired
         * character, which is not encodable as UTF-8 at all - so the driver either throws (the wedge
         * again) or substitutes, writing a mangled value into the user's column.
         */
        private val JSON_WITH_LONE_SURROGATE = """{"a":"\ud800"}"""

        /**
         * Four documents a `jsonb` column stores perfectly, each of which a `double` cannot carry:
         * more significant digits than 53 bits hold, an exponent above and one below `double`'s
         * range, and a trailing-zero exponent form. `jsonb` numbers are `numeric`, so PostgreSQL
         * keeps every one of them as written; a parse-and-re-render round trip keeps none of them,
         * and for the two out-of-range exponents it does not even keep the *type* - `1e400` renders
         * back as the string `"Infinity"` and `1.0e-400` as `0.0`.
         *
         * No single quotes in any of them: they are interpolated into a SQL literal below.
         */
        /**
         * A JSON number outside PostgreSQL's `numeric`, which is what a `jsonb` column stores a
         * number as: `1e131071` is the largest exponent it holds, and `'{"a":1e131072}'::jsonb`
         * answers `ERROR: value overflows numeric format`, server-side.
         */
        private val JSON_WITH_OVERFLOWING_NUMBER = """{"a":1e131072}"""

        /**
         * A reference whose text is longer than the unique btree index over `ed_record_ref.__ext_id`
         * accepts. 2692 bytes is the largest value that index is guaranteed to take, measured on
         * both `postgres:17.11` and the `postgres:12.6` these tests run against; a longer one
         * answers `ERROR: index row size ... exceeds btree version 4 maximum 2704`, server-side.
         */
        private val REF_TEXT_TOO_LONG_TO_INDEX = "abc@" + "d".repeat(2700)

        /**
         * How long the restore waits, once stopped between its check and its write, for the user's
         * mutation to get as far as the links table.
         *
         * Not a pause: the wait ends the moment that mutation arrives there, which it does in
         * milliseconds - the ordinary write path creates the links before it saves the record, and
         * nothing this restore holds is in its way until then. The bound only keeps a change that
         * stops it earlier from hanging the build.
         */
        private val USER_MUTATION_WINDOW: Duration = Duration.ofSeconds(10)

        private val JSON_NUMBER_DOCS = listOf(
            """{"price":12345678901234567.89}""",
            """{"a":1e400}""",
            """{"a":1.0e-400}""",
            """{"a":1e2}"""
        )
    }

    init {
        dataProps = DbEcosDataProps(
            columns = DbEcosDataProps.ColumnsProps(inPlaceAlterMaxRows = 0),
            batch = DbEcosDataProps.BatchProps(batchSize = 3, batchPause = Duration.ZERO)
        )
    }

    private fun drain() {
        val schemaCtx = getTableCtx().getSchemaCtx()
        DbBatchTaskEngine(schemaCtx.dataSourceCtx).drainSchemaOnce(schemaCtx)
    }

    private fun theTask() = TxnContext.doInTxn {
        getTableCtx().getSchemaCtx().batchTaskService.findByTable(tableRef.table).single()
    }

    /**
     * The model change alone does not migrate anything - the schema is reconciled by the next
     * mutation of the table. This record is that mutation, and it carries no value for the
     * attribute being migrated, so it is a row the transfer has nothing to do for.
     */
    private fun triggerTheTransition() {
        createRecord("someAtt" to null)
    }

    /**
     * `NUMBER -> TEXT` is safe, and every value has to be rewritten to perform it.
     */
    private fun changeAttTypeToText() {
        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("someAtt")
                    withType(AttributeType.TEXT)
                }
            )
        )
    }

    @Test
    fun aSafeConversionMovesEveryValueIntoTheNewColumnTest() {

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("someAtt")
                    withType(AttributeType.NUMBER)
                }
            )
        )
        val recs = (0 until 5).map { createRecord("someAtt" to it) }

        changeAttTypeToText()
        createRecord("someAtt" to "already-text")
        drain()

        assertThat(theTask().status).isEqualTo(DbBatchTaskStatus.DONE)
        recs.forEachIndexed { i, rec ->
            assertThat(records.getAtt(rec, "someAtt").asText())
                .describedAs("record $i must have been carried across, not left empty")
                // "0" rather than Double.toString's "0.0": a float8 column's own ::text renders it
                // that way, and this pair has a real in-place path to agree with - see
                // DbNumberToTextConversionTest
                .isEqualTo(i.toString())
        }
    }

    @Test
    fun aValueTheUserWroteDuringTheTransferIsNeverOverwrittenTest() {

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("someAtt")
                    withType(AttributeType.NUMBER)
                }
            )
        )
        val rec = createRecord("someAtt" to 7)

        changeAttTypeToText()
        // the user edits the record after the column moved aside but before the transfer runs
        updateRecord(rec, "someAtt" to "written-by-the-user")
        drain()

        assertThat(records.getAtt(rec, "someAtt").asText())
            .describedAs("the transfer writes only where the target column IS NULL")
            .isEqualTo("written-by-the-user")
        assertThat(theTask().skipped)
            .describedAs("and it says so, rather than reporting the row as processed")
            .isEqualTo(1)
        assertThat(theTask().processed)
            .describedAs("there was no row left for it to carry across")
            .isEqualTo(0)
    }

    /**
     * The association counterpart of
     * [aValueTheUserWroteDuringTheTransferIsNeverOverwrittenTest], which had no test of its own
     * and behaved the other way round: the guard that keeps the transfer off a cell the user has
     * filled sits on the value path only, and the restore of parked links ran unconditionally.
     *
     * A record whose association came back into an attribute the user has meanwhile given a value
     * of their own ended up holding both - the link they chose and the link the backup remembered
     * - with nothing to say where the second one came from.
     */
    @Test
    fun anAssociationTheUserWroteDuringTheRestoreIsNeverAddedToTest() {

        registerAtts(listOf(assocAtt()))
        val oldTarget = createRecord("someAtt" to null)
        val newTarget = createRecord("someAtt" to null)
        val rec = createRecord("someAtt" to listOf(oldTarget.toString()))

        // out of the association group: the links are parked under the backup's registry row
        changeAttTypeToText()
        triggerTheTransition()
        drain()

        // and back in, which queues the restore of those parked links
        registerAtts(listOf(assocAtt()))
        triggerTheTransition()
        // the user picks a different target before the background restore reaches this record
        updateRecord(rec, "someAtt" to listOf(newTarget.toString()))
        assertThat(liveLinksOf(rec))
            .describedAs("the user's choice is the attribute's only value - that is the premise")
            .containsExactly(refId(newTarget))

        drain()

        assertThat(liveLinksOf(rec))
            .describedAs("the parked link must not be laid on top of the value the user chose")
            .containsExactly(refId(newTarget))
    }

    /**
     * The same rule with the user's write where it actually lands in production: not before the
     * restore starts, but **inside** it - after it has asked whether the record has links of its
     * own and before it acts on the answer.
     *
     * [anAssociationTheUserWroteDuringTheRestoreIsNeverAddedToTest] writes before the drain, so the
     * check sees the user's link and the record is skipped. That asserts the check exists, and
     * nothing about whether it holds. A background restore and a user's mutation are two
     * transactions, and a check reads committed rows: what it found is only still true while
     * nothing commits in between. Nothing here made that so - no row of the record was held before
     * the check, and two links to different targets collide on no unique key - so both writes went
     * through and the record ended up holding the link the user chose *and* the one the backup
     * remembered, with nothing to say where the second came from.
     *
     * **What is asserted is that the record never holds both**, and not which of the two it keeps.
     * Which one it is says only who reached the record's row first, and both answers are right: the
     * restore putting the links back before the user's mutation is committed is the same thing as
     * the user not having written yet, and the user's mutation arriving first is the case the test
     * above covers. Only "both" is wrong, and it is what an unserialised check produces.
     *
     * The interleaving is forced rather than waited for: [DataMockFactory.beforeStorageWrite] stops
     * the restore on its own thread immediately before its links go into `ed_associations`, and the
     * user's mutation is started from there on a thread of its own - the only way to get a second
     * transaction here, since the platform transaction and the JDBC connection are both bound to
     * the thread and `TxnContext.doInNewTxn` moves only the first (a managed data source refuses
     * the connection it leaves in place: "Connection can not be used while enlisted in another
     * transaction").
     *
     * The restore then waits for that mutation to reach the links table itself, so that it really
     * is inside the window and not merely started. It gets there either way - the ordinary path
     * writes the links before it saves the record - and what differs is what happens next: with the
     * record's row held, the mutation's own save is refused and takes its link back with it. The
     * wait is bounded only so that a change which stops the mutation earlier fails this test instead
     * of hanging the build.
     */
    @Test
    fun aUserWritingInsideTheRestoreNeverLeavesTheRecordHoldingBothLinksTest() {
        val restore = aRestoreInterruptedByAUserWriting { _, newTarget -> newTarget }
        assertThat(restore.links)
            .describedAs(
                "the restore and the user wrote the same attribute of the same record, so one of " +
                    "them happened after the other: the record holds the link the user chose or the " +
                    "one the backup remembered, and never both"
            )
            .hasSize(1)
        assertThat(restore.links)
            .describedAs("the only two links this record could hold are the ones the test wrote")
            .isSubsetOf(restore.userTarget, restore.parkedTarget)
    }

    /**
     * The same interleaving with the user naming the reference the backup was holding anyway, which
     * is the ordinary case rather than an exotic one: a type changed by mistake and changed back,
     * with somebody re-entering the value they could no longer see.
     *
     * It is a different test because it is a different failure. The two writes here are the **same**
     * row of `ed_associations`, and that table has a unique index on
     * `(source, attribute, target)` - so instead of ending up with both links, the two transactions
     * contend for one index entry, and whether that resolves or deadlocks depends on the order each
     * of them takes the record's row and that entry in. The ordinary write path takes the entry
     * first and the record's row second; a restore that took them the other way round would close
     * the cycle, and PostgreSQL would break it by killing one of the two.
     *
     * So this asserts what an administrator and a user would each see: the record ends up with the
     * one link both of them wanted, and no deadlock was recorded against the task.
     *
     * **Not that the task recorded nothing at all.** Losing a race for one index entry costs the
     * batch a duplicate key and a retry, which is how two writers of the same link have always
     * ended - the same fixture books exactly that against `34818bb`, before any of this. The retry
     * then finds the link there and declines the snapshot the ordinary way. A deadlock is the
     * different thing: it is not the loser being told to try again, it is PostgreSQL choosing a
     * victim between two transactions that can no longer both proceed, and the victim can be the
     * user's.
     */
    @Test
    fun aUserWritingTheSameLinkInsideTheRestoreDeadlocksWithNothingTest() {
        val restore = aRestoreInterruptedByAUserWriting { parkedTarget, _ -> parkedTarget }
        assertThat(restore.links)
            .describedAs("both of them asked for this one link, so this one link is what the record holds")
            .containsExactly(restore.parkedTarget)
        assertThat(restore.taskErrors)
            .describedAs(
                "a restore that took the record's row and the link's index entry in the opposite " +
                    "order to the write path would close a cycle on them, and PostgreSQL would " +
                    "break it by killing one of the two"
            )
            .isEmpty()
    }

    /**
     * The other half of the transfer takes the same two things in the **opposite** order, and this
     * is what that costs.
     *
     * An arrival - text that becomes an association - claims the record's row first (the conditional
     * write of the converted value) and creates the links afterwards. The ordinary write path does
     * it the other way round: links first, record second. With both naming the same reference, which
     * is what a user re-entering a value they can no longer see does, the two hold one of the pair
     * each and wait for the other, and PostgreSQL breaks the cycle by killing one of them.
     *
     * The same shape as [aUserWritingTheSameLinkInsideTheRestoreDeadlocksWithNothingTest] and not
     * the same code: that one is the restore, which now takes them in the ordinary order. This one
     * fails on `55d466c` as released.
     */
    @Test
    fun aUserWritingTheSameLinkInsideAnArrivalDeadlocksWithNothingTest() {

        assumeConcurrentTransactionsSupported()

        registerAtts(listOf(textAtt()))
        val target = createRecord("someKey" to "target")
        val rec = createRecord("someAtt" to listOf(target.toString()))

        registerAtts(listOf(assocAtt()))
        triggerTheTransition()

        val outcome = withAUserWritingFromInsideTheMigration(rec, target)

        assertThat(outcome.links)
            .describedAs("both of them asked for this one link, so this one link is what the record holds")
            .containsExactly(refId(target))
        assertThat(outcome.taskErrors)
            .describedAs(
                "an arrival that claims the record's row before it writes the links takes them in " +
                    "the opposite order to every ordinary write, and the two close a cycle on the " +
                    "pair"
            )
            .isEmpty()
    }

    /**
     * The undo that a claim which did not hold forces, for the one thing it has to take back besides
     * the links: the `_parent` an **arrival** wrote on a record that was nobody's child before.
     *
     * A child association is two facts - a row of `ed_associations` and a back-reference on the
     * child - and the pass writes both of them before it claims the record. So when the claim is
     * then lost, a record is left naming a parent that holds no link to it, given a parent by a
     * transfer that did not happen.
     *
     * **Taking it back means releasing the record, never deleting it.**
     * `RecMutAssocHandler.updateParentRefOfChildren(add = false)` is not a release: it sends
     * `_parent = null` under `MUTATION_FROM_PARENT_FLAG`, which `DbRecordsDao.mutate` reads as "the
     * parent has let this child go" and answers by deleting the record - a record the user made,
     * deleted by a background task whose own write did not stick.
     *
     * A departure is deliberately not the shape here: it leaves `_parent` alone now, so a restore
     * finds the child already its own and writes no back-reference to take back. What is left is
     * the arrival, where the record really had no parent before.
     *
     * The user's mutation names a different reference, so it is the claim and not the unique index
     * that decides, which is what puts the undo on the path.
     */
    @Test
    fun aChildAdoptedByALostClaimIsReleasedRatherThanDeletedTest() {

        assumeConcurrentTransactionsSupported()

        registerAtts(listOf(textAtt()))
        val adopted = createRecord("someKey" to "child")
        val otherTarget = createRecord("someKey" to "other")
        val rec = createRecord("someAtt" to listOf(adopted.toString()))
        assertThat(records.getAtt(adopted, "_parent?id").asText())
            .describedAs("the premise: nobody's child while the attribute holds text")
            .isEmpty()

        // into the association group, which turns the text into links and adopts what it names
        registerAtts(listOf(childAssocAtt()))
        triggerTheTransition()

        val outcome = withAUserWritingFromInsideTheMigration(rec, otherTarget)

        assertThat(outcome.links)
            .describedAs("the user named another reference, so that is the record's only link")
            .containsExactly(refId(otherTarget))
        assertThat(records.getAtt(adopted, "_notExists?bool").asBoolean())
            .describedAs(
                "the undo releases the record it adopted; deleting it would be a background task " +
                    "destroying a user's record because its own claim did not hold"
            )
            .isFalse()
        assertThat(records.getAtt(adopted, "_parent?id").asText())
            .describedAs(
                "the arrival wrote this back-reference and then lost the record, so it takes it " +
                    "back: a record naming a parent that holds no link to it is a parentage " +
                    "nothing gave it"
            )
            .isEmpty()
        assertThat(outcome.taskErrors)
            .describedAs("losing a claim is an ordinary outcome, not a failure of the task")
            .isEmpty()
    }

    /**
     * A child association puts a **third** row in play - the child's - and moving the claim behind
     * the links says nothing about that one: it aligned the link/record pair and left the
     * child-row/link pair as it was.
     *
     * A user adding the same child takes the three in the order `link -> parent's row -> child's
     * row`, the last of them written by `processAssocsAfterMutation`. A restore that wrote the
     * child's `_parent` before the link - which is what it did, so that a child the write path
     * would refuse never became one - held one end each of the child-row/link pair against it, and
     * PostgreSQL broke the cycle by killing one of the two. Which of them it kills is not the
     * migration's to choose, and it can be the user's.
     *
     * So the deciding is a read now and only the writing waits: the link goes in first and the
     * `_parent` after it, leaving the link itself as the first thing both transactions want.
     *
     * Fails on `55d466c` as released, and on every commit of this branch before the reordering.
     */
    @Test
    fun aUserAddingTheSameChildInsideTheRestoreDeadlocksWithNothingTest() {

        assumeConcurrentTransactionsSupported()

        registerAtts(listOf(childAssocAtt()))
        val child = createRecord("someKey" to "child")
        val rec = createRecord("someAtt" to listOf(child.toString()))

        changeAttTypeToText()
        triggerTheTransition()
        drain()

        registerAtts(listOf(childAssocAtt()))
        triggerTheTransition()

        val outcome = withAUserWritingFromInsideTheMigration(rec, child)

        assertThat(outcome.links)
            .describedAs("both of them asked for this one child, so this one link is what the record holds")
            .containsExactly(refId(child))
        assertThat(records.getAtt(child, "_parent?id").asText())
            .describedAs("and whoever wrote it, the child has to know who its parent is")
            .isEqualTo(rec.toString())
        assertThat(outcome.taskErrors)
            .describedAs(
                "the child's row and the parent's link are taken in opposite orders by the two, " +
                    "and a cycle on them is what this asserts cannot happen"
            )
            .isEmpty()
    }

    /**
     * The same third row, reached from the other half of the transfer: text that becomes a **child**
     * association. An arrival writes the same three things a restore does - the parent's link, the
     * child's `_parent` and the parent's row - and if it wrote the child's row first it would hold
     * one end each of the child-row/link pair against a user adding the same child.
     *
     * Kept apart from [aUserAddingTheSameChildInsideTheRestoreDeadlocksWithNothingTest] because the
     * two are different code: `createArrivedAssocs` and `restoreAssocsOfWindow` order their writes
     * separately, and a fix to one says nothing about the other.
     */
    @Test
    fun aUserAddingTheSameChildInsideAnArrivalDeadlocksWithNothingTest() {

        assumeConcurrentTransactionsSupported()

        registerAtts(listOf(textAtt()))
        val child = createRecord("someKey" to "child")
        val rec = createRecord("someAtt" to listOf(child.toString()))

        registerAtts(listOf(childAssocAtt()))
        triggerTheTransition()

        val outcome = withAUserWritingFromInsideTheMigration(rec, child)

        assertThat(outcome.links)
            .describedAs("both of them asked for this one child, so this one link is what the record holds")
            .containsExactly(refId(child))
        assertThat(records.getAtt(child, "_parent?id").asText())
            .describedAs("and whoever wrote it, the child has to know who its parent is")
            .isEqualTo(rec.toString())
        assertThat(outcome.taskErrors)
            .describedAs(
                "an arrival writing a child's `_parent` before the parent's link would take the " +
                    "two in the opposite order to the user's mutation, and a cycle on them is what " +
                    "this asserts cannot happen"
            )
            .isEmpty()
    }

    /**
     * `_parent` and `_parentAtt` do not exist until something writes them, and a transfer into a
     * child association is the first thing that will. Where they are created decides whether a
     * concurrent mutation can deadlock with the transfer: an `ALTER TABLE` wants an
     * `AccessExclusiveLock` on the whole table, so whoever asks for one while the transfer holds
     * row locks waits for the batch - and the batch may then be waiting on a link row of theirs.
     *
     * The invariant that keeps that impossible is this one: by the time the transfer writes its
     * first row, the columns are already there. It holds for a task queued by an older build too,
     * because `prepare` runs before the first batch of every run, whoever queued it.
     */
    @Test
    fun theParentColumnsAreThereBeforeTheTransferWritesAnythingTest() {

        registerAtts(listOf(textAtt()))
        val child = createRecord("someKey" to "child")
        val rec = createRecord("someAtt" to listOf(child.toString()))

        assertThat(physicalColumnNames())
            .describedAs("the premise: nothing on this table has ever been a child")
            .doesNotContain("_parent", "_parentAtt")

        registerAtts(listOf(childAssocAtt()))
        triggerTheTransition()

        // The first write that takes a row lock anything else can want - the engine's own writes of
        // `ed_batch_task` are not that and are passed over.
        val columnsAtTheFirstWrite = AtomicReference<List<String>>()
        beforeStorageWrite = { table, _ ->
            if (table.table == tableRef.table || table.table == DbAssocEntity.MAIN_TABLE) {
                columnsAtTheFirstWrite.compareAndSet(null, physicalColumnNames())
            }
        }
        try {
            drain()
        } finally {
            beforeStorageWrite = null
        }

        assertThat(records.getAtt(rec, "someAtt[]?id").asStrList())
            .describedAs("the premise: the transfer did happen and made the child a link")
            .containsExactly(child.toString())
        assertThat(columnsAtTheFirstWrite.get())
            .describedAs(
                "the first row the transfer wrote found both columns in place, so no mutation of " +
                    "this table has to ask for the table lock an ALTER takes while the batch holds " +
                    "rows of it"
            )
            .contains("_parent", "_parentAtt")
    }

    /**
     * Read from the catalog rather than from `DbTableContext`, whose column list is cached per data
     * service: the columns this test is about are added by the migration's own data service, and
     * every other service of the process learns of them only when something makes it look again.
     */
    private fun physicalColumnNames(): List<String> {
        val dataSourceCtx = getTableCtx().getSchemaCtx().dataSourceCtx
        return dataSourceCtx.dataSource.withTransaction(readOnly = true) {
            dataSourceCtx.schemaDao.getColumns(dataSourceCtx.dataSource, tableRef).map { it.name }
        }
    }

    /**
     * The price of writing a child's `_parent` after its link rather than before: a refusal now
     * arrives when the link already exists, and the link has to go with it.
     *
     * Before the reordering the two could not disagree - a child that could not be told was never
     * linked. Now the deciding read and the write are two steps, and between them the child can
     * have become somebody else's, so the write is allowed to refuse what the read allowed. What
     * must not survive that is a link to a child that does not know it: the parent would hold a
     * reference to a record that answers nothing to a `_parent` predicate and stays undeletable.
     *
     * The refusal is injected at the storage seam, before the row reaches the database, so the
     * batch's transaction is untouched and the handler meets exactly the shape a records-layer
     * refusal has.
     */
    @Test
    fun aChildWhoseRowRefusesTheBackReferenceDoesNotStayALinkTest() {

        registerAtts(listOf(textAtt()))
        val child = createRecord("someKey" to "child")
        val rec = createRecord("someAtt" to listOf(child.toString()))

        registerAtts(listOf(childAssocAtt()))
        triggerTheTransition()

        beforeStorageWrite = { table, entities ->
            if (table.table == tableRef.table && entities.any { it["_parent"] != null }) {
                error("this child is not taking a parent today")
            }
        }
        try {
            drain()
        } finally {
            beforeStorageWrite = null
        }

        assertThat(liveLinksOf(rec))
            .describedAs(
                "the link was created before the back-reference was attempted, and the refusal " +
                    "has to take it out again - a child that does not know its parent must not be " +
                    "one of the parent's links"
            )
            .isEmpty()
        assertThat(records.getAtt(child, "_parent?id").asText())
            .describedAs("and nothing of the refused write survived either")
            .isEmpty()
        @Suppress("UNCHECKED_CAST")
        val backup = backupValuesByExtId("__backup_someAtt_text_multiple")[rec.getLocalId()] as? Array<String>
        assertThat(backup?.toList())
            .describedAs("the whole original is still where a refused row leaves it")
            .containsExactly(child.toString())
        assertThat(taskErrors())
            .describedAs("a reference that cannot become a link is an expected outcome, not a failed task")
            .isEmpty()
    }

    /**
     * The same refusal on the other path. A restore has bookkeeping an arrival does not - the
     * snapshot rows whose metadata is put back (`restoreAssocsMeta`) and the parked snapshot that
     * is spent afterwards - so the link of a refused child has to disappear from that bookkeeping
     * too, and before the metadata of a link that no longer exists is written.
     *
     * **The child is orphaned by hand**, because a departure no longer does it and the restore only
     * writes a back-reference for a child that has none. That state is not hypothetical: every
     * installation that ran a departure of `1.73.0` holds children exactly like this one, parked
     * links and a cleared `_parent`, and the return of the type is where they get their parent back.
     *
     * What the snapshot does here is **not** asserted as desirable: a refusal spends it, exactly as
     * a child refused on the read spent it before the reordering. That is the behaviour of
     * `55d466c`, unchanged, and it is the subject of an open question of its own.
     */
    @Test
    fun aChildRefusingARestoredBackReferenceDoesNotStayALinkEitherTest() {

        registerAtts(listOf(childAssocAtt()))
        val child = createRecord("someKey" to "child")
        val rec = createRecord("someAtt" to listOf(child.toString()))

        // out of the association group, which parks the link
        changeAttTypeToText()
        triggerTheTransition()
        drain()
        orphanTheChildAsAReleasedDepartureDid(child)

        // and back in, which queues the restore
        registerAtts(listOf(childAssocAtt()))
        triggerTheTransition()

        beforeStorageWrite = { table, entities ->
            if (table.table == tableRef.table && entities.any { it["_parent"] != null }) {
                error("this child is not taking its parent back")
            }
        }
        try {
            drain()
        } finally {
            beforeStorageWrite = null
        }

        assertThat(liveLinksOf(rec))
            .describedAs(
                "the restore put the link back before it tried the back-reference, and the refusal " +
                    "has to take it out again rather than leave a child that does not know its parent"
            )
            .isEmpty()
        assertThat(records.getAtt(child, "_parent?id").asText())
            .describedAs("and the back-reference is exactly what could not be written")
            .isEmpty()
        assertThat(taskErrors())
            .describedAs("a link that cannot come back is an expected outcome, not a failed task")
            .isEmpty()
    }

    /**
     * Every error booked on a task of this table. Not [theTask], because a scenario that changes the
     * type twice leaves two task rows and only one of them is the one under test.
     */
    private fun taskErrors(): List<String> = TxnContext.doInTxn {
        getTableCtx().getSchemaCtx().batchTaskService.findByTable(tableRef.table)
            .map { it.error }.filter { it.isNotEmpty() }
    }

    private fun childAssocAtt() = AttributeDef.create {
        withId("someAtt")
        withType(AttributeType.ASSOC)
        withMultiple(true)
        withConfig(ObjectData.create().set("child", true))
    }

    private fun textAtt() = AttributeDef.create {
        withId("someAtt")
        withType(AttributeType.TEXT)
        withMultiple(true)
    }

    /**
     * What a migration and a user writing the same record at the same moment left behind.
     */
    private class MigrationInterruptedByAUser(
        val links: List<Long>,
        val taskErrors: List<String>
    )

    /**
     * The same, for a restore, with the two references the test picked between.
     */
    private class InterruptedRestore(
        val links: List<Long>,
        val parkedTarget: Long,
        val userTarget: Long,
        val taskErrors: List<String>
    )

    /**
     * Drains the pending migration with the user's own mutation of [rec] committed from **inside**
     * it - after it has looked at what the record holds and before it writes.
     *
     * The interleaving is forced rather than waited for. [DataMockFactory.beforeStorageWrite] stops
     * the migration on its own thread immediately before its links go into `ed_associations` and
     * starts the user's mutation from there, on a thread of its own - the only way to get a second
     * transaction here, since the platform transaction and the JDBC connection are both bound to the
     * thread and `TxnContext.doInNewTxn` moves only the first (a managed data source refuses the
     * connection it leaves in place: "Connection can not be used while enlisted in another
     * transaction"). The migration then waits until that mutation reaches the **record's** table,
     * which the ordinary write path reaches only after writing the links - so by then the link the
     * migration has to reckon with is in `ed_associations`, uncommitted. The wait is bounded only so
     * that a change which stops the mutation earlier fails the test instead of hanging the build.
     */
    private fun withAUserWritingFromInsideTheMigration(
        rec: EntityRef,
        chosenByUser: EntityRef
    ): MigrationInterruptedByAUser {

        val userMutation = AtomicReference<Thread>()
        val userMutationFailure = AtomicReference<Throwable>()
        val userMutationReachedTheRecord = CountDownLatch(1)
        var migrationWasStopped = false

        beforeStorageWrite = { table, _ ->
            if (Thread.currentThread() === userMutation.get()) {
                if (table.table == tableRef.table) {
                    userMutationReachedTheRecord.countDown()
                }
            } else if (table.table == DbAssocEntity.MAIN_TABLE && !migrationWasStopped) {
                migrationWasStopped = true
                val thread = Thread({
                    try {
                        updateRecord(rec, "someAtt" to listOf(chosenByUser.toString()))
                    } catch (e: Throwable) {
                        userMutationFailure.set(e)
                    }
                }, "user-mutation")
                userMutation.set(thread)
                thread.start()
                userMutationReachedTheRecord.await(
                    USER_MUTATION_WINDOW.toMillis(),
                    TimeUnit.MILLISECONDS
                )
            }
        }
        try {
            drain()
        } finally {
            beforeStorageWrite = null
        }

        assertThat(migrationWasStopped)
            .describedAs(
                "the migration never reached the write these tests are about, so nothing was " +
                    "interleaved with it and what they assert would hold for the wrong reason"
            )
            .isTrue()
        val thread = userMutation.get()
        thread.join(Duration.ofMinutes(1).toMillis())
        assertThat(thread.isAlive)
            .describedAs("the user's mutation is still waiting for something the migration never released")
            .isFalse()
        // Either outcome is legitimate: it wrote its value, or it was refused because the migration
        // had rewritten the record's row under it - which is what the record's own optimistic lock
        // is for and what the caller retries. What it must not do is fail for any other reason, and
        // a deadlock is exactly such a reason.
        userMutationFailure.get()?.let { failure ->
            val messages = generateSequence(failure) { it.cause }.mapNotNull { it.message }.toList()
            assertThat(messages.any { it.contains("Concurrent modification") })
                .describedAs("the user's mutation failed for a reason these tests did not arrange: $messages")
                .isTrue()
        }
        return MigrationInterruptedByAUser(
            links = liveLinksOf(rec),
            taskErrors = TxnContext.doInTxn {
                getTableCtx().getSchemaCtx().batchTaskService.findByTable(tableRef.table)
                    .map { it.error }.filter { it.isNotEmpty() }
            }
        )
    }

    /**
     * [withAUserWritingFromInsideTheMigration] over a record whose association was parked by a
     * departure and is being restored. [userTarget] picks which reference the user names, given the
     * parked one and a second, unused one.
     */
    private fun aRestoreInterruptedByAUserWriting(
        userTarget: (parked: EntityRef, other: EntityRef) -> EntityRef
    ): InterruptedRestore {

        assumeConcurrentTransactionsSupported()

        registerAtts(listOf(assocAtt()))
        val parkedTarget = createRecord("someAtt" to null)
        val otherTarget = createRecord("someAtt" to null)
        val rec = createRecord("someAtt" to listOf(parkedTarget.toString()))
        val chosenByUser = userTarget.invoke(parkedTarget, otherTarget)

        // out of the association group: the links are parked under the backup's registry row
        changeAttTypeToText()
        triggerTheTransition()
        drain()

        // and back in, which queues the restore of those parked links
        registerAtts(listOf(assocAtt()))
        triggerTheTransition()

        val outcome = withAUserWritingFromInsideTheMigration(rec, chosenByUser)
        return InterruptedRestore(
            links = outcome.links,
            parkedTarget = refId(parkedTarget),
            userTarget = refId(chosenByUser),
            taskErrors = outcome.taskErrors
        )
    }

    /**
     * A task whose transition has been undone by a restore, started again from the admin view.
     *
     * `DbBatchTaskService.restart` accepts any task whose status is final, and a task cancelled
     * because the attribute changed type again is one of those. Running it means running its
     * departure from the association group - and after a restore the links that departure would
     * park are the live value of the column, so it deleted associations the record holds now.
     *
     * The guard is the backup's registry row: a restore is the only transition that turns a backup
     * row live again, so a task whose backup row is no longer a backup describes a transition that
     * no longer exists.
     */
    @Test
    fun aTaskUndoneByARestoreDoesNothingWhenItIsStartedAgainTest() {

        registerAtts(listOf(assocAtt()))
        val target = createRecord("someAtt" to null)
        val rec = createRecord("someAtt" to listOf(target.toString()))

        changeAttTypeToText()
        triggerTheTransition()
        // deliberately not drained: the task is still waiting when the type changes back, which is
        // what gets it cancelled rather than finished
        val superseded = theTask()

        registerAtts(listOf(assocAtt()))
        triggerTheTransition()
        assertThat(liveLinksOf(rec))
            .describedAs("the restore put the links back - that is the premise")
            .containsExactly(refId(target))

        val schemaCtx = getTableCtx().getSchemaCtx()
        assertThat(schemaCtx.batchTaskService.restart(superseded.id))
            .describedAs("the admin view offers restart on a cancelled task, so this has to be reachable")
            .isTrue()
        DbBatchTaskEngine(schemaCtx.dataSourceCtx).runTask(schemaCtx, superseded.id)

        assertThat(liveLinksOf(rec))
            .describedAs("and running it again must not take them away")
            .containsExactly(refId(target))
        // The run has to be stopped whole, not merely left without an estimate: `prepare`'s return
        // value only fills `total`, and the engine goes on to processBatch and onFinish however it
        // answers. The status is where that shows.
        assertThat(TxnContext.doInTxn { schemaCtx.batchTaskService.getById(superseded.id) }?.status)
            .describedAs("a task whose transition no longer exists must end, not run batch by batch")
            .isEqualTo(DbBatchTaskStatus.CANCELLED)
    }

    /**
     * The same defect without a restore anywhere in the sequence: the attribute leaves the
     * association group, changes type a second time into another assoc-like type, is given a new
     * link by the user, and the **first** task is started again from the admin view.
     *
     * The first fix keyed staleness on the source backup - has it been restored into the
     * attribute's place - and that answers nothing here: a second transition leaves the first
     * one's backup exactly where it was, so the task looked current and its departure took the
     * link the user had just made. What actually went stale is the column the task writes into,
     * and its registry row id is the only thing that says so.
     */
    @Test
    fun aTaskWhoseColumnWasReplacedAgainDoesNothingWhenItIsStartedAgainTest() {

        registerAtts(listOf(assocAtt()))
        val target = createRecord("someAtt" to null)
        val rec = createRecord("someAtt" to listOf(target.toString()))

        changeAttTypeToText()
        triggerTheTransition()
        val superseded = theTask()

        // a second change, into an assoc-like type again, and with no backup of that type to
        // restore - so nothing turns the first task's backup row live
        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("someAtt")
                    withType(AttributeType.PERSON)
                    withMultiple(true)
                }
            )
        )
        triggerTheTransition()
        val person = EntityRef.valueOf("emodel/person@someone")
        updateRecord(rec, "someAtt" to listOf(person.toString()))
        assertThat(liveLinksOf(rec))
            .describedAs("the user's link is the attribute's value now - that is the premise")
            .containsExactly(refId(person))

        val schemaCtx = getTableCtx().getSchemaCtx()
        assertThat(schemaCtx.batchTaskService.restart(superseded.id)).isTrue()
        DbBatchTaskEngine(schemaCtx.dataSourceCtx).runTask(schemaCtx, superseded.id)

        assertThat(liveLinksOf(rec))
            .describedAs("a task queued for a column that has been replaced must not touch the links")
            .containsExactly(refId(person))
    }

    /**
     * A backup of associations is a snapshot of one departure, and one registry row can depart more
     * than once: a restore makes it live again and the next change makes it a backup again. Adding
     * the new snapshot to what the key already held merged two generations, and the restore after
     * that handed the record both - including the link it had replaced a round trip earlier.
     *
     * Reached through the skip of
     * [anAssociationTheUserWroteDuringTheRestoreIsNeverAddedToTest]: that is what leaves an
     * unconsumed snapshot under the key for the next departure to meet.
     */
    @Test
    fun aSnapshotLeftByASkippedRestoreIsNotMergedIntoTheNextOneTest() {

        registerAtts(listOf(assocAtt()))
        val oldTarget = createRecord("someAtt" to null)
        val newTarget = createRecord("someAtt" to null)
        val rec = createRecord("someAtt" to listOf(oldTarget.toString()))

        changeAttTypeToText()
        triggerTheTransition()
        drain()
        registerAtts(listOf(assocAtt()))
        triggerTheTransition()
        // the restore is skipped and the old snapshot stays in the backup
        updateRecord(rec, "someAtt" to listOf(newTarget.toString()))
        drain()
        assertThat(liveLinksOf(rec)).containsExactly(refId(newTarget))

        // a second round trip of the same attribute, which departs under the same registry row
        changeAttTypeToText()
        triggerTheTransition()
        drain()
        registerAtts(listOf(assocAtt()))
        triggerTheTransition()
        drain()

        assertThat(liveLinksOf(rec))
            .describedAs("the target the user replaced a round trip ago must not come back with the current one")
            .containsExactly(refId(newTarget))
    }

    /**
     * The snapshot of a record whose links were **cleared** before the next departure.
     *
     * A departure only ever sees the records that have links to copy: it finds its sources by
     * walking `ed_associations`, and a record with nothing there is not among them. So replacing
     * the key's contents inside the copy reaches exactly the records that still have something -
     * and a record the user emptied keeps whatever an earlier, skipped restore left parked, for the
     * next restore to hand back.
     */
    @Test
    fun aSnapshotOfARecordWhoseLinksWereClearedIsNotBroughtBackTest() {

        registerAtts(listOf(assocAtt()))
        val oldTarget = createRecord("someAtt" to null)
        val newTarget = createRecord("someAtt" to null)
        val rec = createRecord("someAtt" to listOf(oldTarget.toString()))

        changeAttTypeToText()
        triggerTheTransition()
        drain()
        registerAtts(listOf(assocAtt()))
        triggerTheTransition()
        updateRecord(rec, "someAtt" to listOf(newTarget.toString()))
        drain()

        // the user empties the attribute, so the next departure has nothing of this record to copy
        updateRecord(rec, "someAtt" to emptyList<String>())
        assertThat(liveLinksOf(rec))
            .describedAs("the attribute is empty now - that is the premise")
            .isEmpty()

        changeAttTypeToText()
        triggerTheTransition()
        drain()
        registerAtts(listOf(assocAtt()))
        triggerTheTransition()
        drain()

        assertThat(liveLinksOf(rec))
            .describedAs("an attribute the user emptied must not be refilled from a snapshot of two round trips ago")
            .isEmpty()
    }

    /**
     * The registry row of a target column is reused, so its id is not a generation.
     *
     * `ASSOC -> TEXT` queues a task whose target is the text column's row. The attribute then
     * becomes `PERSON`, and then `TEXT` again by restoring that very same text row - which makes
     * the row live again under the same id. The task now looks current by any comparison of the
     * target, and running it copies what the attribute holds *now* into the backup that was written
     * for what it held *then*: the record's original associations are overwritten in the backup by
     * the ones the user has since chosen, and the way back is gone.
     */
    @Test
    fun aTaskWhoseTargetRowCameBackUnderTheSameIdIsStillStaleTest() {

        registerAtts(listOf(assocAtt()))
        val target = createRecord("someAtt" to null)
        val rec = createRecord("someAtt" to listOf(target.toString()))

        changeAttTypeToText()
        triggerTheTransition()
        val superseded = theTask()

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("someAtt")
                    withType(AttributeType.PERSON)
                    withMultiple(true)
                }
            )
        )
        triggerTheTransition()
        val person = EntityRef.valueOf("emodel/person@someone")
        updateRecord(rec, "someAtt" to listOf(person.toString()))

        // back to TEXT, which restores the very text row the superseded task was queued against
        changeAttTypeToText()
        triggerTheTransition()

        val schemaCtx = getTableCtx().getSchemaCtx()
        val parkedBefore = TxnContext.doInTxn {
            schemaCtx.assocBackupService.findByColumnMeta(
                DbColumnMigrationParams.from(superseded.params).backupColumnMetaId,
                refId(rec)
            ).map { it.targetId }
        }
        assertThat(parkedBefore)
            .describedAs("the original association is the one waiting in the backup - the premise")
            .containsExactly(refId(target))

        assertThat(schemaCtx.batchTaskService.restart(superseded.id)).isTrue()
        DbBatchTaskEngine(schemaCtx.dataSourceCtx).runTask(schemaCtx, superseded.id)

        val parkedAfter = TxnContext.doInTxn {
            schemaCtx.assocBackupService.findByColumnMeta(
                DbColumnMigrationParams.from(superseded.params).backupColumnMetaId,
                refId(rec)
            ).map { it.targetId }
        }
        assertThat(parkedAfter)
            .describedAs("a stale task must not overwrite the backup it was written for")
            .containsExactly(refId(target))
    }

    private fun assocAtt() = AttributeDef.create {
        withId("someAtt")
        withType(AttributeType.ASSOC)
        withMultiple(true)
    }

    private fun refId(ref: EntityRef): Long = dbRecordRefService.getIdByEntityRef(ref)

    /**
     * The target ids the record holds in `ed_associations` right now - the attribute's real value
     * while it is assoc-like, as opposed to the first ten the column caches.
     */
    private fun liveLinksOf(ref: EntityRef): List<Long> = TxnContext.doInTxn {
        DbDataServiceImpl(
            DbAssocEntity::class.java,
            DbDataServiceConfig.create { withTable(DbAssocEntity.MAIN_TABLE) },
            getTableCtx().getSchemaCtx()
        ).findAll(Predicates.eq(DbAssocEntity.SOURCE_ID, refId(ref))).map { it.targetId }
    }

    @Test
    fun aRowThatCannotBeConvertedDoesNotStopTheOthersTest() {

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("someAtt")
                    withType(AttributeType.TEXT)
                }
            )
        )
        createRecord("someAtt" to "12")
        createRecord("someAtt" to "not-a-number")
        createRecord("someAtt" to "34")

        // TEXT -> NUMBER is lossy, and the middle row is exactly the value that does not fit
        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("someAtt")
                    withType(AttributeType.NUMBER)
                }
            )
        )
        triggerTheTransition()
        drain()

        val task = theTask()
        assertThat(task.status)
            .describedAs("a broken row is counted, not a reason to fail the task")
            .isEqualTo(DbBatchTaskStatus.DONE)
        assertThat(task.processed)
            .describedAs(
                "the two that fit, and the row the transition created, whose empty cell is carried " +
                    "across as an empty cell"
            )
            .isEqualTo(3)
        assertThat(task.failed)
            .describedAs(
                "a cell the new type cannot hold is a normal outcome of changing the type, so " +
                    "nothing here is a failure of the task"
            )
            .isEqualTo(0)
        assertThat(task.skipped)
            .describedAs("only the middle row, left alone with its original in the backup")
            .isEqualTo(1)
    }

    @Test
    fun theOriginalValuesAreStillReadableFromTheBackupAfterALossyTransferTest() {

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("someAtt")
                    withType(AttributeType.TEXT)
                }
            )
        )
        val rec = createRecord("someAtt" to "not-a-number")
        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("someAtt")
                    withType(AttributeType.NUMBER)
                }
            )
        )
        triggerTheTransition()
        drain()

        assertThat(getTableCtx().getAllPhysicalColumns().map { it.name })
            .describedAs("the user's plan B: whatever the conversion could not do, the old value is still there")
            .contains("__backup_someAtt_text")
        assertThat(records.getAtt(rec, "someAtt").asText())
            .describedAs("and the new column was left empty rather than filled with something the value is not")
            .isEmpty()
        assertThat(theTask().skipped)
            .describedAs(
                "the row the conversion could not do is reported as left alone, not silently dropped"
            )
            .isEqualTo(1)
        assertThat(theTask().processed)
            .describedAs("and the row the transition created carried its empty cell across")
            .isEqualTo(1)
        assertThat(theTask().failed)
            .describedAs("neither is a failure: the original is exactly where the user left it")
            .isEqualTo(0)
    }

    /**
     * Multi-window accounting **within one run**: with a batch size of three, eight rows are walked
     * in several id windows and every row has to be counted exactly once across them. Resumption
     * *across* runs is the engine's own behaviour and is pinned by
     * [DbBatchTaskEngineTest.theCursorAdvancesWithTheBatchAndAResumeDoesNotReprocessTest]; a single
     * `drain()` here could never have shown it.
     */
    @Test
    fun everyRowIsCountedExactlyOnceAcrossTheWindowsOfOneRunTest() {

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("someAtt")
                    withType(AttributeType.NUMBER)
                }
            )
        )
        (0 until 7).forEach { createRecord("someAtt" to it) }
        changeAttTypeToText()
        triggerTheTransition()

        // a batch size of 3 forces several windows within this single run
        drain()

        val task = theTask()
        assertThat(task.status).isEqualTo(DbBatchTaskStatus.DONE)
        assertThat(task.processed + task.skipped + task.failed)
            .describedAs("every row is accounted for exactly once across all the windows")
            .isEqualTo(8)
        assertThat(task.processed)
            .describedAs(
                "the seven rows that had a value, and the row the transition created, whose empty " +
                    "cell is carried across as an empty cell - nothing had to be left behind"
            )
            .isEqualTo(8)
        assertThat(task.skipped)
            .describedAs("so nothing was left behind")
            .isEqualTo(0)
    }

    @Test
    fun aFinishedTransferIsNotRunAgainOnTheNextTickTest() {

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("someAtt")
                    withType(AttributeType.NUMBER)
                }
            )
        )
        createRecord("someAtt" to 1)
        changeAttTypeToText()
        triggerTheTransition()

        drain()
        val first = theTask()
        assertThat(first.processed)
            .describedAs(
                "fixture: the first drain really did carry the one row across, and the empty cell " +
                    "of the row the transition created with it"
            )
            .isEqualTo(2)
        // the drain runs again on the next tick; a finished task must not be restarted, and
        // nothing it already counted may be counted a second time
        drain()
        val second = theTask()

        assertThat(second.status).isEqualTo(DbBatchTaskStatus.DONE)
        assertThat(second.processed).isEqualTo(first.processed)
        assertThat(second.skipped).isEqualTo(first.skipped)
        assertThat(second.failed).isEqualTo(first.failed)
    }

    @Test
    fun theAdministratorSeesHowMuchWorkThereIsTest() {

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("someAtt")
                    withType(AttributeType.NUMBER)
                }
            )
        )
        (0 until 4).forEach { createRecord("someAtt" to it) }
        changeAttTypeToText()
        triggerTheTransition()
        drain()

        assertThat(theTask().total)
            .describedAs("the admin view shows processed/total, so prepare has to produce an estimate")
            .isGreaterThan(0)
    }

    /**
     * The same again, for a value the *target column* cannot hold rather than one the conversion
     * cannot express.
     *
     * `BINARY -> TEXT` is always lossy, so it always lands here. A `bytea` carrying a
     * `0x00` renders to a Java string with a `U+0000` in it, and PostgreSQL refuses to bind that to
     * a `text` parameter. Rejected at conversion time it is one `skipped` row; carried as far as the
     * write it throws out of `processBatch`, rolls the cursor back, and - `batch.maxAttempts`
     * defaults to 0, meaning unlimited retries - the same window is retried for ever behind the
     * table's lock, starving every other batch task queued against that table.
     */
    @Test
    fun aValueTheTargetColumnCannotHoldIsOneSkippedRowNotAWedgedTaskTest() {

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("someAtt")
                    withType(AttributeType.BINARY)
                }
            )
        )
        val readable = createRecord("someAtt" to "readable".toByteArray(Charsets.UTF_8))
        val unstorable = createRecord("someAtt" to BYTES_WITH_NUL)
        val alsoReadable = createRecord("someAtt" to "also-readable".toByteArray(Charsets.UTF_8))

        changeAttTypeToText()
        triggerTheTransition()
        drain()

        val task = theTask()
        assertThat(task.status)
            .describedAs(
                "a value the column cannot store must end the task, not wedge it: a batch that " +
                    "throws is retried for ever by default, behind this table's lock"
            )
            .isEqualTo(DbBatchTaskStatus.DONE)
        assertThat(task.processed)
            .describedAs("the rows that could be carried across were, and so was the transition's empty one")
            .isEqualTo(3)
        assertThat(task.skipped)
            .describedAs("and the one that could not is left alone, not failed")
            .isEqualTo(1)
        assertThat(task.failed)
            .describedAs("a value the column cannot store is a normal outcome, not a failed batch")
            .isEqualTo(0)

        assertThat(records.getAtt(readable, "someAtt").asText()).isEqualTo("readable")
        assertThat(records.getAtt(alsoReadable, "someAtt").asText()).isEqualTo("also-readable")
        assertThat(records.getAtt(unstorable, "someAtt").asText())
            .describedAs("nothing half-converted was written for the row that stayed behind")
            .isEmpty()
        assertThat(backupValuesByExtId("__backup_someAtt_binary")[unstorable.getLocalId()] as ByteArray)
            .describedAs("the user's plan B: the bytes the text column cannot hold are still exactly where they were")
            .isEqualTo(BYTES_WITH_NUL)
    }

    /**
     * The same wedge as [aValueTheTargetColumnCannotHoldIsOneSkippedRowNotAWedgedTaskTest], reached
     * through the other target family - which is why the guard is one rule both of them use rather
     * than a check on the text branch.
     *
     * `TEXT -> JSON` is always lossy, so it always lands here. The source column holds a
     * perfectly ordinary ASCII string; it is only once parsed and handed to a `jsonb` cast that
     * PostgreSQL refuses it, server-side, aborting the transaction.
     */
    @Test
    fun aJsonDocumentTheTargetColumnCannotHoldIsOneSkippedRowTooTest() {

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("someAtt")
                    withType(AttributeType.TEXT)
                }
            )
        )
        val storable = createRecord("someAtt" to """{"a":"plain"}""")
        val unstorable = createRecord("someAtt" to JSON_WITH_ESCAPED_NUL)
        val alsoStorable = createRecord("someAtt" to """{"a":"also-plain"}""")

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("someAtt")
                    withType(AttributeType.JSON)
                }
            )
        )
        triggerTheTransition()
        drain()

        val task = theTask()
        assertThat(task.status)
            .describedAs(
                "a document the jsonb column cannot store must end the task, not wedge it: the " +
                    "server rejects it, which aborts the transaction and fails the whole batch"
            )
            .isEqualTo(DbBatchTaskStatus.DONE)
        assertThat(task.processed)
            .describedAs("the documents that could be carried across were, and so was the transition's empty row")
            .isEqualTo(3)
        assertThat(task.skipped)
            .describedAs("and the one that could not is left alone, not failed")
            .isEqualTo(1)
        assertThat(task.failed)
            .describedAs("a document the column cannot store is a normal outcome, not a failed batch")
            .isEqualTo(0)

        assertThat(records.getAtt(storable, "someAtt?json").get("a").asText()).isEqualTo("plain")
        assertThat(records.getAtt(alsoStorable, "someAtt?json").get("a").asText()).isEqualTo("also-plain")
        assertThat(records.getAtt(unstorable, "someAtt").asText())
            .describedAs("nothing half-converted was written for the row that stayed behind")
            .isEmpty()
        assertThat(backupValuesByExtId("__backup_someAtt_text")[unstorable.getLocalId()])
            .describedAs("the user's plan B: the text the jsonb column cannot hold is still exactly where it was")
            .isEqualTo(JSON_WITH_ESCAPED_NUL)
    }

    /**
     * C3: the third member of the unstorable-value class, and the only one that could have written a
     * wrong value rather than stalling.
     */
    @Test
    fun aJsonDocumentWithALoneSurrogateIsOneSkippedRowTooTest() {

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("someAtt")
                    withType(AttributeType.TEXT)
                }
            )
        )
        val storable = createRecord("someAtt" to """{"a":"plain"}""")
        val unstorable = createRecord("someAtt" to JSON_WITH_LONE_SURROGATE)
        val alsoStorable = createRecord("someAtt" to """{"a":"also-plain"}""")

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("someAtt")
                    withType(AttributeType.JSON)
                }
            )
        )
        triggerTheTransition()
        drain()

        val task = theTask()
        assertThat(task.status).isEqualTo(DbBatchTaskStatus.DONE)
        assertThat(task.processed).isEqualTo(3)
        assertThat(task.skipped)
            .describedAs("a value whose storability depends on how the driver handles an unencodable character is left behind, not written")
            .isEqualTo(1)
        assertThat(task.failed)
            .describedAs("refusing to write it is the expected outcome, not a failure")
            .isEqualTo(0)

        assertThat(records.getAtt(storable, "someAtt?json").get("a").asText()).isEqualTo("plain")
        assertThat(records.getAtt(alsoStorable, "someAtt?json").get("a").asText()).isEqualTo("also-plain")
        assertThat(records.getAtt(unstorable, "someAtt").asText())
            .describedAs("above all: nothing mangled was written into the user's column")
            .isEmpty()
        assertThat(backupValuesByExtId("__backup_someAtt_text")[unstorable.getLocalId()])
            .isEqualTo(JSON_WITH_LONE_SURROGATE)
    }

    /**
     * C4: the same class of defect as the lone surrogate, in the target the enumeration claimed to
     * have finished - and the only one so far that reported **success** while writing a different
     * document than the user had.
     *
     * `TEXT -> JSON` is always lossy, so it always comes through this handler. Every
     * document here is one a `jsonb` column takes without complaint, so nothing fails and the task
     * reports DONE either way: the only thing that can catch it is asserting on what the column
     * **holds**, against what the in-place `::jsonb` cast - the other path for the same pair
     * agree value for value" - would have put there. The two are computed in the same query, by the
     * same server, so this cannot drift with a driver or a locale.
     *
     * PostgreSQL only: `jsonb` numbers are `numeric`, so the stored representation is the one that
     * shows the difference. The in-memory backend keeps whatever string it was handed.
     */
    @Test
    fun aJsonNumberIsStoredAsTheInPlaceCastWouldStoreItTest() {

        assumeRawSqlSupported()

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("someAtt")
                    withType(AttributeType.TEXT)
                }
            )
        )
        val recordsByDoc = JSON_NUMBER_DOCS.associateWith { createRecord("someAtt" to it) }

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("someAtt")
                    withType(AttributeType.JSON)
                }
            )
        )
        triggerTheTransition()
        drain()

        val task = theTask()
        assertThat(task.status).isEqualTo(DbBatchTaskStatus.DONE)
        assertThat(task.skipped)
            .describedAs("every one of these documents is one a jsonb column accepts, so nothing was left behind")
            .isEqualTo(0)
        assertThat(task.processed)
            .describedAs("the documents, and the empty cell of the row the transition created")
            .isEqualTo(JSON_NUMBER_DOCS.size + 1L)

        // compared as whole maps rather than one document at a time, so a failure names every
        // document that differs instead of stopping at the first
        val whatTheColumnHolds = recordsByDoc.mapValues { selectRecFromDb(it.value, "\"someAtt\"::text") }
        val whatTheInPlaceCastWouldHold = recordsByDoc.mapValues {
            selectRecFromDb(it.value, "('${it.key}'::jsonb)::text")
        }
        assertThat(whatTheColumnHolds)
            .describedAs(
                "the row-by-row path and the in-place cast have to agree value for value, so " +
                    "the column must hold exactly what the same document cast to jsonb " +
                    "holds - re-rendering the document through a parser of its own is a second " +
                    "opinion about the user's data, and this one is silent: jsonb takes every one " +
                    "of these, so the row is counted processed either way"
            )
            .isEqualTo(whatTheInPlaceCastWouldHold)
    }

    /**
     * The other half of C4, and the reason writing the source text needs a guard the re-rendering
     * did not: a `jsonb` column stores JSON numbers as `numeric`, which overflows above 131072
     * digits before the point or 16383 after. Re-rendering used to hide such a number by turning it
     * into the string `"Infinity"` - accepted, and wrong. Passed through as written it is refused by
     * the server, which aborts the transaction and wedges the batch, so it has to be refused here.
     *
     * A large number the column *does* hold is in the same batch, because a guard that refused
     * large numbers in general would be just as wrong as one that mangled them.
     */
    @Test
    fun aJsonNumberTheColumnCannotStoreIsOneSkippedRowTest() {

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("someAtt")
                    withType(AttributeType.TEXT)
                }
            )
        )
        // 1e300 rather than the boundary 1e131071: numeric holds the boundary quite happily, but
        // reading it back renders 131072 digits, and Jackson refuses a number literal over 1000
        // characters (StreamReadConstraints), so nothing in the platform could read the row
        val storable = createRecord("someAtt" to """{"a":1e300}""")
        val unstorable = createRecord("someAtt" to JSON_WITH_OVERFLOWING_NUMBER)

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("someAtt")
                    withType(AttributeType.JSON)
                }
            )
        )
        triggerTheTransition()
        drain()

        val task = theTask()
        assertThat(task.status)
            .describedAs("a number the column overflows on must end the task, not wedge it")
            .isEqualTo(DbBatchTaskStatus.DONE)
        assertThat(task.processed)
            .describedAs(
                "a large number numeric holds is carried across, not refused with it - and so is " +
                    "the empty cell of the row the transition created"
            )
            .isEqualTo(2)
        assertThat(task.skipped)
            .describedAs("the one the column overflows on is left behind with its original")
            .isEqualTo(1)
        assertThat(task.failed)
            .describedAs("and that is not a failure of the task")
            .isEqualTo(0)

        assertThat(records.getAtt(storable, "someAtt?json").get("a").isNumber())
            .describedAs(
                "the large number is in the column and still reads back as a JSON number - the " +
                    "exact digits differ between the backends only because PostgreSQL renders a " +
                    "numeric in full and the in-memory backend keeps the text it was given"
            )
            .isTrue()
        assertThat(records.getAtt(unstorable, "someAtt").asText())
            .describedAs("nothing was written for the row that could not be stored")
            .isEmpty()
        assertThat(backupValuesByExtId("__backup_someAtt_text")[unstorable.getLocalId()])
            .describedAs("the user's plan B: the document is still exactly where it was")
            .isEqualTo(JSON_WITH_OVERFLOWING_NUMBER)
    }

    /**
     * I1: the reference family's "the column refuses nothing" was true about characters and silent
     * about length.
     *
     * `TEXT -> ASSOC` is always lossy, so it always comes through this handler. The stored
     * value really is a `bigint` that refuses nothing, but registering the reference writes its text
     * into `ed_record_ref.__ext_id`, which carries a unique btree index, and an index row cannot
     * exceed a third of a page.
     *
     * This never wedged: that insert runs in a transaction of its own, so the throw arrives as one
     * skipped row anyway. What it cost is agreement between the two paths - on PostgreSQL the row stayed
     * behind, in memory the same input was stored - which is why the bound lives in the shared
     * converter and why this test runs on **both** backends and expects the same counters from each.
     */
    @Test
    fun aReferenceTooLongToIndexIsTheSameSkippedRowOnEveryBackendTest() {

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("someAtt")
                    withType(AttributeType.TEXT)
                }
            )
        )
        val storable = createRecord("someAtt" to "abc@def")
        val unstorable = createRecord("someAtt" to REF_TEXT_TOO_LONG_TO_INDEX)

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("someAtt")
                    withType(AttributeType.ASSOC)
                }
            )
        )
        triggerTheTransition()
        drain()

        val task = theTask()
        assertThat(task.status).isEqualTo(DbBatchTaskStatus.DONE)
        assertThat(task.processed)
            .describedAs(
                "the reference that fits was registered and carried across, and so was the empty " +
                    "cell of the row the transition created"
            )
            .isEqualTo(2)
        assertThat(task.skipped)
            .describedAs(
                "and the one that does not fit is left behind on every backend, not a value that " +
                    "stores in memory and is refused on PostgreSQL"
            )
            .isEqualTo(1)
        assertThat(task.failed)
            .describedAs("a reference the index cannot carry is a normal outcome, not a failure")
            .isEqualTo(0)

        assertThat(records.getAtt(storable, "someAtt?id").asText()).endsWith("abc@def")
        assertThat(records.getAtt(unstorable, "someAtt").asText())
            .describedAs("nothing was written for the row that could not be registered")
            .isEmpty()
        assertThat(backupValuesByExtId("__backup_someAtt_text")[unstorable.getLocalId()])
            .describedAs("the user's plan B: the text is still exactly where it was")
            .isEqualTo(REF_TEXT_TOO_LONG_TO_INDEX)
    }

    /**
     * C2: `TEXT -> DATETIME` is always lossy, so it always lands here. `Instant.parse`
     * accepts the ISO-8601 expanded-year form and nothing in Java overflows - the value is only
     * refused by the database, server-side, which is the wedge again.
     */
    @Test
    fun aTimestampOutsideTheColumnsRangeIsOneSkippedRowTest() {

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("someAtt")
                    withType(AttributeType.TEXT)
                }
            )
        )
        val storable = createRecord("someAtt" to "2020-01-01T00:00:00Z")
        val unstorable = createRecord("someAtt" to "+1000000-01-01T00:00:00Z")
        val alsoStorable = createRecord("someAtt" to "2021-06-15T12:00:00Z")

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("someAtt")
                    withType(AttributeType.DATETIME)
                }
            )
        )
        triggerTheTransition()
        drain()

        val task = theTask()
        assertThat(task.status)
            .describedAs("a timestamp out of range must end the task, not wedge it")
            .isEqualTo(DbBatchTaskStatus.DONE)
        assertThat(task.processed)
            .describedAs("the two that store, and the empty cell of the transition's own row")
            .isEqualTo(3)
        assertThat(task.skipped)
            .describedAs("the year no column can store stays behind")
            .isEqualTo(1)
        assertThat(task.failed).isEqualTo(0)

        assertThat(records.getAtt(storable, "someAtt").asText()).isNotEmpty()
        assertThat(records.getAtt(alsoStorable, "someAtt").asText()).isNotEmpty()
        assertThat(records.getAtt(unstorable, "someAtt").asText()).isEmpty()
        assertThat(backupValuesByExtId("__backup_someAtt_text")[unstorable.getLocalId()])
            .describedAs("and the year nobody can store is still exactly where the user left it")
            .isEqualTo("+1000000-01-01T00:00:00Z")
    }

    /**
     * C2 again, for `date`, whose range ends much later than `timestamptz`'s - 5874897 AD rather
     * than 294276 AD - which is why a value that overflows one is not enough to test the other.
     */
    @Test
    fun aDateOutsideTheColumnsRangeIsOneSkippedRowTest() {

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("someAtt")
                    withType(AttributeType.TEXT)
                }
            )
        )
        val storable = createRecord("someAtt" to "2020-01-01T00:00:00Z")
        val unstorable = createRecord("someAtt" to "+6000000-01-01T00:00:00Z")
        val alsoStorable = createRecord("someAtt" to "2021-06-15T00:00:00Z")

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("someAtt")
                    withType(AttributeType.DATE)
                }
            )
        )
        triggerTheTransition()
        drain()

        val task = theTask()
        assertThat(task.status)
            .describedAs("a date out of range must end the task, not wedge it")
            .isEqualTo(DbBatchTaskStatus.DONE)
        assertThat(task.processed)
            .describedAs("the two that store, and the empty cell of the transition's own row")
            .isEqualTo(3)
        assertThat(task.skipped)
            .describedAs("the year no column can store stays behind")
            .isEqualTo(1)
        assertThat(task.failed).isEqualTo(0)

        assertThat(records.getAtt(storable, "someAtt").asText()).isNotEmpty()
        assertThat(records.getAtt(alsoStorable, "someAtt").asText()).isNotEmpty()
        assertThat(records.getAtt(unstorable, "someAtt").asText()).isEmpty()
        assertThat(backupValuesByExtId("__backup_someAtt_text")[unstorable.getLocalId()])
            .isEqualTo("+6000000-01-01T00:00:00Z")
    }

    /**
     * The evidence behind the converter table's "NUMBER: refuses nothing" row, which is the kind of
     * claim that has been wrong here before when it was only reasoned about.
     *
     * `toDoubleOrNull` parses "NaN", "Infinity" and "-Infinity" into the corresponding doubles, and
     * those are the only unusual values it can produce. `float8` accepts all three - asserted here
     * end to end against the real column rather than assumed.
     */
    @Test
    fun theNumbersFloat8AcceptsAreCarriedAcrossRatherThanRefusedTest() {

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("someAtt")
                    withType(AttributeType.TEXT)
                }
            )
        )
        createRecord("someAtt" to "NaN")
        createRecord("someAtt" to "Infinity")
        createRecord("someAtt" to "-Infinity")
        val ordinary = createRecord("someAtt" to "12.5")

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("someAtt")
                    withType(AttributeType.NUMBER)
                }
            )
        )
        triggerTheTransition()
        drain()

        val task = theTask()
        assertThat(task.status).isEqualTo(DbBatchTaskStatus.DONE)
        assertThat(task.processed)
            .describedAs(
                "all four store - a float8 column takes NaN and both infinities - and the " +
                    "transition's own empty row is carried across with them"
            )
            .isEqualTo(5)
        assertThat(task.skipped)
            .describedAs("none of them is a value the column refuses, so nothing was left behind")
            .isEqualTo(0)
        assertThat(records.getAtt(ordinary, "someAtt").asDouble()).isEqualTo(12.5)
    }

    /**
     * I3: `ASSOC -> TEXT` is lossy, so no in-place path contradicts whatever this writes -
     * but a reference column stores an `ed_record_ref` id, and writing that id out as text hands the
     * user an internal surrogate key (`"1742"`) in place of their data (`emodel@doc$x`). Getting
     * their data back is the point of the transfer, so the id is resolved to the reference it stands
     * for.
     */
    @Test
    fun anAssociationCarriedIntoTextArrivesAsTheReferenceNotItsInternalIdTest() {

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("someAtt")
                    withType(AttributeType.ASSOC)
                }
            )
        )
        val rec = createRecord("someAtt" to "emodel/doc@the-target")

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("someAtt")
                    withType(AttributeType.TEXT)
                }
            )
        )
        triggerTheTransition()
        drain()

        val task = theTask()
        assertThat(task.status).isEqualTo(DbBatchTaskStatus.DONE)
        assertThat(task.processed)
            .describedAs("the association, and the empty cell of the row the transition created")
            .isEqualTo(2)
        assertThat(task.skipped)
            .describedAs("nothing was left behind")
            .isEqualTo(0)

        assertThat(records.getAtt(rec, "someAtt").asText())
            .describedAs(
                "the user's association, not the row number ed_record_ref happens to keep it under"
            )
            .isEqualTo("emodel/doc@the-target")
    }

    /**
     * C5: the same substitution as the JSON round trip, in the NUMBER branch.
     *
     * `TEXT -> NUMBER` is always lossy, so it always lands here. `Double.parseDouble`
     * answers `Infinity` for `1e400` and `0.0` for `1e-400`, and a `float8` column takes both - so
     * before the guard the task reported every row carried across while the user's number was replaced
     * by one PostgreSQL's own input function refuses: `'1e400'::float8` and `'1e-400'::float8` both
     * answer `ERROR: "..." is out of range for type double precision`.
     *
     * The values that survive are in the same batch on purpose: `NaN`, both infinities **as the user
     * spelled them**, and a subnormal, all of which `float8` really does hold.
     */
    @Test
    fun aNumberOutsideFloat8sRangeIsOneSkippedRowRatherThanASubstituteTest() {

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("someAtt")
                    withType(AttributeType.TEXT)
                }
            )
        )
        val overflow = createRecord("someAtt" to "1e400")
        val negativeOverflow = createRecord("someAtt" to "-1e400")
        val underflow = createRecord("someAtt" to "1e-400")
        createRecord("someAtt" to "Infinity")
        val subnormal = createRecord("someAtt" to "4.9e-324")
        val ordinary = createRecord("someAtt" to "12.5")

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("someAtt")
                    withType(AttributeType.NUMBER)
                }
            )
        )
        triggerTheTransition()
        drain()

        val task = theTask()
        assertThat(task.status).isEqualTo(DbBatchTaskStatus.DONE)
        assertThat(task.skipped)
            .describedAs(
                "a literal the column's own input function refuses must be left behind, not an " +
                    "Infinity or a zero written in place of the user's number"
            )
            .isEqualTo(3)
        assertThat(task.failed)
            .describedAs("and none of the three is a failure of the task")
            .isEqualTo(0)
        assertThat(task.processed)
            .describedAs(
                "and the three float8 really holds are carried across, with the transition's own " +
                    "empty row"
            )
            .isEqualTo(4)

        listOf(overflow, negativeOverflow, underflow).forEach {
            assertThat(records.getAtt(it, "someAtt").asText())
                .describedAs("nothing was substituted for the value the column cannot hold")
                .isEmpty()
        }
        assertThat(backupValuesByExtId("__backup_someAtt_text")[underflow.getLocalId()])
            .describedAs("the user's plan B: the number is still exactly where it was")
            .isEqualTo("1e-400")

        // the infinity the user spelled is counted among the processed rows above rather than read
        // back here: an infinity has no JSON representation, so the attribute read path cannot
        // return one whatever the column holds
        assertThat(records.getAtt(subnormal, "someAtt").asDouble()).isEqualTo(Double.MIN_VALUE)
        assertThat(records.getAtt(ordinary, "someAtt").asDouble()).isEqualTo(12.5)
    }

    /**
     * The other half of `DbShadowColumnTransitionTest.theTransitionBuildsNoIndexOnTheColumnItAddsTest`.
     * The transition deliberately adds the replacement column without the model's index, because
     * building a btree inside a user's mutation is the very thing it exists to avoid. Nothing else
     * will ever build it - after the transition the column matches the model, so it never appears
     * among the missing columns again - so an indexed attribute that changes type would lose its
     * index for good if this handler did not build it here.
     */
    @Test
    fun theIndexTheTransitionDeferredIsBuiltOnceTheColumnIsFullTest() {

        val indexedAtt = { type: AttributeType ->
            AttributeDef.create {
                withId("someAtt")
                withType(type)
                withIndex(AttIndexDef.create().withEnabled(true).build())
            }
        }
        registerAtts(listOf(indexedAtt(AttributeType.NUMBER)))
        createRecord("someAtt" to 1)
        registerAtts(listOf(indexedAtt(AttributeType.TEXT)))
        triggerTheTransition()

        val dataSource = getTableCtx().getSchemaCtx().dataSourceCtx.dataSource
        val whileTransferring = dataSource.watchSchemaCommands { drain() }

        assertThat(theTask().status).isEqualTo(DbBatchTaskStatus.DONE)
        assertThat(whileTransferring.filter { it.contains("index", ignoreCase = true) })
            .describedAs("the index the model asks for is built once the column holds the values")
            .anyMatch { it.contains("someAtt") }

        val again = dataSource.watchSchemaCommands { reEnterOnFinish() }
        assertThat(again.filter { it.contains("index", ignoreCase = true) })
            .describedAs(
                "onFinish is re-entered whenever publishing DONE loses its compare-and-set, so a " +
                    "second index over the same column must not be built"
            )
            .isEmpty()
    }

    /**
     * The same deferral, on the branch that used to queue nothing at all.
     *
     * `NUMBER -> DATE` is class NONE - there is no way to make a calendar date out of a double, so
     * the transition moves the column aside and the new one stays empty. It still strips the model's
     * index off that new column - unconditionally, before it ever asks whether anything is
     * transferable - and with no task queued there was no `onFinish` to put the index back. An
     * indexed attribute lost its index permanently on the cheapest-looking change of them all.
     *
     * The target has to be a type the backend indexes at all: `DbSchemaDaoPg.INDEXED_COLUMN_TYPES`
     * leaves `bytea` out, so a BINARY target would have asserted something PostgreSQL never does.
     */
    @Test
    fun theDeferredIndexIsBuiltEvenWhenThereIsNothingToTransferTest() {

        val indexedAtt = { type: AttributeType ->
            AttributeDef.create {
                withId("someAtt")
                withType(type)
                withIndex(AttIndexDef.create().withEnabled(true).build())
            }
        }
        registerAtts(listOf(indexedAtt(AttributeType.NUMBER)))
        createRecord("someAtt" to 1)
        registerAtts(listOf(indexedAtt(AttributeType.DATE)))
        triggerTheTransition()

        val dataSource = getTableCtx().getSchemaCtx().dataSourceCtx.dataSource
        val whileDraining = dataSource.watchSchemaCommands { drain() }

        val queued = TxnContext.doInTxn {
            getTableCtx().getSchemaCtx().batchTaskService.findByTable(tableRef.table)
        }
        assertThat(queued)
            .describedAs("a class-NONE transition queues a task too - the index is why")
            .hasSize(1)
        assertThat(theTask().status).isEqualTo(DbBatchTaskStatus.DONE)
        assertThat(theTask().processed)
            .describedAs("and it carries no value across: there is nothing transferable")
            .isEqualTo(0)
        assertThat(whileDraining.filter { it.contains("index", ignoreCase = true) })
            .describedAs("the index the model asks for must survive a type change with no transfer")
            .anyMatch { it.contains("someAtt") }
    }

    @Test
    fun noIndexIsBuiltForAnAttributeThatDoesNotAskForOneTest() {

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("someAtt")
                    withType(AttributeType.NUMBER)
                }
            )
        )
        createRecord("someAtt" to 1)
        changeAttTypeToText()
        triggerTheTransition()

        val dataSource = getTableCtx().getSchemaCtx().dataSourceCtx.dataSource
        val whileTransferring = dataSource.watchSchemaCommands { drain() }

        assertThat(theTask().status).isEqualTo(DbBatchTaskStatus.DONE)
        assertThat(whileTransferring.filter { it.contains("index", ignoreCase = true) })
            .describedAs("the index is rebuilt only where the model asks for one")
            .noneMatch { it.contains("someAtt") }
    }

    /**
     * Every row's value in [backupColumn], keyed by ext id, read through the one service allowed to
     * see a backup column - [DbDataServiceConfig.includeBackupColumns]. Raw SQL would have been
     * shorter but `selectRecFromDb` is PostgreSQL-only, and the point being asserted (the original
     * data is still there) holds on both backends.
     */
    private fun backupValuesByExtId(backupColumn: String): Map<String, Any?> {
        val schemaCtx = getTableCtx().getSchemaCtx()
        val rawService = DbDataServiceImpl(
            DbEntity::class.java,
            DbDataServiceConfig.create {
                withTable(tableRef.table)
                withIncludeBackupColumns(true)
            },
            schemaCtx
        )
        return TxnContext.doInNewTxn(readOnly = true) {
            rawService.findRaw(
                Predicates.alwaysTrue(),
                emptyList(),
                DbFindPage.ALL,
                emptyList(),
                emptyList(),
                emptyList(),
                false
            ).entities.associate { (it[DbEntity.EXT_ID] as String) to it[backupColumn] }
        }
    }

    /**
     * The source of an assoc-like transfer is `ed_associations`, not the column - the column is
     * truncated to ten, so reading it loses everything from the eleventh value on.
     *
     * A multi-valued assoc column is a cache of the first ten target ids and nothing more:
     * `DbRecordsMutateDao` writes ten of them and `DbRecord` re-reads `ed_associations` the
     * moment it finds exactly ten. So a transfer that reads the column hands the user ten of their
     * fifteen links and counts every row `processed` - the failure this whole handler's KDoc calls
     * the worse kind, because the migration reports success.
     *
     * Fifteen rather than eleven so that the answer is wrong by more than one, and the order is
     * asserted too: `__index` **is** the value of a multi-valued attribute, and it is the order
     * `?id` answered in before the change.
     */
    @Test
    fun everyLinkOfATruncatedAssocColumnIsCarriedAcrossTest() {

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("someAtt")
                    withType(AttributeType.ASSOC)
                    withMultiple(true)
                }
            )
        )
        val targets = (0 until 15).map { "emodel/doc@target-$it" }
        val rec = createRecord("someAtt" to targets)

        assertThat(records.getAtt(rec, "someAtt[]?id").asStrList())
            .describedAs("the fixture itself: the attribute answers with all fifteen before the change")
            .hasSize(15)

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("someAtt")
                    withType(AttributeType.TEXT)
                    withMultiple(true)
                }
            )
        )
        triggerTheTransition()
        drain()

        val task = theTask()
        assertThat(task.status).isEqualTo(DbBatchTaskStatus.DONE)
        assertThat(task.skipped)
            .describedAs("no link of any row was left behind")
            .isEqualTo(0)
        assertThat(records.getAtt(rec, "someAtt[]").asStrList())
            .describedAs("all fifteen links, in the order the attribute answered in - not the ten the column cached")
            .isEqualTo(targets)
    }

    /**
     * A string becoming an association is two writes - resolving the text to a `refId` **and**
     * filling `ed_associations` - and only the resolve was implemented. The converted ids went into
     * the `bigint[]` column and nothing ever reached the table that owns an association, so the
     * column and `ed_associations` disagreed for ever.
     *
     * **Exactly ten values, because ten is the count at which the omission stops being incomplete
     * and becomes empty.** A multi-valued assoc column caches ten and
     * `DbRecord` reads a cached array of exactly ten as "there may be more" and goes to
     * `ed_associations` for the whole list - so at ten the attribute reads back **nothing at all**,
     * not ten of fifteen.
     *
     * **The query assertion is the half that is wrong at every count**, ten or not: a predicate on
     * an assoc-like attribute is rewritten into an `EXISTS` over `ed_associations`
     * (`DbRecordsQueryDao` and `DbEntityRepoPg` build it), never into a read of the column,
     * so a journal filter on the migrated attribute finds no record at all while the read path
     * happily shows the values.
     */
    @Test
    fun aTextArrayConvertedIntoAnAssocArrayFillsTheAssociationsTableTest() {

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("someAtt")
                    withType(AttributeType.TEXT)
                    withMultiple(true)
                }
            )
        )
        val targets = (0 until 10).map { createRecord("someKey" to "t-$it") }
        val rec = createRecord("someAtt" to targets.map { it.toString() })

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("someAtt")
                    withType(AttributeType.ASSOC)
                    withMultiple(true)
                }
            )
        )
        triggerTheTransition()
        remoteActionsClient.clear()
        drain()

        val task = theTask()
        assertThat(task.status).isEqualTo(DbBatchTaskStatus.DONE)
        assertThat(task.skipped)
            .describedAs(
                "the ten target records and the row the transition created never carried this " +
                    "attribute, so their empty cells carried across; nothing was left behind"
            )
            .isEqualTo(0)

        assertThat(records.getAtt(rec, "someAtt[]?id").asStrList())
            .describedAs(
                "ten is exactly the count at which the read path stops trusting the column and asks " +
                    "ed_associations, so a transfer into an assoc-like type that never filled it reads back empty"
            )
            .isEqualTo(targets.map { it.toString() })

        assertThat(
            records.query(baseQuery.copy { withQuery(Predicates.eq("someAtt", targets[3])) }).getRecords()
        ).describedAs(
            "and this is wrong at every count, not only at ten: a predicate on an assoc-like " +
                "attribute is an EXISTS over ed_associations, so an empty table means the journal " +
                "filter finds nothing at all"
        ).containsExactly(rec)

        assertThat(liveLinkTargetIds(rec))
            .describedAs(
                "the links themselves, in the order the converted array had - " +
                    "`__index` is what getTargetAssocs orders by and what the attribute answers in"
            )
            .isEqualTo(targets.map { refIdOf(it) })

        val notification = remoteActionsClient.assocUpdatesFor("someAtt").single()
        assertThat(notification.sourceRef.getLocalId())
            .describedAs("a link appearing is notified just like a link leaving")
            .isEqualTo(rec.getLocalId())
        val diff = notification.assocsDiff.single { it.assocId == "someAtt" }
        assertThat(diff.added.map { it.getLocalId() })
            .describedAs(
                "through createAssocs and the ordinary notification, not a raw insert - an " +
                    "association may point at an entity another application owns, and a link that " +
                    "appears without that application hearing about it is as inconsistent as one " +
                    "that disappears without it"
            )
            .containsExactlyElementsOf(targets.map { it.getLocalId() })
        assertThat(diff.child)
            .describedAs("from the attribute definition, not a guess: this one is not a child assoc")
            .isFalse()
    }

    /**
     * The same defect at a count the read path is happy with, which is what makes it worse than a
     * truncation: **three** values, so the column cache is trusted whole
     * (`DbRecord` only re-reads at exactly ten) and the attribute answers correctly -
     * while a journal filter on it answers nothing, because the filter is an `EXISTS` over
     * `ed_associations` and never looks at the column. Read and query disagree about the same
     * record, silently, at every count.
     */
    @Test
    fun aConvertedAssocArrayIsFoundByAPredicateOnItTest() {

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("someAtt")
                    withType(AttributeType.TEXT)
                    withMultiple(true)
                }
            )
        )
        val targets = (0 until 3).map { createRecord("someKey" to "t-$it") }
        val rec = createRecord("someAtt" to targets.map { it.toString() })

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("someAtt")
                    withType(AttributeType.ASSOC)
                    withMultiple(true)
                }
            )
        )
        triggerTheTransition()
        drain()

        assertThat(records.getAtt(rec, "someAtt[]?id").asStrList())
            .describedAs("the read path is happy at three - the column cache is trusted whole below ten")
            .isEqualTo(targets.map { it.toString() })

        assertThat(
            records.query(baseQuery.copy { withQuery(Predicates.eq("someAtt", targets[1])) }).getRecords()
        ).describedAs(
            "and the query path is not: the arrival fills ed_associations, and the journal " +
                "filter is an EXISTS over exactly that table"
        ).containsExactly(rec)
    }

    /**
     * The column cache after an arrival: once the links exist, the column has
     * to say what `DbRecordsMutateDao` would have said, which for a multi-valued attribute is the
     * **first ten** and not everything the source array held.
     *
     * Fifteen values and one of them written twice, so the three ways the column and
     * `ed_associations` can disagree are all in one record: the count (15 against a cache of 10),
     * the duplicate (`createAssocs` is keyed on the target, so fourteen links come from fifteen
     * array elements) and the order (`__index` order, which is what the attribute answers in).
     * Leaving the converted array in the column would be a second disagreement of our own making in
     * place of the first one.
     */
    @Test
    fun theColumnOfAnArrivedAssocArrayCachesTheFirstTenAndNoMoreTest() {

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("someAtt")
                    withType(AttributeType.TEXT)
                    withMultiple(true)
                }
            )
        )
        val targets = (0 until 15).map { createRecord("someKey" to "t-$it") }
        // the seventh reference twice: an array column takes a duplicate, ed_associations does not
        val written = targets.map { it.toString() } + targets[7].toString()
        val rec = createRecord("someAtt" to written)

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("someAtt")
                    withType(AttributeType.ASSOC)
                    withMultiple(true)
                }
            )
        )
        triggerTheTransition()
        drain()

        assertThat(theTask().skipped)
            .describedAs("nothing was left behind")
            .isEqualTo(0)

        assertThat(liveLinkTargetIds(rec))
            .describedAs("fifteen links from sixteen array elements - the repeated target is one link")
            .isEqualTo(targets.map { refIdOf(it) })

        assertThat(cachedTargetIds(rec))
            .describedAs(
                "the column caches the first ten in __index order, not the sixteen the " +
                    "converted array held - writing them all would make the column disagree with " +
                    "ed_associations in a new way instead of the old one"
            )
            .isEqualTo(targets.take(10).map { refIdOf(it) })

        assertThat(records.getAtt(rec, "someAtt[]?id").asStrList())
            .describedAs("and the attribute still answers with all fifteen, because ten means 'go and read them all'")
            .isEqualTo(targets.map { it.toString() })
    }

    /**
     * The migrated column of one record as it physically is, in order. Read raw, because the whole
     * question is what the *column* holds rather than what the attribute answers - the two are
     * deliberately different above ten values.
     */
    private fun cachedTargetIds(rec: EntityRef): List<Long> {
        return when (val value = backupValuesByExtId("someAtt")[rec.getLocalId()]) {
            is LongArray -> value.toList()
            is Array<*> -> value.map { (it as Number).toLong() }
            is Collection<*> -> value.map { (it as Number).toLong() }
            else -> error("Not an array column value: $value")
        }
    }

    /**
     * `__child` on a link an assoc-like arrival creates comes from the **attribute definition**, and
     * there is nowhere else it could come from: `child` lives in `AttributeDef.config`, which
     * neither `ed_column_meta` nor [DbColumnSemanticType] records, so the transition writes it into
     * the task's params and the handler reads it back.
     *
     * A child association on purpose, because `false` is what a hardcoded flag would have said - and
     * a child link stored as an ordinary one is not a cosmetic difference: a cascade delete and
     * every parent/child query gate on exactly this column.
     */
    @Test
    fun anArrivedChildAssocIsStoredAsAChildTest() {

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("someAtt")
                    withType(AttributeType.TEXT)
                    withMultiple(true)
                }
            )
        )
        val targets = (0 until 3).map { createRecord("someKey" to "t-$it") }
        val rec = createRecord("someAtt" to targets.map { it.toString() })

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("someAtt")
                    withType(AttributeType.ASSOC)
                    withMultiple(true)
                    withConfig(ObjectData.create().set("child", true))
                }
            )
        )
        triggerTheTransition()
        remoteActionsClient.clear()
        drain()

        assertThat(theTask().skipped)
            .describedAs("nothing was left behind")
            .isEqualTo(0)
        assertThat(liveLinks(rec).map { it.child })
            .describedAs("the model calls this attribute a child association, so its links are children")
            .containsExactly(true, true, true)

        val diff = remoteActionsClient.assocUpdatesFor("someAtt").single().assocsDiff
            .single { it.assocId == "someAtt" }
        assertThat(diff.child)
            .describedAs("and the applications holding the other end are told the same thing")
            .isTrue()
    }

    /**
     * A string becoming an association is lossy for exactly this reason: a free-text attribute a
     * customer turns into a child association holds department names, legacy codes and labels, and
     * none of those is a reference. Such a row is left alone, a warning names it, and the original
     * stays in the backup - **not** a reference invented for it.
     *
     * `EntityRef.valueOf("Отдел кадров")` is not empty: with no `@` in the string the whole of it
     * becomes the local id and the source id is blank, so the ref resolves, `ed_record_ref` gains a
     * row for `test-app/@Отдел кадров`, and the column gets its id. Turn that id into an
     * `ed_associations` row with `__child = true` and the record becomes **undeletable**: the
     * cascade in `DbRecordsDeleteDao` selects `SOURCE_ID = refId AND CHILD = true` and hands the
     * targets to `recordsService.delete`, which has no source `''` to delete them from.
     *
     * The platform's own write path refuses to create this state - the same prose written into the
     * same child-assoc attribute through an ordinary mutate throws while setting the child's
     * `_parent` - so a migration that creates it manufactures a row no user could have made, and
     * reports the row as carried across while doing it.
     */
    @Test
    fun proseConvertedIntoAChildAssocLeavesADeletableRecordTest() {

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("someAtt")
                    withType(AttributeType.TEXT)
                }
            )
        )
        val rec = createRecord("someAtt" to "Отдел кадров")

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("someAtt")
                    withType(AttributeType.ASSOC)
                    withConfig(ObjectData.create().set("child", true))
                }
            )
        )
        triggerTheTransition()
        drain()

        // The headline, and the reason this is critical rather than untidy: an ordinary operation
        // on an ordinary record, after an automatic background migration nobody asked for.
        records.delete(rec)

        assertThat(records.getAtt(rec, "_notExists?bool").asBoolean())
            .describedAs("the record deletes, rather than throwing RecordsSourceNotFoundException")
            .isTrue()
        assertThat(theTask().skipped)
            .describedAs(
                "and the administrator is told: a row that is not a reference is left alone with " +
                    "its original, not a silently invented one reported as carried across"
            )
            .isEqualTo(1)
        assertThat(theTask().processed)
            .describedAs("only the row the transition created, whose cell was empty either way")
            .isEqualTo(1)
    }

    /**
     * A child association is two facts, not one: the `ed_associations` row with `__child = true`,
     * and `_parent`/`_parentAtt` on the child. `DbRecordsMutateDao` writes both - the second through
     * `RecMutAssocHandler.processChildrenAfterMutation` - and an arrival owes both too, because it
     * creates its links through the same path.
     *
     * Writing only the first leaves a third table disagreeing with the other two, which is the
     * defect class this whole branch exists to remove. Three answers depend on the one that was
     * missing, and each of them is a different wrong answer rather than a missing one:
     *
     *  - a `_parent` predicate finds nothing, because `DbRecordsQueryDao` deliberately keeps
     *    `_parent` out of the `ed_associations` join rewrite and reads the `__parent` column;
     *  - `_pathByParent` degrades with it - the child becomes its own root;
     *  - permission inheritance is lost, because `DefaultDbPermsComponent` answers
     *    `canRead = true` / `EVERYONE` / `ALL` for a record whose `_parent` is empty.
     */
    @Test
    fun theTargetsOfAnArrivedChildAssocAreToldWhoTheirParentIsTest() {

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("someAtt")
                    withType(AttributeType.TEXT)
                    withMultiple(true)
                }
            )
        )
        val targets = (0 until 3).map { createRecord("someKey" to "t-$it") }
        val rec = createRecord("someAtt" to targets.map { it.toString() })
        assertThat(records.getAtt(targets[0], "_parent?id").asText())
            .describedAs("the fixture: nothing has claimed these records yet")
            .isEmpty()

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("someAtt")
                    withType(AttributeType.ASSOC)
                    withMultiple(true)
                    withConfig(ObjectData.create().set("child", true))
                }
            )
        )
        triggerTheTransition()
        drain()

        assertThat(theTask().skipped)
            .describedAs("nothing was left behind")
            .isEqualTo(0)
        targets.forEachIndexed { i, target ->
            assertThat(records.getAtt(target, "_parent?id").asText())
                .describedAs("target $i is a child of the record whose attribute names it")
                .isEqualTo(rec.toString())
            assertThat(records.getAtt(target, "_parentAtt").asText())
                .describedAs("under the attribute that names it, which is what a delete reads back")
                .isEqualTo("someAtt")
        }

        assertThat(
            records.query(baseQuery.copy { withQuery(Predicates.eq("_parent", rec)) }).getRecords()
        ).describedAs(
            "a _parent filter reads the __parent column and never ed_associations, so the journal " +
                "tree view answers nothing at all for children a migration created"
        ).containsExactlyElementsOf(targets)
    }

    /**
     * The sharpest of the three, and the one that leaves a record permanently wrong rather than
     * merely invisible. `DbRecordsDeleteDao` excludes **child** links from the "someone points at
     * me" cleanup on purpose, because the `notifyParent` block is what handles them - and that block
     * is gated on `_parent` plus `_parentAtt`. With neither set, nothing removes the parent's link,
     * so the parent answers with a reference to a record that no longer exists, for ever, and a
     * journal filter on the attribute still finds the parent by it.
     */
    @Test
    fun deletingAChildOfAnArrivedChildAssocLeavesTheParentNoDanglingLinkTest() {

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("someAtt")
                    withType(AttributeType.TEXT)
                    withMultiple(true)
                }
            )
        )
        val targets = (0 until 3).map { createRecord("someKey" to "t-$it") }
        val rec = createRecord("someAtt" to targets.map { it.toString() })

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("someAtt")
                    withType(AttributeType.ASSOC)
                    withMultiple(true)
                    withConfig(ObjectData.create().set("child", true))
                }
            )
        )
        triggerTheTransition()
        drain()

        records.delete(targets[0])

        assertThat(records.getAtt(rec, "someAtt[]?id").asStrList())
            .describedAs(
                "the deleted child is gone from the parent's attribute, the way it would be had " +
                    "the link been made through an ordinary mutation"
            )
            .containsExactly(targets[1].toString(), targets[2].toString())
        assertThat(
            records.query(baseQuery.copy { withQuery(Predicates.eq("someAtt", targets[0])) }).getRecords()
        ).describedAs("and the journal no longer finds the parent by a link to a deleted record")
            .isEmpty()
    }

    /**
     * The other half of the same class as
     * [proseConvertedIntoAChildAssocLeavesADeletableRecordTest], and the one a source-id guard alone
     * does not close. `user@example.com` **does** name a source - `EntityRef.valueOf` reads
     * everything before the `@` as one - so it is a syntactically perfect reference to a source that
     * does not exist. The ordinary write path refuses it: creating a record with that value in a
     * child-assoc attribute throws `RecordsSourceNotFoundException` for source `'user'` while
     * setting the child's `_parent`. The migration now refuses it through the same mutation, which
     * is what keeps the two acceptance sets equal.
     *
     * Emails in a free-text attribute are not an exotic fixture, and without this the record would
     * be exactly as undeletable as the prose one: the cascade would hand `test-app/user@example.com`
     * to `recordsService.delete`.
     */
    @Test
    fun aReferenceToASourceThatDoesNotExistIsARefusedRowNotAChildLinkTest() {

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("someAtt")
                    withType(AttributeType.TEXT)
                }
            )
        )
        val rec = createRecord("someAtt" to "user@example.com")

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("someAtt")
                    withType(AttributeType.ASSOC)
                    withConfig(ObjectData.create().set("child", true))
                }
            )
        )
        triggerTheTransition()
        drain()

        assertThat(liveLinks(rec))
            .describedAs("a child whose source does not exist is not a child the platform can hold")
            .isEmpty()
        assertThat(records.getAtt(rec, "someAtt?id").asText())
            .describedAs("and the column is rolled back with it, so the two still agree")
            .isEmpty()
        assertThat(backupValuesByExtId("__backup_someAtt_text")[rec.getLocalId()])
            .describedAs("class C: the original is where reverting the type would find it")
            .isEqualTo("user@example.com")

        records.delete(rec)
        assertThat(records.getAtt(rec, "_notExists?bool").asBoolean()).isTrue()
        assertThat(theTask().skipped)
            .describedAs(
                "the row is left as it was - refusing a link the platform cannot hold is the " +
                    "expected outcome, not a failure"
            )
            .isEqualTo(1)
        assertThat(theTask().failed).isEqualTo(0)
    }

    /**
     * The assoc half of [aValueTheUserWroteDuringTheTransferIsNeverOverwrittenTest], and the
     * invariant the two-step write exists for: the conditional update on `target IS NULL` is what
     * **claims** the record, and the links are created only after it has won. A record the transfer
     * skipped must therefore end up with the user's links and nothing else - not four links, not the
     * migration's set on top of the user's.
     *
     * Reachable in one instance because the user's mutation lands *before* the drain rather than
     * inside its transaction; the genuine interleaving still needs a harness nobody here has.
     */
    @Test
    fun aRecordTheTransferSkippedKeepsItsOwnLinksAndGainsNoOthersTest() {

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("someAtt")
                    withType(AttributeType.TEXT)
                    withMultiple(true)
                }
            )
        )
        val migrationTargets = (0 until 3).map { createRecord("someKey" to "m-$it") }
        val userTargets = (0 until 2).map { createRecord("someKey" to "u-$it") }
        val rec = createRecord("someAtt" to migrationTargets.map { it.toString() })

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("someAtt")
                    withType(AttributeType.ASSOC)
                    withMultiple(true)
                }
            )
        )
        // the user edits the record after the column moved aside but before the transfer runs, and
        // this mutation is also what reconciles the schema
        updateRecord(rec, "someAtt" to userTargets.map { it.toString() })
        drain()

        assertThat(records.getAtt(rec, "someAtt[]?id").asStrList())
            .describedAs("the transfer writes only where the target column IS NULL")
            .isEqualTo(userTargets.map { it.toString() })
        assertThat(liveLinkTargetIds(rec))
            .describedAs(
                "and exactly those links, not five: the links are created after the conditional " +
                    "update and only when it won, so a record the transfer declined to touch never " +
                    "gets the migration's set added on top of the user's"
            )
            .isEqualTo(userTargets.map { refIdOf(it) })
        assertThat(theTask().skipped)
            .describedAs(
                "and it says so: the one row the user had already claimed is the one row the " +
                    "transfer left alone"
            )
            .isEqualTo(1)
        assertThat(theTask().processed)
            .describedAs(
                "the other five are the target records, which never carried this attribute, so " +
                    "their empty cells carried across"
            )
            .isEqualTo(5)
    }

    /**
     * The third of the three, and the one with the worst failure direction: `DefaultDbPermsComponent`
     * does not fall through to anything strict when `_parent?id` is empty - it answers
     * `canRead = true`, `EVERYONE` and `ALL`. So a child of a restricted parent whose `_parent` the
     * migration never wrote is readable by a stranger, and the record it is a child of is not.
     *
     * Not a regression against the state before the arrival branch existed - no link existed then
     * either, so the target was nobody's child and equally readable. It is the disagreement that is
     * new: `ed_associations` asserting a parent/child relationship the permission system does not
     * see.
     *
     * Only the negative half is asserted, on purpose. What the child inherits *positively* depends
     * on the deployment's own [ru.citeck.ecos.data.sql.records.perms.DbPermsComponent] - this
     * fixture's records an explicit authority set per record and only falls through to
     * [ru.citeck.ecos.data.sql.records.perms.DefaultDbPermsComponent] where it has none - so an
     * assertion about who *can* read the child would be asserting the mock. That a stranger cannot
     * is the part that holds whatever component is installed, because it follows from `_parent`
     * being set at all.
     */
    @Test
    fun aChildOfARestrictedParentInheritsItsPermissionsAfterTheArrivalTest() {

        // someKey is registered here, unlike in the other fixtures, because this test has to read
        // it back through the permission check
        val someKey = AttributeDef.create {
            withId("someKey")
            withType(AttributeType.TEXT)
        }
        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("someAtt")
                    withType(AttributeType.TEXT)
                    withMultiple(true)
                },
                someKey
            )
        )
        val target = createRecord("someKey" to "t-0")
        val rec = createRecord("someAtt" to listOf(target.toString()))
        setAuthoritiesWithReadPerms(rec, "owner-only")

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("someAtt")
                    withType(AttributeType.ASSOC)
                    withMultiple(true)
                    withConfig(ObjectData.create().set("child", true))
                },
                someKey
            )
        )
        triggerTheTransition()
        drain()

        // in the EVERYONE group, which is the whole point: that is the authority
        // DefaultDbPermsComponent hands out to a record with no parent
        AuthContext.runAs("a-stranger", listOf(AuthGroup.EVERYONE)) {
            assertThat(records.getAtt(rec, "someKey").asText())
                .describedAs("the fixture: the parent itself is not readable by a stranger")
                .isEmpty()
            assertThat(records.getAtt(target, "someKey").asText())
                .describedAs(
                    "and neither is its child, because a child with no _parent is not treated as " +
                        "restricted by default - it is treated as world-readable"
                )
                .isEmpty()
        }
    }

    /**
     * The `ed_associations` rows of one record, in `__index` order.
     */
    /**
     * The targets of the child links this record has parked in `ed_associations_backup` - the
     * attribute's real value while it is not assoc-like, as `liveLinks` is while it is.
     */
    private fun parkedChildrenOf(rec: EntityRef): List<Long> {
        val schemaCtx = getTableCtx().getSchemaCtx()
        val sourceId = refIdOf(rec)
        return TxnContext.doInTxn {
            DbDataServiceImpl(
                DbAssocBackupEntity::class.java,
                DbDataServiceConfig.create { withTable(DbAssocBackupEntity.TABLE) },
                schemaCtx
            ).findAll(
                Predicates.and(
                    Predicates.eq(DbAssocBackupEntity.SOURCE_ID, sourceId),
                    Predicates.eq(DbAssocBackupEntity.CHILD, true)
                )
            ).map { it.targetId }
        }
    }

    private fun liveLinks(rec: EntityRef): List<DbAssocEntity> {
        val schemaCtx = getTableCtx().getSchemaCtx()
        val sourceId = refIdOf(rec)
        return TxnContext.doInTxn {
            DbDataServiceImpl(
                DbAssocEntity::class.java,
                DbDataServiceConfig.create { withTable(DbAssocEntity.MAIN_TABLE) },
                schemaCtx
            ).findAll(
                Predicates.eq(DbAssocEntity.SOURCE_ID, sourceId),
                listOf(DbFindSort(DbAssocEntity.INDEX, true))
            )
        }
    }

    /**
     * The same defect from the other direction: `ENTITY_REF` becoming an association, which passes
     * as a no-op and leaves data that lies.
     *
     * Harder to notice than a string arriving, and therefore worth its own test: **both columns are already
     * `bigint[]` and both already hold the right ids**, so nothing about the column looks wrong
     * afterwards and the read path answers correctly at any count below ten. What is missing is the
     * table that now owns the value - `ENTITY_REF` writes no `ed_associations` rows at all
     * (`DbRecordsUtils.isStoredInAssocsTable` excludes it) - so a journal filter on the attribute
     * finds nothing, and the next
     * departure parks nothing.
     */
    @Test
    fun anEntityRefArrayConvertedIntoAnAssocArrayFillsTheAssociationsTableTest() {

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("someAtt")
                    withType(AttributeType.ENTITY_REF)
                    withMultiple(true)
                }
            )
        )
        val targets = (0 until 3).map { createRecord("someKey" to "t-$it") }
        val rec = createRecord("someAtt" to targets.map { it.toString() })
        assertThat(liveLinkTargetIds(rec))
            .describedAs("the fixture: an ENTITY_REF attribute has no rows in ed_associations at all")
            .isEmpty()

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("someAtt")
                    withType(AttributeType.ASSOC)
                    withMultiple(true)
                }
            )
        )
        triggerTheTransition()
        drain()

        assertThat(theTask().skipped)
            .describedAs("nothing was left behind")
            .isEqualTo(0)
        assertThat(liveLinkTargetIds(rec))
            .describedAs("the source of truth moves, so the links have to be created")
            .isEqualTo(targets.map { refIdOf(it) })
        assertThat(
            records.query(baseQuery.copy { withQuery(Predicates.eq("someAtt", targets[2])) }).getRecords()
        ).describedAs("which is what makes the attribute filterable again")
            .containsExactly(rec)
        assertThat(records.getAtt(rec, "someAtt[]?id").asStrList())
            .describedAs("and the read path still answers exactly what it did before the change")
            .isEqualTo(targets.map { it.toString() })
    }

    /**
     * The third seeding case, and the upgrade scenario this whole feature exists for: a physical
     * `varchar` column with no registry row while the model says `ASSOC` - exactly the state an
     * installation is left in when a type change was silently ignored. Seeding names it
     * [ru.citeck.ecos.data.sql.columnmeta.DbColumnSemanticType.Raw], the class comes out LOSSY
     * because the most cautious `VARCHAR` candidate decides, and the transfer therefore runs with a
     * source the registry never identified. Nothing in the suite produced that column before, so
     * "a `Raw` source takes the arrival branch at all" was unpinned.
     *
     * **Three rows, because the three answers are different and all three matter.** A real
     * reference converts and gets its link; prose is one row left behind with its original still in
     * the backup and *no* fabricated `ed_record_ref` behind it; an empty string was always left
     * behind too. The middle one is the case that used to convert - see
     * [proseConvertedIntoAChildAssocLeavesADeletableRecordTest] for what it cost when the attribute
     * was a child association.
     *
     * Raw DDL is the only way to fabricate a pre-registry column, so this is a PostgreSQL-only
     * fixture; what it asserts is backend-independent.
     */
    @Test
    fun aRawVarcharColumnTheModelCallsAnAssocIsMigratedRowByRowTest() {

        assumeRawSqlSupported()

        registerAtts(listOf(AttributeDef.create { withId("textAtt") }))
        val target = createRecord("textAtt" to "the target of the one real reference")
        val withRef = createRecord("textAtt" to "holds a reference")
        val withProse = createRecord("textAtt" to "holds prose")
        val withEmpty = createRecord("textAtt" to "holds an empty string")

        // a column that exists physically as VARCHAR and has no registry row, exactly like every
        // column of an installation upgraded from before the registry existed
        sqlUpdate("ALTER TABLE ${tableRef.fullName} ADD COLUMN \"brokenAtt\" VARCHAR")
        fun seed(rec: EntityRef, value: String) {
            sqlUpdate(
                "UPDATE ${tableRef.fullName} SET \"brokenAtt\" = '$value' " +
                    "WHERE \"${DbEntity.EXT_ID}\" = '${rec.getLocalId()}'"
            )
        }
        seed(withRef, target.toString())
        seed(withProse, "just some prose, not a reference")
        seed(withEmpty, "")
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
        // the mutation that reconciles the schema, and a row the transfer has nothing to do for
        createRecord("textAtt" to "the mutation that reconciles the schema")
        drain()

        val task = theTask()
        assertThat(task.status).isEqualTo(DbBatchTaskStatus.DONE)
        assertThat(task.processed)
            .describedAs(
                "the one row that really holds a reference is carried across, and so are the " +
                    "empty cells of the target record and of the record created to reconcile the " +
                    "schema, neither of which had anything under this attribute"
            )
            .isEqualTo(3)
        assertThat(task.failed)
            .describedAs("a value that is not a reference is a normal outcome, not a failure of the task")
            .isEqualTo(0)
        assertThat(task.skipped)
            .describedAs(
                "the two that are not references are left alone, not invented - prose names " +
                    "neither a source nor an application, and an empty string is no reference at " +
                    "all; the counters add up to the five rows the table holds"
            )
            .isEqualTo(2)

        assertThat(liveLinkTargetIds(withRef))
            .describedAs(
                "a Raw source takes the arrival branch like any other and fills " +
                    "ed_associations, so the upgrade repairs the installation rather than leaving " +
                    "the column and the table disagreeing"
            )
            .isEqualTo(listOf(refIdOf(target)))
        assertThat(records.getAtt(withRef, "brokenAtt?id").asText()).isEqualTo(target.toString())

        assertThat(liveLinks(withProse))
            .describedAs("prose is not a reference, so no link and no reference are created for it")
            .isEmpty()
        assertThat(records.getAtt(withProse, "brokenAtt?id").asText())
            .describedAs("the attribute is empty rather than pointing at something invented")
            .isEmpty()
        assertThat(liveLinks(withEmpty)).isEmpty()

        val backup = backupValuesByExtId("__backup_brokenAtt_raw_text")
        assertThat(backup[withProse.getLocalId()])
            .describedAs("class C's promise for a value that does not fit: the original is still there")
            .isEqualTo("just some prose, not a reference")
        assertThat(backup[withEmpty.getLocalId()]).isEqualTo("")
        assertThat(backup[withRef.getLocalId()])
            .describedAs("and a value that did fit keeps its original too - the backup is never dropped")
            .isEqualTo(target.toString())
    }

    /**
     * `someAtt` as a multi-valued child association. [registerAtts] cannot carry a config.
     */
    private fun asChildAssoc() {
        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("someAtt")
                    withType(AttributeType.ASSOC)
                    withMultiple(true)
                    withConfig(ObjectData.create().set("child", true))
                }
            )
        )
    }

    /**
     * `someAtt` as a multi-valued text attribute - the departure out of the association group.
     */
    private fun asTextArray() {
        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("someAtt")
                    withType(AttributeType.TEXT)
                    withMultiple(true)
                }
            )
        )
    }

    /**
     * A child whose link is parked is still an ordinary record: **it deletes.**
     *
     * The state a departure leaves is a child that names its parent under an attribute which is no
     * longer a child association, and every deletion of such a child sends its parent the same
     * `att_remove` notification every deleted child sends. `RecMutAssocHandler.validateChildAssocs`
     * used to answer that with `'someAtt' is not a child association` - not now and not ever, the
     * same throw meeting every attempt, so the record could never be deleted again.
     *
     * The parent takes it as what it is instead: the links are in `ed_associations_backup`, so the
     * notification is applied there and the attribute - text now - is left alone.
     *
     * **A deletability test and not a column-comparison test on purpose**, because a disagreement
     * between the tables is only worth a round of work if an ordinary operation on an ordinary
     * record answers wrongly because of it. The type change here is the feature's own scenario - a
     * child association an administrator turns into text - so this is reachable on completely
     * ordinary data.
     */
    @Test
    fun aChildIsStillDeletableWhileItsLinkIsParkedTest() {

        asChildAssoc()
        val child = createRecord("someKey" to "c")
        val rec = createRecord("someAtt" to listOf(child.toString()))

        assertThat(records.getAtt(child, "_parent?id").asText())
            .describedAs("the fixture: an ordinary child of an ordinary child association")
            .isEqualTo(rec.toString())

        asTextArray()
        triggerTheTransition()
        drain()

        assertThat(liveLinks(rec))
            .describedAs("the departure took the link out of ed_associations")
            .isEmpty()

        // The headline. Before the parent learned to apply the notification to the snapshot this
        // threw "'someAtt' is not a child association".
        records.delete(child)

        assertThat(records.getAtt(child, "_notExists?bool").asBoolean())
            .describedAs("an ordinary record, deleted by an ordinary delete")
            .isTrue()
    }

    /**
     * The same state read from the other side: a departure **keeps** the back-reference.
     *
     * A child whose `_parent` is cleared is an orphan for as long as the attribute stays
     * non-assoc-like: nothing names it, no `_parent` predicate finds it, no interface leads to it,
     * and `DefaultDbPermsComponent` reads a record with no `_parent` as readable by everybody. The
     * links are what left the association group, not the parentage - so the two columns stay
     * exactly as the user's own mutation wrote them, and every operation that used to need them
     * cleared is taught the state instead.
     *
     * Both halves, because a `_parent` without a `_parentAtt` is the shape `DbRecordsDeleteDao`
     * gates its parent notification on: keeping one and dropping the other would leave the child
     * deletable and its parent holding a snapshot that still names it.
     */
    @Test
    fun aDepartureKeepsBothHalvesOfTheBackReferenceTest() {

        asChildAssoc()
        val child = createRecord("someKey" to "c")
        val rec = createRecord("someAtt" to listOf(child.toString()))

        asTextArray()
        triggerTheTransition()
        drain()

        assertThat(records.getAtt(child, "_parent?id").asText())
            .describedAs("the child still knows whose it is, so an interface can still reach it")
            .isEqualTo(rec.toString())
        assertThat(records.getAtt(child, "_parentAtt").asText())
            .describedAs("and under which attribute, which is what makes it deletable")
            .isEqualTo("someAtt")
    }

    /**
     * A child deleted while its link is parked is **forgotten by the snapshot**, so the day the
     * attribute becomes a child association again it is not linked back.
     *
     * The deletion reaches the parent as an ordinary `att_remove` notification - the same one every
     * deleted child sends - and the parent has no association to remove anything from: its
     * attribute holds text now and its links are in `ed_associations_backup`. Leaving the snapshot
     * alone would mean the restore creating a link to a record that no longer exists, which is the
     * dangling reference the arrival path refuses to create in the first place.
     */
    @Test
    fun aChildDeletedWhileItsLinkIsParkedIsNotLinkedBackByTheReturnTest() {

        asChildAssoc()
        val staying = createRecord("someKey" to "staying")
        val deleted = createRecord("someKey" to "deleted")
        val rec = createRecord("someAtt" to listOf(staying.toString(), deleted.toString()))

        asTextArray()
        triggerTheTransition()
        drain()

        assertThat(parkedChildrenOf(rec))
            .describedAs("the fixture: both children's links are parked while the attribute holds text")
            .containsExactlyInAnyOrder(refId(staying), refId(deleted))

        records.delete(deleted)
        assertThat(records.getAtt(deleted, "_notExists?bool").asBoolean())
            .describedAs("an ordinary child, deleted while the attribute holds text")
            .isTrue()
        assertThat(parkedChildrenOf(rec))
            .describedAs(
                "the headline: the deletion reached the snapshot, so nothing there names a record " +
                    "that is gone"
            )
            .containsExactly(refId(staying))

        asChildAssoc()
        triggerTheTransition()
        drain()

        assertThat(liveLinks(rec).map { it.targetId })
            .describedAs("the return restores the link of the child that is still there, and only it")
            .containsExactly(refId(staying))
        assertThat(records.getAtt(staying, "_parent?id").asText())
            .describedAs("and the surviving child is this record's again")
            .isEqualTo(rec.toString())
    }

    /**
     * A child **taken by another parent** while its link is parked is forgotten by the snapshot too.
     *
     * The same notification as a deletion and a different reason for it: a record has one `_parent`,
     * so giving it to somebody else takes it from the record that parked the link, and that record
     * hears about it as an `att_remove` from the child. Leaving the snapshot alone would mean the
     * return of the type handing the child back to its old parent - a migration undoing a move the
     * user made, which is the one thing every refusal in this handler exists to prevent.
     */
    @Test
    fun aChildTakenByAnotherParentWhileItsLinkIsParkedIsNotTakenBackByTheReturnTest() {

        asChildAssoc()
        val child = createRecord("someKey" to "c")
        val rec = createRecord("someAtt" to listOf(child.toString()))

        asTextArray()
        triggerTheTransition()
        drain()

        // a second table would be the honest shape of "another parent", but the attribute of this
        // one is text now, so the new parent is an ordinary record of the same table under a child
        // association of its own
        registerAtts(
            listOf(
                textAtt(),
                AttributeDef.create {
                    withId("otherAtt")
                    withType(AttributeType.ASSOC)
                    withMultiple(true)
                    withConfig(ObjectData.create().set("child", true))
                }
            )
        )
        val newParent = createRecord("otherAtt" to listOf(child.toString()))

        assertThat(records.getAtt(child, "_parent?id").asText())
            .describedAs("the fixture: the user moved the child to another parent")
            .isEqualTo(newParent.toString())
        assertThat(parkedChildrenOf(rec))
            .describedAs("and the record that parked the link was told, so its snapshot let go")
            .isEmpty()

        asChildAssoc()
        triggerTheTransition()
        drain()

        assertThat(liveLinks(rec))
            .describedAs("so the return gives the old parent nothing back")
            .isEmpty()
        assertThat(records.getAtt(child, "_parent?id").asText())
            .describedAs("and the child stays where the user put it")
            .isEqualTo(newParent.toString())
    }

    /**
     * Deleting a parent while its child links are parked takes the children with it.
     *
     * The cascade `DbRecordsDeleteDao` performs is driven from `ed_associations`, and during the
     * window that table holds none of this attribute's rows - they are in the backup. Without the
     * snapshot being read there, deleting the parent would leave every child of a departed child
     * association behind it, each one still naming a record that no longer exists: unreachable from
     * the parent that is gone and undeletable by the notification it would send.
     */
    @Test
    fun deletingAParentWhileItsChildLinksAreParkedTakesTheChildrenWithItTest() {

        asChildAssoc()
        val child = createRecord("someKey" to "c")
        val rec = createRecord("someAtt" to listOf(child.toString()))

        asTextArray()
        triggerTheTransition()
        drain()

        records.delete(rec)

        assertThat(records.getAtt(child, "_notExists?bool").asBoolean())
            .describedAs("a child of a parked child association is deleted with its parent, as it would be otherwise")
            .isTrue()
    }

    /**
     * The departure has **two** callers and this is the other one.
     *
     * `DbColumnMigrationHandler.prepare` performs it once per run of the task; `DbShadowColumnTransition`
     * performs it when a second type change cancels the task before any drain tick reached it
     *, and that one runs inside a user's mutation rather than under the batch engine's
     * `runAsSystem`. They share one `DbAssocGroupDeparture`, so they share the release - but "they
     * share the code" is exactly the kind of claim that was true of the last round too, and the
     * cheapest way to keep it true is to drain neither and make the cancellation do the work.
     *
     * No `drain()` here on purpose: the first task is queued and never run, so the only thing that
     * can have released this child is the cancellation path.
     */
    @Test
    fun theDepartureACancellationPerformsReleasesTheChildrenTooTest() {

        asChildAssoc()
        val child = createRecord("someKey" to "c")
        val rec = createRecord("someAtt" to listOf(child.toString()))

        asTextArray()
        triggerTheTransition()

        // a second type change before the first transfer ran: the queued task is cancelled, and
        // DbShadowColumnTransition performs that task's departure itself
        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("someAtt")
                    withType(AttributeType.NUMBER)
                    withMultiple(true)
                }
            )
        )
        triggerTheTransition()

        assertThat(records.getAtt(child, "_parent?id").asText())
            .describedAs("the cancellation parks the links and leaves the parentage alone, as a drain does")
            .isEqualTo(rec.toString())

        records.delete(child)

        assertThat(records.getAtt(child, "_notExists?bool").asBoolean())
            .describedAs("and the child is deletable afterwards, just as it would be after a drain")
            .isTrue()
    }

    /**
     * And it releases **only** the children of the attribute that left.
     *
     * A record has one `_parent` and one `_parentAtt`, but a table can have several child
     * associations, and a departure of one of them must not touch the children of the others. The
     * conditional update names both old values for this reason.
     */
    @Test
    fun aDepartureLeavesTheChildrenOfOtherAttributesAloneTest() {

        val otherChildAssoc = AttributeDef.create {
            withId("otherAtt")
            withType(AttributeType.ASSOC)
            withMultiple(true)
            withConfig(ObjectData.create().set("child", true))
        }
        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("someAtt")
                    withType(AttributeType.ASSOC)
                    withMultiple(true)
                    withConfig(ObjectData.create().set("child", true))
                },
                otherChildAssoc
            )
        )
        val leaving = createRecord("someKey" to "leaving")
        val staying = createRecord("someKey" to "staying")
        val rec = createRecord(
            "someAtt" to listOf(leaving.toString()),
            "otherAtt" to listOf(staying.toString())
        )

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("someAtt")
                    withType(AttributeType.TEXT)
                    withMultiple(true)
                },
                otherChildAssoc
            )
        )
        triggerTheTransition()
        drain()

        assertThat(records.getAtt(leaving, "_parent?id").asText())
            .describedAs("the child of the attribute that left keeps the parent it was given")
            .isEqualTo(rec.toString())
        assertThat(records.getAtt(staying, "_parent?id").asText())
            .describedAs("the child of the attribute that stayed is not touched at all")
            .isEqualTo(rec.toString())
        assertThat(records.getAtt(staying, "_parentAtt").asText()).isEqualTo("otherAtt")
    }

    /**
     * MAJOR-1's shape: the target of an arriving child association is **already this record's
     * child, under a different attribute**.
     *
     * Ordinary enough to meet on real data - a collection of contracts under `kids` and the "main"
     * one named again in a free-text field beside it - and a shape the platform cannot represent:
     * `_parent` and `_parentAtt` are one column each, so a record is a child of this record under
     * exactly one attribute. Linking it under a second one is measurably worse than useless:
     * deleting the child removes the link of the attribute `_parentAtt` names and leaves the other
     * one pointing at a record that no longer exists, for ever, with a journal filter on that
     * attribute still finding the parent by it. Which is exactly what
     * [deletingAChildOfAnArrivedChildAssocLeavesTheParentNoDanglingLinkTest] asserts must not
     * happen - that test simply never had a second attribute in play.
     *
     * The branch this pins is "the target is already ours", which used to compare `_parent` alone.
     */
    @Test
    fun aTargetAlreadyOurChildUnderAnotherAttributeDoesNotBecomeASecondLinkTest() {

        val kids = AttributeDef.create {
            withId("kids")
            withType(AttributeType.ASSOC)
            withMultiple(true)
            withConfig(ObjectData.create().set("child", true))
        }
        registerAtts(
            listOf(
                kids,
                AttributeDef.create {
                    withId("someAtt")
                    withType(AttributeType.TEXT)
                }
            )
        )
        val kid = createRecord("someKey" to "k")
        val rec = createRecord("kids" to listOf(kid.toString()), "someAtt" to kid.toString())

        assertThat(records.getAtt(kid, "_parentAtt").asText())
            .describedAs("the fixture: the record is already this record's child, under `kids`")
            .isEqualTo("kids")

        registerAtts(
            listOf(
                kids,
                AttributeDef.create {
                    withId("someAtt")
                    withType(AttributeType.ASSOC)
                    withConfig(ObjectData.create().set("child", true))
                }
            )
        )
        triggerTheTransition()
        drain()

        assertThat(records.getAtt(kid, "_parentAtt").asText())
            .describedAs("the back-reference the user made is left alone")
            .isEqualTo("kids")
        assertThat(liveLinks(rec).count { it.child })
            .describedAs("and no second child link was created beside it")
            .isEqualTo(1)
        assertThat(theTask().skipped)
            .describedAs("the row is reported as left alone, with its original still in the backup")
            .isEqualTo(1)
        assertThat(theTask().failed)
            .describedAs("refusing a second parent link is the expected outcome, not a failure")
            .isEqualTo(0)

        records.delete(kid)

        assertThat(
            records.query(baseQuery.copy { withQuery(Predicates.eq("someAtt", kid)) }).getRecords()
        ).describedAs("so deleting the child leaves the parent no link to a record that is gone")
            .isEmpty()
    }

    /**
     * A row whose value named several references of which only some could be linked is **not** a
     * transferred row.
     *
     * It is not a failure either. Changing a column's type is a dangerous operation, and a cell that
     * cannot be carried over whole is one of its expected outcomes - the administrator's safety net
     * is the backup column, which still holds every reference the value named, not a counter that
     * reads zero. So the row goes to `skipped`: the target column was not filled with the value, and
     * the original is untouched where reverting the type would find it.
     *
     * What it must **not** be is `processed`. A row that put one of its two values into the column
     * and left the whole of it in the backup, reported as carried across, is indistinguishable from
     * a complete transfer, and the per-row warning is what names it.
     *
     * The column is deliberately **not** rolled back: it agrees with `ed_associations`, and undoing
     * that would trade a visible partial transfer for an invisible one.
     */
    @Test
    fun aRowOnlySomeOfWhoseValuesBecameLinksIsNotCountedAsTransferredTest() {

        val kids = AttributeDef.create {
            withId("kids")
            withType(AttributeType.ASSOC)
            withMultiple(true)
            withConfig(ObjectData.create().set("child", true))
        }
        registerAtts(
            listOf(
                kids,
                AttributeDef.create {
                    withId("someAtt")
                    withType(AttributeType.TEXT)
                    withMultiple(true)
                }
            )
        )
        val free = createRecord("someKey" to "free")
        val taken = createRecord("someKey" to "taken")
        val otherParent = createRecord("kids" to listOf(taken.toString()))
        val rec = createRecord("someAtt" to listOf(free.toString(), taken.toString()))

        registerAtts(
            listOf(
                kids,
                AttributeDef.create {
                    withId("someAtt")
                    withType(AttributeType.ASSOC)
                    withMultiple(true)
                    withConfig(ObjectData.create().set("child", true))
                }
            )
        )
        triggerTheTransition()
        drain()

        assertThat(records.getAtt(taken, "_parent?id").asText())
            .describedAs("somebody else's child is not taken over - that part was already right")
            .isEqualTo(otherParent.toString())
        assertThat(records.getAtt(rec, "someAtt[]?id").asStrList())
            .describedAs("and the half that could be linked is linked, column and links agreeing")
            .containsExactly(free.toString())
        assertThat(theTask().skipped)
            .describedAs(
                "the headline: a partially transferred row is not a transferred row. It is left " +
                    "alone instead, with the whole original in the backup, and it is the only row " +
                    "of the table that is - the two targets, the other parent and the row the " +
                    "transition created all carried their empty cells across"
            )
            .isEqualTo(1)
        assertThat(theTask().processed)
            .describedAs("so it is not among the rows reported as carried across")
            .isEqualTo(4)
        assertThat(theTask().failed)
            .describedAs("and a cell that could not be carried over whole is not a failure")
            .isEqualTo(0)
    }

    /**
     * MAJOR-3: with no records service registered for the table, **no child links are created at
     * all**.
     *
     * The back-reference goes through `RecordsService`, and a `DbBatchTaskHandler` reaches one
     * through `DbSchemaContext.getRecordsService`. The engine hangs its drain on the scheduler and
     * nothing orders that after the daos of the schema are up, so a tick can arrive first. Creating
     * the links anyway - which is what the code did, with a warning - manufactures exactly the state
     * the rest of this class refuses to create: a child that does not know it has a parent, whose
     * deletion leaves the parent a dangling link, which answers nothing to a `_parent` predicate and
     * which `DefaultDbPermsComponent` reads as world-readable. Refusing costs one row left behind
     * with its original in the backup; creating costs an irreversible disagreement between three
     * tables.
     */
    @Test
    fun withNoRecordsServiceForTheTableNoChildLinkIsCreatedAtAllTest() {

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("someAtt")
                    withType(AttributeType.TEXT)
                    withMultiple(true)
                }
            )
        )
        val targets = (0 until 2).map { createRecord("someKey" to "t-$it") }
        val rec = createRecord("someAtt" to targets.map { it.toString() })

        asChildAssoc()
        triggerTheTransition()

        getTableCtx().getSchemaCtx().unregisterRecordsService(tableRef.table)
        try {
            drain()
        } finally {
            getTableCtx().getSchemaCtx().registerRecordsService(tableRef.table, records)
        }

        assertThat(liveLinks(rec))
            .describedAs("not one link was created without its back-reference")
            .isEmpty()
        assertThat(theTask().skipped)
            .describedAs("the row is left alone, so the work is still there to do and the original kept")
            .isEqualTo(1)
        assertThat(theTask().failed)
            .describedAs("a drain that arrives before the daos are up has not failed anything")
            .isEqualTo(0)
        targets.forEach {
            assertThat(records.getAtt(it, "_parent?id").asText()).isEmpty()
        }
    }

    /**
     * MINOR-4: adopting a child is a background migration's write, not the child's own edit.
     *
     * `RecMutAssocHandler.updateParentRefOfChildren` goes through the whole records mutation path,
     * which stamps `_modified`/`_modifier` and emits a change event. On a table with a million
     * children that is a million rewritten "last modified" dates and a million events, produced by a
     * task nobody started, and both are wrong answers rather than merely expensive ones: a user
     * looking at a document sees it as changed today by `system`. The mutation path provides
     * `DISABLE_AUDIT`/`DISABLE_EVENTS` for exactly this and the engine runs the drain under
     * `runAsSystem` so that they may be used.
     */
    @Test
    fun adoptingAChildDoesNotRewriteItsModifiedDateTest() {

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("someAtt")
                    withType(AttributeType.TEXT)
                    withMultiple(true)
                }
            )
        )
        val target = createRecord("someKey" to "t")
        createRecord("someAtt" to listOf(target.toString()))
        val modifiedBefore = records.getAtt(target, "_modified").asText()

        asChildAssoc()
        triggerTheTransition()
        drain()

        assertThat(records.getAtt(target, "_parent?id").asText())
            .describedAs("the control: the adoption really happened")
            .isNotEmpty()
        assertThat(records.getAtt(target, "_modified").asText())
            .describedAs("and it left the child's own audit fields alone")
            .isEqualTo(modifiedBefore)
    }

    /**
     * MINOR-1: a bare Alfresco node ref is a reference the platform resolves, so the migration has
     * to resolve it too.
     *
     * `workspace://SpacesStore/...` has no `@` in it, so `EntityRef.valueOf` puts the whole string in
     * the local id and leaves both the source and the application blank - the very shape the guard
     * against prose refuses. But `DbRecordRefService.fixEntityRef` has a branch of its own for it and
     * stores it as `alfresco/@workspace://SpacesStore/...`, and the ordinary write path accepts it.
     * Measuring the guard on the stored form, against this application's own name, is what keeps the
     * two acceptance sets from drifting apart over a legitimate value.
     */
    @Test
    fun aBareAlfrescoNodeRefIsAReferenceAndNotARefusedRowTest() {

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("someAtt")
                    withType(AttributeType.TEXT)
                }
            )
        )
        val rec = createRecord("someAtt" to "workspace://SpacesStore/2ff0f0a1-0000-0000-0000-000000000001")

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("someAtt")
                    withType(AttributeType.ASSOC)
                }
            )
        )
        triggerTheTransition()
        drain()

        assertThat(theTask().processed)
            .describedAs(
                "a value the ordinary write path stores is not a value that does not fit, and the " +
                    "transition's own empty row carries across beside it"
            )
            .isEqualTo(2)
        assertThat(theTask().skipped)
            .describedAs("nothing was left behind")
            .isEqualTo(0)
        assertThat(records.getAtt(rec, "someAtt?id").asText())
            .describedAs("and it is stored the way the ref service stores it")
            .isEqualTo("alfresco/@workspace://SpacesStore/2ff0f0a1-0000-0000-0000-000000000001")
    }

    /**
     * Clears a child's `_parent`/`_parentAtt` the way the departure of `1.73.0` did, leaving its
     * link parked and the child an orphan.
     *
     * Written here rather than performed by production code, which no longer does it - and kept,
     * rather than dropped with the code, because the rows that release wrote are still in every
     * database it ran on. A restore meeting one of those is the only path on which it writes a
     * back-reference at all, so it is the path the writing and its refusal have to be tested
     * through.
     */
    private fun orphanTheChildAsAReleasedDepartureDid(child: EntityRef) {
        val schemaCtx = getTableCtx().getSchemaCtx()
        TxnContext.doInTxn {
            val service = DbDataServiceImpl(
                DbEntity::class.java,
                DbDataServiceConfig.create { withTable(tableRef.table) },
                schemaCtx
            )
            val entity = service.findAll(
                Predicates.eq(DbEntity.REF_ID, dbRecordRefService.getIdByEntityRef(child))
            ).single()
            service.updateByIdIfMatches(
                entity.id,
                mapOf(RecordConstants.ATT_PARENT to entity.attributes[RecordConstants.ATT_PARENT]),
                mapOf(
                    RecordConstants.ATT_PARENT to null,
                    RecordConstants.ATT_PARENT_ATT to null
                )
            )
        }
    }

    /**
     * The `ed_record_ref` id of a record, which is what an association keys on.
     */
    private fun refIdOf(ref: EntityRef): Long {
        return TxnContext.doInTxn { dbRecordRefService.getIdByEntityRef(ref) }
    }

    /**
     * The targets of one record's `ed_associations` rows, in `__index` order.
     */
    private fun liveLinkTargetIds(rec: EntityRef): List<Long> {
        return liveLinks(rec).map { it.targetId }
    }

    /**
     * Calls `onFinish` a second time on the finished task, the way the engine does when the
     * compare-and-set that publishes DONE has to be retried.
     */
    private fun reEnterOnFinish() {
        val schemaCtx = getTableCtx().getSchemaCtx()
        val handler = DbColumnMigrationHandler(schemaCtx.dataSourceCtx)
        TxnContext.doInTxn {
            handler.onFinish(DbBatchTaskContext(theTask(), schemaCtx))
        }
    }
}
