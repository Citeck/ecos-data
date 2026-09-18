package ru.citeck.ecos.data.sql.test.records

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import ru.citeck.ecos.commons.data.DataValue
import ru.citeck.ecos.commons.data.ObjectData
import ru.citeck.ecos.context.lib.auth.AuthContext
import ru.citeck.ecos.context.lib.auth.AuthGroup
import ru.citeck.ecos.context.lib.auth.AuthUser
import ru.citeck.ecos.data.sql.batch.DbBatchTaskAdminDao
import ru.citeck.ecos.data.sql.batch.DbBatchTaskCancelAction
import ru.citeck.ecos.data.sql.batch.DbBatchTaskEngine
import ru.citeck.ecos.data.sql.columnmeta.DbColumnMetaDto
import ru.citeck.ecos.data.sql.columnmeta.DbColumnSemanticType
import ru.citeck.ecos.data.sql.migration.column.DbColumnRestore
import ru.citeck.ecos.data.sql.props.DbEcosDataProps
import ru.citeck.ecos.data.sql.records.assocs.DbAssocEntity
import ru.citeck.ecos.data.sql.repo.entity.DbEntity
import ru.citeck.ecos.data.sql.repo.find.DbFindPage
import ru.citeck.ecos.data.sql.repo.find.DbFindSort
import ru.citeck.ecos.data.sql.service.DbDataServiceConfig
import ru.citeck.ecos.data.sql.service.DbDataServiceImpl
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType
import ru.citeck.ecos.records2.predicate.model.Predicates
import ru.citeck.ecos.txn.lib.TxnContext
import ru.citeck.ecos.webapp.api.entity.EntityRef
import java.time.Duration
import java.time.Instant

/**
 * The scenario the whole effort is named after: a type changed by mistake and changed back.
 *
 * `inPlaceAlterMaxRows` is pinned at zero for the same reason as in [DbColumnMigrationHandlerTest]:
 * every test here needs the type change to take the shadow-column path, and `NUMBER -> TEXT` is a
 * pair PostgreSQL can express as an `ALTER ... USING`, so on a small table it would be converted
 * where it stands and there would be no backup to restore at all.
 *
 * The batch size is three so that the rows of one restore are walked in several id windows.
 */
class DbColumnRestoreTest : DbRecordsTestBase() {

    init {
        dataProps = DbEcosDataProps(
            columns = DbEcosDataProps.ColumnsProps(inPlaceAlterMaxRows = 0),
            batch = DbEcosDataProps.BatchProps(batchSize = 3, batchPause = Duration.ZERO)
        )
    }

    companion object {
        /**
         * An OPTIONS attribute validates every value written into it against the options its config
         * declares, so a chain that writes through an OPTIONS phase has to declare them. The values
         * written under TEXT are declared here too: nothing re-validates a stored value when the
         * type is relabelled, but the same strings are written under both types in these chains.
         */
        private val OPTIONS_CONFIG: ObjectData = ObjectData.create()
            .set("source", "values")
            .set(
                "values",
                DataValue.createArr()
                    .add(DataValue.createObj().set("label", "opt-a").set("value", "opt-a"))
                    .add(DataValue.createObj().set("label", "live-text").set("value", "live-text"))
            )
    }

    private fun drain() {
        val schemaCtx = getTableCtx().getSchemaCtx()
        DbBatchTaskEngine(schemaCtx.dataSourceCtx).drainSchemaOnce(schemaCtx)
    }

    private fun asType(type: AttributeType, multiple: Boolean = false) {
        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("att")
                    withType(type)
                    withMultiple(multiple)
                    if (type == AttributeType.OPTIONS) {
                        withConfig(OPTIONS_CONFIG)
                    }
                }
            )
        )
    }

    /**
     * Resolves a record's reference to its `ed_record_ref` id, which is what an association keys on.
     */
    private fun getIdByRef(ref: EntityRef): Long {
        return dbRecordRefService.getIdByEntityRef(ref)
    }

    /**
     * The raw `ed_associations` rows of one record, in `__index` order. Read raw because a
     * `DbAssocDto` carries none of the three fields a parked link has to keep.
     */
    private fun liveLinks(sourceId: Long): List<DbAssocEntity> {
        val schemaCtx = getTableCtx().getSchemaCtx()
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
     * Writes [value] into the attribute's backup column of one record, through the only service
     * allowed to see a backup column ([DbDataServiceConfig.includeBackupColumns]).
     *
     * Arranged rather than produced, and that is the point: while the attribute is not an
     * association its links are parked and the column caching them is aside, so nothing a user can
     * do makes the cache disagree with `ed_associations`. The restore must not trust that cache
     * anyway - rewriting it from the links is exactly what it does - and this is the only way to put
     * a test in a position to tell whether it did.
     */
    private fun writeIntoBackupColumn(rec: EntityRef, value: Any?) {
        val schemaCtx = getTableCtx().getSchemaCtx()
        val rawService = DbDataServiceImpl(
            DbEntity::class.java,
            DbDataServiceConfig.create {
                withTable(tableRef.table)
                withIncludeBackupColumns(true)
            },
            schemaCtx
        )
        val column = getTableCtx().getAllPhysicalColumns().map { it.name }.single { it.startsWith("__backup_att") }
        TxnContext.doInTxn {
            val row = rawService.findRaw(
                Predicates.alwaysTrue(),
                emptyList(),
                DbFindPage.ALL,
                emptyList(),
                emptyList(),
                emptyList(),
                false
            ).entities.single { it[DbEntity.EXT_ID] == rec.getLocalId() }
            val updated = rawService.updateByIdIfMatches(
                row[DbEntity.ID] as Long,
                mapOf(column to row[column]),
                mapOf(column to value)
            )
            check(updated) { "The stale value was not written into '$column', so the test would prove nothing" }
        }
    }

    /**
     * The `cancel` row action, pressed on the one task this table has. Goes through
     * [DbBatchTaskCancelAction] rather than [ru.citeck.ecos.data.sql.batch.DbBatchTaskService.cancel]
     * so that the scenario is the one an administrator can actually reach from the admin view.
     */
    private fun cancelTheActiveTaskAsAnAdministrator() {
        val service = getTableCtx().getSchemaCtx().batchTaskService
        val adminDao = DbBatchTaskAdminDao(service)
        val cancelAction = DbBatchTaskCancelAction(service)
        records.register(adminDao)
        records.register(cancelAction)
        val task = TxnContext.doInTxn { service.findByTable(tableRef.table) }
            .single { !it.status.isFinal() }
        records.mutate(
            EntityRef.create(APP_NAME, cancelAction.getId(), ""),
            mapOf("recordRef" to EntityRef.create(APP_NAME, adminDao.getId(), task.id.toString()))
        )
    }

    @Test
    fun theOriginalValuesComeBackWhenTheTypeDoesTest() {

        asType(AttributeType.TEXT)
        val rec = createRecord("att" to "original")

        asType(AttributeType.NUMBER)
        createRecord("att" to 1)
        drain()

        asType(AttributeType.TEXT)
        createRecord("att" to "anything")
        drain()

        assertThat(records.getAtt(rec, "att").asText())
            .describedAs("this is the user's plan B, and it is the reason the old column is never dropped")
            .isEqualTo("original")
    }

    @Test
    fun whatWasTypedUnderTheWrongTypeIsNotThrownAwayTest() {

        asType(AttributeType.TEXT)
        val rec = createRecord("att" to "original")

        asType(AttributeType.NUMBER)
        drain()
        updateRecord(rec, "att" to 42)

        asType(AttributeType.TEXT)
        createRecord("att" to "anything")
        drain()

        val backups = TxnContext.doInTxn {
            getTableCtx().getSchemaCtx().columnMetaService.findBackups(tableRef.table, "att")
        }
        assertThat(backups.map { it.attType.asString() })
            .describedAs("the NUMBER column the user filled becomes a backup of its own; nothing is dropped")
            .contains("number")
        assertThat(backups.map { it.attType.asString() })
            .describedAs("and the text backup is gone from the list because it is the live column again")
            .doesNotContain("text")
    }

    @Test
    fun aRowWithNoOriginalIsFilledFromTheInterveningColumnTest() {

        asType(AttributeType.TEXT)
        val old = createRecord("att" to "original")

        asType(AttributeType.NUMBER)
        drain()
        val born = createRecord("att" to 42)

        asType(AttributeType.TEXT)
        createRecord("att" to "anything")
        drain()

        assertThat(records.getAtt(old, "att").asText())
            .describedAs("a row that has an original keeps it - restoration wins over conversion")
            .isEqualTo("original")
        assertThat(records.getAtt(born, "att").asText())
            .describedAs("a row with no original is filled from the intervening column")
            // "42" and not "42.0": a NUMBER is rendered the way the `float8` column's own `::text`
            // cast renders it, which is what keeps the row-by-row path and the in-place one
            // producing the same string - see DbDoubleText.
            .isEqualTo("42")
    }

    /**
     * Filling a row from the intervening column, for a child association: the same line of code as
     * an arrival, so it owes the same back-reference. A record given a child-assoc value while the
     * attribute was `TEXT[]` has nothing in `ed_associations_backup` to give it back, so the return
     * fills it from the intervening column - through the same path, which includes telling the
     * child who its parent is.
     *
     * The restored half is the control in the same fixture: a departure removes links through
     * `DbAssocsService` and never touches `_parent`, so those targets keep the back-reference they
     * never lost, and the two halves must end up saying the same thing about themselves.
     */
    @Test
    fun theChildrenStep4CreatesAreAlsoToldWhoTheirParentIsTest() {

        asChildAssoc()
        val restoredTargets = (0 until 2).map { createRecord("someKey" to "r-$it") }
        val old = createRecord("att" to restoredTargets.map { it.toString() })

        asType(AttributeType.TEXT, multiple = true)
        createRecord("someKey" to "unrelated")
        drain()

        // written while the attribute was text, so there is no parked link set for this record and
        // step 4 is the only thing that can give it links on the way back
        val newTargets = (0 until 2).map { createRecord("someKey" to "n-$it") }
        val born = createRecord("att" to newTargets.map { it.toString() })

        asChildAssoc()
        createRecord("someKey" to "unrelated-2")
        drain()

        assertThat(records.getAtt(born, "att[]?id").asStrList())
            .describedAs("step 4 filled the row that had no original of its own")
            .isEqualTo(newTargets.map { it.toString() })
        newTargets.forEachIndexed { i, target ->
            assertThat(records.getAtt(target, "_parent?id").asText())
                .describedAs("and target $i of it knows who created the link")
                .isEqualTo(born.toString())
            assertThat(records.getAtt(target, "_parentAtt").asText()).isEqualTo("att")
        }
        restoredTargets.forEachIndexed { i, target ->
            assertThat(records.getAtt(target, "_parent?id").asText())
                .describedAs(
                    "control: restored target $i has the back-reference the departure cleared and " +
                        "the return put back, so both halves of the return agree about themselves"
                )
                .isEqualTo(old.toString())
            assertThat(records.getAtt(target, "_parentAtt").asText()).isEqualTo("att")
        }
    }

    /**
     * **A child of a restricted parent stays restricted while the attribute is not an association.**
     *
     * The departure takes the links and leaves `_parent`/`_parentAtt` exactly as they were, and this
     * is the user-visible reason why. `DefaultDbPermsComponent` answers `canRead = true` /
     * `EVERYONE` for a record with no `_parent`, so a departure that cleared the back-reference
     * would publish every child of every restricted parent for as long as an administrator left the
     * attribute as text - a window of arbitrary length, opened by a type change nobody connected
     * with permissions.
     *
     * Asserted through the permission check rather than through the column, because that is the
     * consequence and the column is only how it is reached.
     */
    @Test
    fun aChildOfARestrictedParentStaysRestrictedWhileItsLinkIsParkedTest() {

        // registered because this test has to read it back through the permission check
        val someKey = AttributeDef.create {
            withId("someKey")
            withType(AttributeType.TEXT)
        }
        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("att")
                    withType(AttributeType.ASSOC)
                    withMultiple(true)
                    withConfig(ObjectData.create().set("child", true))
                },
                someKey
            )
        )
        val child = createRecord("someKey" to "c")
        val rec = createRecord("att" to listOf(child.toString()))
        setAuthoritiesWithReadPerms(rec, "owner-only")

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("att")
                    withType(AttributeType.TEXT)
                    withMultiple(true)
                },
                someKey
            )
        )
        createRecord("someKey" to "unrelated")
        drain()

        assertThat(records.getAtt(child, "_parent?id").asText())
            .describedAs("while the attribute is text the child still names its parent")
            .isEqualTo(rec.toString())
        AuthContext.runAs("a-stranger", listOf(AuthGroup.EVERYONE)) {
            assertThat(records.getAtt(child, "someKey").asText())
                .describedAs("so a stranger cannot read it, as before the type changed")
                .isEmpty()
        }

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("att")
                    withType(AttributeType.ASSOC)
                    withMultiple(true)
                    withConfig(ObjectData.create().set("child", true))
                },
                someKey
            )
        )
        createRecord("someKey" to "unrelated-2")
        drain()

        assertThat(records.getAtt(rec, "att[]?id").asStrList())
            .describedAs("the link came back out of the association backup")
            .containsExactly(child.toString())
        assertThat(records.getAtt(child, "_parent?id").asText())
            .describedAs("and the back-reference is the one the user made, never taken away")
            .isEqualTo(rec.toString())
        assertThat(records.getAtt(child, "_parentAtt").asText()).isEqualTo("att")
        AuthContext.runAs("a-stranger", listOf(AuthGroup.EVERYONE)) {
            assertThat(records.getAtt(child, "someKey").asText())
                .describedAs("and the child is restricted with its parent, as it was throughout")
                .isEmpty()
        }
    }

    /**
     * And a child whose link is parked is deletable while the attribute is text, keeping its parent
     * all the while.
     *
     * Read here rather than only in [DbColumnMigrationHandlerTest] because this file is where the
     * round trip lives: the point is that the record is deletable **and** that the parked state is
     * undone by the return, not one or the other.
     */
    @Test
    fun aChildWhoseLinkIsParkedIsDeletableWhileTheAttributeIsTextTest() {

        asChildAssoc()
        val child = createRecord("someKey" to "c")
        createRecord("att" to listOf(child.toString()))

        asType(AttributeType.TEXT, multiple = true)
        createRecord("someKey" to "unrelated")
        drain()

        records.delete(child)

        assertThat(records.getAtt(child, "_notExists?bool").asBoolean()).isTrue()
    }

    /**
     * [asType] cannot carry a config, and `child` lives in one.
     */
    private fun asChildAssoc() {
        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("att")
                    withType(AttributeType.ASSOC)
                    withMultiple(true)
                    withConfig(ObjectData.create().set("child", true))
                }
            )
        )
    }

    @Test
    fun theLinksOfTheChosenBackupComeBackAndOnlyThoseTest() {

        asType(AttributeType.ASSOC, multiple = true)
        val targets = (0 until 12).map { createRecord("someKey" to "t-$it") }
        val src = createRecord("att" to targets.map { it.toString() })

        asType(AttributeType.TEXT, multiple = true)
        createRecord("someKey" to "unrelated")
        drain()

        asType(AttributeType.ASSOC, multiple = true)
        createRecord("someKey" to "unrelated-2")
        drain()

        assertThat(records.getAtt(src, "att[]").asStrList())
            .describedAs("all twelve, not the ten the column ever cached")
            .hasSize(12)

        val schemaCtx = getTableCtx().getSchemaCtx()
        val srcId = TxnContext.doInTxn { getIdByRef(src) }
        val metas = TxnContext.doInTxn { schemaCtx.columnMetaService.findBackups(tableRef.table, "att") }
        val stillParked = TxnContext.doInTxn {
            metas.flatMap { schemaCtx.assocBackupService.findByColumnMeta(it.id, srcId) }
        }
        assertThat(stillParked)
            .describedAs("restoring consumes the backup, because the links are back where they belong")
            .isEmpty()
    }

    /**
     * The same column restored twice, with the user rewriting it in between: what comes back the
     * second time has to be what the column held when it left the second time, not the first.
     *
     * Every `asType` here is followed by a mutation, because a model change alone migrates nothing -
     * the schema is reconciled by the next mutation of the table. Without one this test would
     * change no column at all and assert a value that never left it.
     */
    @Test
    fun theFreshestMatchingBackupIsTheOneRestoredTest() {

        asType(AttributeType.TEXT)
        val rec = createRecord("att" to "first")

        asType(AttributeType.NUMBER)
        createRecord("att" to 1)
        drain()
        asType(AttributeType.TEXT)
        createRecord("att" to "x")
        drain()
        assertThat(records.getAtt(rec, "att").asText())
            .describedAs("sanity: the first restore brought the original back")
            .isEqualTo("first")
        updateRecord(rec, "att" to "second")

        asType(AttributeType.NUMBER)
        createRecord("att" to 2)
        drain()
        asType(AttributeType.TEXT)
        createRecord("att" to "y")
        drain()

        assertThat(records.getAtt(rec, "att").asText())
            .describedAs("the freshest backup of a matching type, not the oldest")
            .isEqualTo("second")
    }

    @Test
    fun aTypeThatWasNeverSeenBeforeIsAnOrdinaryMigrationTest() {

        asType(AttributeType.TEXT)
        createRecord("att" to "v")
        asType(AttributeType.BOOLEAN)
        createRecord("att" to true)
        drain()

        val backups = TxnContext.doInTxn {
            getTableCtx().getSchemaCtx().columnMetaService.findBackups(tableRef.table, "att")
        }
        assertThat(backups.map { it.attType.asString() })
            .describedAs("no BOOLEAN backup has ever existed, so there is nothing to restore")
            .containsExactly("text")
    }

    /**
     * The near miss the match rules out with its second half: a backup of the right type but
     * of the wrong arity describes a column of a different physical type, so restoring it would put
     * a `text` column where the model asks for `text[]` - and the registry row would then contradict
     * the model on every later diff, moving the column aside again and again.
     */
    @Test
    fun aBackupOfADifferentArityIsNotRestoredTest() {

        asType(AttributeType.TEXT)
        val rec = createRecord("att" to "original")

        asType(AttributeType.NUMBER)
        createRecord("att" to 1)
        drain()

        asType(AttributeType.TEXT, multiple = true)
        createRecord("att" to listOf("anything"))
        drain()

        assertThat(records.getAtt(rec, "att[]").asStrList())
            .describedAs("a single-valued backup must not be restored into a multi-valued attribute")
            .isEmpty()

        val backups = TxnContext.doInTxn {
            getTableCtx().getSchemaCtx().columnMetaService.findBackups(tableRef.table, "att")
        }
        assertThat(backups.map { it.attType.asString() to it.multiple })
            .describedAs("the single-valued original is still a backup, waiting for the arity to come back")
            .contains("text" to false)

        val chosen = TxnContext.doInTxn {
            DbColumnRestore(getTableCtx()).findRestorableBackup("att", AttributeType.TEXT, true)
        }
        assertThat(chosen)
            .describedAs("the arity is part of the match, not an afterthought of the restore itself")
            .isNull()
    }

    /**
     * The other half of the match, pinned directly on the chooser: `findBackups` answers
     * newest first and the restore takes the first match. An end-to-end scenario cannot show this -
     * a restore reclaims the backup it restores, so one attribute never accumulates two live
     * backups of the same type and arity - so the two rows are written into the registry by hand.
     */
    @Test
    fun theFreshestOfTwoMatchingBackupsIsTheOneChosenTest() {

        asType(AttributeType.TEXT)
        createRecord("att" to "v")

        val columnMetaService = getTableCtx().getSchemaCtx().columnMetaService
        fun backupRow(columnName: String): DbColumnMetaDto {
            return TxnContext.doInTxn {
                columnMetaService.save(
                    DbColumnMetaDto(
                        id = DbColumnMetaDto.NEW_REC_ID,
                        table = tableRef.table,
                        columnName = columnName,
                        attId = "att",
                        attType = DbColumnSemanticType.Model(AttributeType.TEXT),
                        multiple = false,
                        backup = true,
                        created = Instant.now(),
                        creator = "admin"
                    )
                )
            }
        }
        backupRow("__backup_att_text")
        val newer = backupRow("__backup_att_text_2")

        val found = TxnContext.doInTxn {
            DbColumnRestore(getTableCtx()).findRestorableBackup("att", AttributeType.TEXT, false)
        }
        assertThat(found?.id)
            .describedAs("the freshest backup of a matching type, not the oldest")
            .isEqualTo(newer.id)
    }

    /**
     * `__index`, `__created` and `__creator` are the link's own data, and a link given
     * back with today's timestamp, today's author or a renumbered position is a different link.
     * `DbAssocsService.createAssocs` - the path the restore has to use, because it is the one the
     * other applications hear about - writes `Instant.now()`, the creator it is handed, and an
     * index of `max + 1`, so all three have to be put back afterwards.
     *
     * The gap in the middle is what makes the index assertion mean something: a renumbering from
     * `max + 1` would turn 0, 2, 3 into 0, 1, 2 and nobody would notice from the order alone.
     *
     * The author assertion here is the weaker of the three and deliberately not the whole statement:
     * every link in this fixture has the same author, and `createAssocs` is handed that author, so
     * it would come out right even if the write-back never touched `__creator`. What it does pin is
     * that the author is not the **system** user the transfer runs as. The case that needs the
     * write-back - several authors under one attribute - is
     * [theRestoredLinksKeepTheirOwnAuthorWhenTheyDoNotShareOneTest].
     */
    @Test
    fun theRestoredLinksKeepTheIndexTimeAndAuthorTheyLeftWithTest() {

        asType(AttributeType.ASSOC, multiple = true)
        val targets = (0 until 4).map { createRecord("someKey" to "t-$it") }
        // as somebody who is not the system user the background transfer runs as, so that
        // "__creator survived" is a statement about the link and not about the two being the same
        val src = AuthContext.runAs("link-author") {
            val rec = createRecord("att" to targets.map { it.toString() })
            updateRecord(rec, "att" to listOf(targets[0], targets[2], targets[3]).map { it.toString() })
            rec
        }

        val srcId = TxnContext.doInTxn { getIdByRef(src) }
        val before = liveLinks(srcId)
        assertThat(before.map { it.index })
            .describedAs("sanity: removing the second link leaves a gap in the indexes")
            .containsExactly(0, 2, 3)

        asType(AttributeType.TEXT, multiple = true)
        createRecord("someKey" to "unrelated")
        drain()

        asType(AttributeType.ASSOC, multiple = true)
        createRecord("someKey" to "unrelated-2")
        drain()

        val after = liveLinks(srcId)
        assertThat(after.map { it.targetId })
            .describedAs("sanity: the same links came back")
            .isEqualTo(before.map { it.targetId })
        assertThat(after.map { it.index })
            .describedAs("__index is the order the user chose, not a number re-derived from max + 1")
            .isEqualTo(before.map { it.index })
        assertThat(after.map { it.created })
            .describedAs("__created is when the user made the link, not when the migration gave it back")
            .isEqualTo(before.map { it.created })
        assertThat(after.map { it.creator })
            .describedAs("__creator is who made the link, not the system user the transfer runs as")
            .isEqualTo(before.map { it.creator })
        assertThat(before.map { it.creator }.distinct())
            .describedAs("sanity: the author of the links is not the user the restore runs as")
            .doesNotContain(TxnContext.doInTxn { getIdByRef(EntityRef.create("emodel", "person", AuthUser.SYSTEM)) })
    }

    /**
     * The case the single-author fixture above cannot distinguish, and the one the `__creator`
     * write-back exists for.
     *
     * `DbColumnMigrationHandler` re-creates a record's links with **one** creator - it has one call
     * of `createAssocs` per child flag and passes `links.first().creator`, the author of the first
     * parked link, as a provisional value. Where every link shares an author that provisional value
     * is already right and `restoreAssocsMeta` has nothing to correct. Where they do not, only the
     * first link's author survives the re-creation and the rest come back attributed to somebody who
     * never made them - unless the write-back puts each one's own author back.
     *
     * Hence A, B, A: the middle link is the assertion, and the two links around it are what makes
     * the provisional value look convincing.
     */
    @Test
    fun theRestoredLinksKeepTheirOwnAuthorWhenTheyDoNotShareOneTest() {

        asType(AttributeType.ASSOC, multiple = true)
        val targets = (0 until 3).map { createRecord("someKey" to "t-$it") }

        // one link per mutation, so that each one is created under its own author: a single mutation
        // writing all three would stamp them all with whoever ran it
        val src = AuthContext.runAs("author-a") { createRecord("att" to listOf(targets[0].toString())) }
        AuthContext.runAs("author-b") {
            updateRecord(src, "att" to targets.take(2).map { it.toString() })
        }
        AuthContext.runAs("author-a") {
            updateRecord(src, "att" to targets.map { it.toString() })
        }

        val srcId = TxnContext.doInTxn { getIdByRef(src) }
        val before = liveLinks(srcId)
        assertThat(before.map { it.creator }.distinct())
            .describedAs("sanity: the links really do have two different authors, or this proves nothing")
            .hasSize(2)

        asType(AttributeType.TEXT, multiple = true)
        createRecord("someKey" to "unrelated")
        drain()

        asType(AttributeType.ASSOC, multiple = true)
        createRecord("someKey" to "unrelated-2")
        drain()

        val after = liveLinks(srcId)
        assertThat(after.map { it.targetId })
            .describedAs("sanity: the same links came back")
            .isEqualTo(before.map { it.targetId })
        assertThat(after.map { it.creator })
            .describedAs(
                "each link keeps its own author: the restore re-creates them all under the first " +
                    "one's author and has to hand every other link its own back"
            )
            .isEqualTo(before.map { it.creator })
    }

    /**
     * The assoc column is a **cache** of `ed_associations`, and this is the test that can tell
     * whether the restore rewrites it.
     *
     * The two below cannot: a departure moves the assoc column aside with its cached value intact,
     * so the column the restore renames back into place already holds the right `bigint` and would
     * read correctly even if nothing rewrote it. Here the cached value is made to disagree with the
     * links before the restore runs - which is the state the cache exists to be corrected from - and
     * the read then has to answer what `ed_associations` says, not what the column happened to hold.
     */
    @Test
    fun aRestoredAssocColumnAnswersFromTheLinksAndNotFromItsStaleCacheTest() {

        asType(AttributeType.ASSOC)
        val linked = createRecord("someKey" to "the-linked-target")
        val neverLinked = createRecord("someKey" to "never-linked")
        val src = createRecord("att" to linked.toString())

        asType(AttributeType.TEXT)
        createRecord("someKey" to "unrelated")
        drain()

        val staleId = TxnContext.doInTxn { getIdByRef(neverLinked) }
        writeIntoBackupColumn(src, staleId)

        asType(AttributeType.ASSOC)
        createRecord("someKey" to "unrelated-2")
        drain()

        assertThat(records.getAtt(src, "att?id").asText())
            .describedAs("the restored column has to be rewritten from ed_associations, not trusted")
            .isEqualTo(linked.toString())
    }

    /**
     * A single-valued assoc-like attribute keeps no list in `ed_associations` to fall back on: its
     * column **is** the read path. This pins that the value that ends up there is the one the
     * ordinary mutate path would have written through `RecMutConverter` - a `bigint` id in a `LONG`
     * column - end to end through a departure and a return.
     *
     * It does **not** pin the rewrite itself: the departure moves the column aside with its cached
     * value intact, so this would pass even if nothing rewrote the restored column. That is
     * [aRestoredAssocColumnAnswersFromTheLinksAndNotFromItsStaleCacheTest]'s job, and this one is
     * the end-to-end smoke test around it.
     */
    @Test
    fun aSingleValuedAssocComesBackThroughTheColumnThatIsItsOnlyReadPathTest() {

        asType(AttributeType.ASSOC)
        val target = createRecord("someKey" to "the-target")
        val src = createRecord("att" to target.toString())

        asType(AttributeType.TEXT)
        createRecord("someKey" to "unrelated")
        drain()

        asType(AttributeType.ASSOC)
        createRecord("someKey" to "unrelated-2")
        drain()

        assertThat(records.getAtt(src, "att?id").asText())
            .describedAs("the column cache of a single-valued assoc is what the reader answers from")
            .isEqualTo(target.toString())
    }

    /**
     * A parked link is keyed on `ed_column_meta.__column_meta_id`, and a restore raises
     * the links of *the row it restored*. Those two only meet if a registry row never stops
     * describing the column whose data it describes.
     *
     * This is the invariant itself, asserted on the one transition that can break it: the row that
     * described the association column while it was live has to be the row the departure parks that
     * column's links under. A transition that instead recycles the live row to describe the brand
     * new, empty column of the *other* type - and mints a fresh row for the data - breaks the tie
     * silently, and every later transition of the attribute recycles it further away from the links
     * it owns.
     */
    @Test
    fun theParkedLinksAreKeyedToTheRowThatDescribedTheColumnTheyLeftTest() {

        asType(AttributeType.ASSOC, multiple = true)
        val targets = (0 until 12).map { createRecord("someKey" to "t-$it") }
        val src = createRecord("att" to targets.map { it.toString() })

        val schemaCtx = getTableCtx().getSchemaCtx()
        val srcId = TxnContext.doInTxn { getIdByRef(src) }
        val assocRowId = TxnContext.doInTxn {
            schemaCtx.columnMetaService.getByTableAndColumn(tableRef.table, "att")
        }!!.id

        asType(AttributeType.TEXT, multiple = true)
        createRecord("someKey" to "unrelated")
        drain()

        val parked = TxnContext.doInTxn { schemaCtx.assocBackupService.findByColumnMeta(assocRowId, srcId) }
        assertThat(parked)
            .describedAs("the links stay with the registry row that described the column that cached them")
            .hasSize(12)

        val assocRowAfter = TxnContext.doInTxn { schemaCtx.columnMetaService.getByTable(tableRef.table) }
            .single { it.id == assocRowId }
        assertThat(assocRowAfter.attType.asString() to assocRowAfter.backup)
            .describedAs("and that row now describes the same column under its backup name")
            .isEqualTo("assoc" to true)
    }

    /**
     * Critical 2, route A continued into route B: an administrator cancels the restore task before
     * any drain tick reached it, and the attribute then changes type twice more.
     *
     * The cancel is the task row's own action and needs no race. It leaves the links of the record
     * parked and `ed_associations` empty - the known cost of cancelling a half-done restore, and
     * one an administrator can undo with `restart`. What must **not** follow from it is the links
     * becoming unreachable: while the registry row they are keyed to keeps describing the
     * association column, every later return to the type finds them again. That is the second
     * goal, and it is the whole reason the old column is never dropped.
     */
    @Test
    fun linksParkedByACancelledRestoreAreStillGivenBackByTheNextReturnToTheTypeTest() {

        asType(AttributeType.ASSOC, multiple = true)
        val targets = (0 until 12).map { createRecord("someKey" to "t-$it") }
        val src = createRecord("att" to targets.map { it.toString() })

        asType(AttributeType.TEXT, multiple = true)
        createRecord("someKey" to "unrelated")
        drain()

        // the restore queues its task; the administrator stops it before the first tick
        asType(AttributeType.ASSOC, multiple = true)
        createRecord("someKey" to "unrelated-2")
        cancelTheActiveTaskAsAnAdministrator()
        drain()

        asType(AttributeType.NUMBER)
        createRecord("someKey" to "unrelated-3")
        drain()

        asType(AttributeType.ASSOC, multiple = true)
        createRecord("someKey" to "unrelated-4")
        drain()

        assertThat(records.getAtt(src, "att[]").asStrList())
            .describedAs("a cancelled restore parks the links; it does not put them out of reach")
            .hasSize(12)
    }

    /**
     * The other half of the cancellation, in the one direction where running it destroys the
     * attribute's live value.
     *
     * A second type change cancels the task the first one queued, and performs that task's
     * departure from the association group first, so that the backup holds the links as they were
     * when the attribute left. That reasoning holds for every direction but one: when the
     * transition doing the cancelling is itself **returning** the attribute to the association
     * group, the links it would take out of `ed_associations` are the attribute's live value, and
     * the only thing that would put them back is the very task this transition queues - which
     * leaves the attribute reading empty for the whole of the background window, over a departure
     * that never needed to happen.
     */
    @Test
    fun aReturnToTheAssociationGroupDoesNotRunTheCancelledTasksDepartureTest() {

        asType(AttributeType.ASSOC, multiple = true)
        val targets = (0 until 12).map { createRecord("someKey" to "t-$it") }
        val src = createRecord("att" to targets.map { it.toString() })
        val srcId = TxnContext.doInTxn { getIdByRef(src) }

        // no drain: the task of this transition is queued and its departure has not run
        asType(AttributeType.TEXT, multiple = true)
        createRecord("someKey" to "unrelated")

        // this cancels that task - and must not strip the links it is about to depend on
        asType(AttributeType.ASSOC, multiple = true)
        createRecord("someKey" to "unrelated-2")

        assertThat(liveLinks(srcId).map { it.targetId })
            .describedAs("the links never left ed_associations, so there was nothing to give back")
            .hasSize(12)
        assertThat(records.getAtt(src, "att[]").asStrList())
            .describedAs("and the attribute reads them without waiting for a drain tick")
            .hasSize(12)
    }

    /**
     * The same smoke test, for an assoc-like type that is not ASSOC: all four share the `LONG` column.
     */
    @Test
    fun aPersonLinkComesBackTheSameWayAnAssocDoesTest() {

        asType(AttributeType.PERSON)
        val src = createRecord("att" to "emodel/person@some-user")

        asType(AttributeType.TEXT)
        createRecord("someKey" to "unrelated")
        drain()

        asType(AttributeType.PERSON)
        createRecord("someKey" to "unrelated-2")
        drain()

        assertThat(records.getAtt(src, "att?id").asText())
            .describedAs("PERSON, AUTHORITY and AUTHORITY_GROUP live in ed_associations exactly as ASSOC does")
            .isEqualTo("emodel/person@some-user")
    }

    /**
     * A restore takes "the freshest", and a registry row's **id** is not what says which one
     * that is.
     *
     * A backup row is the row that described the column while it was live, so its id is minted when
     * the column was created, not when it became a backup. Two ordinary features move a row's role
     * without minting anything: [DbColumnRestore.restore] makes an *old* row live again, so the live
     * row's id can go **down**, and a `TEXT <-> OPTIONS` change relabels a *live*
     * row's type in place without consuming any backup of the new type. Between them a low-id row
     * can become the freshest backup of a type that already has a higher-id, staler backup - and an
     * ordering by id then hands the user the stale one, for ever, because the ids never move again.
     *
     * The chain below is seven ordinary model changes and two ordinary user edits. It ends with two
     * OPTIONS backups of one attribute, of which the freshest carries the smallest id:
     *
     * ```
     * row 1|__backup_att_options_2|options|backup=true|created=...  <- freshest, holds "live-text"
     * row 3|__backup_att_options  |options|backup=true|created=...  <- stale,    holds "opt-a"
     * ```
     */
    @Test
    fun theFreshestBackupWinsEvenWhenItsRowIsTheOldestOneTest() {

        asType(AttributeType.TEXT)
        val rec = createRecord("att" to "v1")

        asType(AttributeType.NUMBER)
        createRecord("someKey" to "u1")
        drain()

        asType(AttributeType.OPTIONS)
        createRecord("someKey" to "u2")
        drain()
        updateRecord(rec, "att" to "opt-a")

        asType(AttributeType.NUMBER)
        createRecord("someKey" to "u3")
        drain()

        // the restore that makes an old row live again: the live row's id goes down
        asType(AttributeType.TEXT)
        createRecord("someKey" to "u4")
        drain()
        assertThat(records.getAtt(rec, "att").asText())
            .describedAs("sanity: the first backup came back, so its row - the oldest one - is live again")
            .isEqualTo("v1")
        updateRecord(rec, "att" to "live-text")

        // the same bytes under a new name, so the live row is relabelled where it
        // is and no backup of the new type is consumed
        asType(AttributeType.OPTIONS)
        createRecord("someKey" to "u5")
        drain()

        // and now that low-id row departs as the freshest OPTIONS backup there is
        asType(AttributeType.NUMBER)
        createRecord("someKey" to "u6")
        drain()

        val optionsBackups = TxnContext.doInTxn {
            getTableCtx().getSchemaCtx().columnMetaService.findBackups(tableRef.table, "att")
        }.filter { it.attType.asString() == "options" }
        assertThat(optionsBackups.map { it.id })
            .describedAs("sanity: there really are two OPTIONS backups and the freshest is not the newest row")
            .hasSize(2)
        assertThat(optionsBackups.maxByOrNull { it.created }!!.id)
            .describedAs("sanity: the freshest of the two carries the *smallest* id, or this proves nothing")
            .isEqualTo(optionsBackups.minOf { it.id })

        asType(AttributeType.OPTIONS)
        createRecord("someKey" to "u7")
        drain()

        assertThat(records.getAtt(rec, "att").asText())
            .describedAs(
                "the freshest backup is the one that comes back. Ordered by id it " +
                    "is the pre-NUMBER value that comes back instead, and it comes back for ever - " +
                    "the row holding what the user last typed keeps the smallest id"
            )
            .isEqualTo("live-text")
    }

    /**
     * The same defect between assoc-like types, where it is worse than a stale read: a restore raises
     * the parked links of the row it restored through `createAssocs`, which is **additive**, so
     * restoring the staler of two `ASSOC[]` backups resurrects a link set the user moved on from -
     * exactly the resurrection a departure exists to prevent, reached from a new direction.
     *
     * The shape is the scalar chain's: a `PERSON[]` backup is restored (an old row becomes live
     * again), relabelled to `ASSOC[]` by a registry-only change, and then departs - by which
     * time a higher-id row has already been an `ASSOC[]` backup for two transitions.
     */
    @Test
    fun theFreshestParkedLinkSetWinsEvenWhenItsRowIsTheOldestOneTest() {

        val people = (0 until 2).map { "emodel/person@user-$it" }
        asType(AttributeType.PERSON, multiple = true)
        val src = createRecord("att" to people)

        asType(AttributeType.TEXT, multiple = true)
        createRecord("someKey" to "u1")
        drain()

        asType(AttributeType.ASSOC, multiple = true)
        createRecord("someKey" to "u2")
        drain()
        val assocTargets = (0 until 4).map { createRecord("someKey" to "t-$it") }
        updateRecord(src, "att" to assocTargets.map { it.toString() })

        // the ASSOC[] row departs here, and it is the one with the *larger* id
        asType(AttributeType.TEXT, multiple = true)
        createRecord("someKey" to "u3")
        drain()

        // the PERSON[] row - the oldest of them all - becomes live again
        asType(AttributeType.PERSON, multiple = true)
        createRecord("someKey" to "u4")
        drain()
        assertThat(records.getAtt(src, "att[]?id").asStrList())
            .describedAs("sanity: the person links came back, so the oldest row is live again")
            .containsExactlyInAnyOrderElementsOf(people)

        // the same bigint[] under a new name, so the live row is relabelled in place
        asType(AttributeType.ASSOC, multiple = true)
        createRecord("someKey" to "u5")
        drain()

        // and now that low-id row departs as the freshest ASSOC[] backup there is
        asType(AttributeType.TEXT, multiple = true)
        createRecord("someKey" to "u6")
        drain()

        val assocBackups = TxnContext.doInTxn {
            getTableCtx().getSchemaCtx().columnMetaService.findBackups(tableRef.table, "att")
        }.filter { it.attType.asString() == "assoc" }
        assertThat(assocBackups.map { it.id })
            .describedAs("sanity: two ASSOC[] backups, each owning its own parked link set")
            .hasSize(2)
        assertThat(assocBackups.maxByOrNull { it.created }!!.id)
            .describedAs("sanity: the freshest of the two carries the *smallest* id, or this proves nothing")
            .isEqualTo(assocBackups.minOf { it.id })

        asType(AttributeType.ASSOC, multiple = true)
        createRecord("someKey" to "u7")
        drain()

        assertThat(records.getAtt(src, "att[]?id").asStrList())
            .describedAs(
                "the freshest parked link set comes back, and the " +
                    "four links the user moved on from two transitions ago stay parked"
            )
            .containsExactlyInAnyOrderElementsOf(people)
    }

    /**
     * A registry-only relabel against the backup match, and the ladder in
     * `DbDataServiceImpl.ensureColumnsExistImpl` is what decides between them: `TEXT -> OPTIONS` is
     * the same bytes with the same meaning, so the live column already holds the attribute's current
     * values and there is nothing to restore. Asking for a restore first would put a stale backup of
     * the *target* type in their place - the live values would survive, in a new backup, but the
     * attribute would read the old ones.
     *
     * This is a two-line edit away at all times (`tryRestoreColumn` sits directly below
     * `isRegistryOnlyChange`), and nothing else in the suite notices it: the ladder's order is
     * otherwise pinned by a KDoc alone.
     */
    @Test
    fun aRegistryOnlyChangeKeepsTheLiveValueInsteadOfRestoringABackupOfTheNewTypeTest() {

        asType(AttributeType.OPTIONS)
        val rec = createRecord("att" to "opt-a")

        asType(AttributeType.NUMBER)
        createRecord("someKey" to "u1")
        drain()

        asType(AttributeType.TEXT)
        createRecord("someKey" to "u2")
        drain()
        updateRecord(rec, "att" to "live-text")

        asType(AttributeType.OPTIONS)
        createRecord("someKey" to "u3")
        drain()

        assertThat(records.getAtt(rec, "att").asText())
            .describedAs(
                "the relabel renames the type, not the data: what the user typed under TEXT is what the " +
                    "attribute reads under OPTIONS, not the OPTIONS backup left behind two changes ago"
            )
            .isEqualTo("live-text")
    }

    /**
     * The same ordering, inside `G_ASSOC`, where getting it wrong resurrects links instead of a
     * string. Any move inside the group is registry-only, so the live links stay live;
     * a restore asked first would additionally raise the set parked when the attribute last left
     * the group, and `createAssocs` is additive, so the user would get both.
     */
    @Test
    fun aRegistryOnlyReturnToTheAssociationGroupDoesNotResurrectParkedLinksTest() {

        asType(AttributeType.ASSOC, multiple = true)
        val targets = (0 until 12).map { createRecord("someKey" to "t-$it") }
        val src = createRecord("att" to targets.map { it.toString() })

        asType(AttributeType.TEXT, multiple = true)
        createRecord("someKey" to "u1")
        drain()

        asType(AttributeType.PERSON, multiple = true)
        createRecord("someKey" to "u2")
        drain()
        val people = (0 until 2).map { "emodel/person@user-$it" }
        updateRecord(src, "att" to people)

        asType(AttributeType.ASSOC, multiple = true)
        createRecord("someKey" to "u3")
        drain()

        assertThat(records.getAtt(src, "att[]?id").asStrList())
            .describedAs(
                "the twelve links parked when the attribute left the group must not come " +
                    "back alongside the two it has now - a relabel restores nothing"
            )
            .containsExactlyInAnyOrderElementsOf(people)
    }

    /**
     * The cancellation, aimed at the one case it was narrowed for and at the case next door.
     *
     * A departure skipped because "the attribute is assoc-like after this transition" is skipped too
     * often: when the transition is an ordinary move aside into a
     * *different* member of the group - an ordinary `DbShadowColumnTransition.moveAside` - the new
     * `bigint[]` column arrives empty while the links the
     * cancelled task was going to park are still live in `ed_associations`. The read path then
     * answers from the column and the query path from an `EXISTS` over `ed_associations`, and they
     * disagree about the same record.
     */
    @Test
    fun aCancelledDepartureIntoADifferentAssocTypeLeavesTheReadAndTheQueryAgreeingTest() {

        asType(AttributeType.ASSOC, multiple = true)
        val targets = (0 until 12).map { createRecord("someKey" to "t-$it") }
        val src = createRecord("att" to targets.map { it.toString() })

        // The premise, asserted rather than assumed: this test ends on a pair of negatives, so a
        // setup that stopped producing links - an assoc write path that silently no-ops, a change
        // to an arrival - would satisfy it for the wrong reason.
        val srcId = TxnContext.doInTxn { getIdByRef(src) }
        assertThat(liveLinks(srcId))
            .describedAs("twelve live links before the TEXT[] leg, or the negatives below prove nothing")
            .hasSize(12)
        assertThat(
            records.query(baseQuery.copy { withQuery(Predicates.eq("att", targets[0])) }).getRecords()
        ).describedAs("and the query path finds the record by them, which is the half that must stop too")
            .containsExactly(src)

        // no drain: the task of this transition is queued and its departure has not run
        asType(AttributeType.TEXT, multiple = true)
        createRecord("someKey" to "u1")

        // PERSON[] is assoc-like, but there is no PERSON[] backup, so this is an ordinary
        // transition and the column that arrives is empty
        asType(AttributeType.PERSON, multiple = true)
        createRecord("someKey" to "u2")

        val readBack = records.getAtt(src, "att[]").asStrList()
        val queryMatches = records.query(
            baseQuery.copy { withQuery(Predicates.eq("att", targets[0])) }
        ).getRecords()
        assertThat(queryMatches to readBack.size)
            .describedAs(
                "the column holds nothing, so ed_associations must hold nothing either: a record " +
                    "that reads as having no links must not be found by a predicate on one"
            )
            .isEqualTo(emptyList<EntityRef>() to 0)
    }

    /**
     * The **restore-side** half of the same narrowing, and the stronger of the two: here the
     * cancelling transition is a restore of a *different* `G_ASSOC` row while a departure is still
     * pending, which is where the additive `createAssocs` of a restore does the damage.
     *
     * ```
     * ASSOC[]x12 -> TEXT[] (drained) -> PERSON[] (+2 user links) -> TEXT[] (restore, undrained)
     *            -> ASSOC[]
     * ```
     *
     * The last leg restores the `ASSOC[]` row and its twelve parked links. The pending `TEXT[]`
     * task is a departure of the **`PERSON[]`** row, whose two links are still live; its
     * `backupColumnMetaId` is not the row being restored, so the narrowing lets that departure run
     * and those two are parked under `__backup_att_person_multiple`. Under the coarse condition -
     * "the attribute is assoc-like after this transition" - the departure is skipped, the two stay
     * live, and the twelve are raised on top of them: **14** links where the user has 12. A departure
     * in one line, reached from the side neither committed test takes.
     *
     * Both other tests of this narrowing take the `moveAside` leg, where the arriving column is
     * empty. This one takes the restore leg, where it is not.
     */
    @Test
    fun aRestoreOfAnotherAssocRowDoesNotRaiseTheParkedLinksOntoTheLiveOnesTest() {

        asType(AttributeType.ASSOC, multiple = true)
        val targets = (0 until 12).map { createRecord("someKey" to "t-$it") }
        val src = createRecord("att" to targets.map { it.toString() })
        val srcId = TxnContext.doInTxn { getIdByRef(src) }
        assertThat(liveLinks(srcId)).describedAs("the fixture: twelve live links to move on from").hasSize(12)

        asType(AttributeType.TEXT, multiple = true)
        createRecord("someKey" to "u1")
        drain()
        assertThat(liveLinks(srcId))
            .describedAs("the departure really ran, so the twelve are parked rather than still live")
            .isEmpty()

        val people = (0 until 2).map { "emodel/person@user-$it" }
        asType(AttributeType.PERSON, multiple = true)
        createRecord("someKey" to "u2")
        drain()
        updateRecord(src, "att" to people)
        assertThat(liveLinks(srcId))
            .describedAs("and these two are the attribute's live value now - the only one it has")
            .hasSize(2)

        // a restore of the TEXT[] backup, left undrained: its task is a departure of the PERSON[]
        // row, and it is the task the next transition has to cancel
        asType(AttributeType.TEXT, multiple = true)
        createRecord("someKey" to "u3")

        asType(AttributeType.ASSOC, multiple = true)
        createRecord("someKey" to "u4")
        drain()

        assertThat(records.getAtt(src, "att[]?id").asStrList())
            .describedAs(
                "the twelve the user moved on from come back because the ASSOC[] row is " +
                    "the one restored - but not raised on top of the two person links, which the " +
                    "cancelled task's departure parked before they could be resurrected"
            )
            .containsExactlyInAnyOrderElementsOf(targets.map { it.toString() })

        val schemaCtx = getTableCtx().getSchemaCtx()
        val personBackup = TxnContext.doInTxn {
            schemaCtx.columnMetaService.findBackups(tableRef.table, "att")
        }.single { it.attType.asString() == "person" }
        val parkedPersonLinks = TxnContext.doInTxn {
            schemaCtx.assocBackupService.findByColumnMeta(personBackup.id, srcId)
        }
        assertThat(parkedPersonLinks)
            .describedAs(
                "and nothing was lost by parking them: the two are in the backup of the row that " +
                    "described the column they were cached in, which is what a later return to " +
                    "PERSON[] out of the group gives back"
            )
            .hasSize(2)
    }

    /**
     * The other half: nothing was lost by that departure - the links come back with the type.
     */
    @Test
    fun linksParkedByACancelledDepartureComeBackWithTheTypeTheyLeftTest() {

        asType(AttributeType.ASSOC, multiple = true)
        val targets = (0 until 12).map { createRecord("someKey" to "t-$it") }
        val src = createRecord("att" to targets.map { it.toString() })
        val srcId = TxnContext.doInTxn { getIdByRef(src) }

        asType(AttributeType.TEXT, multiple = true)
        createRecord("someKey" to "u1")

        asType(AttributeType.PERSON, multiple = true)
        createRecord("someKey" to "u2")
        drain()

        asType(AttributeType.TEXT, multiple = true)
        createRecord("someKey" to "u3")
        drain()

        // The links really left, which the final count alone cannot tell: twelve links that never
        // departed and stayed live in ed_associations the whole time would satisfy it too - the
        // opposite of what this test is named for.
        assertThat(liveLinks(srcId))
            .describedAs("the cancelled task's departure really ran, so nothing is live to come back on its own")
            .isEmpty()

        asType(AttributeType.ASSOC, multiple = true)
        createRecord("someKey" to "u4")
        drain()

        assertThat(records.getAtt(src, "att[]").asStrList())
            .describedAs(
                "the cancelled task's links are parked under the row that described the column " +
                    "they were cached in, which is the row a return to ASSOC[] restores"
            )
            .hasSize(12)
    }
}
