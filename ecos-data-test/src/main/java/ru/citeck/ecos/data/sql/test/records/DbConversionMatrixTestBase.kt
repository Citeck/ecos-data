package ru.citeck.ecos.data.sql.test.records

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Assertions.assertAll
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import org.junit.jupiter.params.provider.ValueSource
import ru.citeck.ecos.commons.data.DataValue
import ru.citeck.ecos.commons.data.MLText
import ru.citeck.ecos.commons.data.ObjectData
import ru.citeck.ecos.data.sql.batch.DbBatchTaskEngine
import ru.citeck.ecos.data.sql.columnmeta.DbAttTypeColumns
import ru.citeck.ecos.data.sql.columnmeta.DbColumnConversions
import ru.citeck.ecos.data.sql.columnmeta.DbColumnSemanticType
import ru.citeck.ecos.data.sql.columnmeta.DbConversionClass
import ru.citeck.ecos.data.sql.dto.DbColumnType
import ru.citeck.ecos.data.sql.props.DbEcosDataProps
import ru.citeck.ecos.data.sql.records.DbRecordsUtils
import ru.citeck.ecos.data.sql.repo.entity.DbEntity
import ru.citeck.ecos.data.sql.repo.find.DbFindPage
import ru.citeck.ecos.data.sql.service.DbDataServiceConfig
import ru.citeck.ecos.data.sql.service.DbDataServiceImpl
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType
import ru.citeck.ecos.records2.predicate.model.Predicates
import ru.citeck.ecos.txn.lib.TxnContext
import ru.citeck.ecos.webapp.api.entity.EntityRef
import java.time.Duration
import java.time.Instant
import java.time.ZoneOffset
import java.util.stream.Stream

/**
 * The conversion map, turned from a table into something that fails a build: for a change
 * `(from, sourceMultiple) -> (to, targetMultiple)`, what the machinery does has to be what
 * [DbColumnConversions.classify] promises.
 *
 * **Source and target multiplicity move independently**, because the narrowing rule - keep the first
 * element, leave the rest in the backup - is about `(type, true) -> (type, false)` and nothing else.
 * One flag for both sides would walk `(from, m) -> (to, m)` for ever and never reach it.
 *
 * Three statements about every change, in one fixture and under [assertAll] so one failure does not
 * hide the others:
 *
 *  1. **The original is still recoverable** - either a `__backup_` holds all of it (for an
 *     assoc-like source counted where the links are, not in a column that cached ten), or nothing
 *     moved because nothing had to ([movesNoBytes]) and the live column reads back unchanged. A
 *     change that did neither has thrown the user's data away.
 *  2. **A SAFE change transfers value for value**, not "something non-empty arrived": a value stored
 *     unfaithfully is worse than one refused, because the row is counted `processed`. Compared
 *     wherever [expectedAfter] can say what the value has to be, and where it cannot the assertion
 *     says so rather than falling back to "not null".
 *  3. **The read path never fails while a migration is pending**, because between the model change
 *     and the mutation that reconciles the schema the two disagree and a journal still has to return
 *     rows. Asserted before the other two, since a query reconciles nothing and cannot disturb them.
 *
 * [DbEcosDataProps.ColumnsProps.inPlaceAlterMaxRows] is pinned at **zero**, so every transition takes
 * the shadow-column path on both backends - the row-by-row transfer is the universal path, and a
 * backend expressing a change as one `ALTER ... USING` is optimising, not changing the class. At the
 * default, PostgreSQL would convert these small tables in place and statement 1 would be asserting
 * the backend rather than the map. Which pairs may take the in-place path is
 * [DbConversionPathAgreementTest]'s subject.
 */
abstract class DbConversionMatrixTestBase : DbRecordsTestBase() {

    companion object {

        const val ATT = "att"

        /**
         * An OPTIONS attribute is validated against the options its config declares, so both sample
         * values have to be among them.
         */
        private val OPTIONS_CONFIG: ObjectData = ObjectData.create()
            .set("source", "values")
            .set(
                "values",
                DataValue.createArr()
                    .add(DataValue.createObj().set("label", "option").set("value", "option"))
                    .add(DataValue.createObj().set("label", "option-2").set("value", "option-2"))
            )

        /**
         * The types whose column holds an `ed_record_ref` id - the ones [readAtt] has to ask for by
         * id. The same set [DbColumnValueConverter][ru.citeck.ecos.data.sql.migration.column.DbColumnValueConverter]
         * calls `REFERENCE_TYPES`.
         */
        private val REFERENCE_TYPES = setOf(
            AttributeType.ASSOC,
            AttributeType.PERSON,
            AttributeType.AUTHORITY,
            AttributeType.AUTHORITY_GROUP,
            AttributeType.ENTITY_REF
        )

        /**
         * `G_STR`: the three targets that are a `text` column and read back as a string.
         */
        private val TEXT_TARGETS = setOf(AttributeType.TEXT, AttributeType.OPTIONS, AttributeType.MLTEXT)

        /**
         * How many target ids the column of a multi-valued assoc-like attribute caches.
         * The same ten `DbRecordsMutateDao` writes and `DbColumnMigrationHandler` names, restated
         * here rather than imported because what this class needs it for is to be **above** it - see
         * [sampleValuesFor].
         */
        private const val ASSOC_COLUMN_CACHE_SIZE = 10

        /**
         * [ASSOC_COLUMN_CACHE_SIZE] distinct references and one more.
         */
        private fun referenceSamples(prefix: String): List<EntityRef> {
            return (0..ASSOC_COLUMN_CACHE_SIZE).map { EntityRef.valueOf("$prefix-$it") }
        }

        /**
         * An attribute of one type, carrying whatever definition that type needs to be writable -
         * which today is the options config and nothing else. Shared with
         * [DbConversionPathAgreementTest], which sweeps the same types under a different attribute
         * id and would otherwise have to remember the same exception.
         */
        @JvmStatic
        fun attDefOf(id: String, type: AttributeType, multiple: Boolean): AttributeDef {
            return AttributeDef.create {
                withId(id)
                withType(type)
                withMultiple(multiple)
                if (type == AttributeType.OPTIONS) {
                    withConfig(OPTIONS_CONFIG)
                }
            }
        }

        /**
         * The groups the conversion map reasons in, used to pick the representatives of the
         * reduced matrix.
         *
         * The four assoc-like members are interchangeable to every rule in the map, and so are
         * `TEXT` and `OPTIONS`. **MLTEXT is not**, and it is its own group here: `MLTEXT -> TEXT`,
         * `MLTEXT -> OPTIONS` and `MLTEXT -> JSON` are LOSSY where the rest of the string group is
         * SAFE, and `JSON -> MLTEXT` is LOSSY where `JSON -> TEXT` is SAFE. Letting `TEXT` stand for
         * it would drop the map's three most idiosyncratic pairs out of the cross-backend set.
         */
        @JvmStatic
        fun conversionGroupOf(type: AttributeType): String {
            return when (type) {
                AttributeType.TEXT, AttributeType.OPTIONS -> "G_STR_PLAIN"
                AttributeType.ASSOC, AttributeType.PERSON,
                AttributeType.AUTHORITY, AttributeType.AUTHORITY_GROUP -> "G_ASSOC"
                else -> type.name
            }
        }

        /**
         * One attribute type per physical column type, in enum order, so that the multiplicity axis
         * of the reduced matrix can be carried by eight representatives instead of the full cross
         * product. The property there is the widening and the narrowing of each physical column -
         * `text` against `text[]`, `float8` against `float8[]` - and the attribute type that sits on
         * top of it does not change what an array of it does.
         */
        @JvmStatic
        fun oneAttributeTypePerColumnType(): List<AttributeType> {
            return AttributeType.entries
                .groupBy { DbAttTypeColumns.getColumnType(it) }
                .values
                .map { it.first() }
        }

        @JvmStatic
        fun classOf(
            from: AttributeType,
            sourceMultiple: Boolean,
            to: AttributeType,
            targetMultiple: Boolean
        ): DbConversionClass {
            return DbColumnConversions.classify(
                DbColumnSemanticType.Model(from),
                sourceMultiple,
                to,
                targetMultiple
            )
        }

        /**
         * Every attribute type at both multiplicities, for the fixture check below - which has to
         * be one case per `(type, multiple)` because changing an attribute's arity mid-test is
         * itself a migration.
         */
        @JvmStatic
        fun typesAndMultiplicity(): Stream<Arguments> {
            return AttributeType.entries.flatMap { type ->
                listOf(false, true).map { multiple -> Arguments.of(type, multiple) }
            }.stream()
        }

        /**
         * Two realistic, distinct values of every attribute type, deliberately free of anything
         * pathological: what a value the target column refuses does to the transfer is
         * `DbColumnValueConverterTest`'s subject, and a matrix that fed one in would be asserting
         * that instead of the classification. The temporal ones carry no sub-second fraction for the
         * same reason - a fraction's rendering is [DbConversionPathAgreementTest]'s subject.
         *
         * **Two, because one cannot tell a narrowing from a no-op.** `[a] -> a` and
         * `[a] -> [a]` are the same observation; `[a, b] -> a` is the rule.
         *
         * **Eleven for a reference type, because two cannot tell a truncated source from a whole
         * one.** An assoc-like column is a cache of the first ten values and nothing more
         * ([ASSOC_COLUMN_CACHE_SIZE]); the values themselves are rows of `ed_associations`. A
         * fixture of two fits in that cache, so every statement about an assoc-like source held
         * whether the transfer read the column or the links - which is how "read the links, not the
         * truncated column" stayed unasserted while the transfer read the column. Eleven is the
         * smallest number that tells them apart, and `ENTITY_REF` carries the same eleven as the
         * control: its column is **not** a cache, so it has to keep answering with all of them.
         *
         * CONTENT gets one: it is never stored as an array ([DbAttTypeColumns.isMultiple]), so its
         * column is scalar whatever the model's flag says.
         */
        @JvmStatic
        fun sampleValuesFor(type: AttributeType): List<Any> {
            return when (type) {
                AttributeType.ASSOC -> referenceSamples("abc@def")
                AttributeType.PERSON -> referenceSamples("person@user")
                AttributeType.AUTHORITY -> referenceSamples("person@user")
                AttributeType.AUTHORITY_GROUP -> referenceSamples("authority-group@GROUP")
                AttributeType.ENTITY_REF -> referenceSamples("abc@def")
                AttributeType.TEXT -> listOf("text", "text-2")
                AttributeType.OPTIONS -> listOf("option", "option-2")
                AttributeType.MLTEXT -> listOf(MLText("abcd"), MLText("efgh"))
                AttributeType.NUMBER -> listOf(123, 456)
                AttributeType.BOOLEAN -> listOf(true, false)
                AttributeType.DATE -> listOf(
                    Instant.parse("2021-03-04T05:06:07Z"),
                    Instant.parse("2022-07-08T09:10:11Z")
                )
                AttributeType.DATETIME -> listOf(
                    Instant.parse("2021-03-04T05:06:07Z"),
                    Instant.parse("2022-07-08T09:10:11Z")
                )
                // documents, not strings that happen to look like documents: a JSON column takes a
                // bare string perfectly well and stores it as a JSON string node, which is a
                // different shape and a less representative one
                AttributeType.JSON -> listOf(
                    DataValue.createObj().set("aa", "bb"),
                    DataValue.createObj().set("cc", "dd")
                )
                // printable on purpose: `BINARY -> TEXT` decodes the bytes as UTF-8, and a NUL among
                // them would be refused by the target column - a value-level question this class
                // deliberately leaves to DbColumnValueConverterTest
                AttributeType.BINARY -> listOf(
                    "binary-value".toByteArray(Charsets.UTF_8),
                    "binary-value-2".toByteArray(Charsets.UTF_8)
                )
                AttributeType.CONTENT -> listOf(ContentUtils.createContentObjFromText("abc"))
            }
        }

        @JvmStatic
        fun sampleValueFor(type: AttributeType, multiple: Boolean): Any {
            val values = sampleValuesFor(type)
            return if (multiple && DbAttTypeColumns.isMultiple(type, true)) values else values.first()
        }

        /**
         * What a value of [type] has to look like once it has been written into a `text` column, or
         * null when this class deliberately does not write that rendering down.
         *
         * Only the four unambiguous ones are here. A JSON source is not: its rendering is whatever
         * the backend's own `jsonb` output is - PostgreSQL re-spaces `{"aa": "bb"}` and the in-memory
         * backend keeps the text as written - so the assertion for it compares **documents**, not
         * spellings. A BINARY source is not either: `String(bytes, UTF_8)` is what makes that pair
         * lossy, and `DbColumnValueConverterTest` owns it.
         */
        private fun renderedAsText(type: AttributeType, value: Any): String? {
            return when (type) {
                // a bare string, which the target column holds as it is or
                // wraps in an MLText without touching it
                AttributeType.TEXT, AttributeType.OPTIONS -> value.toString()
                // an integral double renders without a `.0`, the way `float8::text` renders it - see
                // DbDoubleText, and DbConversionPathAgreementTest for the values where that is hard
                AttributeType.NUMBER -> value.toString()
                AttributeType.BOOLEAN -> value.toString()
                AttributeType.DATE -> (value as Instant).atOffset(ZoneOffset.UTC).toLocalDate().toString()
                AttributeType.DATETIME -> value.toString()
                else -> null
            }
        }
    }

    init {
        dataProps = DbEcosDataProps(
            columns = DbEcosDataProps.ColumnsProps(inPlaceAlterMaxRows = 0),
            batch = DbEcosDataProps.BatchProps(batchSize = 100, batchPause = Duration.ZERO)
        )
    }

    /**
     * The fixture check the rest of the matrix rests on, and the one whose absence would be
     * invisible: if a type's sample values never reach its column, every change with that type as
     * its **source** migrates a null, and statement 1 goes on passing - there is still a backup
     * column, it is simply empty. The matrix would be exhaustive and vacuous at the same time.
     *
     * The element count is half the check: a multi-valued sample that arrived as one element would
     * make every narrowing assertion below vacuous in exactly the same way.
     */
    @ParameterizedTest(name = "{0}, multiple={1}")
    @MethodSource("ru.citeck.ecos.data.sql.test.records.DbConversionMatrixTestBase#typesAndMultiplicity")
    fun theSampleValuesOfEveryTypeAreActuallyStoredTest(type: AttributeType, multiple: Boolean) {

        val rec = givenRecordWith(type, multiple)
        val stored = readAtt(rec, type, multiple)

        if (DbAttTypeColumns.isMultiple(type, multiple)) {
            assertThat(stored.size())
                .describedAs("every sample value of $type must be stored, or every narrowing of it is vacuous")
                .isEqualTo(sampleValuesFor(type).size)
        } else {
            assertThat(stored.isNotNull())
                .describedAs("the sample value of $type must be stored, or every change from it is vacuous")
                .isTrue()
        }
    }

    /**
     * A model that did not change is not a migration. One fixture per multiplicity rather than one
     * per type: every attribute type is registered at once as its own attribute, so the statement
     * covers all fifteen and costs two type-model reconciliations instead of thirty.
     */
    @ParameterizedTest(name = "multiple={0}")
    @ValueSource(booleans = [false, true])
    fun anUnchangedModelMovesNothingTest(multiple: Boolean) {

        val defs = AttributeType.entries.map { attDefOf(attIdOf(it), it, multiple) }
        registerAtts(defs)
        val atts = ObjectData.create()
        AttributeType.entries.forEach { atts[attIdOf(it)] = sampleValueFor(it, multiple) }
        val rec = createRecord(atts)

        val before = AttributeType.entries.associateWith { readAtt(rec, it, multiple, attIdOf(it)) }

        // exactly the same model again, and the mutation that would reconcile a schema if there were
        // anything to reconcile
        registerAtts(defs)
        createRecord(attIdOf(AttributeType.TEXT) to null)
        drain()

        assertAll(
            {
                assertThat(getTableCtx().getAllPhysicalColumns().map { it.name })
                    .describedAs("an unchanged type is not a migration and must not move anything")
                    .noneMatch { it.startsWith("__backup_") }
            },
            {
                val after = AttributeType.entries.associateWith { readAtt(rec, it, multiple, attIdOf(it)) }
                assertThat(after)
                    .describedAs("and every value still reads back exactly as it was written")
                    .isEqualTo(before)
            }
        )
    }

    @ParameterizedTest(name = "{0}(multiple={1}) -> {2}(multiple={3})")
    @MethodSource("pairs")
    fun everyChangeIsMigratedAsTheMapPromisesTest(
        from: AttributeType,
        sourceMultiple: Boolean,
        to: AttributeType,
        targetMultiple: Boolean
    ) {
        val rec = givenRecordWith(from, sourceMultiple)
        val before = readAtt(rec, from, sourceMultiple)

        registerAtts(listOf(attDefOf(ATT, to, targetMultiple)))
        // statement 3's moment, and the only one there is: the model asks for one type, the column
        // holds another, and nothing has reconciled them. A query never runs a migration, so taking
        // it here cannot disturb the two statements that follow.
        val whilePending = TxnContext.doInTxn { records.query(baseQuery) }.getRecords()

        // the schema is reconciled by the next mutation of the table, and this is that mutation; it
        // carries no value for the attribute being migrated, so the transfer has nothing to do for it
        createRecord(ATT to null)
        drain()

        val backups = backupColumnsOfAtt()
        val after = readAtt(rec, to, targetMultiple)

        assertAll(
            {
                assertThat(whilePending)
                    .describedAs("$from -> $to: a journal must keep returning rows while a migration is pending")
                    .isNotEmpty()
            },
            { assertOriginalIsRecoverable(from, sourceMultiple, to, targetMultiple, rec, backups, before, after) },
            { assertTheValueTheChangeProducesIsTheOnePromised(from, sourceMultiple, to, targetMultiple, before, after) }
        )
    }

    /**
     * Statement 1.
     */
    private fun assertOriginalIsRecoverable(
        from: AttributeType,
        sourceMultiple: Boolean,
        to: AttributeType,
        targetMultiple: Boolean,
        rec: EntityRef,
        backups: List<String>,
        before: DataValue,
        after: DataValue
    ) {
        if (movesNoBytes(from, sourceMultiple, to, targetMultiple)) {
            // the same bytes, meaning the same thing to both types. There is
            // nothing to back up because nothing moves - so "the original is still recoverable" is
            // said about the live column instead, and it is the stronger half: a backup here would
            // be a full column copied aside for a change that moves no byte.
            assertThat(backups)
                .describedAs("$from -> $to stores the same bytes, so moving the column aside would cost the user an empty attribute for nothing")
                .isEmpty()
            assertThat(after)
                .describedAs("$from -> $to moves no bytes, so the value has to read back exactly as it was")
                .isEqualTo(applyArity(before, readsAsArray(from, sourceMultiple), readsAsArray(to, targetMultiple)))
            return
        }
        assertThat(backups)
            .describedAs("$from -> $to (class ${classOf(from, sourceMultiple, to, targetMultiple)}): the original value must still be recoverable")
            .isNotEmpty()

        // and "recoverable" means the value, not the column: a backup that came out empty is a
        // column kept for ever with nothing in it, and every assertion about a backup existing would
        // go on passing
        val backupValue = backupValuesByExtId(backups.single())[rec.getLocalId()]
        assertThat(backupValue)
            .describedAs("$from -> $to: the backup column has to hold what the attribute held, not just exist")
            .isNotNull()

        if (!isPhysicalArray(from, sourceMultiple)) {
            return
        }
        if (DbRecordsUtils.isStoredInAssocsTable(from)) {
            // An assoc-like column never held the value in the first place: it caches the first ten
            // target ids and the links themselves are rows of `ed_associations`. So
            // "all of it is still there" is not a statement about the backup column at all, and
            // asserting it there would assert the cache size. Where the links are depends on
            // whether this change took the attribute out of the association group:
            // parked under this backup's registry row if it did, still live if the target keeps its
            // values there too.
            assertThat(recoverableLinkCount(rec, backups.single(), to))
                .describedAs("$from -> $to: every link the attribute held has to still be somewhere, not just the ten the column cached")
                .isEqualTo(sampleValuesFor(from).size)
            return
        }
        // the half a narrowing is named lossy for: what the new column could not keep
        // is still there, all of it. A JSON column is excluded because its array lives inside
        // the document rather than in the column - see DbColumnValueConverter.convert.
        assertThat(physicalElementCount(backupValue))
            .describedAs("$from -> $to: every element the source column held has to be in the backup, not just the ones that fit")
            .isEqualTo(sampleValuesFor(from).size)
    }

    /**
     * How many of an assoc-like attribute's links are still recoverable after the change, read from
     * whichever table the change left them in.
     *
     * A departure from the association group parks them in `ed_associations_backup` under the
     * registry row of the column that moved aside; a change that keeps the attribute assoc-like -
     * a narrowing inside `G_ASSOC` - leaves them exactly where they were.
     */
    private fun recoverableLinkCount(rec: EntityRef, backupColumn: String, to: AttributeType): Int {
        val schemaCtx = getTableCtx().getSchemaCtx()
        return TxnContext.doInTxn(readOnly = true) {
            val sourceId = dbRecordRefService.getIdByEntityRef(rec)
            if (DbRecordsUtils.isStoredInAssocsTable(to)) {
                schemaCtx.assocsService.getTargetAssocs(sourceId, ATT, DbFindPage.ALL).entities.size
            } else {
                val meta = schemaCtx.columnMetaService.getByTableAndColumn(tableRef.table, backupColumn)
                    ?: error("No ed_column_meta row for backup column '$backupColumn'")
                schemaCtx.assocBackupService.findByColumnMeta(meta.id, sourceId).size
            }
        }
    }

    /**
     * Statement 2, and it is **not** gated on SAFE.
     *
     * It was, and that gate hid the narrowing entirely. Every real narrowing is
     * LOSSY - that is what the elements it leaves behind make it - so "keep the first element" was
     * asserted nowhere at all: statement 1 asserts what the *backup* kept and said nothing about
     * what the live column got. The gate is therefore on whether [expectedAfter] can say what the
     * answer is, not on the class; a narrowing whose type does not change has a defined answer
     * whatever its class, and so does a reference type read back through the column that holds its
     * id.
     *
     * The class still decides two things. A **NONE** change is the one class with an answer of its
     * own - the new column stays empty - and it is asserted here rather than left to the gate,
     * because "nothing was transferred" is a statement about a value exactly like the others and
     * nothing else in this suite makes it. And a LOSSY change whose rendering this class does not
     * write down is simply not compared - that is `DbColumnValueConverterTest`'s subject - while a
     * SAFE one has to be named as an exception.
     */
    private fun assertTheValueTheChangeProducesIsTheOnePromised(
        from: AttributeType,
        sourceMultiple: Boolean,
        to: AttributeType,
        targetMultiple: Boolean,
        before: DataValue,
        after: DataValue
    ) {
        val cls = classOf(from, sourceMultiple, to, targetMultiple)
        val expected = expectedAfter(from, sourceMultiple, to, targetMultiple, before)
        if (cls == DbConversionClass.NONE) {
            // Class NONE's whole promise, and it is a statement about a value like any other: the
            // new column stays **empty** and the backup is the only copy there will ever be
            // . Nothing asserted it until now - [expectedAfter] has no answer for a NONE
            // change, so the comparison below was skipped and the carve-out after it returned - and
            // the half that was missing is the half that matters: widen
            // `DbColumnConversions.isTransferable` to admit NONE, or drop the handler's guard on
            // it, and `CONTENT[] -> TEXT[]` writes raw `ed_content` surrogate ids into the user's
            // text column, which are from another id space and mean nothing there.
            // It is also what makes sampling NONE by source group sound: "nothing is transferred"
            // really is the same statement whatever the target is, so any target stands for the
            // rest.
            assertThat(expected)
                .describedAs("$from -> $to is class NONE, so there is no rendering to expect - add the pair to the map or to expectedAfter")
                .isNull()
            assertThat(isEmptyValue(after))
                .describedAs("$from -> $to is class NONE, so the new column has to stay empty, but it holds: $after")
                .isTrue()
            return
        }
        if (expected != null) {
            // One caveat a reader should have rather than discover: both sides are [DataValue]s, and
            // `DataValue.create` **parses** text that reads as JSON. So the number `123` and the
            // string `"123"` collapse to the same node here, and this comparison cannot tell a
            // NUMBER that stayed a number from one that became its own text. That is what makes the
            // JSON document comparison below possible at all, and it is why this class is not the
            // place the *type* of a converted value is pinned - `DbColumnValueConverterTest` asserts
            // on the converter's own return value, which is a `Double`, a `Boolean` or a `String`.
            assertThat(after)
                .describedAs("$from -> $to (class $cls): this is the value the user has to get back")
                .isEqualTo(expected)
            return
        }
        if (cls != DbConversionClass.SAFE) {
            // A LOSSY change whose rendering is not written down here - MLTEXT's carve-outs in both
            // directions (`MLTEXT -> TEXT/OPTIONS`, `MLTEXT -> JSON` and `JSON -> MLTEXT`, the
            // last of which this list used to leave unnamed and which therefore has no value
            // assertion at any level), `DATETIME -> DATE`, `BINARY -> text`.
            // The map promises only that some values may not fit, and what each of them renders to
            // is DbColumnValueConverterTest's subject.
            // Statement 1 has already asserted that the original is still recoverable for all of
            // them, which is the promise that is the map's to keep.
            return
        }
        if (from == AttributeType.JSON && to in TEXT_TARGETS) {
            // The one SAFE change whose text this class will not spell: a `jsonb` column's own
            // output is the backend's business - PostgreSQL re-spaces `{"aa": "bb"}` - so the
            // document is asserted and the spelling is DbConversionPathAgreementTest's subject.
            //
            // Exactly one element comes out, whatever either arity says, and that is not an
            // oversight: a JSON column holds **one document** whatever the model's flag says (see
            // DbColumnValueConverter.convert), so there is one value to convert and the array
            // shape - if the model asked for one - is inside it.
            assertThat(asDocuments(after))
                .describedAs("$from -> $to is SAFE, so the whole document has to come out, whatever the backend spells it like")
                .isEqualTo(listOf(storedJsonDocument(sourceMultiple)))
            return
        }
        // Everything left is SAFE and unrendered, which is `G_STR -> BINARY`.
        // `String.toByteArray(UTF_8)` is a rendering this class has no business writing down and
        // DbColumnValueConverterTest owns; what SAFE still promises here is that every element
        // arrives, and that is what is asserted.
        assertThat(from in TEXT_TARGETS && to == AttributeType.BINARY)
            .describedAs("$from -> $to is SAFE and its rendering is not defined here - add it to expectedAfter or name it as an exception")
            .isTrue()
        assertThat(elementCountOf(after, targetMultiple))
            .describedAs("$from -> $to is SAFE, so every element has to arrive even where this class does not spell the rendering")
            .isEqualTo(expectedElementsOf(from, sourceMultiple, targetMultiple).size)
    }

    /**
     * What the attribute has to read back as after a SAFE change, or null when this class does not
     * write that rendering down.
     *
     * Three kinds of answer, and none of them is computed by the code under test:
     *  - **the stored bytes mean the same thing to both types**, or the type did not change at all
     *    - so nothing is rendered and the answer is what was read before, with the arity rule
     *    applied to it: a narrowing is the first element, a widening is a
     *    one-element array. Type-agnostic on purpose - it holds for all fifteen types without a
     *    rendering for any of them;
     *  - **a text target whose rendering is written down** ([renderedAsText]), wrapped in an
     *    [MLText] when that target is MLTEXT;
     *  - **`DATE -> DATETIME`**, which is midnight UTC of the stored date.
     */
    private fun expectedAfter(
        from: AttributeType,
        sourceMultiple: Boolean,
        to: AttributeType,
        targetMultiple: Boolean,
        before: DataValue
    ): DataValue? {
        if (from == to || DbColumnConversions.isSameStoredForm(DbColumnSemanticType.Model(from), to)) {
            // no rendering is involved either way - the stored bytes mean the same thing to both
            // types, so anything that changed is the arity, and the
            // answer is what was read before presented the way the model now asks for it
            return applyArity(before, readsAsArray(from, sourceMultiple), readsAsArray(to, targetMultiple))
        }
        if (from in REFERENCE_TYPES && to in REFERENCE_TYPES) {
            // the half `isSameStoredForm` does not cover: `ENTITY_REF` and the assoc-like types share
            // the same `bigint` column of `ed_record_ref` ids and `toRefId` passes a number straight
            // through, so the id - and therefore the reference the reader resolves from it - is
            // untouched whichever way the change goes
            return applyArity(before, readsAsArray(from, sourceMultiple), readsAsArray(to, targetMultiple))
        }
        if (from in REFERENCE_TYPES && to in TEXT_TARGETS) {
            // the column holds an `ed_record_ref` id, which is an internal surrogate
            // key, and `userText` resolves it back to the reference the user actually wrote - which
            // is exactly what `?id` answered before the change. The one SAFE-looking part of a LOSSY
            // rule, and the one that hid `[J@...` in the user's column until `elementsOf` learned
            // about `long[]`.
            val kept = applyArity(before, readsAsArray(from, sourceMultiple), readsAsArray(to, targetMultiple))
            return if (to == AttributeType.MLTEXT) asMlTextDocuments(kept) else kept
        }
        if (to in TEXT_TARGETS) {
            val kept = expectedElementsOf(from, sourceMultiple, targetMultiple)
            // an MLTEXT target is the same rendering wrapped: the converter writes
            // `MLText(renderedText)` serialized, and the attribute reads that document back
            val rendered = kept.map { text ->
                val asText = renderedAsText(from, text) ?: return null
                if (to == AttributeType.MLTEXT) DataValue.create(MLText(asText)) else DataValue.create(asText)
            }
            return if (isPhysicalArray(to, targetMultiple)) DataValue.create(rendered) else rendered.first()
        }
        if (from == AttributeType.DATE && to == AttributeType.DATETIME) {
            // the one pair with an in-place expression of its own: midnight UTC of the
            // stored date, which is what both paths produce
            val kept = expectedElementsOf(from, sourceMultiple, targetMultiple)
                .map { (it as Instant).atOffset(ZoneOffset.UTC).toLocalDate().atStartOfDay(ZoneOffset.UTC).toInstant().toString() }
            return if (isPhysicalArray(to, targetMultiple)) DataValue.create(kept) else DataValue.create(kept.first())
        }
        return null
    }

    /**
     * The same texts, each wrapped the way an MLTEXT column stores one.
     */
    private fun asMlTextDocuments(texts: DataValue): DataValue {
        return if (texts.isArray()) {
            DataValue.create(texts.map { DataValue.create(MLText(it.asText())) })
        } else {
            DataValue.create(MLText(texts.asText()))
        }
    }

    /**
     * The one document a JSON column holds, which is an array when the model asked for one.
     */
    private fun storedJsonDocument(sourceMultiple: Boolean): DataValue {
        val values = sampleValuesFor(AttributeType.JSON)
        return if (sourceMultiple) DataValue.create(values) else DataValue.create(values.first())
    }

    /**
     * The arity rule applied to a value that was actually read back: narrow to the first, widen to one.
     */
    private fun applyArity(value: DataValue, sourceIsArray: Boolean, targetIsArray: Boolean): DataValue {
        return when {
            sourceIsArray && !targetIsArray -> value.get(0)
            !sourceIsArray && targetIsArray -> DataValue.createArr().add(value)
            else -> value
        }
    }

    /**
     * The source values that survive the change, before any type rendering is applied.
     */
    private fun expectedElementsOf(
        from: AttributeType,
        sourceMultiple: Boolean,
        targetMultiple: Boolean
    ): List<Any> {
        val stored = if (isPhysicalArray(from, sourceMultiple)) {
            sampleValuesFor(from)
        } else {
            sampleValuesFor(from).take(1)
        }
        return if (targetMultiple) stored else stored.take(1)
    }

    /**
     * Whether this change moves no bytes at all, which is the condition `isRegistryOnlyChange`
     * decides on: the stored form means the same thing to both types, the
     * physical column is the same column, and the map calls the whole change SAFE.
     */
    private fun movesNoBytes(
        from: AttributeType,
        sourceMultiple: Boolean,
        to: AttributeType,
        targetMultiple: Boolean
    ): Boolean {
        if (!DbColumnConversions.isSameStoredForm(DbColumnSemanticType.Model(from), to)) {
            return false
        }
        if (classOf(from, sourceMultiple, to, targetMultiple) != DbConversionClass.SAFE) {
            return false
        }
        return DbAttTypeColumns.getColumnType(from) == DbAttTypeColumns.getColumnType(to) &&
            isPhysicalArray(from, sourceMultiple) == isPhysicalArray(to, targetMultiple)
    }

    /**
     * Whether the physical column really is an array - which is what decides how many values the
     * converter is handed. Not the same question as the model's flag: a CONTENT column is a single
     * `ed_content` id whatever the flag says, and a JSON column holds one document with the array
     * inside it (see `DbColumnValueConverter.convert`).
     */
    private fun isPhysicalArray(type: AttributeType, multiple: Boolean): Boolean {
        return DbAttTypeColumns.isMultiple(type, multiple) && DbAttTypeColumns.getColumnType(type) != DbColumnType.JSON
    }

    /**
     * Whether the **read path** presents the attribute as an array, which is a different question
     * again: it follows the model's flag, so a multi-valued JSON attribute answers `att[]` with one
     * element per document even though the column holds a single one. Both backends agree on this.
     */
    private fun readsAsArray(type: AttributeType, multiple: Boolean): Boolean {
        return DbAttTypeColumns.isMultiple(type, multiple)
    }

    private fun attIdOf(type: AttributeType): String = ATT + "_" + type.name.lowercase()

    private fun givenRecordWith(type: AttributeType, multiple: Boolean): EntityRef {
        registerAtts(listOf(attDefOf(ATT, type, multiple)))
        return createRecord(ATT to sampleValueFor(type, multiple))
    }

    private fun drain() {
        val schemaCtx = getTableCtx().getSchemaCtx()
        DbBatchTaskEngine(schemaCtx.dataSourceCtx).drainSchemaOnce(schemaCtx)
    }

    /**
     * Every row's value in [backupColumn], keyed by ext id, read through the one service allowed to
     * see a backup column - [DbDataServiceConfig.includeBackupColumns]. Raw SQL would have been
     * shorter but it is PostgreSQL-only, and this has to answer the same on both backends.
     */
    private fun backupValuesByExtId(backupColumn: String): Map<String, Any?> {
        val rawService = DbDataServiceImpl(
            DbEntity::class.java,
            DbDataServiceConfig.create {
                withTable(tableRef.table)
                withIncludeBackupColumns(true)
            },
            getTableCtx().getSchemaCtx()
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

    private fun backupColumnsOfAtt(): List<String> {
        return getTableCtx().getAllPhysicalColumns().map { it.name }.filter { it.startsWith("__backup_$ATT") }
    }

    /**
     * How many elements a physical column value holds, as `findRaw` hands it over.
     *
     * Reflective rather than `is Array<*>`, because that is **false** for a primitive array: a
     * `float8[]` column arrives as a `double[]` on the in-memory backend and as a `Double[]` on
     * PostgreSQL, and only the second is an `Array<*>`. Asking the array itself how long it is works
     * for both. A scalar `bytea` never reaches here - the caller asks only where the column really
     * is an array, where a `bytea[]` is a `byte[][]` and its length is the element count.
     */
    private fun physicalElementCount(value: Any?): Int? {
        return when {
            value is Collection<*> -> value.size
            value != null && value.javaClass.isArray -> java.lang.reflect.Array.getLength(value)
            else -> null
        }
    }

    /**
     * Whether the attribute answers with nothing at all - which is how a column that was never
     * written reads back, in every shape the read path presents one in: a null for a scalar, an
     * empty array where the model asks for one, an empty string for a `text` column a backend hands
     * over as `""`, and an empty document for an MLTEXT read through `?json`.
     */
    private fun isEmptyValue(value: DataValue): Boolean {
        return value.isNull() ||
            (value.isArray() && value.size() == 0) ||
            (value.isObject() && value.size() == 0) ||
            value.asText().isEmpty()
    }

    private fun elementCountOf(value: DataValue, multiple: Boolean): Int {
        return if (multiple) {
            value.size()
        } else if (value.isNotNull()) {
            1
        } else {
            0
        }
    }

    private fun asDocuments(value: DataValue): List<DataValue> {
        val texts = if (value.isArray()) value.map { it.asText() } else listOf(value.asText())
        return texts.map { DataValue.create(it) }
    }

    /**
     * The attribute's own value, asked for in the form that type answers in.
     *
     * The `?id` on a reference type is not a convenience: a reference attribute read without a
     * scalar answers its target's **display name**, and every target in this fixture is a reference
     * to a record that does not exist, so that answer is empty for a link that is perfectly stored.
     * Reading the id asks the column what it holds.
     */
    private fun readAtt(
        rec: EntityRef,
        type: AttributeType,
        multiple: Boolean,
        attId: String = ATT
    ): DataValue {
        val scalar = when {
            type in REFERENCE_TYPES -> "?id"
            // an MLTEXT attribute read without a scalar answers the localized string when it is
            // scalar and the serialized MLText when it is an array. `?json` answers the document
            // either way, so the two arities are the same statement.
            type == AttributeType.MLTEXT -> "?json"
            else -> ""
        }
        return records.getAtt(rec, if (readsAsArray(type, multiple)) "$attId[]$scalar" else "$attId$scalar")
    }
}
