package ru.citeck.ecos.data.sql.test.records

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import ru.citeck.ecos.data.sql.batch.DbBatchTaskEngine
import ru.citeck.ecos.data.sql.columnmeta.DbColumnConversions
import ru.citeck.ecos.data.sql.columnmeta.DbColumnSemanticType
import ru.citeck.ecos.data.sql.columnmeta.DbConversionClass
import ru.citeck.ecos.data.sql.migration.column.DbColumnMigrationParams
import ru.citeck.ecos.data.sql.migration.column.DbColumnValueConverter
import ru.citeck.ecos.data.sql.repo.entity.DbEntity
import ru.citeck.ecos.data.sql.repo.find.DbFindPage
import ru.citeck.ecos.data.sql.service.DbDataServiceConfig
import ru.citeck.ecos.data.sql.service.DbDataServiceImpl
import ru.citeck.ecos.data.sql.type.DbDoubleText
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType
import ru.citeck.ecos.records2.predicate.model.Predicates
import ru.citeck.ecos.txn.lib.TxnContext
import java.time.Instant
import java.util.stream.Stream

/**
 * The two conversion paths agreeing, asserted where it can actually be broken: **every pair a
 * backend converts in place**.
 *
 * A type change takes one of two paths, chosen by the table's row count against
 * `inPlaceAlterMaxRows`: the backend rewrites the column where it stands, or the column moves aside
 * and [DbColumnValueConverter] rewrites it row by row in the background. The two have
 * to produce the same value, and a disagreement is invisible - both paths "succeed", and which
 * string the user ends up with is decided by how many rows their table happened to have.
 *
 * `DbSchemaDaoPg.getConversion` emits an expression for six model-level changes of a single-valued
 * attribute that are also class SAFE - `NUMBER`, `BOOLEAN`, `JSON`, `DATE` and `DATETIME` into
 * `TEXT`, plus `DATE -> DATETIME` - and for three of a multi-valued one, the three whose expression
 * is a plain cast and therefore survives an array: `NUMBER[]` and `BOOLEAN[]` into `TEXT[]`, and
 * `DATE[] -> DATETIME[]`. There is one test below for each of the nine, plus one that walks every
 * candidate at both arities and asserts that the set of changes taking the in-place path is exactly
 * the set compared here. Nothing here writes down what the result should look like: the in-place
 * side is produced by the real type change through whatever expression the backend chose, and the
 * row-by-row side by the converter the background handler calls, fed the same physical value the
 * handler would read.
 *
 * This class leaves `inPlaceAlterMaxRows` at its default so the type change takes the in-place path
 * - the opposite of [DbColumnMigrationHandlerTest], which pins it at zero. On a backend that does
 * not convert values in place at all (the in-memory one - see its `setColumnType`) there is no
 * second path to agree with and every test here is skipped.
 */
class DbConversionPathAgreementTest : DbRecordsTestBase() {

    companion object {

        /**
         * The changes this class compares, path against path, as `(from, to, multiple)`.
         *
         * An OPTIONS target is listed next to every TEXT one because it is the same assertion: both
         * are a `text` column, `DbSchemaDaoPg.getConversion` emits the same expression for both, and
         * [DbColumnValueConverter] answers them in one branch - `TEXT, OPTIONS ->`.
         *
         * **The array half is deliberately shorter, and the backend is why.** `ALTER ... USING` is
         * not array-aware and a subquery is forbidden in a transform expression, so only conversions
         * that are a plain cast survive the array case: `::text[]` for `float8[]` and `bool[]`, and
         * `::timestamptz[]` for `date[]`. The ones built from functions - `to_char` for a date,
         * `to_json` for a timestamp - are single-value only, and a `jsonb` column is never an array
         * at all, so `DATE`, `DATETIME` and `JSON` into text take the shadow column when the
         * attribute is multi-valued and there is no second path for them to disagree with.
         */
        private val CHANGES_WITH_AN_ASSERTED_AGREEMENT: Set<Triple<AttributeType, AttributeType, Boolean>> =
            buildSet {
                listOf(
                    AttributeType.NUMBER,
                    AttributeType.BOOLEAN,
                    AttributeType.JSON,
                    AttributeType.DATE,
                    AttributeType.DATETIME
                ).forEach { source ->
                    listOf(AttributeType.TEXT, AttributeType.OPTIONS).forEach { target ->
                        add(Triple(source, target, false))
                    }
                }
                add(Triple(AttributeType.DATE, AttributeType.DATETIME, false))

                listOf(AttributeType.NUMBER, AttributeType.BOOLEAN).forEach { source ->
                    listOf(AttributeType.TEXT, AttributeType.OPTIONS).forEach { target ->
                        add(Triple(source, target, true))
                    }
                }
                add(Triple(AttributeType.DATE, AttributeType.DATETIME, true))
            }

        /**
         * Every change that could reach the in-place branch at all: class SAFE, a real change of
         * type, and not one `DbColumnConversions.isSameStoredForm` calls free - those are taken by
         * `isRegistryOnlyChange` before the in-place branch is ever reached, so they leave no backup
         * for reasons that have nothing to do with a conversion.
         *
         * Both arities, because the array form is its own expression on the backend's side
         * (`DbSchemaDaoPg.getConversion` builds an array conversion for it) and the `LocalDate[]`
         * defect this very branch found is the reminder that arrays are where things hide.
         */
        @JvmStatic
        fun changesThatCouldBeConvertedInPlace(): Stream<Arguments> {
            return AttributeType.entries.flatMap { from ->
                AttributeType.entries.flatMap { to ->
                    listOf(false, true).mapNotNull { multiple ->
                        val safe = DbConversionMatrixTestBase.classOf(from, multiple, to, multiple) ==
                            DbConversionClass.SAFE
                        if (from != to &&
                            safe &&
                            !DbColumnConversions.isSameStoredForm(DbColumnSemanticType.Model(from), to)
                        ) {
                            Arguments.of(from, to, multiple)
                        } else {
                            null
                        }
                    }
                }
            }.stream()
        }
    }

    /**
     * `.5` and `.120` are the ones that mattered: `to_json` trims a fractional second's trailing
     * zeros where `Instant.toString` pads to three digits. The rest are the neighbouring cases.
     */
    private val timestamps = listOf(
        "2020-01-01T10:00:00.5Z",
        "2020-01-01T10:00:00.120Z",
        "2020-01-01T10:00:00.123456Z",
        "2020-01-01T10:00:00Z",
        "1999-12-31T23:59:59.999Z"
    )

    /**
     * Everyday values first, because this is not reached only by pathological data: `100` and `0`
     * disagreed. The rest are the boundaries of PostgreSQL's two formatting rules - positional
     * inside decimal exponent `[-4, 15)`, exponential outside it - and the extremes of the type.
     *
     * The last two are the divergence a later sweep found above 2^52, one at each end of the
     * rounding interval: the shortest round-trip rendering of each lands exactly on `v -/+ ulp/2`,
     * which `Double.toString` emits and `float8out` refuses. They are here because they were the
     * 0.55% of doubles above 1e16 on which the two paths silently disagreed - see [DbDoubleText].
     */
    private val numbers = listOf(
        100.0, 0.0, 1.0, 12.5, 0.1, 1e14, 1e15, 1e16, 1e-4, 1e-5, 1e-7, 1e300,
        -2.5e-13, 123456789.123456, Double.MIN_VALUE, Double.MAX_VALUE,
        58722292754477504.0, 5.299064834871378e16
    )

    @Test
    fun theSameNumberRendersIdenticallyWhicheverPathConvertsItTest() {
        assertPathsAgreeOnText(AttributeType.NUMBER, numbers)
    }

    /**
     * The same numbers in a `float8[]`, which PostgreSQL converts with `::text[]` and the converter
     * with its multiplicity wrapper. Worth its own test rather than assumed from the scalar one: the
     * wrapper is where a primitive `double[]` from one backend and a boxed `Double[]` from another
     * meet, and treating the first as a single value wrote the JVM's identity string into the user's
     * column until `asList` learned the primitive forms.
     */
    @Test
    fun theSameNumberArrayRendersIdenticallyWhicheverPathConvertsItTest() {
        assertPathsAgreeOnText(AttributeType.NUMBER, numbers, multiple = true)
    }

    @Test
    fun theSameBooleanArrayRendersIdenticallyWhicheverPathConvertsItTest() {
        assertPathsAgreeOnText(AttributeType.BOOLEAN, listOf(true, false), multiple = true)
    }

    @Test
    fun theSameTimestampRendersIdenticallyWhicheverPathConvertsItTest() {
        assertPathsAgreeOnText(AttributeType.DATETIME, timestamps.map { Instant.parse(it) })
    }

    @Test
    fun theSameDateRendersIdenticallyWhicheverPathConvertsItTest() {
        // Gregorian years only, for two reasons recorded in the
        // converter's KDoc, neither of which is the converter's rendering:
        //  - above year 9999 and before year 1 the in-place `to_char(col, 'YYYY-MM-DD')` disagrees
        //    with LocalDate.toString - `10000-01-01` against `+10000-01-01`, and a BC date loses its
        //    era altogether - which is a defect of that expression;
        //  - before the Gregorian cutover the JDBC *read* hands the converter a date two days off
        //    (`0001-01-01` in the column arrives as `0001-01-03`), which no rule in the converter
        //    could see, let alone correct.
        assertPathsAgreeOnText(AttributeType.DATE, listOf("2020-01-01", "1970-01-01", "1583-01-01", "9999-12-31"))
    }

    @Test
    fun theSameBooleanRendersIdenticallyWhicheverPathConvertsItTest() {
        assertPathsAgreeOnText(AttributeType.BOOLEAN, listOf(true, false))
    }

    @Test
    fun theSameJsonDocumentRendersIdenticallyWhicheverPathConvertsItTest() {
        assertPathsAgreeOnText(
            AttributeType.JSON,
            listOf("""{"a":1}""", """{"b":"text","a":[1,2]}""", """[1,2,3]""", """"plain"""")
        )
    }

    /**
     * The boundary of this class's subject: the pair that **looks** like one of the five above and
     * must not be converted in place at all.
     *
     * `NUMBER -> MLTEXT` is class SAFE (a scalar rendered into a string, and the string group
     * includes MLTEXT), and `DOUBLE -> TEXT` is a physical change PostgreSQL performs with
     * `ALTER ... USING col::text`. So this pair used to take the in-place path, leaving the bare
     * string `100` in a column the registry now calls MLTEXT. `DbRecordsQueryDao` rewrites an `EQ`
     * on an MLTEXT attribute into `CONTAINS '"value"'` - the **quoted** form - so a bare `100` never
     * matches `%"100"%`, and the attribute's own equality search stops finding the row for good, on
     * every table below `inPlaceAlterMaxRows`, with nothing queued to repair it. The row-by-row path
     * writes `{"en":"100"}` and gets this right, which is why `isInPlaceConversionSafe` now refuses
     * every MLTEXT target.
     *
     * The same hazard the conversion map already recognises for `TEXT/OPTIONS -> MLTEXT`, which is
     * safe *with an `UPDATE`*, which is why `isSameStoredForm` refuses to call it free - reappearing
     * on a different branch for `NUMBER`, `BOOLEAN`, `DATE` and `DATETIME`.
     */
    @Test
    fun aNumberBecomingMlTextIsStillFoundByItsOwnEqualitySearchTest() {

        val rec = givenRecordsWith(AttributeType.NUMBER, listOf(100.0)).values.single()

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("someAtt")
                    withType(AttributeType.MLTEXT)
                }
            )
        )
        // the schema is reconciled by the next mutation of the table, and this is that mutation
        createRecord("someAtt" to null)
        val schemaCtx = getTableCtx().getSchemaCtx()
        DbBatchTaskEngine(schemaCtx.dataSourceCtx).drainSchemaOnce(schemaCtx)

        assertThat(getTableCtx().getAllPhysicalColumns().map { it.name })
            .describedAs("an MLTEXT target belongs on the shadow-column path whatever the row count")
            .anyMatch { it.startsWith("__backup_") }

        val found = records.query(createQuery { withQuery(Predicates.eq("someAtt", "100")) }).getRecords()
        assertThat(found)
            .describedAs(
                "the attribute's own equality search must still find the record it found before " +
                    "the type change - an in-place col::text leaves a bare 100, which the " +
                    "CONTAINS of the quoted form an MLTEXT equality becomes can never match"
            )
            .containsExactly(rec)
    }

    @Test
    fun theSameDateBecomesTheSameInstantWhicheverPathConvertsItTest() {

        assertDatesBecomeTheSameInstants(multiple = false)
    }

    /**
     * `date[] -> timestamptz[]` is the one array conversion the backend keeps, because it is a plain
     * cast where the others are functions. Its expression also pins the session time zone, which the
     * scalar one does not need, so it is a different expression and gets its own comparison.
     */
    @Test
    fun theSameDateArrayBecomesTheSameInstantsWhicheverPathConvertsItTest() {
        assertDatesBecomeTheSameInstants(multiple = true)
    }

    private fun assertDatesBecomeTheSameInstants(multiple: Boolean) {

        assumeColumnValuesConvertedInPlace()

        val values = listOf("2020-03-01", "1970-01-01", "2024-02-29")
        val recordByValue = givenRecordsWith(AttributeType.DATE, values, multiple)
        val rowByRow = rowByRowResults(AttributeType.DATE, AttributeType.DATETIME, recordByValue, multiple)

        changeAttTypeTo(AttributeType.DATETIME, multiple)

        val inPlace = recordByValue.mapValues { readTexts(it.value, multiple).map { text -> Instant.parse(text) } }
        assertThat(inPlace)
            .describedAs(
                "a date is the same instant whichever path made a datetime of it " +
                    "(multiple=$multiple)"
            )
            .isEqualTo(rowByRow.mapValues { asInstants(it.value) })
    }

    private fun asInstants(value: Any?): List<Instant> {
        return if (value is List<*>) value.map { it as Instant } else listOf(value as Instant)
    }

    /**
     * The boundary of the nine comparisons above, asserted rather than assumed and in **both**
     * directions: which changes the backend converts in place is not this class's choice, and a rule
     * change can hand it a new one or take an old one away.
     *
     * `DbConversionMatrixTest` pins `inPlaceAlterMaxRows` at zero, so every change there takes the
     * row-by-row path and the matrix says nothing about this one. That leaves two ways to lose the
     * comparison silently. A change **gains** an in-place conversion - somebody widens a rule, it
     * becomes class SAFE and a physical change, `isInPlaceConversionSafe` lets it through - and on
     * every table below the threshold the backend's expression alone decides what the user's value
     * turns into. Or a change **loses** one, and then the nine comparisons above go on passing while
     * comparing the row-by-row path against itself, because `assumeColumnValuesConvertedInPlace` is
     * a statement about the backend and not about the change in front of it.
     *
     * So this asserts set equality, case by case: converted in place **if and only if** the change
     * is in [CHANGES_WITH_AN_ASSERTED_AGREEMENT].
     */
    @ParameterizedTest(name = "{0} -> {1}, multiple={2}")
    @MethodSource("changesThatCouldBeConvertedInPlace")
    fun theInPlacePathIsTakenByExactlyTheChangesThisClassComparesTest(
        from: AttributeType,
        to: AttributeType,
        multiple: Boolean
    ) {
        assumeColumnValuesConvertedInPlace()

        registerAtts(listOf(DbConversionMatrixTestBase.attDefOf("someAtt", from, multiple)))
        createRecord("someAtt" to DbConversionMatrixTestBase.sampleValueFor(from, multiple))

        registerAtts(listOf(DbConversionMatrixTestBase.attDefOf("someAtt", to, multiple)))
        // the schema is reconciled by the next mutation of the table, and this is that mutation
        createRecord("someAtt" to null)

        val convertedInPlace = getTableCtx().getAllPhysicalColumns().none { it.name.startsWith("__backup_") }
        val compared = Triple(from, to, multiple) in CHANGES_WITH_AN_ASSERTED_AGREEMENT
        assertThat(convertedInPlace)
            .describedAs(
                if (compared) {
                    "$from -> $to (multiple=$multiple) is compared path against path by this class, " +
                        "and that comparison only means anything while the backend really does " +
                        "convert it where it stands. It no longer does, so those tests are now " +
                        "comparing the row-by-row path against itself and passing"
                } else {
                    "$from -> $to (multiple=$multiple) is now converted by the backend where it " +
                        "stands, so what the user ends up with is decided by the backend's " +
                        "expression on a small table and by DbColumnValueConverter on a large one. " +
                        "The two have to agree, and no test in this class compares " +
                        "them for it - add one, or refuse it in isInPlaceConversionSafe"
                }
            )
            .isEqualTo(compared)
    }

    /**
     * The shared shape of every test above: create the records, ask the converter what it would
     * write for each (giving it the physical value the handler's `findRaw` would hand it), then let
     * the backend convert the column in place and compare.
     */
    private fun assertPathsAgreeOnText(source: AttributeType, values: List<Any>, multiple: Boolean = false) {

        assumeColumnValuesConvertedInPlace()

        val recordByValue = givenRecordsWith(source, values, multiple)
        val rowByRow = rowByRowResults(source, AttributeType.TEXT, recordByValue, multiple)

        changeAttTypeTo(AttributeType.TEXT, multiple)

        val inPlace = recordByValue.mapValues { readTexts(it.value, multiple) }
        assertThat(inPlace)
            .describedAs(
                "$source -> TEXT (multiple=$multiple) is class SAFE and the backend has " +
                    "an expression for it, so the same value must come out the same whichever path " +
                    "converted it - and which path a user gets is decided by their table's row " +
                    "count alone"
            )
            .isEqualTo(rowByRow.mapValues { asTexts(it.value) })
    }

    /**
     * Both sides of a text comparison as a list, so the two arities are the same assertion.
     */
    private fun readTexts(rec: ru.citeck.ecos.webapp.api.entity.EntityRef, multiple: Boolean): List<String> {
        return if (multiple) {
            records.getAtt(rec, "someAtt[]").asStrList()
        } else {
            listOf(records.getAtt(rec, "someAtt").asText())
        }
    }

    private fun asTexts(value: Any?): List<String> {
        return if (value is List<*>) value.map { it.toString() } else listOf(value.toString())
    }

    private fun givenRecordsWith(
        source: AttributeType,
        values: List<Any>,
        multiple: Boolean = false
    ): Map<Any, ru.citeck.ecos.webapp.api.entity.EntityRef> {
        registerAtts(listOf(DbConversionMatrixTestBase.attDefOf("someAtt", source, multiple)))
        return values.associateWith { createRecord("someAtt" to if (multiple) listOf(it) else it) }
    }

    /**
     * What the background handler would have written for each record, asked before the type changes.
     */
    private fun rowByRowResults(
        source: AttributeType,
        target: AttributeType,
        recordByValue: Map<Any, ru.citeck.ecos.webapp.api.entity.EntityRef>,
        multiple: Boolean = false
    ): Map<Any, Any?> {
        val stored = storedValuesByExtId()
        val params = DbColumnMigrationParams(
            attId = "someAtt",
            backupColumn = "__backup_someAtt",
            targetColumn = "someAtt",
            sourceType = DbColumnSemanticType.Model(source),
            sourceMultiple = multiple,
            targetType = target,
            targetMultiple = multiple,
            // these params never reach a transfer - the converter is called directly - and
            // `child` is a property of a link, which the converter does not create
            targetChild = false,
            targetIndexEnabled = false,
            conversionClass = DbConversionClass.SAFE,
            backupColumnMetaId = 1L
        )
        val tableCtx = getTableCtx()
        return recordByValue.mapValues { (_, rec) ->
            DbColumnValueConverter.convert(stored[rec.getLocalId()], params, tableCtx)
        }
    }

    private fun changeAttTypeTo(target: AttributeType, multiple: Boolean = false) {
        registerAtts(listOf(DbConversionMatrixTestBase.attDefOf("someAtt", target, multiple)))
        // the schema is reconciled by the next mutation of the table, and this is that mutation
        createRecord("someAtt" to null)

        assertThat(getTableCtx().getAllPhysicalColumns().map { it.name })
            .describedAs(
                "sanity: this table is far below inPlaceAlterMaxRows, so the type change has to " +
                    "have happened in place - a backup column would mean the test compared the " +
                    "row-by-row path against itself"
            )
            .noneMatch { it.startsWith("__backup_") }
    }

    /**
     * The column's physical value per record - a `Double` for NUMBER, a `Timestamp` for DATETIME and
     * so on - which is exactly what `DbColumnMigrationHandler` passes to the converter.
     */
    private fun storedValuesByExtId(): Map<String, Any?> {
        val rawService = DbDataServiceImpl(
            DbEntity::class.java,
            DbDataServiceConfig.create { withTable(tableRef.table) },
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
            ).entities.associate { (it[DbEntity.EXT_ID] as String) to it["someAtt"] }
        }
    }
}
