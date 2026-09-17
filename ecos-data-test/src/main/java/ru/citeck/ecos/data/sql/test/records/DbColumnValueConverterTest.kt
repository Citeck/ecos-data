package ru.citeck.ecos.data.sql.test.records

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatCode
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.assertj.core.api.Assertions.catchThrowable
import org.junit.jupiter.api.Test
import ru.citeck.ecos.data.sql.columnmeta.DbColumnSemanticType
import ru.citeck.ecos.data.sql.columnmeta.DbConversionClass
import ru.citeck.ecos.data.sql.migration.column.DbColumnMigrationParams
import ru.citeck.ecos.data.sql.migration.column.DbColumnValueConverter
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType

/**
 * The enumeration behind [DbColumnValueConverter]'s "what each target column physically refuses"
 * table, walked as a test rather than left as prose.
 *
 * Three rounds of review each closed one member of the same class and left the next standing,
 * because each fix was justified by an argument about the pairs somebody had thought of. This test
 * walks the **targets**: every [AttributeType] must appear in `TARGET_CASES` or in `NEVER_A_TARGET`,
 * every case must have a representative value that converts, and a case naming a value the column
 * cannot hold must have it refused. A new attribute type cannot be added to the platform without
 * failing [everyAttributeTypeIsAccountedForTest] until somebody has decided what its column refuses.
 *
 * It asserts the converter's own verdict, not the database's, which is what makes it portable: the
 * converter is the single place both backends go through, and that is the whole reason the rule
 * lives there. The end-to-end proof that the database really does refuse these values -
 * and that the handler then leaves one row behind instead of wedging - is in
 * [DbColumnMigrationHandlerTest], on PostgreSQL.
 */
class DbColumnValueConverterTest : DbRecordsTestBase() {

    companion object {

        /**
         * A raw string literal, so these really are the six characters of an escape, not a NUL.
         */
        private const val JSON_ESCAPED_NUL = """{"a":"b\u0000c"}"""

        /**
         * Likewise: an escaped high surrogate with no low surrogate after it.
         */
        private const val JSON_LONE_SURROGATE = """{"a":"\ud800"}"""

        /**
         * A real NUL character, which is what a bytea source renders to.
         */
        private const val TEXT_WITH_NUL = "a\u0000b"

        /**
         * A high surrogate with nothing after it - not encodable as UTF-8 in any form.
         */
        private const val TEXT_WITH_LONE_SURROGATE = "a\uD800b"

        /**
         * A correctly paired surrogate: the control that must NOT be refused.
         */
        private const val TEXT_WITH_EMOJI = "a😀b"

        /**
         * A number no `double` survives, in a form `jsonb` keeps exactly - its numbers are
         * `numeric`. Storable, and the control for the rule that the document is written as the user
         * wrote it rather than re-rendered from a parsed tree.
         */
        private const val JSON_WITH_EXACT_NUMBER = """{"price":12345678901234567.89}"""

        /**
         * `1e131071` is the largest exponent `numeric` holds; `1e131072` overflows it.
         */
        private const val JSON_WITH_HUGE_NUMBER = """{"a":1e131071}"""
        private const val JSON_WITH_OVERFLOWING_NUMBER = """{"a":1e131072}"""

        /**
         * A reference whose text is longer than the unique btree index over `ed_record_ref.__ext_id`
         * can hold - 2692 bytes is the largest that always fits.
         */
        private val REF_TEXT_TOO_LONG_TO_INDEX = "abc@" + "d".repeat(2700)

        /**
         * One row per target. `unstorable` is null where the column refuses nothing this converter
         * can produce - which is an answer; the reason belongs in the converter's table.
         *
         * `refusedBecause` is a fragment of the message the *guard* produces. Asserting only that
         * something was thrown would let a row pass for the wrong reason: delete the guard and the
         * reference-family rows below would still throw, from PostgreSQL, at the insert into
         * `ed_record_ref` - which is precisely the failure this converter exists to prevent.
         */
        private val TARGET_CASES = listOf(
            TargetCase(AttributeType.TEXT, TEXT_WITH_EMOJI, TEXT_WITH_NUL, "Value contains a NUL character"),
            TargetCase(
                AttributeType.TEXT,
                TEXT_WITH_EMOJI,
                TEXT_WITH_LONE_SURROGATE,
                "Value contains an unpaired high surrogate"
            ),
            TargetCase(AttributeType.OPTIONS, TEXT_WITH_EMOJI, TEXT_WITH_NUL, "Value contains a NUL character"),
            TargetCase(
                AttributeType.OPTIONS,
                TEXT_WITH_EMOJI,
                TEXT_WITH_LONE_SURROGATE,
                "Value contains an unpaired high surrogate"
            ),
            TargetCase(AttributeType.MLTEXT, TEXT_WITH_EMOJI, TEXT_WITH_NUL, "Value contains a NUL character"),
            TargetCase(
                AttributeType.MLTEXT,
                TEXT_WITH_EMOJI,
                TEXT_WITH_LONE_SURROGATE,
                "Value contains an unpaired high surrogate"
            ),
            TargetCase(
                AttributeType.JSON,
                """{"a":"b"}""",
                JSON_ESCAPED_NUL,
                "A JSON string value contains a NUL character"
            ),
            TargetCase(
                AttributeType.JSON,
                """{"a":"b"}""",
                JSON_LONE_SURROGATE,
                "A JSON string value contains an unpaired high surrogate"
            ),
            TargetCase(
                AttributeType.JSON,
                """{"a":"b"}""",
                """{"a":"b"} trailing""",
                "Value is not valid JSON"
            ),
            // the exponent jsonb still holds is the storable representative, so this row also says
            // that the guard is a bound rather than a fear of large numbers
            TargetCase(
                AttributeType.JSON,
                JSON_WITH_HUGE_NUMBER,
                JSON_WITH_OVERFLOWING_NUMBER,
                "is outside what a jsonb column can store"
            ),
            // NaN and the two infinities are storable - float8 takes all three - but only when the
            // user wrote them. A literal that *parses* to one is the substitution C5 was about:
            // PostgreSQL answers "is out of range for type double precision" for 1e400 and 1e-400
            // alike, so producing Infinity or 0.0 for them writes a value the column would have
            // refused
            TargetCase(AttributeType.NUMBER, "NaN", "1e400", "overflows what a float8 column can store"),
            TargetCase(
                AttributeType.NUMBER,
                "Infinity",
                "-1e400",
                "overflows what a float8 column can store"
            ),
            TargetCase(
                AttributeType.NUMBER,
                "-Infinity",
                "1e-400",
                "underflows what a float8 column can store"
            ),
            // and the control for the underflow rule: a subnormal is not zero, and float8 holds it
            TargetCase(AttributeType.NUMBER, "4.9e-324", null, null),
            TargetCase(AttributeType.BOOLEAN, "true", null, null),
            TargetCase(
                AttributeType.DATE,
                "2020-01-01T00:00:00Z",
                "+6000000-01-01T00:00:00Z",
                "outside what a date column can store"
            ),
            TargetCase(
                AttributeType.DATE,
                "2020-01-01T00:00:00Z",
                "-9999999-01-01T00:00:00Z",
                "outside what a date column can store"
            ),
            TargetCase(
                AttributeType.DATETIME,
                "2020-01-01T00:00:00Z",
                "+1000000-01-01T00:00:00Z",
                "outside what a timestamp column can store"
            ),
            TargetCase(
                AttributeType.DATETIME,
                "2020-01-01T00:00:00Z",
                "-9999999-01-01T00:00:00Z",
                "outside what a timestamp column can store"
            ),
            // a bytea takes a NUL happily; what it cannot survive is this converter's String -> bytes step
            TargetCase(
                AttributeType.BINARY,
                TEXT_WITH_NUL,
                TEXT_WITH_LONE_SURROGATE,
                "Value contains an unpaired high surrogate"
            ),
            TargetCase(
                AttributeType.ASSOC,
                "abc@def",
                TEXT_WITH_NUL,
                "Entity reference contains a NUL character"
            ),
            TargetCase(
                AttributeType.ASSOC,
                "abc@def",
                REF_TEXT_TOO_LONG_TO_INDEX,
                "carries a unique btree index"
            ),
            TargetCase(
                AttributeType.PERSON,
                "person@admin",
                TEXT_WITH_LONE_SURROGATE,
                "Entity reference contains an unpaired high surrogate"
            ),
            TargetCase(
                AttributeType.AUTHORITY,
                "person@admin",
                TEXT_WITH_NUL,
                "Entity reference contains a NUL character"
            ),
            TargetCase(
                AttributeType.AUTHORITY_GROUP,
                "authority-group@GROUP",
                TEXT_WITH_NUL,
                "Entity reference contains a NUL character"
            ),
            TargetCase(
                AttributeType.ENTITY_REF,
                "abc@def",
                TEXT_WITH_LONE_SURROGATE,
                "Entity reference contains an unpaired high surrogate"
            )
        )

        /**
         * The one type with no case: every pair involving CONTENT is class NONE, so no
         * migration task can name it as a target. Asserted by its refusal rather than omitted.
         */
        private val NEVER_A_TARGET = setOf(AttributeType.CONTENT)
    }

    private class TargetCase(
        val target: AttributeType,
        val storable: Any,
        val unstorable: Any?,
        /**
         * a fragment of the message the guard itself produces; null exactly when [unstorable] is
         */
        val refusedBecause: String?
    ) {
        /**
         * Names the row in a failure, since a target alone appears several times.
         */
        override fun toString(): String {
            return "$target <- ${abbreviate(unstorable ?: storable)}"
        }

        private fun abbreviate(value: Any): String {
            val text = value.toString()
            return if (text.length > 40) text.take(40) + "...(${text.length})" else text
        }
    }

    /**
     * The converter's own rule for a reference target, in one place: **"not empty" is not the same
     * as "a reference"**, and everything here parses into a perfectly non-empty [EntityRef].
     *
     * `EntityRef.valueOf` is subtractive. With no `@` in the string the whole of it becomes the
     * local id and the source id is blank, so prose resolves; with anything before the `@` the
     * source id is set even when the local id is blank, so `person@` resolves too. Resolving either
     * one mints an `ed_record_ref` row and hands the column its id, and the arrival then turns
     * that id into an `ed_associations` row - for a **child** association, one whose cascade delete
     * hands `recordsService.delete` a source that does not exist, leaving the record permanently
     * undeletable while the task reports the row as carried across.
     *
     * A blank **application** is the one half that is fine and common: `DbRecordRefService` defaults
     * it to the current application, which is how `abc@def` is stored. So the rule is *a source or
     * an application, and a local id*, and nothing else about the string is inspected.
     *
     * The bare Alfresco node ref is in the **accepted** list, and the two lists together are what
     * make the rule say what it means: "names a source, or an application other than this one",
     * measured on the form `DbRecordRefService.getStoredExtId` will really store. Prose comes back
     * from that as `this-app/@Отдел кадров` and is refused; a bare node ref comes back as
     * `alfresco/@workspace://...` and is not.
     */
    @Test
    fun aStringThatIsNotAReferenceIsARefusedRowRatherThanAnInventedReferenceTest() {

        registerAtts(listOf(AttributeDef.create { withId("someAtt") }))

        val referenceTargets = listOf(
            AttributeType.ASSOC,
            AttributeType.PERSON,
            AttributeType.AUTHORITY,
            AttributeType.AUTHORITY_GROUP,
            AttributeType.ENTITY_REF
        )
        val refused = listOf(
            "Отдел кадров" to "names neither a source nor an application",
            "just some prose, not a reference" to "names neither a source nor an application",
            // a bare login, which is what a free-text "responsible" column holds and what a PERSON
            // attribute accepts through the ordinary write path, storing it as `this-app/@admin`.
            // Refused here deliberately: it is a reference to nothing, `_notExists` is true of it,
            // and for a child association the same shape is what makes a record undeletable. The map
            // calls the pair class C, and class C's promise is a row left behind, named in a
            // warning, with the original in the backup - rather than a column full of references
            // that resolve to nothing, reported as carried across
            "admin" to "names neither a source nor an application",
            "Иванов Иван" to "names neither a source nor an application",
            "abc@" to "names no record",
            "emodel/person@" to "names no record"
        )
        refused.forEach { (value, because) ->
            referenceTargets.forEach { target ->
                assertThatThrownBy { convert(value, target) }
                    .describedAs("$target <- '$value'")
                    .hasMessageContaining(because)
            }
        }

        // and the forms that really are references still convert, including the one whose
        // application the ref service fills in and the **bare** Alfresco node ref, which has no `@`
        // in it at all and which `DbRecordRefService.fixEntityRef` rewrites into `alfresco/@...` -
        // an addressable reference the ordinary write path stores, so the guard is measured on the
        // stored form and compared against this application's own name rather than against blank
        listOf(
            "abc@def",
            "emodel/person@admin",
            "alfresco/@workspace://SpacesStore/2ff0f0a1",
            "workspace://SpacesStore/2ff0f0a1"
        ).forEach { value ->
            referenceTargets.forEach { target ->
                assertThatCode { convert(value, target) }
                    .describedAs("$target <- '$value'")
                    .doesNotThrowAnyException()
            }
        }
    }

    private fun params(target: AttributeType) = DbColumnMigrationParams(
        attId = "someAtt",
        backupColumn = "__backup_someAtt_text",
        targetColumn = "someAtt",
        sourceType = DbColumnSemanticType.Model(AttributeType.TEXT),
        sourceMultiple = false,
        targetType = target,
        targetMultiple = false,
        // these params never reach a transfer - the converter is called directly - and
        // `child` is a property of a link, which the converter does not create
        targetChild = false,
        targetIndexEnabled = false,
        conversionClass = DbConversionClass.LOSSY,
        backupColumnMetaId = 1L
    )

    private fun convert(value: Any?, target: AttributeType): Any? {
        return DbColumnValueConverter.convert(value, params(target), getTableCtx())
    }

    /**
     * An array column arrives as a **primitive** array from one backend and as a boxed one from
     * another, and `is Array<*>` is false for the primitive form.
     *
     * A `float8[]` column is a `double[]` out of the in-memory backend and a `Double[]` out of
     * pgjdbc; a `bool[]` is a `boolean[]` or a `Boolean[]`; a `bigint[]` is a `long[]` or a `Long[]`.
     * Before this was handled, the multiplicity wrapper treated a primitive array as a single value
     * and the whole array went through `toText`, so `NUMBER[] -> TEXT[]` wrote one element reading
     * `[D@615b5480` - the JVM's identity string - into the user's column, and the row was counted
     * `processed`. This converter is the path every backend shares, so how a driver
     * chooses to box an array cannot be allowed to change what it produces.
     *
     * `bytea` is deliberately not in the list: a scalar `bytea` value **is** a `ByteArray` and is one
     * value, not a list of bytes. A `bytea[]` arrives as `byte[][]`, which is an `Array<*>` already.
     */
    @Test
    fun aPrimitiveArrayIsAsManyValuesAsABoxedOneTest() {

        registerAtts(listOf(AttributeDef.create { withId("someAtt") }))

        val boxedAndPrimitive = listOf(
            arrayOf(1.0, 2.0) to doubleArrayOf(1.0, 2.0),
            arrayOf(true, false) to booleanArrayOf(true, false),
            arrayOf(1L, 2L) to longArrayOf(1L, 2L)
        )
        boxedAndPrimitive.forEach { (boxed, primitive) ->
            val params = params(AttributeType.TEXT).copy(sourceMultiple = true, targetMultiple = true)
            assertThat(DbColumnValueConverter.convert(primitive, params, getTableCtx()))
                .describedAs("a ${primitive.javaClass.simpleName} has to convert to what a ${boxed.javaClass.simpleName} converts to")
                .isEqualTo(DbColumnValueConverter.convert(boxed, params, getTableCtx()))
        }
    }

    /**
     * The narrowing rule - **keep the first element** - which for a long time no test at any level
     * touched: the only two cases that set `sourceMultiple` also set `targetMultiple`, so
     * [DbColumnValueConverter]'s narrowing branch had no coverage at all.
     *
     * That is the direction the rule is actually about, and it is where the primitive-array defect
     * would have been worst: `NUMBER[] -> TEXT` narrowing wrote the whole array through `toText`, so
     * the user's scalar column got `[D@615b5480` and the row was counted `processed` - and the
     * widening direction, which is what the matrix axis used to reach, would not have shown it.
     *
     * Every reachable array shape is walked, primitive and boxed, with **two** elements, because one
     * element cannot tell "keeps the first" from "keeps the array".
     */
    @Test
    fun aNarrowingKeepsTheFirstElementAndOnlyThatTest() {

        registerAtts(listOf(AttributeDef.create { withId("someAtt") }))

        val narrowing = params(AttributeType.TEXT).copy(sourceMultiple = true, targetMultiple = false)
        val storedForms = listOf<Pair<Any, String>>(
            doubleArrayOf(1.0, 2.0) to "1",
            booleanArrayOf(true, false) to "true",
            longArrayOf(10L, 20L) to "10",
            intArrayOf(10, 20) to "10",
            arrayOf(1.0, 2.0) to "1",
            arrayOf("a", "b") to "a",
            listOf("a", "b") to "a"
        )
        storedForms.forEach { (stored, expected) ->
            assertThat(DbColumnValueConverter.convert(stored, narrowing, getTableCtx()))
                .describedAs("a ${stored.javaClass.simpleName} narrows to its first element")
                .isEqualTo(expected)
        }
    }

    /**
     * The other half of the rule above: a `bytea` is one value even though it is a `ByteArray`.
     */
    @Test
    fun aByteArrayIsOneValueAndNotAListOfBytesTest() {

        registerAtts(listOf(AttributeDef.create { withId("someAtt") }))

        val params = params(AttributeType.TEXT).copy(sourceMultiple = true, targetMultiple = true)
        assertThat(DbColumnValueConverter.convert("ab".toByteArray(Charsets.UTF_8), params, getTableCtx()))
            .describedAs("a bytea column's value is one value; splitting it would make a text of every byte")
            .isEqualTo(listOf("ab"))
    }

    @Test
    fun everyAttributeTypeIsAccountedForTest() {

        registerAtts(listOf(AttributeDef.create { withId("someAtt") }))

        assertThat(TARGET_CASES.map { it.target }.toSet() + NEVER_A_TARGET)
            .describedAs(
                "every attribute type must be a row in the converter's table - one saying the " +
                    "column refuses nothing is an answer, one nobody looked at is the defect this " +
                    "test exists to prevent"
            )
            .isEqualTo(AttributeType.entries.toSet())

        NEVER_A_TARGET.forEach { target ->
            assertThatThrownBy { convert("anything", target) }
                .describedAs("$target is never a migration target and must say so rather than guess")
                .hasMessageContaining("never a migration target")
        }
    }

    @Test
    fun everyTargetAcceptsItsRepresentativeStorableValueTest() {

        registerAtts(listOf(AttributeDef.create { withId("someAtt") }))

        TARGET_CASES.forEach { case ->
            assertThatCode { convert(case.storable, case.target) }
                .describedAs("${case.target}: this value is storable and must not be refused")
                .doesNotThrowAnyException()
            assertThat(convert(case.storable, case.target))
                .describedAs("${case.target}: a storable value must produce a value, not null")
                .isNotNull()
        }
    }

    @Test
    fun everyTargetRefusesWhatItsColumnCannotHoldTest() {

        registerAtts(listOf(AttributeDef.create { withId("someAtt") }))

        val checked = TARGET_CASES.filter { it.unstorable != null }
        assertThat(checked)
            .describedAs("sanity: the table has to name some unstorable values for this to mean anything")
            .isNotEmpty
        checked.forEach { case ->
            // catchThrowable rather than assertThatThrownBy, because assertThatThrownBy runs its
            // throw-check *before* describedAs is attached: a row that fails by not throwing at all
            // then reports "Expecting code to raise a throwable" with no clue which row it was
            val because = case.refusedBecause
                ?: error("$case names a value the column cannot hold but not the guard that refuses it")
            val thrown = catchThrowable { convert(case.unstorable, case.target) }
            assertThat(thrown)
                .describedAs(
                    "$case: this is a value the column cannot hold, so it has to be refused here - " +
                        "carried as far as the write it fails the whole batch and the task retries " +
                        "that window for ever"
                )
                .isInstanceOf(Exception::class.java)
                // and refused by the guard, not by something else that happens to throw: without
                // the message every reference-family row here would pass just as well on a
                // PostgreSQL error raised at the insert the guard exists to prevent
                .hasMessageContaining(because)
        }
    }

    /**
     * C4: `jsonb` takes every one of these, so nothing here can be caught by a refusal - only by
     * looking at what comes out. A parsed-and-re-rendered document is not the user's document:
     * Jackson builds a `DoubleNode` for every floating-point token, which rounded
     * `12345678901234567.89`, turned `1e2` into `100.0`, flattened `1.0e-400` to `0.0` and rendered
     * `1e400` as the *string* `"Infinity"` - a different JSON type from the one the user had.
     *
     * The end-to-end proof that this is also what the in-place `::jsonb` cast stores is in
     * [DbColumnMigrationHandlerTest], on PostgreSQL. This one is the rule itself: the bytes out are
     * the bytes in.
     */
    @Test
    fun aJsonDocumentIsWrittenAsTheUserWroteItRatherThanReRenderedTest() {

        registerAtts(listOf(AttributeDef.create { withId("someAtt") }))

        listOf(
            JSON_WITH_EXACT_NUMBER,
            JSON_WITH_HUGE_NUMBER,
            """{"a":1e2}""",
            """{"a":1.0e-400}""",
            """{ "spaced" : [ 1, 2 ] }""",
            """{"a":"$TEXT_WITH_EMOJI"}"""
        ).forEach { document ->
            assertThat(convert(document, AttributeType.JSON))
                .describedAs(
                    "the document is validated by parsing and then written as it stands: " +
                        "re-rendering it is a second opinion about the user's data, and the two paths " +
                        "requires the same bytes the in-place cast would have received"
                )
                .isEqualTo(document)
        }

        assertThat(convert(listOf(JSON_WITH_EXACT_NUMBER, """1e2""", null), AttributeType.JSON))
            .describedAs("and every element of an array source reaches the document as its own original text")
            .isEqualTo("""[$JSON_WITH_EXACT_NUMBER,1e2,null]""")
    }

    /**
     * The bound in the converter's JSON row, walked at both ends. `jsonb` stores numbers as
     * `numeric`, which holds 131072 digits before the decimal point and 16383 after it - measured on
     * `postgres:17.11` and `postgres:12.6`, where `1e131071` and `1e-16383` store and `1e131072` and
     * `1e-16384` both answer `ERROR: value overflows numeric format`.
     *
     * The limits are on digits rather than on the exponent, and PostgreSQL raises rather than
     * rounding into range, which is why this is a refusal rather than anything cleverer.
     */
    @Test
    fun theJsonNumbersNumericHoldsAreCarriedAcrossAndTheOthersRefusedTest() {

        registerAtts(listOf(AttributeDef.create { withId("someAtt") }))

        listOf(
            """{"a":1e131071}""",
            """{"a":1e-16383}""",
            """{"a":-1.5e3}""",
            """{"a":[0,-0.0,12345678901234567.89]}"""
        ).forEach { document ->
            assertThatCode { convert(document, AttributeType.JSON) }
                .describedAs("$document is a number a jsonb column holds and must not be refused")
                .doesNotThrowAnyException()
        }

        listOf(
            """{"a":1e131072}""",
            """{"a":1e-16384}""",
            // an exponent that does not even fit in an Int, which the scan saturates rather than
            // failing to read
            """{"a":1e99999999999}""",
            """{"a":1e-99999999999}"""
        ).forEach { document ->
            assertThatThrownBy { convert(document, AttributeType.JSON) }
                .describedAs("$document overflows numeric, and PostgreSQL raises on it server-side")
                .hasMessageContaining("is outside what a jsonb column can store")
        }

        // and a number *written out* in more than 1000 characters is refused earlier still, by
        // Jackson's StreamReadConstraints, even where numeric would have held it. A conservative
        // refusal: one row left behind with the original in the backup, and nothing to disagree
        // with, every pair with a JSON target being class LOSSY and so never converted in place
        assertThatThrownBy { convert("""{"a":0.${"0".repeat(16382)}1}""", AttributeType.JSON) }
            .describedAs("a number literal longer than the parser accepts is left behind, not stored as a guess")
            .hasMessageContaining("Value is not valid JSON")
    }

    @Test
    fun aPairedSurrogateIsNotRefusedTest() {

        registerAtts(listOf(AttributeDef.create { withId("someAtt") }))

        assertThat(convert(TEXT_WITH_EMOJI, AttributeType.TEXT))
            .describedAs("the rule is about what cannot be encoded, not about non-BMP characters")
            .isEqualTo(TEXT_WITH_EMOJI)
        assertThat(convert("""{"a":"$TEXT_WITH_EMOJI"}""", AttributeType.JSON) as String)
            .describedAs("and the same inside a JSON document")
            .contains(TEXT_WITH_EMOJI)
    }
}
