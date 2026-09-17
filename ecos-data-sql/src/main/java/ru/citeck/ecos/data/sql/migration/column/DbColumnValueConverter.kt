package ru.citeck.ecos.data.sql.migration.column

import com.fasterxml.jackson.core.JsonToken
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import ru.citeck.ecos.commons.data.DataValue
import ru.citeck.ecos.commons.data.MLText
import ru.citeck.ecos.commons.json.Json
import ru.citeck.ecos.data.sql.columnmeta.DbColumnSemanticType
import ru.citeck.ecos.data.sql.context.DbTableContext
import ru.citeck.ecos.data.sql.records.refs.DbRecordRefEntity
import ru.citeck.ecos.data.sql.type.DbDoubleText
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType
import ru.citeck.ecos.webapp.api.entity.EntityRef
import java.sql.Timestamp
import java.time.Instant
import java.time.LocalDate
import java.time.ZoneOffset

/**
 * Converts one stored value from the type a backup column holds into the type its replacement wants.
 *
 * Expressed in application code rather than as an SQL expression, because the row-by-row path is the
 * universal fallback: a backend that can express nothing still performs every conversion identically.
 * A backend-specific single-`UPDATE` is an optimisation, and it must produce exactly what this class
 * produces.
 *
 * The values it is handed are the ones `findRaw` produces - the column's physical type, not the
 * attribute's: a DATETIME column arrives as a [Timestamp], a DATE as a [LocalDate], an array column
 * as an array. What it returns goes back through the same type converter on the way in.
 *
 * Throws whenever a value cannot be converted. The caller counts it and carries on.
 *
 * ## What each target column physically refuses
 *
 * The question is what the **target column** refuses, not what the source held - reasoning from the
 * source left this class with the same defect three times. Every [AttributeType] that can be asked
 * for is below; a row saying "nothing" is an answer, a missing target is not, and
 * `DbColumnValueConverterTest` walks the enum against this table.
 *
 * | target | column | refuses | guarded by |
 * |---|---|---|---|
 * | TEXT, OPTIONS, MLTEXT | `text` | NUL; unpaired surrogate | [toTextColumnValue] -> [requireStorable] |
 * | JSON | `jsonb` | invalid JSON; NUL or unpaired surrogate anywhere in it; a number `numeric` cannot hold. Also, conservatively, three documents `jsonb` would have taken - Jackson refuses a number literal over 1000 characters, nesting over 1000 deep and a string over 20 MB | [requireStorableJsonText] -> [requireStorableJsonStrings], [requireStorableJsonNumbers] |
 * | NUMBER | `float8` | a literal outside the type's range, where `Double.parseDouble` would answer `Infinity` or `0.0`. `NaN`, the infinities as the user spelled them, and subnormals are accepted | [toStorableDouble] |
 * | BOOLEAN | `boolean` | **nothing** - only `true` and `false` are produced. The parse is stricter than PostgreSQL's own, which is a conservative refusal rather than a substitution | [convertScalar] |
 * | DATE | `date` | a year outside 4713 BC..5874897 AD | [requireStorableInstant] |
 * | DATETIME | `timestamptz` | a year outside 4713 BC..294276 AD | [requireStorableInstant] |
 * | BINARY | `bytea` | **nothing** - any byte sequence. But `String.toByteArray(UTF_8)` substitutes for an unpaired surrogate, so that is refused rather than written mangled | [requireEncodable] |
 * | ASSOC, PERSON, AUTHORITY, AUTHORITY_GROUP, ENTITY_REF | `bigint` | nothing the `bigint` refuses - the value is an id. What refuses is the side effect: registering the reference writes its text into `ed_record_ref.__ext_id`, a `text` column under a unique index, so NUL, an unpaired surrogate and a text over 2692 bytes are refused | [toRefId] -> [requireStorable], [requireIndexableExtId] |
 * | CONTENT | - | never a target: every pair with it is class NONE | [convertScalar] |
 *
 * **"Refuses" is not the whole question.** A value the column accepts but does not store faithfully
 * is the same defect in disguise, and worse, because the row is then counted `processed`. What
 * reaches the column has to be what an in-place conversion of the same value would have put there.
 *
 * ## What each branch does to a value it accepts
 *
 * | branch | its own step | verdict |
 * |---|---|---|
 * | TEXT, OPTIONS from a text source | identity | pass-through |
 * | TEXT, OPTIONS from NUMBER | [DbDoubleText] | **rendered to match** `column::text`; `Double.toString` did not (`100.0` against `100`) |
 * | TEXT, OPTIONS from DATETIME | [renderInstant] | **rendered to match** the in-place `to_json` expression, which trims a fraction's trailing zeros. They agree for years 1..9999; outside that the in-place expression is the one that is wrong - it answers `0002-01-01T00:00:00 BCZ` for a BC instant - and the converter must not be bent to match a malformed string |
 * | TEXT, OPTIONS from DATE | `LocalDate.toString` | faithful, and agrees with `to_char(column, 'YYYY-MM-DD')` for years 1..9999. Outside that they differ, which is a defect of that expression |
 * | TEXT, OPTIONS from BOOLEAN / JSON | `toString` / identity | agree with `column::text` |
 * | TEXT, OPTIONS from BINARY | `String(bytes, UTF_8)` | substitutes `U+FFFD` for invalid UTF-8 - deliberate, and what class LOSSY means here |
 * | TEXT, OPTIONS from a reference | [userText] | resolves the `ed_record_ref` id to the reference text: the id is an internal surrogate key, not the user's data. A dangling id is a row left behind, never a fabricated one |
 * | MLTEXT from a non-MLTEXT source | wraps the text in an [MLText] | a wrapping, not a substitution. `isInPlaceConversionSafe` refuses these targets outright, and has to: `col::text` would leave a bare string in a column the registry calls MLTEXT, which an MLTEXT equality can never match |
 * | MLTEXT from an MLTEXT source | identity | **must not wrap** - the value already is a serialized MLText, and a second wrapping is corruption counted `processed`. Only multiplicity changes reach here |
 * | JSON | validate by parsing, write the **source text** | cannot re-render by construction |
 * | NUMBER | [toStorableDouble] | an out-of-range literal is refused, not saturated |
 * | BOOLEAN | `lowercase` then an exact match | an unrecognised spelling is a row left behind |
 * | DATE | truncates the time of day, at UTC | **truncation on purpose**: `DATETIME -> DATE` is LOSSY and that is what it means |
 * | DATETIME | [toInstant] | faithful. `DATE -> DATETIME` has an in-place expression, and midnight UTC is what both produce |
 * | BINARY | `String.toByteArray(UTF_8)` | substitutes `?` for an unpaired surrogate, so that is refused instead |
 * | reference targets | the id passes through; a text is resolved through [EntityRef] | `EntityRef.valueOf` is subtractive and `fixEntityRef` prepends the app name - a normalization no in-place path competes with |
 * | CONTENT | refused outright | - |
 *
 * ## Multiplicity, and the one place the promise does not hold
 *
 * The multiplicity wrapper in [convert] narrows an array to its first element - a documented loss of
 * the rest, recoverable because they stay in the backup column. **Except for a JSON column.**
 *
 * A JSON column is physically scalar whatever the model says, because no backend emits `JSONB[]`, so
 * the array lives inside the document. Three consequences, all pinned by `DbConversionMatrixTest`:
 *
 *  - a JSON **target** ignores the multiplicity wrapper, which is why both directions are safe;
 *  - a JSON **source** is always one value, so `JSON[] -> TEXT[]` produces a single text holding the
 *    whole array document rather than one text per element. Nothing is lost by it;
 *  - `JSON[] -> JSON` is a registry-only change, so there is **no backup column at all**. The
 *    attribute reads the first document and the rest stay in the live column, unreachable until the
 *    model is widened back. Not a defect: moving a full column aside for a change that alters no
 *    byte would cost the user an empty attribute until a transfer copied the values onto themselves.
 *
 * Every row saying "rendered to match" or "agree" is asserted end to end against the backend's own
 * expression by `DbConversionPathAgreementTest`.
 */
object DbColumnValueConverter {

    /**
     * Strict, unlike [Json.mapper]: `Json.mapper.read` falls back to reading an unparseable string
     * as a JSON string node, so it can never tell a caller that a value is not JSON. Here that
     * answer is the whole point - PostgreSQL's `jsonb` will not accept a value this rejects, and a
     * value handed to it anyway would fail the whole batch instead of the one row it belongs to.
     */
    private val strictJson = ObjectMapper().enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)

    private const val NUL_CHAR = '\u0000'

    /**
     * PostgreSQL's temporal range, in astronomical year numbering, and the bound every backend is
     * held to - see the enumeration in this class's KDoc for why the *narrowest* backend's limit is
     * the portable one.
     *
     * `timestamptz` runs 4713 BC .. 294276 AD and `date` runs 4713 BC .. 5874897 AD; 4713 BC is
     * astronomical year -4712. All four boundaries were measured on `postgres:17.11`:
     * `timestamptz` takes `294276-12-31 23:59:59+00` and refuses `294277-01-01`, `date` takes
     * `5874897-12-31` and refuses `5874898-01-01`, and both take `4713-01-01 BC` and refuse
     * `4714-01-01 BC`. A whole year is therefore storable at each end, so comparing the year alone
     * with `year < MIN || year > max` is **exact**: it refuses nothing those years would have taken.
     *
     * The year is read at UTC, and that is the right zone on both branches rather than a residual
     * risk. A DATE is bound as a zone-free `LocalDate` derived from the same UTC instant, so no zone
     * enters at all. A DATETIME is range-checked by PostgreSQL **after** the offset is applied, on
     * the instant: `'294277-01-01 13:00:00+14'::timestamptz` is accepted and stored as
     * `294276-12-31 23:00:00+00`, while `'294277-01-01 00:00:00+00'` is refused. So the server and
     * this check are comparing the same thing, and a value near a boundary cannot be judged by the
     * neighbouring year in one and not the other.
     */
    private const val MIN_TEMPORAL_YEAR = -4712
    private const val MAX_TIMESTAMP_YEAR = 294276
    private const val MAX_DATE_YEAR = 5874897

    /**
     * PostgreSQL's `numeric`, which is what a `jsonb` column stores a JSON number as: at most 131072
     * digits before the decimal point and 16383 after it. See [requireStorableJsonNumber] for the
     * measurements.
     */
    private const val MAX_NUMERIC_INT_DIGITS = 131072
    private const val MAX_NUMERIC_FRAC_DIGITS = 16383

    /**
     * The longest `ed_record_ref.__ext_id`, in UTF-8 bytes, that the unique btree index over that
     * column is **guaranteed** to accept - see [requireIndexableExtId] for the measurement.
     */
    private const val MAX_REF_EXT_ID_BYTES = 2692

    /**
     * The attribute types whose column holds an `ed_record_ref` id rather than a value the user
     * would recognise - see [userText].
     */
    private val REFERENCE_TYPES = setOf(
        AttributeType.ASSOC,
        AttributeType.PERSON,
        AttributeType.AUTHORITY,
        AttributeType.AUTHORITY_GROUP,
        AttributeType.ENTITY_REF
    )

    @JvmStatic
    fun convert(value: Any?, params: DbColumnMigrationParams, tableCtx: DbTableContext): Any? {
        if (value == null) {
            return null
        }
        if (params.targetType == AttributeType.JSON) {
            // A JSON column is physically scalar whatever the model's multiple flag says - no
            // backend emits `JSONB[]` - so the shape lives inside the document instead of in the
            // column, and the multiplicity rules below have nothing to do here. That is also why
            // both multiplicity directions are safe for a JSON target.
            return toJsonText(value)
        }
        if (params.sourceMultiple && !params.targetMultiple) {
            // keep the first element; the rest stay in the backup, which is why narrowing
            // is classified lossy rather than refused
            val first = asList(value).firstOrNull() ?: return null
            return convertScalar(first, params, tableCtx)
        }
        if (!params.sourceMultiple && params.targetMultiple) {
            return listOf(convertScalar(value, params, tableCtx))
        }
        if (params.targetMultiple) {
            return asList(value).map { convertScalar(it, params, tableCtx) }
        }
        return convertScalar(value, params, tableCtx)
    }

    /**
     * The elements of an array column's value, or the value itself where it holds only one.
     */
    private fun asList(value: Any?): List<Any?> {
        return elementsOf(value) ?: listOf(value)
    }

    /**
     * The elements of a value that really is several values, or null when it is one - and **the**
     * place this class decides that question, so that no caller can answer it a second time and get
     * a different answer. It used to be answered twice: here, and spelled out again as
     * `value is Collection<*> || value is Array<*>` inside [toJsonText], which is one
     * classification away from being reached by a primitive array (what keeps it out today is only
     * every non-string source into JSON being class NONE).
     *
     * **Four of the primitive forms are not a defensive flourish**, and three of them are. A
     * `float8[]` column arrives as a `double[]` from the in-memory backend and as a `Double[]` from
     * pgjdbc, and `is Array<*>` is **false** for the primitive one. Without [DoubleArray] the whole
     * array fell through to the single-value branch and `NUMBER[] -> TEXT[]` wrote one element
     * reading `[D@615b5480` into the user's column, counted `processed`. The same holds for
     * [LongArray] (`bigint[]`, which is every reference type and every assoc cache), [BooleanArray]
     * (`bool[]`) and [IntArray] (`int[]`). This converter is the path every backend
     * shares, so how a driver boxes an array cannot be allowed to change what it produces.
     *
     * [FloatArray], [ShortArray] and [CharArray] are the flourish, and they are kept deliberately
     * rather than removed: no [ru.citeck.ecos.data.sql.dto.DbColumnType] maps to `float`, `short` or
     * `char`, so no column can hand one over today. `DbColumnValueConverterTest`'s array sweep is
     * accurate when it says it walks "every reachable array shape" and stops at four - these three
     * are not reachable and could not be asserted without inventing a column type for them. They
     * cost one `when` branch each and they mean that a backend which one day hands over a `short[]`
     * is right by construction instead of writing `[S@...` into a user's column.
     *
     * [ByteArray] is deliberately absent. A `bytea` column's value **is** a `ByteArray` and is one
     * value; splitting it would make a separate value of every byte. A `bytea[]` arrives as
     * `byte[][]`, which is an `Array<*>` and is taken by the branch above it.
     */
    private fun elementsOf(value: Any?): List<Any?>? {
        return when (value) {
            is Collection<*> -> value.toList()
            is Array<*> -> value.toList()
            is DoubleArray -> value.toList()
            is BooleanArray -> value.toList()
            is LongArray -> value.toList()
            is IntArray -> value.toList()
            is FloatArray -> value.toList()
            is ShortArray -> value.toList()
            is CharArray -> value.toList()
            else -> null
        }
    }

    private fun convertScalar(value: Any?, params: DbColumnMigrationParams, tableCtx: DbTableContext): Any? {
        value ?: return null
        return when (params.targetType) {
            AttributeType.TEXT, AttributeType.OPTIONS -> toTextColumnValue(userText(value, params, tableCtx))
            // an MLTEXT column is a TEXT column holding the serialized MLText, the same form
            // DbTypesConverter writes and DbRecord reads back - so a value that came out of an
            // MLTEXT column is already in it and is passed through. Wrapping it again produces
            // `{"en":"{\"en\":\"hello\",\"ru\":\"privet\"}"}`: the user's document turned into a
            // JSON string wearing an English label, counted `processed` and reported as success.
            // The only pairs that reach here with an MLTEXT source are the multiplicity changes,
            // and neither of them is allowed to touch a value.
            AttributeType.MLTEXT -> if (isMlTextSource(params)) {
                toTextColumnValue(value)
            } else {
                Json.mapper.toString(MLText(toTextColumnValue(userText(value, params, tableCtx))))
                    ?: error("MLText serialization failed for '${toText(value)}'")
            }
            AttributeType.JSON -> toJsonText(value)
            AttributeType.NUMBER -> toStorableDouble(toText(value))
            AttributeType.BOOLEAN -> toText(value).lowercase().let {
                when (it) {
                    "true" -> true
                    "false" -> false
                    else -> error("Not a boolean: '$it'")
                }
            }
            AttributeType.DATE -> requireStorableInstant(
                toInstant(value).atZone(ZoneOffset.UTC).toLocalDate().atStartOfDay(ZoneOffset.UTC).toInstant(),
                MAX_DATE_YEAR,
                "date"
            )
            AttributeType.DATETIME -> requireStorableInstant(toInstant(value), MAX_TIMESTAMP_YEAR, "timestamp")
            // a bytea takes any byte sequence, NUL included - but this String -> bytes step does
            // not, see requireEncodable
            AttributeType.BINARY -> requireEncodable(toText(value), "Value").toByteArray(Charsets.UTF_8)
            AttributeType.ENTITY_REF, AttributeType.ASSOC, AttributeType.PERSON,
            AttributeType.AUTHORITY, AttributeType.AUTHORITY_GROUP -> toRefId(value, tableCtx)
            AttributeType.CONTENT -> error("CONTENT is never a migration target - it is always class NONE")
        }
    }

    /**
     * Whether the value came out of a column that already holds a serialized [MLText].
     *
     * A [DbColumnSemanticType.Raw] source does not count: the registry never recorded what those
     * bytes mean, so treating them as an already-serialized MLText would be a guess, and
     * the wrapping branch is the conservative one - it produces a well-formed MLText whatever the
     * source held.
     */
    private fun isMlTextSource(params: DbColumnMigrationParams): Boolean {
        return (params.sourceType as? DbColumnSemanticType.Model)?.attType == AttributeType.MLTEXT
    }

    /**
     * A reference's stored form is a `Long` id into `ed_record_ref`. Coming from a reference type the
     * value is already one; coming from a string it has to be resolved, and a string that is not a
     * well-formed [EntityRef] is a row left behind rather than a silently created reference.
     *
     * **"Well-formed" has to mean more than "not empty".** [EntityRef.valueOf] is purely subtractive:
     * given a string with no `@`, it puts the whole string in the local id and leaves the source
     * blank, so ordinary prose - a department name, a legacy code - parses into a non-empty
     * reference. Resolving one mints an `ed_record_ref` row and hands the column its id, and for a
     * **child** association that id becomes an `ed_associations` row whose cascade delete has no
     * source to delete from: the record becomes permanently undeletable, and the task reports the row
     * as carried across. The ordinary write path refuses to create that state, so the migration must
     * refuse it too.
     *
     * **The rule: a reference names a source, or an application other than this one.** Measured on
     * the form [DbRecordRefService.getStoredExtId] will really store, then compared against *this*
     * application's name. Measured on the parsed ref instead it would refuse a bare Alfresco node
     * ref, which the ref service has a branch of its own for; measured on the stored form without
     * the comparison it would refuse nothing, because a blank application is defaulted to this one.
     * A blank application with a source is fine and common - `abc@def`.
     *
     * The same reasoning refuses a reference with a blank **local id** (`person@`): it names no
     * record, and the back-reference a child arrival writes would be a mutation of a blank local id,
     * which the records service reads as "create a new record".
     *
     * This is class LOSSY working as designed - the row is left alone, named in a warning, with its
     * original still in the backup, so reverting the type gives it back. The guard is shared with the
     * `ENTITY_REF` target, which wants the same thing of a string and has no associations to make it
     * visible.
     *
     * **What it still refuses that the ordinary write path would take**, named so the asymmetry is a
     * decision rather than an oversight: a plain association accepts `Отдел кадров` and a `PERSON`
     * attribute accepts a bare login. Both are references to nothing, and for a child association
     * the same value is what makes a record permanently undeletable. A row left where it was, with
     * the original in the backup, is strictly better than a column full of references that resolve
     * to nothing, every one reported as carried across.
     */
    private fun toRefId(value: Any?, tableCtx: DbTableContext): Long {
        if (value is Number) {
            return value.toLong()
        }
        // The id itself is a bigint and refuses nothing. This guards the side effect: resolving the
        // reference writes its text form into ed_record_ref, and EntityRef.valueOf is purely
        // subtractive, so anything unstorable in the text below would reach that text column.
        val text = requireStorable(toText(value), "Entity reference")
        val ref = EntityRef.valueOf(text)
        if (EntityRef.isEmpty(ref)) {
            error("Not an entity reference: '$text'")
        }
        val refsService = tableCtx.getRecordRefsService()
        // The text the service will really store: fixEntityRef prepends an app name when the
        // reference has none, and rewrites a bare Alfresco node ref into `alfresco/@...`.
        val storedExtId = refsService.getStoredExtId(ref)
        val storedRef = EntityRef.valueOf(storedExtId)
        if (storedRef.getSourceId().isBlank() &&
            storedRef.getAppName() == tableCtx.getSchemaCtx().dataSourceCtx.appName
        ) {
            error(
                "Not an entity reference: '$text' names neither a source nor an application, so " +
                    "it is a value that does not fit the target type rather than a reference to " +
                    "be created"
            )
        }
        if (storedRef.getLocalId().isBlank()) {
            // `person@` clears the empty check the moment anything precedes the `@`, and it refers
            // to no record at all. Left alone it is worse than untidy: the back-reference a child
            // arrival writes would mutate a reference with a blank local id, which the records
            // service reads as "create a new record", so the migration would manufacture empty
            // records and link to something that is not them.
            error("Not an entity reference: '$text' names no record")
        }
        // and this guards its size, which is the other thing that column refuses - measured on the
        // stored text, because that is the string the unique index sees
        requireIndexableExtId(storedExtId)
        return refsService.getOrCreateIdByEntityRefs(listOf(ref)).first()
    }

    /**
     * The size rule for `ed_record_ref.__ext_id`, whose unique btree index `DbRecordRefEntity`
     * declares is what actually refuses a value here - the `bigint` this converter returns refuses
     * nothing, but the reference has to be registered before there is an id to return.
     *
     * Measured on `postgres:17.11` (and confirmed on the `postgres:12.6` the tests run against) with
     * a `text` column under a unique btree index, inserting incompressible values one byte at a time:
     * 2692 bytes inserts and 2693 answers
     * `ERROR: index row size 2712 exceeds btree version 4 maximum 2704 for index "r_ext_idx"`,
     * server-side. The arithmetic behind it is index-tuple overhead: 8 bytes of header plus a 4-byte
     * varlena header on top of the value, aligned to 8, against the one-third-of-a-page maximum.
     *
     * **Why the bound is the guaranteed one rather than the largest that ever fits.** PostgreSQL
     * tries `pglz` on an index value over 512 bytes, so a *compressible* value well past 2692 bytes
     * also inserts - the same probe stored 100000 bytes of `repeat('a', ...)` without complaint. A
     * bound that depends on how compressible the user's data happens to be is not a bound the
     * in-memory backend could ever agree with, and the two paths have to agree value for value,
     * so the portable rule is the largest size that fits whatever the content.
     *
     * Without this the row is not lost - `saveAtomicallyOrGetExistingByExtId` inserts inside
     * `TxnContext.doInNewTxn` on a connection of its own, so only that transaction is aborted and
     * the throw arrives in the handler's per-row catch as one row left behind rather than as the
     * wedged batch an aborted outer transaction would have been. What it costs is agreement: the same
     * `TEXT -> ASSOC` (always LOSSY, so it always comes here) would be a row left behind on
     * PostgreSQL and a stored value in memory.
     */
    private fun requireIndexableExtId(extId: String): String {
        val bytes = extId.toByteArray(Charsets.UTF_8).size
        if (bytes > MAX_REF_EXT_ID_BYTES) {
            error(
                "Entity reference is $bytes bytes long, and ed_record_ref.__ext_id carries a unique " +
                    "btree index, whose rows cannot exceed $MAX_REF_EXT_ID_BYTES bytes of value. " +
                    "The original is kept in the backup column"
            )
        }
        return extId
    }

    /**
     * The value as a JSON *document*, ready for a `jsonb` column.
     *
     * **Validated by parsing, but what is written is the source text.** The document PostgreSQL
     * receives here has to be the one an in-place `::jsonb` cast would have received, byte for byte
     *, and re-rendering a parsed tree is a second opinion about how the user's data
     * should look rather than the user's data: `readTree` builds a `DoubleNode` for every
     * floating-point token, so `12345678901234567.89` came back as `1.2345678901234568E16`, `1e2`
     * as `100.0`, `1.0e-400` as `0.0` - the value annihilated - and `1e400` as the **string**
     * `"Infinity"`, a different JSON type from the one the user had. A `jsonb` column takes every
     * one of those, so the row was counted `processed` and the migration reported success while the
     * user's number was quietly rewritten. Keeping the text makes the two paths agree by
     * construction instead of by a rendering rule somebody has to keep in step.
     *
     * The parse still earns its keep: it is what rejects a document that is not valid JSON, and what
     * feeds the storability checks below.
     *
     * An array source becomes a JSON array - `TEXT[] -> JSON` is lossy, and the
     * shape change free precisely because the document can hold it. Every element has to be valid
     * JSON in its own right, and reaches the document as **its own** original text, for the same
     * reason: this is the case the old unconditional `::jsonb` cast blew up on, inside the user's
     * mutation, and here it is one row left behind instead.
     */
    private fun toJsonText(value: Any): String {
        val elements = elementsOf(value)
        if (elements != null) {
            // a comma-separated list of valid JSON documents inside brackets is a valid JSON array,
            // so the elements are joined rather than re-rendered through a tree
            return elements.joinToString(",", "[", "]") { element ->
                if (element == null) {
                    "null"
                } else {
                    requireStorableJsonText(toText(element))
                }
            }
        }
        return requireStorableJsonText(toText(value))
    }

    /**
     * Parses [text] to decide whether a `jsonb` column can hold it, and returns [text] itself - see
     * [toJsonText] for why the source text is what gets written.
     */
    private fun requireStorableJsonText(text: String): String {
        val node = try {
            strictJson.readTree(text)
        } catch (e: Exception) {
            // a number literal longer than Jackson's StreamReadConstraints allows (1000 characters
            // by default) arrives here too. That is a *conservative* refusal - PostgreSQL's numeric
            // would take up to 131072 digits - and it costs the user nothing beyond one row left
            // behind with the original still in the backup, because every pair with a JSON target is
            // class LOSSY and so never has an in-place path to disagree with
            throw IllegalStateException("Value is not valid JSON: '$text'", e)
        }
        // an empty or whitespace-only document parses into the missing node rather than throwing,
        // and an empty string is not something a jsonb column accepts either
        if (node == null || node.isMissingNode) {
            error("Value is not valid JSON: '$text'")
        }
        // valid JSON is not the same as storable JSON - see requireStorable
        requireStorableJsonStrings(node)
        requireStorableJsonNumbers(text)
        return text
    }

    /**
     * **A value the target column cannot physically hold is rejected here, before it is ever bound -
     * and never re-encoded.**
     *
     * **Why here and not around the write:** a statement PostgreSQL rejects aborts the whole
     * transaction, so no `catch` at the write could let the rest of the batch carry on - it would be
     * a failed batch, a rolled-back cursor and, with unlimited attempts by default, the same window
     * retried for ever behind the table's lock. Refused here it is one row left behind, which is
     * what class LOSSY promises, with the original untouched in the backup.
     *
     * **Why rejected and not re-encoded** (Base64, escaping, clamping): no in-place SQL expression
     * would produce that, and the two paths have to agree value for value.
     *
     * This one refuses what a text-family or `jsonb` column cannot hold: a NUL, on top of whatever
     * [requireEncodable] already refuses. Both are server-side errors -
     * `invalid byte sequence for encoding "UTF8": 0x00` for a raw NUL, `unsupported Unicode escape
     * sequence` for `\u0000` inside a JSON document.
     *
     * Every pair that can carry one here - `BINARY -> text`, `text -> JSON`, `TEXT -> ASSOC` - is
     * class LOSSY, so `isInPlaceConversionSafe` always refuses it and the value always arrives here
     * rather than at an in-place `ALTER`. This guard cannot be somebody else's job.
     *
     * **Deliberately not this case:** invalid UTF-8 in a `bytea` source. [toText] turns those into
     * `U+FFFD`, an ordinary character the column stores without complaint - lossy, which the class
     * already says, rather than unstorable. Worth stating next to [requireEncodable], which *does*
     * refuse a string UTF-8 cannot encode: the two look alike and only one is rejected by the column.
     */
    private fun requireStorable(text: String, what: String): String {
        requireEncodable(text, what)
        val nulIdx = text.indexOf(NUL_CHAR)
        if (nulIdx >= 0) {
            error(
                "$what contains a NUL character at index $nulIdx, which no text or jsonb column can " +
                    "store. The original is kept in the backup column"
            )
        }
        return text
    }

    /**
     * The half of [requireStorable] that is not about any particular column: a string that cannot be
     * encoded as UTF-8 at all, which is an unpaired surrogate.
     *
     * Separate because `bytea` needs it and does **not** need the NUL rule - a byte column takes a
     * `0x00` quite happily, and refusing one there would reject a value that stores perfectly well.
     * What `bytea` cannot survive is this converter's own `String.toByteArray(UTF_8)` step, which
     * *silently substitutes* `?` for an unpaired surrogate rather than failing. A mangled value
     * written into the user's column is worse than a stalled task, so it is refused instead.
     *
     * A properly paired surrogate - every non-BMP character, every emoji - is untouched by this.
     */
    private fun requireEncodable(text: String, what: String): String {
        var i = 0
        while (i < text.length) {
            val ch = text[i]
            if (Character.isHighSurrogate(ch)) {
                if (i + 1 >= text.length || !Character.isLowSurrogate(text[i + 1])) {
                    error(
                        "$what contains an unpaired high surrogate at index $i, which is not " +
                            "encodable as UTF-8. The original is kept in the backup column"
                    )
                }
                i += 2
                continue
            }
            if (Character.isLowSurrogate(ch)) {
                error(
                    "$what contains an unpaired low surrogate at index $i, which is not encodable " +
                        "as UTF-8. The original is kept in the backup column"
                )
            }
            i++
        }
        return text
    }

    /**
     * [requireStorable] applied to every string a jsonb column would have to store: the string values
     * of the document, and its field names, both of which PostgreSQL refuses a NUL or a lone
     * surrogate in.
     *
     * Checked on the **parsed tree** rather than on the rendered document, for two reasons. A scan of
     * the rendered text could not tell `\u0000` (an escape) from `\\u0000` (a literal backslash
     * followed by `u0000`, which stores perfectly well). And the defect does not depend on what the
     * serializer chooses to emit: Jackson re-emits an escaped NUL as an escape, which PostgreSQL
     * refuses, and re-emits an escaped lone surrogate as the raw character, which is not encodable at
     * all. Guarding the value covers both without depending on either.
     */
    private fun requireStorableJsonStrings(node: JsonNode): JsonNode {
        if (node.isTextual) {
            requireStorable(node.textValue(), "A JSON string value")
        } else if (node.isArray) {
            for (element in node) {
                requireStorableJsonStrings(element)
            }
        } else if (node.isObject) {
            for ((name, value) in node.properties()) {
                requireStorable(name, "A JSON field name")
                requireStorableJsonStrings(value)
            }
        }
        return node
    }

    /**
     * The other thing a `jsonb` column refuses: a number outside what PostgreSQL's `numeric` holds.
     * `jsonb` stores numbers as `numeric`, and `'{"a":1e131072}'::jsonb` answers
     * `ERROR: value overflows numeric format`, server-side - the wedge again, and one this converter
     * used to hide by rendering such a number as the string `"Infinity"` instead (see [toJsonText]).
     *
     * Walked over the parser's **tokens** rather than over the tree, because the tree does not keep
     * the number as the user wrote it: `readTree` turns every floating-point token into a `double`,
     * where `1e400` and `1e131072` are both `Infinity` although a `jsonb` column takes the first
     * quite happily. The literal is what PostgreSQL parses, so the literal is what is measured.
     *
     * [text] has already been through [strictJson] above, so it is well-formed here and this pass
     * cannot fail for any reason but the one it is looking for.
     */
    private fun requireStorableJsonNumbers(text: String) {
        strictJson.createParser(text).use { parser ->
            var token = parser.nextToken()
            while (token != null) {
                if (token == JsonToken.VALUE_NUMBER_INT || token == JsonToken.VALUE_NUMBER_FLOAT) {
                    requireStorableJsonNumber(parser.text)
                }
                token = parser.nextToken()
            }
        }
    }

    /**
     * One JSON number literal against `numeric`'s two limits, measured on this converter's own
     * `postgres:17.11` (and confirmed on the `postgres:12.6` the tests run against): `1e131071`
     * stores and `1e131072` overflows; `1e-16383` stores and `1e-16384` overflows; a literal with
     * 16384 digits after the point overflows just as the exponent form does. Nothing is rounded into
     * range on the way - PostgreSQL raises, it does not truncate - which is why this has to be a
     * refusal rather than anything cleverer.
     *
     * The JSON grammar admits only `-?(0|[1-9]\d*)(\.\d+)?([eE][+-]?\d+)?`, so the digit counts are
     * the integer digits plus the exponent and the fraction digits minus it.
     */
    private fun requireStorableJsonNumber(literal: String) {

        var i = 0
        if (i < literal.length && (literal[i] == '-' || literal[i] == '+')) {
            i++
        }
        val intStart = i
        while (i < literal.length && literal[i] in '0'..'9') {
            i++
        }
        val intDigits = i - intStart
        var fracDigits = 0
        if (i < literal.length && literal[i] == '.') {
            i++
            val fracStart = i
            while (i < literal.length && literal[i] in '0'..'9') {
                i++
            }
            fracDigits = i - fracStart
        }
        val exponent = if (i < literal.length && (literal[i] == 'e' || literal[i] == 'E')) {
            val exponentText = literal.substring(i + 1)
            // an exponent too large for an Int is far outside the range in whichever direction it
            // points, so it is saturated rather than refused separately
            exponentText.toIntOrNull()
                ?: if (exponentText.startsWith('-')) Int.MIN_VALUE else Int.MAX_VALUE
        } else {
            0
        }
        val intLength = intDigits.toLong() + exponent
        val fracLength = fracDigits.toLong() - exponent
        if (intLength > MAX_NUMERIC_INT_DIGITS || fracLength > MAX_NUMERIC_FRAC_DIGITS) {
            error(
                "The JSON number '$literal' is outside what a jsonb column can store: its numbers " +
                    "are numeric, which holds at most $MAX_NUMERIC_INT_DIGITS digits before the " +
                    "decimal point and $MAX_NUMERIC_FRAC_DIGITS after it. The original is kept in " +
                    "the backup column"
            )
        }
    }

    /**
     * The same rule for the temporal columns, whose limit is a *range* rather than a character.
     *
     * `java.time.Instant` reaches year 1000000000 and `Instant.parse` accepts the expanded-year form,
     * so `TEXT -> DATE/DATETIME` (always lossy, so it always comes here) can produce a value
     * no database will take. PostgreSQL answers `ERROR: timestamp out of range` /
     * `ERROR: date out of range`, server-side.
     *
     * The bound lives here, in the backend-agnostic converter, expressed as the *narrowest* backend's
     * range: the two backends have to agree value for value, and a bound that existed only
     * in the PostgreSQL DAO would mean the same input succeeds in memory and wedges on PostgreSQL.
     * Out of range is refused, never clamped or saturated into range, for the same reason a NUL is
     * not Base64-encoded.
     */
    private fun requireStorableInstant(instant: Instant, maxYear: Int, columnKind: String): Instant {
        val year = instant.atZone(ZoneOffset.UTC).year
        if (year < MIN_TEMPORAL_YEAR || year > maxYear) {
            error(
                "Year $year is outside what a $columnKind column can store " +
                    "($MIN_TEMPORAL_YEAR..$maxYear). The original is kept in the backup column"
            )
        }
        return instant
    }

    /**
     * The number a `float8` column can hold, or a row left behind.
     *
     * `toDoubleOrNull` alone was the NUMBER branch's whole implementation, and like every other
     * member of this class it **substituted** rather than refusing: `Double.parseDouble` answers
     * `Infinity` for `1e400`, `-Infinity` for `-1e400` and `0.0` for `1e-400`, all three of which a
     * `float8` column then accepts - so the row was counted `processed`, the task reported every row
     * carried across, and the user's `1e400` read back as `Infinity` while their `1e-400` read back as
     * `0.0`. A string parsed into a number is always LOSSY, so it always comes here.
     *
     * The rule below is PostgreSQL's own, measured on `postgres:17.11` and `postgres:12.6`:
     * `'1e400'::float8`, `'-1e400'::float8`, `'1e-400'::float8` and `'1e309'::float8` all answer
     * `ERROR: "..." is out of range for type double precision`, while `'Infinity'`, `'-Infinity'`,
     * `'NaN'`, `'0'` and the subnormals (`'1e-320'`, `'4.9e-324'`) are accepted. So a literal that is
     * not itself an infinity must not parse to an infinity, and a literal with a non-zero significand
     * must not parse to zero. Refused, never clamped: the original stays in the backup column.
     */
    private fun toStorableDouble(text: String): Double {
        val number = text.toDoubleOrNull() ?: error("Not a number: '$text'")
        if (number.isInfinite() && !isInfinitySpelling(text)) {
            error(
                "'$text' overflows what a float8 column can store - PostgreSQL answers " +
                    "'is out of range for type double precision' for it, and parsing it here would " +
                    "silently substitute ${if (number > 0) "Infinity" else "-Infinity"} for the " +
                    "user's value. The original is kept in the backup column"
            )
        }
        if (number == 0.0 && hasNonZeroSignificand(text)) {
            error(
                "'$text' underflows what a float8 column can store - PostgreSQL answers " +
                    "'is out of range for type double precision' for it, and parsing it here would " +
                    "silently substitute zero for the user's value. The original is kept in the " +
                    "backup column"
            )
        }
        return number
    }

    /**
     * `Infinity` is the only infinity `Double.parseDouble` spells, with an optional sign.
     */
    private fun isInfinitySpelling(text: String): Boolean {
        return text.trim().removePrefix("+").removePrefix("-") == "Infinity"
    }

    /**
     * Whether the literal's significand holds a digit other than zero - i.e. whether parsing it to
     * `0.0` lost something. Hexadecimal literals are here because `toDoubleOrNull` accepts them
     * (`0x1p-2000` underflows just as `1e-400` does), even though no PostgreSQL column produces one.
     */
    private fun hasNonZeroSignificand(text: String): Boolean {
        val body = text.trim().removePrefix("+").removePrefix("-").removeSuffix("f")
            .removeSuffix("F").removeSuffix("d").removeSuffix("D")
        val isHex = body.startsWith("0x") || body.startsWith("0X")
        val significand = if (isHex) {
            body.substring(2).substringBefore('p').substringBefore('P')
        } else {
            body.substringBefore('e').substringBefore('E')
        }
        return significand.any { it != '0' && it != '.' }
    }

    /**
     * The value as the **user's** text, which for a reference source is not the value the column
     * holds.
     *
     * A reference-family column stores a `Long` id into `ed_record_ref`, an internal surrogate key,
     * so `G_ASSOC -> G_STR` rendered that id: a user migrating an association to
     * text got `"1742"` where their data was `emodel@doc$x`. No in-place path contradicts it - every
     * such pair is LOSSY - but an internal key is not the user's data, and getting their data back is
     * the point of the whole transfer.
     *
     * A dangling id (the `ed_record_ref` row is gone, which `DbRecord` already tolerates on the read
     * path) is a row left behind rather than a fabricated one: writing the bare number would be
     * exactly the substitution this class refuses everywhere else, and the original id is still in the
     * backup.
     */
    private fun userText(value: Any, params: DbColumnMigrationParams, tableCtx: DbTableContext): Any {
        val sourceType = (params.sourceType as? DbColumnSemanticType.Model)?.attType
        if (value !is Number || sourceType == null || sourceType !in REFERENCE_TYPES) {
            return value
        }
        val id = value.toLong()
        val ref = tableCtx.getRecordRefsService().getEntityRefsByIdsMap(listOf(id))[id]
            ?: error(
                "Reference id $id has no row in ${DbRecordRefEntity.TABLE} any more, so there is no " +
                    "text form of it to write. The original id is kept in the backup column"
            )
        return ref.toString()
    }

    /**
     * [toText] narrowed to what a text-family column can hold - see [requireStorable].
     */
    private fun toTextColumnValue(value: Any?): String {
        return requireStorable(toText(value), "Value")
    }

    private fun toText(value: Any?): String {
        return when (value) {
            is String -> value
            // both of these would otherwise render in a database-specific format: Timestamp's
            // toString has no zone and a space instead of the 'T', and neither is what the platform
            // stores in a text column for a date attribute
            is Timestamp -> renderInstant(value.toInstant())
            is Instant -> renderInstant(value)
            is LocalDate -> value.toString()
            // and for the same reason: Double.toString renders 100.0 where a float8 column's own
            // ::text renders 100, and NUMBER -> TEXT is the one pair where both paths really run
            is Double -> DbDoubleText.render(value)
            is ByteArray -> String(value, Charsets.UTF_8)
            is DataValue -> if (value.isTextual()) value.asText() else value.toString()
            else -> value.toString()
        }
    }

    /**
     * An instant as the **in-place** path renders it.
     *
     * `DATETIME -> TEXT` is class SAFE, and `DbSchemaDaoPg.getConversion` emits
     * `(to_json(column AT TIME ZONE 'UTC') #>> '{}') || 'Z'` for it, so this pair runs down both
     * paths exactly as `NUMBER -> TEXT` does. The two renderings agree on everything except a
     * fractional second ending in zeros: `to_json` trims them (`10:00:00.5Z`) while
     * `Instant.toString` pads to a multiple of three digits (`10:00:00.500Z`). Measured on
     * `postgres:17.11`: `.5` and `.500` both render as `.5`, `.120` as `.12`, `.123456` unchanged,
     * and a whole second with no fractional part at all.
     *
     * Trimming here is not a re-encoding of the user's value - both forms are the same instant and
     * both parse back with `Instant.parse` - it is producing the same *string* the other path
     * produces, which is what the two paths agreeing means.
     */
    private fun renderInstant(instant: Instant): String {
        val text = instant.toString()
        val dotIdx = text.indexOf('.')
        if (dotIdx < 0) {
            return text
        }
        // ISO_INSTANT always ends in 'Z', so the fraction is everything between the point and it
        var lastKept = text.length - 2
        while (lastKept > dotIdx && text[lastKept] == '0') {
            lastKept--
        }
        val end = if (lastKept == dotIdx) dotIdx else lastKept + 1
        return text.substring(0, end) + "Z"
    }

    private fun toInstant(value: Any?): Instant {
        return when (value) {
            is Timestamp -> value.toInstant()
            is Instant -> value
            is LocalDate -> value.atStartOfDay(ZoneOffset.UTC).toInstant()
            is String -> runCatching { Instant.parse(value) }
                .recoverCatching { LocalDate.parse(value).atStartOfDay(ZoneOffset.UTC).toInstant() }
                .getOrElse { error("Not a date: '$value'") }
            else -> error("Not a date: '$value'")
        }
    }
}
