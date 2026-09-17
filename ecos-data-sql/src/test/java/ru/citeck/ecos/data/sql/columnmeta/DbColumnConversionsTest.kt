package ru.citeck.ecos.data.sql.columnmeta

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource
import ru.citeck.ecos.data.sql.dto.DbColumnType
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType

class DbColumnConversionsTest {

    private val G_STR_MEMBERS = listOf(AttributeType.TEXT, AttributeType.MLTEXT, AttributeType.OPTIONS)

    private val G_ASSOC_MEMBERS = listOf(
        AttributeType.ASSOC,
        AttributeType.PERSON,
        AttributeType.AUTHORITY,
        AttributeType.AUTHORITY_GROUP
    )

    private val SAFE_TO_STRING_MEMBERS = listOf(
        AttributeType.NUMBER,
        AttributeType.BOOLEAN,
        AttributeType.JSON,
        AttributeType.DATE,
        AttributeType.DATETIME
    )

    private val PARSEABLE_FROM_STRING_MEMBERS = listOf(
        AttributeType.NUMBER,
        AttributeType.BOOLEAN,
        AttributeType.DATE,
        AttributeType.DATETIME
    )

    private fun classify(
        from: AttributeType,
        to: AttributeType,
        fromMultiple: Boolean = false,
        toMultiple: Boolean = false
    ): DbConversionClass {
        return DbColumnConversions.classify(
            DbColumnSemanticType.Model(from),
            fromMultiple,
            to,
            toMultiple
        )
    }

    @Test
    fun onlyTypesStoredIdenticallyNeedNoValueTransferTest() {

        val sameForm = { from: AttributeType, to: AttributeType ->
            DbColumnConversions.isSameStoredForm(DbColumnSemanticType.Model(from), to)
        }

        // TEXT and OPTIONS are the same bare string, searched the same way
        assertThat(sameForm(AttributeType.TEXT, AttributeType.OPTIONS)).isTrue()
        assertThat(sameForm(AttributeType.OPTIONS, AttributeType.TEXT)).isTrue()
        // every assoc-like type lives in ed_associations
        for (from in G_ASSOC_MEMBERS) {
            for (to in G_ASSOC_MEMBERS) {
                assertThat(sameForm(from, to))
                    .describedAs("$from -> $to stays in ed_associations")
                    .isTrue()
            }
        }
        // safe but not free: a bare string is not an MLText object, and EQ search stops
        // finding it the moment the model says it is one
        assertThat(sameForm(AttributeType.TEXT, AttributeType.MLTEXT)).isFalse()
        assertThat(sameForm(AttributeType.OPTIONS, AttributeType.MLTEXT)).isFalse()
        assertThat(sameForm(AttributeType.MLTEXT, AttributeType.TEXT)).isFalse()
        // the source of truth moves, even though both are physically one LONG
        assertThat(sameForm(AttributeType.ASSOC, AttributeType.ENTITY_REF)).isFalse()
        // a type that never moved is trivially the same form
        assertThat(sameForm(AttributeType.TEXT, AttributeType.TEXT)).isTrue()
        // nothing ever recorded what these bytes mean, so nothing may call them free
        assertThat(
            DbColumnConversions.isSameStoredForm(
                DbColumnSemanticType.Raw(DbColumnType.TEXT),
                AttributeType.TEXT
            )
        ).isFalse()
    }

    @Test
    fun mlTextLosesLocalesOnTheWayToAPlainStringTest() {
        // @JsonValue getValues() writes every locale; only the closest one survives
        assertThat(classify(AttributeType.MLTEXT, AttributeType.TEXT)).isEqualTo(DbConversionClass.LOSSY)
        assertThat(classify(AttributeType.MLTEXT, AttributeType.OPTIONS)).isEqualTo(DbConversionClass.LOSSY)
        // physically already a JSON string, but legacy bare strings are not valid JSON
        assertThat(classify(AttributeType.MLTEXT, AttributeType.JSON)).isEqualTo(DbConversionClass.LOSSY)
        // arbitrary JSON need not be a valid MLText
        assertThat(classify(AttributeType.JSON, AttributeType.MLTEXT)).isEqualTo(DbConversionClass.LOSSY)
    }

    @Test
    fun aPlainStringBecomingMlTextIsSafeButNotANoOpTest() {
        // EQ is rewritten to CONTAINS '"value"', so bare strings stop being found -
        // the values have to be rewritten as {"en": ...}, which loses nothing
        assertThat(classify(AttributeType.TEXT, AttributeType.MLTEXT)).isEqualTo(DbConversionClass.SAFE)
        assertThat(classify(AttributeType.OPTIONS, AttributeType.MLTEXT)).isEqualTo(DbConversionClass.SAFE)
    }

    @Test
    fun movesInsideOneStorageGroupAreSafeTest() {
        // physically the same column, searched the same way
        assertThat(classify(AttributeType.TEXT, AttributeType.OPTIONS)).isEqualTo(DbConversionClass.SAFE)
        assertThat(classify(AttributeType.OPTIONS, AttributeType.TEXT)).isEqualTo(DbConversionClass.SAFE)
        // all four live in ed_associations - every member against every member, not just
        // one representative pair, so a narrowing regression on any of the six off-diagonal pairs
        // cannot pass silently
        for (from in G_ASSOC_MEMBERS) {
            for (to in G_ASSOC_MEMBERS) {
                assertThat(classify(from, to))
                    .describedAs("$from -> $to")
                    .isEqualTo(DbConversionClass.SAFE)
            }
        }
    }

    @Test
    fun crossingBetweenAssociationsAndEntityRefIsLossyTest() {
        // the source of truth moves; the column caches only the first 10 values - every
        // G_ASSOC member, not just ASSOC/PERSON, in both directions
        for (from in G_ASSOC_MEMBERS) {
            assertThat(classify(from, AttributeType.ENTITY_REF))
                .describedAs("$from -> ENTITY_REF")
                .isEqualTo(DbConversionClass.LOSSY)
            assertThat(classify(AttributeType.ENTITY_REF, from))
                .describedAs("ENTITY_REF -> $from")
                .isEqualTo(DbConversionClass.LOSSY)
        }
    }

    @ParameterizedTest
    @EnumSource(AttributeType::class)
    fun contentNeverConvertsInEitherDirectionTest(other: AttributeType) {
        // ed_content.id and ed_record_ref.id are different id spaces
        if (other != AttributeType.CONTENT) {
            assertThat(classify(AttributeType.CONTENT, other))
                .describedAs("CONTENT -> $other")
                .isEqualTo(DbConversionClass.NONE)
            assertThat(classify(other, AttributeType.CONTENT))
                .describedAs("$other -> CONTENT")
                .isEqualTo(DbConversionClass.NONE)
        }
    }

    @Test
    fun contentToContentIsTheOnlyIdentityThatConvertsTest() {
        // contentNeverConvertsInEitherDirectionTest deliberately excludes this pair, so this is the
        // only test exercising the `from == to` branch of the CONTENT case
        assertThat(classify(AttributeType.CONTENT, AttributeType.CONTENT))
            .describedAs("CONTENT -> CONTENT")
            .isEqualTo(DbConversionClass.SAFE)
    }

    @Test
    fun referencesAndStringsConvertBothWaysWithLossTest() {
        // assoc-like <-> string-like, every member of both groups in both directions
        for (assoc in G_ASSOC_MEMBERS) {
            for (str in G_STR_MEMBERS) {
                assertThat(classify(assoc, str))
                    .describedAs("$assoc -> $str")
                    .isEqualTo(DbConversionClass.LOSSY)
                assertThat(classify(str, assoc))
                    .describedAs("$str -> $assoc")
                    .isEqualTo(DbConversionClass.LOSSY)
            }
        }
        // ENTITY_REF <-> string-like, every member
        for (str in G_STR_MEMBERS) {
            assertThat(classify(AttributeType.ENTITY_REF, str))
                .describedAs("ENTITY_REF -> $str")
                .isEqualTo(DbConversionClass.LOSSY)
            assertThat(classify(str, AttributeType.ENTITY_REF))
                .describedAs("$str -> ENTITY_REF")
                .isEqualTo(DbConversionClass.LOSSY)
        }
    }

    @Test
    fun scalarsToStringsAreSafeAndStringsBackAreNotTest() {
        // DATE and DATETIME are the two genuinely new safe pairs of this work, and every
        // string-like member must receive them, not just TEXT
        for (scalar in SAFE_TO_STRING_MEMBERS) {
            for (str in G_STR_MEMBERS) {
                if (scalar == AttributeType.JSON && str == AttributeType.MLTEXT) {
                    // claimed earlier as LOSSY - already asserted in
                    // mlTextLosesLocalesOnTheWayToAPlainStringTest
                    continue
                }
                assertThat(classify(scalar, str))
                    .describedAs("$scalar -> $str")
                    .isEqualTo(DbConversionClass.SAFE)
            }
        }
        // string-like -> JSON is done unconditionally today and blows up
        for (str in G_STR_MEMBERS) {
            assertThat(classify(str, AttributeType.JSON))
                .describedAs("$str -> JSON")
                .isEqualTo(DbConversionClass.LOSSY)
        }
        // every string-like member into every parseable scalar
        for (str in G_STR_MEMBERS) {
            for (scalar in PARSEABLE_FROM_STRING_MEMBERS) {
                assertThat(classify(str, scalar))
                    .describedAs("$str -> $scalar")
                    .isEqualTo(DbConversionClass.LOSSY)
            }
        }
    }

    @Test
    fun binaryAndDatesFollowTheirOwnRulesTest() {
        // every string-like member, not just TEXT, in both directions
        for (str in G_STR_MEMBERS) {
            assertThat(classify(str, AttributeType.BINARY))
                .describedAs("$str -> BINARY")
                .isEqualTo(DbConversionClass.SAFE)
            assertThat(classify(AttributeType.BINARY, str))
                .describedAs("BINARY -> $str")
                .isEqualTo(DbConversionClass.LOSSY)
        }
        assertThat(classify(AttributeType.DATE, AttributeType.DATETIME)).isEqualTo(DbConversionClass.SAFE)
        assertThat(classify(AttributeType.DATETIME, AttributeType.DATE)).isEqualTo(DbConversionClass.LOSSY)
    }

    @Test
    fun everythingElseIsNotWorthConvertingTest() {
        // a scalar wrapped in JSON has no practical use
        assertThat(classify(AttributeType.NUMBER, AttributeType.JSON)).isEqualTo(DbConversionClass.NONE)
        assertThat(classify(AttributeType.BOOLEAN, AttributeType.NUMBER)).isEqualTo(DbConversionClass.NONE)
        // BINARY -> JSON reaches NONE through the generic fallback, simply because nothing
        // earlier claims it
        assertThat(classify(AttributeType.BINARY, AttributeType.JSON)).isEqualTo(DbConversionClass.NONE)
    }

    @Test
    fun wideningToAnArrayIsSafeAndNarrowingIsNotTest() {
        // multiplicity is orthogonal to the type itself
        assertThat(classify(AttributeType.TEXT, AttributeType.TEXT, fromMultiple = false, toMultiple = true))
            .isEqualTo(DbConversionClass.SAFE)
        assertThat(classify(AttributeType.TEXT, AttributeType.TEXT, fromMultiple = true, toMultiple = false))
            .describedAs("narrowing keeps the first element and leaves the rest in the backup")
            .isEqualTo(DbConversionClass.LOSSY)
    }

    @Test
    fun jsonMultiplicityChangeIsAlwaysANoOpTest() {
        // a JSON column holds whatever shape it is given, so neither widening nor
        // narrowing into/out of JSON loses anything - both directions must stay SAFE
        assertThat(classify(AttributeType.JSON, AttributeType.JSON, fromMultiple = false, toMultiple = true))
            .describedAs("widening a JSON column is a no-op")
            .isEqualTo(DbConversionClass.SAFE)
        assertThat(classify(AttributeType.JSON, AttributeType.JSON, fromMultiple = true, toMultiple = false))
            .describedAs("narrowing a JSON column is also a no-op")
            .isEqualTo(DbConversionClass.SAFE)
    }

    @Test
    fun aContentMultiplicityChangeIsAlsoANoOpTest() {
        // multiplicity reaches CONTENT the same way it reaches JSON, by a different mechanism: a CONTENT
        // column is a single `ed_content` id whatever the model's flag says
        // ([DbAttTypeColumns.isMultiple]), so the column is the same column either way and a
        // "narrowing" has nothing to narrow. Calling it lossy made the map contradict the machinery
        // it describes - `ensureColumnsExistImpl` never even sees a change, because the expected
        // column definition is identical - and `isRegistryOnlyChange`, which asks this map for SAFE,
        // would have refused to recognise a change that moves no bytes at all.
        assertThat(classify(AttributeType.CONTENT, AttributeType.CONTENT, fromMultiple = false, toMultiple = true))
            .describedAs("widening a CONTENT attribute changes no column")
            .isEqualTo(DbConversionClass.SAFE)
        assertThat(classify(AttributeType.CONTENT, AttributeType.CONTENT, fromMultiple = true, toMultiple = false))
            .describedAs("and neither does narrowing it")
            .isEqualTo(DbConversionClass.SAFE)
    }

    @Test
    fun theWorseOfTheTwoDimensionsWinsTest() {
        // a type change and a multiplicity change together are ONE operation
        assertThat(classify(AttributeType.DATE, AttributeType.TEXT, fromMultiple = true, toMultiple = false))
            .describedAs("a safe type change plus a narrowing multiplicity is lossy overall")
            .isEqualTo(DbConversionClass.LOSSY)
        assertThat(classify(AttributeType.CONTENT, AttributeType.TEXT, fromMultiple = false, toMultiple = true))
            .describedAs("NONE is not rescued by a safe multiplicity change")
            .isEqualTo(DbConversionClass.NONE)
    }

    @Test
    fun anUnknownSourceTypeTakesTheMostConservativeClassOfItsCandidatesTest() {
        // a column seeded as Raw could have been any attribute type of that physical shape
        // VARCHAR could have been TEXT, MLTEXT or OPTIONS - all three are LOSSY into ASSOC
        assertThat(
            DbColumnConversions.classify(
                DbColumnSemanticType.Raw(DbColumnType.TEXT),
                false,
                AttributeType.ASSOC,
                false
            )
        ).describedAs("a column the registry never described must still be migratable")
            .isEqualTo(DbConversionClass.LOSSY)

        // LONG could have been any of the four assoc-like types, ENTITY_REF - or CONTENT, which
        // converts to nothing at all, so the conservative answer for the whole group is NONE
        assertThat(
            DbColumnConversions.classify(
                DbColumnSemanticType.Raw(DbColumnType.LONG),
                false,
                AttributeType.TEXT,
                false
            )
        ).describedAs("CONTENT is among the candidates, and it converts to nothing")
            .isEqualTo(DbConversionClass.NONE)
    }

    @Test
    fun onlyTheTwoTransferableClassesAreTransferableTest() {
        assertThat(DbColumnConversions.isTransferable(DbConversionClass.SAFE)).isTrue()
        assertThat(DbColumnConversions.isTransferable(DbConversionClass.LOSSY)).isTrue()
        assertThat(DbColumnConversions.isTransferable(DbConversionClass.NONE)).isFalse()
    }
}
