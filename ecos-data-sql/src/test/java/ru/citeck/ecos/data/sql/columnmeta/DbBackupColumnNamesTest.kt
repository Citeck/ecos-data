package ru.citeck.ecos.data.sql.columnmeta

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import ru.citeck.ecos.data.sql.dto.DbColumnType
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType

class DbBackupColumnNamesTest {

    private val model = DbColumnSemanticType.Model(AttributeType.TEXT)

    @Test
    fun theReadableFormIsPrefixAttributeAndTypeTest() {
        assertThat(DbBackupColumnNames.build("regNum", model, false, 63, emptySet()))
            .isEqualTo("__backup_regNum_text")
    }

    @Test
    fun anArrayGetsAMultipleSuffixTest() {
        assertThat(
            DbBackupColumnNames.build(
                "members",
                DbColumnSemanticType.Model(AttributeType.AUTHORITY_GROUP),
                true,
                63,
                emptySet()
            )
        ).isEqualTo("__backup_members_authority_group_multiple")
    }

    @Test
    fun anUnknownSourceTypeIsSpelledByItsPhysicalShapeTest() {
        // no AttributeType is named raw_..., so the two forms cannot be confused
        assertThat(
            DbBackupColumnNames.build("regNum", DbColumnSemanticType.Raw(DbColumnType.TEXT), false, 63, emptySet())
        ).isEqualTo("__backup_regNum_raw_text")
    }

    @Test
    fun aTakenReadableNameGetsACounterSuffixTest() {
        // the same attribute backed up twice at the same type - the second one must not collide
        val taken = setOf("__backup_regNum_text")
        assertThat(DbBackupColumnNames.build("regNum", model, false, 63, taken))
            .isEqualTo("__backup_regNum_text_2")
        assertThat(
            DbBackupColumnNames.build("regNum", model, false, 63, taken + "__backup_regNum_text_2")
        ).isEqualTo("__backup_regNum_text_3")
    }

    @Test
    fun anAttributeIdTooLongToSpellOutFallsBackToANumberedNameTest() {
        // an aspect attribute id of the form 'aspect:attName' can occupy all 63 bytes by itself
        val longId = "a".repeat(63)
        val name = DbBackupColumnNames.build(longId, model, false, 63, emptySet())
        assertThat(name)
            .describedAs("the readable form does not fit, so the name degrades to the numbered one")
            .isEqualTo("__backup_column_1")
        assertThat(name.toByteArray(Charsets.UTF_8).size).isLessThanOrEqualTo(63)
    }

    @Test
    fun theNumberedFallbackCountsWithinTheTableTest() {
        val longId = "a".repeat(63)
        val taken = setOf("__backup_column_1", "__backup_column_2")
        assertThat(DbBackupColumnNames.build(longId, model, false, 63, taken))
            .isEqualTo("__backup_column_3")
    }

    @Test
    fun theNumberedFallbackCannotBeMistakenForAReadableNameTest() {
        // the last segment is numeric, and no AttributeType or DbColumnType value is a number -
        // the Raw form spells its type as raw_<DbColumnType>, so DbColumnType feeds the
        // same argument as AttributeType does for the Model form
        assertThat(AttributeType.entries.map { it.name.lowercase() })
            .describedAs("if this ever stops holding, the two naming forms become ambiguous")
            .noneMatch { it.toLongOrNull() != null }
        assertThat(DbColumnType.entries.map { it.name.lowercase() })
            .describedAs("if this ever stops holding, the Raw-typed readable form becomes ambiguous too")
            .noneMatch { it.toLongOrNull() != null }
    }

    @Test
    fun aCounterSuffixNeverPushesTheNameOverTheLimitTest() {
        // right at the boundary: the readable form fits, but the readable form plus '_2' does not
        val id = "b".repeat(63 - "__backup_".length - "_text".length)
        val readable = DbBackupColumnNames.build(id, model, false, 63, emptySet())
        assertThat(readable.toByteArray(Charsets.UTF_8).size).isEqualTo(63)

        val second = DbBackupColumnNames.build(id, model, false, 63, setOf(readable))
        assertThat(second)
            .describedAs("no room for a counter, so it degrades rather than being truncated into a collision")
            .isEqualTo("__backup_column_1")
        assertThat(second.toByteArray(Charsets.UTF_8).size).isLessThanOrEqualTo(63)
    }

    @Test
    fun aMultiByteAttributeIdIsMeasuredInBytesNotCharsTest() {
        // PostgreSQL's limit is 63 BYTES; a Cyrillic id is two bytes per character
        val id = "и".repeat(30)
        val name = DbBackupColumnNames.build(id, model, false, 63, emptySet())
        assertThat(name.toByteArray(Charsets.UTF_8).size).isLessThanOrEqualTo(63)
    }

    @Test
    fun bothFormsAreRecognisedAsBackupNamesTest() {
        assertThat(DbBackupColumnNames.isBackupName("__backup_regNum_text")).isTrue()
        assertThat(DbBackupColumnNames.isBackupName("__backup_column_7")).isTrue()
        assertThat(DbBackupColumnNames.isBackupName("regNum")).isFalse()
        assertThat(DbBackupColumnNames.isBackupName("__created"))
            .describedAs("a system column is not a backup")
            .isFalse()
    }

    @Test
    fun aMaxBytesTooSmallForEvenTheNumberedFallbackFailsLoudlyTest() {
        // "__backup_column_1" is 17 bytes; a limit of 16 leaves nothing sensible to return - not
        // even the last-resort fallback fits. Silently truncating or handing back an over-long name
        // would surface as an opaque DDL error far away from this call; failing here, naming the
        // attribute and the limit, is more useful.
        assertThatThrownBy { DbBackupColumnNames.build("regNum", model, false, 16, emptySet()) }
            .describedAs("nothing sensible can be returned, so this must fail loudly rather than emit an oversized name")
            .isInstanceOf(IllegalStateException::class.java)
            .hasMessageContaining("regNum")
            .hasMessageContaining("16")
    }
}
