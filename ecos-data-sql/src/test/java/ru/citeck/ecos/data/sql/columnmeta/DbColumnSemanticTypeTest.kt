package ru.citeck.ecos.data.sql.columnmeta

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import ru.citeck.ecos.data.sql.dto.DbColumnType
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType

class DbColumnSemanticTypeTest {

    @Test
    fun everyModelTypeSurvivesARoundTripTest() {
        for (attType in AttributeType.entries) {
            val type = DbColumnSemanticType.Model(attType)
            assertThat(DbColumnSemanticType.parse(type.asString())).isEqualTo(type)
        }
    }

    @Test
    fun everyRawTypeSurvivesARoundTripTest() {
        for (columnType in DbColumnType.entries) {
            val type = DbColumnSemanticType.Raw(columnType)
            assertThat(DbColumnSemanticType.parse(type.asString())).isEqualTo(type)
        }
    }

    @Test
    fun rawFormIsNeverConfusedWithAModelFormTest() {
        val modelForms = AttributeType.entries.map { DbColumnSemanticType.Model(it).asString() }
        val rawForms = DbColumnType.entries.map { DbColumnSemanticType.Raw(it).asString() }
        assertThat(rawForms).doesNotContainAnyElementsOf(modelForms)
    }

    @Test
    fun modelFormIsTheLowercasedEnumNameTest() {
        assertThat(DbColumnSemanticType.Model(AttributeType.AUTHORITY_GROUP).asString())
            .isEqualTo("authority_group")
        assertThat(DbColumnSemanticType.Raw(DbColumnType.TEXT).asString()).isEqualTo("raw_text")
    }

    @Test
    fun unknownValueThrowsTest() {
        // downgrading the platform is forbidden, so there is no older instance to protect by
        // degrading gracefully - an unreadable value is a corrupted column registry row or a bug
        for (value in listOf("something_else", "raw_something", "")) {
            assertThatThrownBy { DbColumnSemanticType.parse(value) }
                .describedAs("value: '%s'", value)
                .isInstanceOf(IllegalStateException::class.java)
                .hasMessageContaining("registry")
        }
    }
}
