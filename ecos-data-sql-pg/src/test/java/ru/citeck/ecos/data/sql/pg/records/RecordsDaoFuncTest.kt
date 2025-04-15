package ru.citeck.ecos.data.sql.pg.records

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import ru.citeck.ecos.commons.data.DataValue
import ru.citeck.ecos.data.sql.pg.records.commons.DbRecordsTestBase
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType

class RecordsDaoFuncTest : DbRecordsTestBase() {

    @Test
    fun substringTest() {

        registerAtts(
            listOf(
                AttributeDef.create()
                    .withId("text")
                    .build()
            )
        )

        val srcText = "emodel/test@abc"
        val ref = createRecord("text" to srcText)
        val checkValue: (String, Any) -> Unit = { att, exp ->
            assertThat(records.getAtt(ref, att).getAs(exp::class.java)).isEqualTo(exp)
        }

        checkValue("substring(text, 1)", srcText.substring(0))
        checkValue("substring(text, 4)", srcText.substring(3))
        checkValue("substring(text, 4, 2)", srcText.substring(3, 5))

        checkValue("substringBefore(text, '@')", "emodel/test")

        createRecord("text" to "emodel/aaaa@def")
        createRecord("text" to "emodel/aaaa@def")
        createRecord("text" to "emodel/aaaa@def")
        createRecord("text" to "eproc/bbb@def")
        createRecord("text" to "eproc/bbbbbb@def")
        createRecord("text" to "eproc/bbbbbb2def")
        createRecord("text" to "abc")
        createRecord("text" to "")

        val queryRes = records.query(
            baseQuery.copy()
                .withGroupBy(listOf("substringBefore(text, '@')"))
                .withSortBy(emptyList())
                .build(),
            mapOf(
                "srcId" to "substringBefore(text, '@')"
            )
        ).getRecords().map { it.getAtt("srcId").asText() }

        assertThat(queryRes).containsExactlyInAnyOrder("emodel/test", "emodel/aaaa", "eproc/bbb", "eproc/bbbbbb", "")
    }

    @Test
    fun concatTest() {

        registerType()
            .withAttributes(
                AttributeDef.create().withId("text"),
                AttributeDef.create().withId("num").withType(AttributeType.NUMBER)
            ).register()

        val rec = createRecord("text" to "value", "num" to 10)

        fun assertValue(att: String, expected: Any?) {
            assertThat(records.getAtt(rec, att)).isEqualTo(DataValue.of(expected))
        }

        assertValue("concat('aa','bb', text)", "aabbvalue")
        assertValue("concat_ws('-','bb', text, num)", "bb-value-10")
    }
}
