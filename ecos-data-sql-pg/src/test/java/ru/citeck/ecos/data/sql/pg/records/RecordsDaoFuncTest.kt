package ru.citeck.ecos.data.sql.pg.records

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef

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
}
