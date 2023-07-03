package ru.citeck.ecos.data.sql.pg.records

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import ru.citeck.ecos.commons.data.ObjectData
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType

class DbRecordsExpressionsTest : DbRecordsTestBase() {

    @Test
    fun test() {

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("num0")
                    withType(AttributeType.NUMBER)
                },
                AttributeDef.create {
                    withId("num1")
                    withType(AttributeType.NUMBER)
                },
                AttributeDef.create {
                    withId("text0")
                },
                AttributeDef.create {
                    withId("text1")
                }
            )
        )
        createRecord(ObjectData.create(RecordData("0", 0.0, 1L, "text-0", "text-1")))
        createRecord(ObjectData.create(RecordData("1", 1.0, 2L, "text-1", "text-2")))
        createRecord(ObjectData.create(RecordData("2", 3.0, 4L, "text-1", "text-4")))

        val exprNum0PlusNum1Div2 = "(num0 + num1 / 2)"
        val result0 = records.query(
            baseQuery,
            listOf("id", exprNum0PlusNum1Div2)
        ).getRecords().associateBy { it.getId().getLocalId() }

        assertThat(result0).hasSize(3)
        assertThat(result0["0"]!!.getAtt(exprNum0PlusNum1Div2).asDouble()).isEqualTo(0.0 + 1.0 / 2)
        assertThat(result0["1"]!!.getAtt(exprNum0PlusNum1Div2).asDouble()).isEqualTo(1.0 + 2.0 / 2)
        assertThat(result0["2"]!!.getAtt(exprNum0PlusNum1Div2).asDouble()).isEqualTo(3.0 + 4.0 / 2)

        val exprAvgNum0 = "avg(num0)"
        val exprCount = "count(*)"
        val result1 = records.query(
            baseQuery.copy {
                withGroupBy(listOf("text0"))
                withSortBy(emptyList())
            },
            listOf("text0", exprAvgNum0, exprCount)
        )
            .getRecords()
            .associateBy { it.getAtt("text0", "") }

        assertThat(result1).hasSize(2)
        assertThat(result1["text-0"]!!.getAtt(exprAvgNum0).asDouble()).isEqualTo(0.0)
        assertThat(result1["text-0"]!!.getAtt(exprCount).asInt()).isEqualTo(1)
        assertThat(result1["text-1"]!!.getAtt(exprAvgNum0).asDouble()).isEqualTo(2.0)
        assertThat(result1["text-1"]!!.getAtt(exprCount).asInt()).isEqualTo(2)

        val result2 = records.query(
            baseQuery,
            listOf("text0", exprNum0PlusNum1Div2, exprAvgNum0, exprCount)
        ).getRecords()

        assertThat(result2).hasSize(3)

        assertThat(result2[0].getAtt("text0").asText()).isEqualTo("text-0")
        assertThat(result2[0].getAtt(exprAvgNum0).asDouble()).isEqualTo(0.0)
        assertThat(result2[0].getAtt(exprCount).asInt()).isEqualTo(1)
        assertThat(result2[0].getAtt(exprNum0PlusNum1Div2).asDouble()).isEqualTo(0.5)

        assertThat(result2[1].getAtt("text0").asText()).isEqualTo("text-1")
        assertThat(result2[1].getAtt(exprAvgNum0).asDouble()).isEqualTo(1.0)
        assertThat(result2[1].getAtt(exprCount).asInt()).isEqualTo(1)
        assertThat(result2[1].getAtt(exprNum0PlusNum1Div2).asDouble()).isEqualTo(1.0 + 2.0 / 2.0)

        assertThat(result2[2].getAtt("text0").asText()).isEqualTo("text-1")
        assertThat(result2[2].getAtt(exprAvgNum0).asDouble()).isEqualTo(3.0)
        assertThat(result2[2].getAtt(exprCount).asInt()).isEqualTo(1)
        assertThat(result2[2].getAtt(exprNum0PlusNum1Div2).asDouble()).isEqualTo(3.0 + 4.0 / 2.0)

        val expr3 = "coalesce(1)"
        val result3 = records.query(
            baseQuery.copy { withGroupBy(listOf(expr3)); withSortBy(emptyList()) },
            listOf(expr3),
        ).getRecords()

        assertThat(result3).hasSize(1)
        assertThat(result3[0].getAtt(expr3).asLong()).isEqualTo(1L)
    }

    @Test
    fun countByFieldTest() {

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("text")
                }
            )
        )

        createRecord("id" to "1", "text" to null)
        createRecord("id" to "2", "text" to "abc")
        createRecord("id" to "3", "text" to "def")

        val res = records.query(baseQuery, listOf("count(text)")).getRecords().associate {
            it.getId().getLocalId() to it.getAtts()
        }
        assertThat(res["1"]!!["count(text)"].asInt()).isEqualTo(0)
        assertThat(res["2"]!!["count(text)"].asInt()).isEqualTo(1)
        assertThat(res["3"]!!["count(text)"].asInt()).isEqualTo(1)
    }

    class RecordData(
        val id: String,
        val num0: Double,
        val num1: Long,
        val text0: String,
        val text1: String
    )
}
