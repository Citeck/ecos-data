package ru.citeck.ecos.data.sql.pg.records

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import ru.citeck.ecos.commons.data.DataValue
import ru.citeck.ecos.commons.json.Json
import ru.citeck.ecos.commons.json.YamlUtils
import ru.citeck.ecos.data.sql.pg.records.commons.DbRecordsTestBase
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef

class DbRecordsYamlDataTest : DbRecordsTestBase() {

    companion object {
        const val ATT_TEXT = "description"
        const val ATT_NUMBER = "numbers"
        const val ATT_DATETIME = "test_time"

        const val dateTimeValue = "2022-04-11T00:00:00Z"
        const val textValue = "<camel:route>\n" +
            "    <camel:from uri=\"direct:start\" />\n" +
            "    <camel:split streaming=\"true\">\n" +
            "        <camel:jsonpath>\$</camel:jsonpath>\n" +
            "        <camel:aggregate completionSize=\"5\"\n" +
            "            completionTimeout=\"1000\" groupExchanges=\"true\">\n" +
            "            <camel:correlationExpression>\n" +
            "                <camel:constant>true</camel:constant>\n" +
            "            </camel:correlationExpression>\n" +
            "            <camel:to uri=\"mock:result\"></camel:to>\n" +
            "        </camel:aggregate>\n" +
            "    </camel:split>\n" +
            "</camel:route>"
        const val numberValue = 700900
    }

    @Test
    fun test() {
        registerAtts(
            listOf(
                AttributeDef.create { withId(ATT_TEXT) },
                AttributeDef.create { withId(ATT_NUMBER) },
                AttributeDef.create { withId(ATT_DATETIME) }
            )
        )
        val ref = createRecord(ATT_TEXT to textValue, ATT_DATETIME to dateTimeValue, ATT_NUMBER to numberValue)

        val yamlData = records.getAtt(ref, "?json|yaml()").asText()
        val data = Json.mapper.convertNotNull(
            YamlUtils.read(yamlData, Json.context)!!,
            DataValue::class.java
        )

        assertThat(data[ATT_NUMBER].asInt()).isEqualTo(numberValue)
        assertThat(data[ATT_TEXT].asText()).isEqualTo(textValue)
        assertThat(data[ATT_DATETIME].asText()).isEqualTo(dateTimeValue)
    }
}
