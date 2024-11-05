package ru.citeck.ecos.data.sql.pg.records

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import ru.citeck.ecos.commons.data.DataValue
import ru.citeck.ecos.commons.data.MLText
import ru.citeck.ecos.commons.data.ObjectData
import ru.citeck.ecos.context.lib.i18n.I18nContext
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType
import ru.citeck.ecos.model.lib.attributes.dto.computed.ComputedAttDef
import ru.citeck.ecos.model.lib.attributes.dto.computed.ComputedAttType

class DbRecordsOptionsAttTest : DbRecordsTestBase() {

    @Test
    fun test() {

        val optionValues = DataValue.create(
            """
            [
                {
                    "value": "value0",
                    "label": {
                        "ru": "Значение 0",
                        "en": "Value 0"
                    }
                },
                {
                    "value": "value1",
                    "label": {
                        "ru": "Значение 1",
                        "en": "Value 1"
                    }
                },
                {
                    "value": "value2",
                    "label": {
                        "ru": "Значение 2",
                        "en": "Value 2"
                    }
                }
            ]
            """.trimIndent()
        )

        val scriptOptions = DataValue.create(
            """
            [
                {"value":"script-val-0", "label": "Script Label 0"},
                {"value":"script-val-1", "label": "Script Label 1"}
            ]
            """.trimIndent()
        )

        registerAtts(
            listOf(
                AttributeDef.create()
                    .withId("optionsValues")
                    .withType(AttributeType.OPTIONS)
                    .withConfig(
                        ObjectData.create()
                            .set("source", "values")
                            .set("values", optionValues)
                    ).build(),
                AttributeDef.create()
                    .withId("optionsAtt")
                    .withType(AttributeType.OPTIONS)
                    .withConfig(
                        ObjectData.create()
                            .set("source", "attribute")
                            .set("attribute", "attWithOptions")
                    ).build(),
                AttributeDef.create()
                    .withId("attWithOptions")
                    .withType(AttributeType.JSON)
                    .withMultiple(true)
                    .withComputed(
                        ComputedAttDef.create()
                            .withType(ComputedAttType.SCRIPT)
                            .withConfig(ObjectData.create().set("fn", """return $scriptOptions;"""))
                            .build()
                    ).build()
            )
        )

        val ex = assertThrows<Exception> {
            createRecord("optionsValues" to "unknown")
        }
        assertThat(ex.message).contains("Invalid value 'unknown' of options attribute: optionsValues")

        val rec0 = createRecord()

        val optionsValuesFromAtt = records.getAtt(rec0, "_edge.optionsValues.options[]?json")
        assertThat(optionsValuesFromAtt).isEqualTo(optionValues)

        val optionsValuesFromAtt2 = records.getAtt(rec0, "_edge.optionsValues.options[]{value,label}")
        assertThat(optionsValuesFromAtt2).isEqualTo(
            DataValue.createArr()
                .add(DataValue.createObj().set("value", "value0").set("label", "Value 0"))
                .add(DataValue.createObj().set("value", "value1").set("label", "Value 1"))
                .add(DataValue.createObj().set("value", "value2").set("label", "Value 2"))
        )

        val optionsValuesFromAtt3 = I18nContext.doWithLocale(I18nContext.RUSSIAN) {
            records.getAtt(rec0, "_edge.optionsValues.options[]{value,label}")
        }
        assertThat(optionsValuesFromAtt3).isEqualTo(
            DataValue.createArr()
                .add(DataValue.createObj().set("value", "value0").set("label", "Значение 0"))
                .add(DataValue.createObj().set("value", "value1").set("label", "Значение 1"))
                .add(DataValue.createObj().set("value", "value2").set("label", "Значение 2"))
        )

        val optionsForEmptyVal = records.getAtt(rec0.withLocalId(""), "_edge.optionsValues.options[]?json")
        assertThat(optionsForEmptyVal).isEqualTo(optionValues)

        val optionsAttFromAtt = records.getAtt(rec0, "_edge.optionsAtt.options[]?json")
        val expectedOptionsAtt = DataValue.create(scriptOptions.map { it.set("label", MLText(it["label"].asText())) })
        assertThat(optionsAttFromAtt).isEqualTo(expectedOptionsAtt)

        val optionsAttFromAtt2 = records.getAtt(rec0.withLocalId(""), "_edge.optionsAtt.options[]?json")
        assertThat(optionsAttFromAtt2).isEqualTo(expectedOptionsAtt)
    }
}
