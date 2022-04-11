package ru.citeck.ecos.data.sql.pg.records

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import ru.citeck.ecos.commons.data.MLText
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType
import java.util.*

class AttributeTypesTest : DbRecordsTestBase() {

    @Test
    fun test() {

        registerAtts(
            listOf(
                AttributeDef.create()
                    .withId("name")
                    .withType(AttributeType.MLTEXT)
                    .build()
            )
        )

        val mlTextName = MLText(
            Locale.ENGLISH to "english-value",
            Locale.FRANCE to "france-value"
        )
        val rec = createRecord("name" to mlTextName)

        val mlTextNameRes = records.getAtt(rec, "name?json").getAs(MLText::class.java)
        assertThat(mlTextNameRes).isEqualTo(mlTextName)
    }
}
