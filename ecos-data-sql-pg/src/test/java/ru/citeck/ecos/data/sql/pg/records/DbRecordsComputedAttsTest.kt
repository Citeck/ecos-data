package ru.citeck.ecos.data.sql.pg.records

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import ru.citeck.ecos.commons.data.ObjectData
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.attributes.dto.computed.ComputedAttDef
import ru.citeck.ecos.model.lib.attributes.dto.computed.ComputedAttStoringType
import ru.citeck.ecos.model.lib.attributes.dto.computed.ComputedAttType

class DbRecordsComputedAttsTest : DbRecordsTestBase() {

    @Test
    fun test() {

        fun getComputedAttId(type: ComputedAttStoringType): String = "computed-storing-$type"

        registerAtts(
            listOf(
                AttributeDef.create()
                    .withId("simple")
                    .build(),
                *ComputedAttStoringType.values().map { storingType ->
                    AttributeDef.create()
                        .withId(getComputedAttId(storingType))
                        .withComputed(
                            ComputedAttDef.create()
                                .withType(ComputedAttType.ATTRIBUTE)
                                .withStoringType(storingType)
                                .withConfig(ObjectData.create("""{"attribute":"simple"}"""))
                                .build()
                        )
                        .build()
                }.toTypedArray()
            )
        )

        createRecord(
            "simple" to "computedAttId",
            *ComputedAttStoringType.values().map {
                getComputedAttId(it) to "$it-value"
            }.toTypedArray()
        )

        val columns = getColumns().associateBy { it.name }

        for (type in ComputedAttStoringType.values()) {
            if (type == ComputedAttStoringType.NONE) {
                assertThat(columns.containsKey(getComputedAttId(type))).isFalse
            } else {
                assertThat(columns.containsKey(getComputedAttId(type))).describedAs(type.toString()).isTrue
            }
        }
    }
}
