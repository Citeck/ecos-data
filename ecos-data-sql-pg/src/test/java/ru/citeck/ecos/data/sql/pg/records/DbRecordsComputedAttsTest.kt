package ru.citeck.ecos.data.sql.pg.records

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import ru.citeck.ecos.commons.data.ObjectData
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType
import ru.citeck.ecos.model.lib.attributes.dto.computed.ComputedAttDef
import ru.citeck.ecos.model.lib.attributes.dto.computed.ComputedAttStoringType
import ru.citeck.ecos.model.lib.attributes.dto.computed.ComputedAttType
import ru.citeck.ecos.model.lib.num.dto.NumTemplateDef
import ru.citeck.ecos.model.lib.type.dto.TypeInfo
import ru.citeck.ecos.model.lib.type.dto.TypeModelDef
import ru.citeck.ecos.webapp.api.entity.EntityRef
import ru.citeck.ecos.webapp.api.entity.toEntityRef

class DbRecordsComputedAttsTest : DbRecordsTestBase() {

    @Test
    fun docNumTest() {

        registerType(
            TypeInfo.create {
                withId(REC_TEST_TYPE_ID)
                withNumTemplateRef("emodel/num-template@test".toEntityRef())
                withModel(
                    TypeModelDef.create()
                        .withAttributes(listOf(AttributeDef.create().withId("abc").build()))
                        .build()
                )
            }
        )
        registerNumTemplate(
            NumTemplateDef.create {
                withId("test")
            }
        )

        val record1 = createRecord("abc" to "def")
        val docNum1 = records.getAtt(record1, "_docNum").asText()
        assertThat(docNum1).isEqualTo("1")

        val record2 = createRecord("abc" to "def")
        val docNum2 = records.getAtt(record2, "_docNum").asText()
        assertThat(docNum2).isEqualTo("2")
    }

    @Test
    fun test() {

        fun getComputedAttId(type: ComputedAttStoringType): String = "computed-storing-$type"

        val computedAssocRef = EntityRef.valueOf("emodel/person@abc")

        registerAtts(
            listOf(
                AttributeDef.create()
                    .withId("simple")
                    .build(),
                AttributeDef.create()
                    .withId("computedAssoc")
                    .withType(AttributeType.ASSOC)
                    .withComputed(ComputedAttDef.create()
                        .withType(ComputedAttType.VALUE)
                        .withStoringType(ComputedAttStoringType.ON_EMPTY)
                        .withConfig(ObjectData.create("""{"value":"$computedAssocRef"}"""))
                        .build())
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

        val record = createRecord(
            "simple" to "computedAttId",
            *ComputedAttStoringType.values().map {
                getComputedAttId(it) to "$it-value"
            }.toTypedArray()
        )

        assertThat(records.getAtt(record, "computedAssoc?id").toEntityRef()).isEqualTo(computedAssocRef)

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
