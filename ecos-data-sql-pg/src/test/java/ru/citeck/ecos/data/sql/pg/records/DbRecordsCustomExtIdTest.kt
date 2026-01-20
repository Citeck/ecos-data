package ru.citeck.ecos.data.sql.pg.records

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import ru.citeck.ecos.commons.data.DataValue
import ru.citeck.ecos.commons.data.ObjectData
import ru.citeck.ecos.context.lib.auth.AuthContext
import ru.citeck.ecos.data.sql.pg.records.commons.DbRecordsTestBase
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType
import ru.citeck.ecos.model.lib.attributes.dto.computed.ComputedAttDef
import ru.citeck.ecos.model.lib.attributes.dto.computed.ComputedAttStoringType
import ru.citeck.ecos.model.lib.attributes.dto.computed.ComputedAttType
import ru.citeck.ecos.model.lib.num.dto.NumTemplateDef
import ru.citeck.ecos.webapp.api.entity.EntityRef

class DbRecordsCustomExtIdTest : DbRecordsTestBase() {

    @Test
    fun assocTest() {

        val projectCtx = registerType()
            .withId("project")
            .withLocalIdTemplate("{{key}}")
            .withSourceId("project-src")
            .withAttributes(
                AttributeDef.create().withId("key")
            ).register()

        registerType()
            .withLocalIdTemplate("{{project.key}}-{{number}}")
            .withAttributes(
                AttributeDef.create().withId("project").withType(AttributeType.ASSOC),
                AttributeDef.create().withId("number").withType(AttributeType.NUMBER)
            )
            .register()

        val projectSd = projectCtx.createRecord("key" to "SD")
        assertThat(projectSd.getLocalId()).isEqualTo("SD")
        val projectEpt = projectCtx.createRecord("key" to "EPT")
        assertThat(projectEpt.getLocalId()).isEqualTo("EPT")

        val rec0 = createRecord("project" to projectSd, "number" to 23)
        assertThat(rec0.getLocalId()).isEqualTo("SD-23")

        val rec1 = createRecord("project" to projectSd, "number" to 33)
        assertThat(rec1.getLocalId()).isEqualTo("SD-33")

        val rec2 = createRecord("project" to projectEpt, "number" to 33)
        assertThat(rec2.getLocalId()).isEqualTo("EPT-33")
    }

    @Test
    fun customIdWithLocalIdTemplateTest() {

        registerType()
            .withLocalIdTemplate("\${scope}$\${id}")
            .withAttributes(AttributeDef.create().withId("scope"))
            .register()

        val ref = createRecord("scope" to "uiserv", "id" to "function-enabled")
        assertThat(ref.getLocalId()).isEqualTo("uiserv\$function-enabled")

        val json = records.getAtt(ref, "?json")
        assertThat(json).isEqualTo(
            DataValue.createObj()
                .set("id", "function-enabled")
                .set("scope", "uiserv")
        )
        assertThat(records.getAtt(ref, "id").asText()).isEqualTo("function-enabled")
    }

    @Test
    fun copyTest() {

        registerType()
            .withLocalIdTemplate("SD-\${text}")
            .withAttributes(
                AttributeDef.create().withId("text")
            )
            .register()

        fun assertCount(expected: Long) {
            assertThat(records.query(baseQuery).getTotalCount()).isEqualTo(expected)
        }

        assertCount(0L)

        val ref0 = createRecord("text" to "abc")

        assertThat(ref0.getLocalId()).isEqualTo("SD-abc")
        assertCount(1L)

        val ref1 = records.mutate(
            ref0,
            mapOf(
                "id" to "new-id",
                "text" to "def"
            )
        )

        assertThat(ref1.getLocalId()).isEqualTo("SD-abc")
        assertThat(ref1).isEqualTo(ref0)
        assertThat(records.getAtt(ref1, "text").asText()).isEqualTo("def")

        // copy by mutation of id att is not allowed with custom localIdTemplate
        assertCount(1L)
    }

    @Test
    fun testWithComputedAtts() {

        registerNumTemplate(NumTemplateDef.create().withId("custom").build())
        registerNumTemplate(NumTemplateDef.create().withId("custom2").build())

        fun registerTypeWithLocalIdTemplate(template: String) {
            registerType()
                .withLocalIdTemplate(template)
                .withNumTemplateRef(EntityRef.valueOf("custom"))
                .withAttributes(
                    AttributeDef.create().withId("text"),
                    AttributeDef.create()
                        .withId("attWithCounter")
                        .withComputed(
                            ComputedAttDef.create()
                                .withType(ComputedAttType.COUNTER)
                                .withConfig(ObjectData.create().set("numTemplateRef", "custom2"))
                                .withStoringType(ComputedAttStoringType.ON_CREATE)
                                .build()
                        ),
                    AttributeDef.create().withId("attWithScript")
                        .withComputed(
                            ComputedAttDef.create()
                                .withType(ComputedAttType.SCRIPT)
                                .withConfig(
                                    ObjectData.create().set(
                                        "fn",
                                        """
                                    return "text__" + value.load('text')
                                        """.trimIndent()
                                    )
                                )
                                .withStoringType(ComputedAttStoringType.ON_CREATE)
                                .build()
                        )
                )
                .register()
        }
        val createdRecords = mutableListOf<EntityRef>()

        fun testWithCounterAtt(att: String) {
            registerTypeWithLocalIdTemplate("CUSTOM--\${$att}")
            (1..3).map {
                val counterNextVal = createdRecords.size + 1
                val ref = createRecord("text" to "abc-$counterNextVal")
                createdRecords.add(ref)
                assertThat(ref.getLocalId()).isEqualTo("CUSTOM--$counterNextVal")
                assertThat(records.getAtt(ref, att).asInt()).isEqualTo(counterNextVal)
            }
        }
        testWithCounterAtt("_docNum")
        testWithCounterAtt("attWithCounter")

        records.getAtts(createdRecords, listOf("id")).forEachIndexed { idx, atts ->
            assertThat(atts["id"].asText()).isEqualTo(createdRecords[idx].getLocalId())
        }

        records.mutate(createdRecords[2], "text" to "abc")

        records.getAtts(createdRecords, listOf("id")).forEachIndexed { idx, atts ->
            assertThat(atts["id"].asText()).isEqualTo(createdRecords[idx].getLocalId())
        }

        registerTypeWithLocalIdTemplate("prefix-{{attWithScript}}")

        val mutEx0 = assertThrows<RuntimeException> { createRecord() }
        assertThat(mutEx0.message).contains("Attribute 'text' required for localIdTemplate is not present")

        val ref = createRecord("text" to "value")
        assertThat(ref.getLocalId()).isEqualTo("prefix-text__value")
    }

    @Test
    fun testWithConstantLocalIdTemplate() {
        registerType()
            .withLocalIdTemplate("abc")
            .withAttributes(AttributeDef.create().withId("text"))
            .register()

        AuthContext.runAs("user") {
            createRecord()
            val ex = assertThrows<RuntimeException> { createRecord() }
            assertThat(ex.message).contains("id must be unique")
        }
    }

    @Test
    fun test() {
        registerType()
            .withLocalIdTemplate("{{project}}-{{number}}")
            .withAttributes(
                AttributeDef.create().withId("project"),
                AttributeDef.create().withId("number").withType(AttributeType.NUMBER)
            ).register()

        val ref = createRecord("project" to "SD", "number" to 123)
        assertThat(ref.getLocalId()).isEqualTo("SD-123")

        val json = records.getAtt(ref, "?json")
        assertThat(json).isEqualTo(
            DataValue.createObj()
                .set("id", "SD-123")
                .set("project", "SD")
                .set("number", 123.0)
        )

        val mutEx0 = assertThrows<RuntimeException> { createRecord("project" to "EPT") }
        assertThat(mutEx0.message).contains("Attribute 'number' required for localIdTemplate is not present")
    }
}
