package ru.citeck.ecos.data.sql.pg.records

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import ru.citeck.ecos.commons.data.ObjectData
import ru.citeck.ecos.context.lib.auth.AuthContext
import ru.citeck.ecos.data.sql.pg.records.commons.DbRecordsTestBase
import ru.citeck.ecos.data.sql.records.DbRecordsControlAtts
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType
import ru.citeck.ecos.model.lib.attributes.dto.computed.ComputedAttDef
import ru.citeck.ecos.model.lib.attributes.dto.computed.ComputedAttStoringType
import ru.citeck.ecos.model.lib.attributes.dto.computed.ComputedAttType
import ru.citeck.ecos.model.lib.num.dto.NumTemplateDef
import ru.citeck.ecos.model.lib.type.dto.TypeInfo
import ru.citeck.ecos.model.lib.type.dto.TypeModelDef
import ru.citeck.ecos.records2.RecordConstants
import ru.citeck.ecos.records2.predicate.model.Predicates
import ru.citeck.ecos.webapp.api.constants.AppName
import ru.citeck.ecos.webapp.api.entity.EntityRef
import ru.citeck.ecos.webapp.api.entity.toEntityRef

class DbRecordsComputedAttsTest : DbRecordsTestBase() {

    @Test
    fun metaAttsTest() {

        fun createPersonAtt(att: String, storing: ComputedAttStoringType): AttributeDef {
            return AttributeDef.create()
                .withId("custom$att$storing")
                .withType(AttributeType.PERSON)
                .withComputed(
                    ComputedAttDef.create()
                        .withType(ComputedAttType.ATTRIBUTE)
                        .withConfig(
                            ObjectData.create()
                                .set("attribute", att)
                        )
                        .withStoringType(storing)
                        .build()
                ).build()
        }

        fun createDateTimeAtt(att: String, storing: ComputedAttStoringType): AttributeDef {
            return AttributeDef.create()
                .withId("custom$att$storing")
                .withType(AttributeType.DATETIME)
                .withComputed(
                    ComputedAttDef.create()
                        .withType(ComputedAttType.ATTRIBUTE)
                        .withConfig(
                            ObjectData.create()
                                .set("attribute", att)
                        )
                        .withStoringType(storing)
                        .build()
                ).build()
        }

        registerAtts(
            listOf(
                createPersonAtt(RecordConstants.ATT_CREATOR, ComputedAttStoringType.ON_CREATE),
                createPersonAtt(RecordConstants.ATT_CREATOR, ComputedAttStoringType.ON_MUTATE),
                createPersonAtt(RecordConstants.ATT_MODIFIER, ComputedAttStoringType.ON_CREATE),
                createPersonAtt(RecordConstants.ATT_MODIFIER, ComputedAttStoringType.ON_MUTATE),
                createDateTimeAtt(RecordConstants.ATT_CREATED, ComputedAttStoringType.ON_CREATE),
                createDateTimeAtt(RecordConstants.ATT_CREATED, ComputedAttStoringType.ON_MUTATE),
                createDateTimeAtt(RecordConstants.ATT_MODIFIED, ComputedAttStoringType.ON_CREATE),
                createDateTimeAtt(RecordConstants.ATT_MODIFIED, ComputedAttStoringType.ON_MUTATE),
                AttributeDef.create()
                    .withId("text")
                    .build()
            )
        )

        fun assertMetaAtts(
            rec: EntityRef,
            expCreatorOnCreate: String,
            expCreatorOnMutate: String,
            expModifierOnCreate: String,
            expModifierOnMutate: String
        ) {
            val atts = records.getAtts(
                rec,
                mapOf(
                    "customCreatorOnCreate" to "custom_creatorON_CREATE?id",
                    "customCreatorOnMutate" to "custom_creatorON_MUTATE?id",
                    "customModifierOnCreate" to "custom_modifierON_CREATE?id",
                    "customModifierOnMutate" to "custom_modifierON_MUTATE?id",
                    "customCreatedOnCreate" to "custom_createdON_CREATE?str",
                    "customCreatedOnMutate" to "custom_createdON_MUTATE?str",
                    "customModifiedOnCreate" to "custom_modifiedON_CREATE?str",
                    "customModifiedOnMutate" to "custom_modifiedON_MUTATE?str",
                    "created" to "_created?str",
                    "modified" to "_modified?str",
                )
            )
            fun assertPersonRef(attKey: String, expectedId: String) {
                val ref = atts[attKey].asText().toEntityRef()
                val desc = "attKey: $attKey expectedId: $expectedId"
                assertThat(ref.getAppName()).describedAs(desc).isEqualTo(AppName.EMODEL)
                assertThat(ref.getSourceId()).describedAs(desc).isEqualTo("person")
                assertThat(ref.getLocalId()).describedAs(desc).isEqualTo(expectedId)
            }
            assertPersonRef("customCreatorOnCreate", expCreatorOnCreate)
            assertPersonRef("customCreatorOnMutate", expCreatorOnMutate)
            assertPersonRef("customModifierOnCreate", expModifierOnCreate)
            assertPersonRef("customModifierOnMutate", expModifierOnMutate)

            assertThat(atts["customCreatedOnCreate"]).isEqualTo(atts["created"])
            assertThat(atts["customCreatedOnMutate"]).isEqualTo(atts["created"])
            assertThat(atts["customModifiedOnCreate"]).isEqualTo(atts["created"])
            assertThat(atts["customModifiedOnMutate"]).isEqualTo(atts["modified"])
        }

        val rec0 = createRecord()
        assertMetaAtts(
            rec0,
            expCreatorOnCreate = "system",
            expCreatorOnMutate = "system",
            expModifierOnCreate = "system",
            expModifierOnMutate = "system"
        )

        val rec1 = AuthContext.runAs("customUser") { createRecord() }
        assertMetaAtts(
            rec1,
            expCreatorOnCreate = "customUser",
            expCreatorOnMutate = "customUser",
            expModifierOnCreate = "customUser",
            expModifierOnMutate = "customUser"
        )
        AuthContext.runAs("customUser") {
            records.mutateAtt(rec0, "text", "value")
        }
        assertMetaAtts(
            rec0,
            expCreatorOnCreate = "system",
            expCreatorOnMutate = "system",
            expModifierOnCreate = "system",
            expModifierOnMutate = "customUser"
        )
    }

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
                    .withComputed(
                        ComputedAttDef.create()
                            .withType(ComputedAttType.VALUE)
                            .withStoringType(ComputedAttStoringType.ON_EMPTY)
                            .withConfig(ObjectData.create("""{"value":"$computedAssocRef"}"""))
                            .build()
                    )
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

    @Test
    fun computedAssocTest() {

        registerType()
            .withAttributes(
                AttributeDef.create().withId("assoc").withType(AttributeType.ASSOC),
                AttributeDef.create()
                    .withId("computedAssoc")
                    .withType(AttributeType.ASSOC)
                    .withComputed(
                        ComputedAttDef.create()
                            .withType(ComputedAttType.ATTRIBUTE)
                            .withConfig(ObjectData.create().set("attribute", "assoc"))
                            .withStoringType(ComputedAttStoringType.ON_MUTATE)
                            .build()
                    )
            )
            .register()

        val ref0 = createRecord("id" to "target")
        val ref1 = createRecord("id" to "source", "assoc" to ref0)

        assertThat(records.getAtt(ref1, "assoc?id").asText().toEntityRef()).isEqualTo(ref0)
        assertThat(records.getAtt(ref1, "computedAssoc?id").asText().toEntityRef()).isEqualTo(ref0)

        fun assertQuery(att: String, expected: List<EntityRef>) {
            val refs = records.query(
                baseQuery.copy()
                    .withQuery(Predicates.eq(att, ref0)).build()
            ).getRecords()
            assertThat(refs).containsExactlyElementsOf(expected)
        }

        assertQuery("assoc", listOf(ref1))
        assertQuery("computedAssoc", listOf(ref1))

        registerType()
            .withAttributes(
                AttributeDef.create().withId("assoc").withType(AttributeType.ASSOC),
                AttributeDef.create().withId("computedAssoc").withType(AttributeType.ASSOC),
                AttributeDef.create()
                    .withId("computedAssoc2")
                    .withType(AttributeType.ASSOC)
                    .withComputed(
                        ComputedAttDef.create()
                            .withType(ComputedAttType.ATTRIBUTE)
                            .withConfig(ObjectData.create().set("attribute", "assoc"))
                            .withStoringType(ComputedAttStoringType.ON_MUTATE)
                            .build()
                    )
            )
            .register()

        assertQuery("assoc", listOf(ref1))
        assertQuery("computedAssoc", listOf(ref1))
        assertQuery("computedAssoc2", emptyList())

        records.mutateAtt(ref1, DbRecordsControlAtts.UPDATE_CALCULATED_ATTS, true)

        assertQuery("assoc", listOf(ref1))
        assertQuery("computedAssoc", listOf(ref1))
        assertQuery("computedAssoc2", listOf(ref1))
    }
}
