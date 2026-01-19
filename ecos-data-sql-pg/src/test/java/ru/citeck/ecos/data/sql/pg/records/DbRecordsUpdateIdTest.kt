package ru.citeck.ecos.data.sql.pg.records

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import ru.citeck.ecos.commons.data.DataValue
import ru.citeck.ecos.commons.data.ObjectData
import ru.citeck.ecos.context.lib.auth.AuthContext
import ru.citeck.ecos.data.sql.dto.DbTableRef
import ru.citeck.ecos.data.sql.pg.records.commons.DbRecordsTestBase
import ru.citeck.ecos.data.sql.records.DbRecordsControlAtts
import ru.citeck.ecos.data.sql.records.listener.DbRecordRefChangedEvent
import ru.citeck.ecos.data.sql.records.listener.DbRecordsListenerAdapter
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType
import ru.citeck.ecos.model.lib.attributes.dto.computed.ComputedAttDef
import ru.citeck.ecos.model.lib.attributes.dto.computed.ComputedAttStoringType
import ru.citeck.ecos.model.lib.attributes.dto.computed.ComputedAttType
import ru.citeck.ecos.model.lib.num.dto.NumTemplateDef
import ru.citeck.ecos.model.lib.type.dto.TypeInfo
import ru.citeck.ecos.model.lib.type.dto.TypeModelDef
import ru.citeck.ecos.model.lib.utils.ModelUtils
import ru.citeck.ecos.webapp.api.entity.EntityRef

class DbRecordsUpdateIdTest : DbRecordsTestBase() {

    @Test
    fun idUpdateTest() {

        registerType()
            .withAttributes(
                AttributeDef.create().withId("text"),
                AttributeDef.create().withId("assoc").withType(AttributeType.ASSOC),
            )
            .register()

        val ref = createRecord("text" to "abc")
        records.mutateAtt(ref, DbRecordsControlAtts.UPDATE_ID, true)

        assertThat(records.getAtt(ref, "text").asText()).isEqualTo("abc")

        val ref2 = createRecord("text" to "second", "assoc" to ref)
        assertThat(records.getAtt(ref2, "assoc?localId").asText()).isEqualTo(ref.getLocalId())

        records.mutateAtt(ref, DbRecordsControlAtts.UPDATE_ID, "customId1")
        assertThat(records.getAtt(ref, "text").asText()).isEqualTo("")
        assertThat(records.getAtt(ref, "_notExists?bool").asBoolean()).isEqualTo(true)
        assertThat(records.getAtt(ref2, "assoc?localId").asText()).isEqualTo("customId1")
        assertThat(records.getAtt(ref.withLocalId("customId1"), "text").asText()).isEqualTo("abc")
    }

    @Test
    fun crossSchemaAssocsTest() {

        registerType()
            .withAttributes(
                AttributeDef.create().withId("text"),
                AttributeDef.create().withId("assoc").withType(AttributeType.ASSOC),
            )
            .register()

        registerType(
            TypeInfo.create()
                .withId("ext-type")
                .withSourceId("ext-src")
                .withModel(
                    TypeModelDef.create()
                        .withAttributes(listOf(AttributeDef.create().withId("text").build()))
                        .build()
                ).build()
        )

        val extSchemaDao = createRecordsDao(
            tableRef = DbTableRef("ext-schema", "ext-table"),
            typeRef = ModelUtils.getTypeRef("ext-type"),
            "ext-src",
            true
        )

        val extRef = extSchemaDao.createRecord("text" to "abc")
        assertThat(records.getAtt(extRef, "text").asText()).isEqualTo("abc")

        val intRef = createRecord("text" to "def", "assoc" to extRef)
        assertThat(records.getAtt(intRef, "text").asText()).isEqualTo("def")
        assertThat(records.getAtt(intRef, "assoc?id").asText()).isEqualTo(extRef.toString())

        val extRef1 = extSchemaDao.createRecord("text" to "hij")
        val exception = assertThrows<RuntimeException> {
            records.mutateAtt(extRef, DbRecordsControlAtts.UPDATE_ID, extRef1.getLocalId())
        }
        assertThat(exception.message).contains("Record '$extRef1' already exists. The id must be unique.")

        records.mutateAtt(extRef, DbRecordsControlAtts.UPDATE_ID, "custom-id")

        assertThat(records.getAtt(extRef.withLocalId("custom-id"), "text").asText()).isEqualTo("abc")
        assertThat(records.getAtt(intRef, "assoc?localId").asText()).isEqualTo("custom-id")
    }

    @Test
    fun testWithExtIdTemplate() {

        registerNumTemplate(NumTemplateDef.create().withId("test-num").build())
        registerNumTemplate(NumTemplateDef.create().withId("custom2").build())

        fun registerTypeWithExtIdTemplate(template: String) {
            registerType()
                .withNumTemplateRef(EntityRef.valueOf("test-num"))
                .withExtIdTemplate(template)
                .withAttributes(
                    AttributeDef.create().withId("text"),
                    AttributeDef.create().withId("project").withType(AttributeType.ASSOC),
                    AttributeDef.create()
                        .withId("attWithCounter")
                        .withType(AttributeType.NUMBER)
                        .withComputed(
                            ComputedAttDef.create()
                                .withType(ComputedAttType.COUNTER)
                                .withConfig(ObjectData.create().set("numTemplateRef", "custom2"))
                                .withStoringType(ComputedAttStoringType.ON_CREATE)
                                .build()
                        )
                )
                .register()
        }
        registerTypeWithExtIdTemplate("\${project.key}-\${_docNum}")

        val projectCtx = registerType()
            .withId("project")
            .withSourceId("project")
            .withAttributes(
                AttributeDef.create().withId("key")
            )
            .register()

        val eptProject = projectCtx.createRecord("key" to "EPT")
        val sdProject = projectCtx.createRecord("key" to "SD")

        val ept1Ref = createRecord("text" to "abc", "project" to eptProject)
        assertThat(ept1Ref.getLocalId()).isEqualTo("EPT-1")
        assertThat(records.getAtt(ept1Ref, "attWithCounter?num").asInt()).isEqualTo(1)

        AuthContext.runAs("user") {
            records.mutateAtt(ept1Ref, "project", sdProject)
            assertThat(records.getAtt(ept1Ref, "text").asText()).isEqualTo("abc")

            val ex = assertThrows<RuntimeException> {
                records.mutateAtt(ept1Ref, DbRecordsControlAtts.UPDATE_ID, true)
            }
            assertThat(ex.message).contains("Id update allowed only for admin")
        }

        val newRef = records.mutateAtt(ept1Ref, DbRecordsControlAtts.UPDATE_ID, true)
        assertThat(newRef.getLocalId()).isEqualTo("SD-1")

        assertThat(records.getAtt(ept1Ref, "text").asText()).isEqualTo("")
        assertThat(records.getAtt(newRef, "text").asText()).isEqualTo("abc")
        assertThat(records.getAtt(newRef, "attWithCounter?num").asInt()).isEqualTo(1)

        registerTypeWithExtIdTemplate("\${project.key}-\${attWithCounter}-Q")

        assertThat(records.getAtt(newRef, "text").asText()).isEqualTo("abc")
        assertThat(records.getAtt(newRef, "attWithCounter?num").asInt()).isEqualTo(1)

        val newRef2 = records.mutateAtt(newRef, DbRecordsControlAtts.UPDATE_ID, true)
        assertThat(newRef2.getLocalId()).isEqualTo("SD-1-Q")
    }

    @Test
    fun testWithCustomIdAndExtIdTemplate() {

        registerType()
            .withExtIdTemplate("\${scope}$\${id}")
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

        val events = mutableListOf<DbRecordRefChangedEvent>()

        recordsDao.addListener(object : DbRecordsListenerAdapter() {
            override fun onRecordRefChangedEvent(event: DbRecordRefChangedEvent) {
                events.add(event)
            }
        })

        val newRef = records.mutateAtt(ref, DbRecordsControlAtts.UPDATE_ID, "function-enabled-new")
        assertThat(newRef.getLocalId()).isEqualTo("uiserv\$function-enabled-new")

        assertThat(events).hasSize(1)
        assertThat(events[0].before.getLocalId()).isEqualTo("uiserv\$function-enabled")
        assertThat(events[0].after.getLocalId()).isEqualTo("uiserv\$function-enabled-new")

        assertThat(records.getAtt(ref, "_notExists").asBoolean()).isEqualTo(true)

        assertThat(records.getAtt(newRef, "id").asText()).isEqualTo("function-enabled-new")
        val json2 = records.getAtt(newRef, "?json")
        assertThat(json2).isEqualTo(
            DataValue.createObj()
                .set("id", "function-enabled-new")
                .set("scope", "uiserv")
        )

        val migrationRecords = mainCtx.selectAllFromTable("ed_record_ref_migration")
        assertThat(migrationRecords).hasSize(1)
        val migrationRecord = migrationRecords[0]

        val refsService = recordsDao.getRecordsDaoCtx().recordRefService
        val migratedFrom = refsService.getEntityRefById(migrationRecord["__from_ref"] as Long)
        val migratedTo = refsService.getEntityRefById(migrationRecord["__to_ref"] as Long)
        assertThat(migratedFrom.getLocalId()).isEqualTo("uiserv\$function-enabled")
        assertThat(migratedTo.getLocalId()).isEqualTo("uiserv\$function-enabled-new")

        val newRef2 = records.mutateAtt(newRef, DbRecordsControlAtts.UPDATE_ID, "qwerty")
        assertThat(records.getAtt(newRef2, "id").asText()).isEqualTo("qwerty")

        val migrationRecords2 = mainCtx.selectAllFromTable("ed_record_ref_migration")
        assertThat(migrationRecords2).hasSize(2)
    }
}
