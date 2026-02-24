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
import ru.citeck.ecos.records2.RecordConstants
import ru.citeck.ecos.webapp.api.entity.EntityRef
import ru.citeck.ecos.webapp.api.entity.toEntityRef

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
    fun testIdUpdateWithCounterUpdateWithChangedPropInKeyTemplate() {
        testIdUpdateWithCounterAndProjectMove(
            counterAtt = "_docNum",
            counterStoringType = ComputedAttStoringType.ON_CREATE,
            useTypeNumTemplate = true
        )
    }

    @Test
    fun testIdUpdateWithOnEmptyCounterUpdate() {
        testIdUpdateWithCounterAndProjectMove(
            counterAtt = "customCounter",
            counterStoringType = ComputedAttStoringType.ON_EMPTY,
            useTypeNumTemplate = false
        )
    }

    private fun testIdUpdateWithCounterAndProjectMove(
        counterAtt: String,
        counterStoringType: ComputedAttStoringType,
        useTypeNumTemplate: Boolean
    ) {
        val numTemplateId = "test-num-$counterAtt"

        registerNumTemplate(
            NumTemplateDef.create()
                .withId(numTemplateId)
                .withCounterKey("ept-project-{{project.key}}")
                .withModelAttributes(listOf("project.key"))
                .build()
        )

        val typeReg = registerType()
            .withLocalIdTemplate("{{project.key}}-{{$counterAtt}}")

        if (useTypeNumTemplate) {
            typeReg.withNumTemplateRef(EntityRef.valueOf(numTemplateId))
                .withAttributes(
                    AttributeDef.create().withId("project").withType(AttributeType.ASSOC)
                )
        } else {
            typeReg.withAttributes(
                AttributeDef.create().withId("project").withType(AttributeType.ASSOC),
                AttributeDef.create()
                    .withId(counterAtt)
                    .withType(AttributeType.NUMBER)
                    .withComputed(
                        ComputedAttDef.create()
                            .withType(ComputedAttType.COUNTER)
                            .withConfig(ObjectData.create().set("numTemplateRef", numTemplateId))
                            .withStoringType(counterStoringType)
                            .build()
                    )
            )
        }
        typeReg.register()

        val projectCtx = registerType()
            .withId("project")
            .withSourceId("project")
            .withAttributes(
                AttributeDef.create().withId("key")
            )
            .register()

        val eptProject = projectCtx.createRecord("key" to "EPT")
        val sdProject = projectCtx.createRecord("key" to "SD")

        val ref0 = createRecord("project" to sdProject)
        assertThat(ref0.getLocalId()).isEqualTo("SD-1")
        assertThat(records.getAtt(ref0, "$counterAtt?num").asInt()).isEqualTo(1)

        repeat(5) {
            updateRecord(ref0, DbRecordsControlAtts.UPDATE_COUNTER_ATT to counterAtt)
        }
        assertThat(records.getAtt(ref0, "$counterAtt?num").asInt()).isEqualTo(6)

        val ref1 = updateRecord(
            ref0,
            "project" to eptProject,
            DbRecordsControlAtts.UPDATE_COUNTER_ATT to counterAtt,
            DbRecordsControlAtts.UPDATE_ID to true
        )

        assertThat(ref1.getLocalId()).isEqualTo("EPT-1")
        assertThat(records.getAtt(ref1, "$counterAtt?num").asInt()).isEqualTo(1)

        val ref2 = updateRecord(
            ref1,
            "project" to sdProject,
            DbRecordsControlAtts.UPDATE_COUNTER_ATT to counterAtt,
            DbRecordsControlAtts.UPDATE_ID to true
        )

        assertThat(ref2.getLocalId()).isEqualTo("SD-7")
        assertThat(records.getAtt(ref2, "$counterAtt?num").asInt()).isEqualTo(7)
    }

    @Test
    fun testWithLocalIdTemplate() {

        registerNumTemplate(NumTemplateDef.create().withId("test-num").build())
        registerNumTemplate(NumTemplateDef.create().withId("custom2").build())

        fun registerTypeWithLocalIdTemplate(template: String) {
            registerType()
                .withNumTemplateRef(EntityRef.valueOf("test-num"))
                .withLocalIdTemplate(template)
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
        registerTypeWithLocalIdTemplate("\${project.key}-\${_docNum}")

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

        registerTypeWithLocalIdTemplate("\${project.key}-\${attWithCounter}-Q")

        assertThat(records.getAtt(newRef, "text").asText()).isEqualTo("abc")
        assertThat(records.getAtt(newRef, "attWithCounter?num").asInt()).isEqualTo(1)

        val newRef2 = records.mutateAtt(newRef, DbRecordsControlAtts.UPDATE_ID, true)
        assertThat(newRef2.getLocalId()).isEqualTo("SD-1-Q")

        val refWithSd = createRecord("project" to sdProject)
        assertThat(refWithSd.getLocalId()).isEqualTo("SD-2-Q")

        val refWithEpt = updateRecord(
            refWithSd,
            "project" to eptProject,
            DbRecordsControlAtts.UPDATE_ID to true
        )
        assertThat(refWithEpt.getLocalId()).isEqualTo("EPT-2-Q")

        updateRecord(refWithEpt, DbRecordsControlAtts.UPDATE_COUNTER_ATT to "_docNum")
        assertThat(records.getAtt(refWithEpt, RecordConstants.ATT_DOC_NUM).asInt()).isEqualTo(3)

        updateRecord(refWithEpt, DbRecordsControlAtts.UPDATE_COUNTER_ATT to "attWithCounter")
        assertThat(records.getAtt(refWithEpt, "attWithCounter").asInt()).isEqualTo(3)

        val refWithSdWithUpdatedDocNum = updateRecord(
            refWithEpt,
            "project" to sdProject,
            DbRecordsControlAtts.UPDATE_COUNTER_ATT to "attWithCounter",
            DbRecordsControlAtts.UPDATE_ID to true
        )
        assertThat(refWithSdWithUpdatedDocNum.getLocalId()).isEqualTo("SD-4-Q")

        registerTypeWithLocalIdTemplate("\${project.key}-\${_docNum}")

        val refWithDocNumInLocalId = updateRecord(
            refWithSdWithUpdatedDocNum,
            DbRecordsControlAtts.UPDATE_ID to true
        )

        assertThat(refWithDocNumInLocalId.getLocalId()).isEqualTo("SD-3")

        val refWithDocNumInLocalIdAndEptProject = updateRecord(
            refWithDocNumInLocalId,
            "project" to eptProject,
            DbRecordsControlAtts.UPDATE_ID to true
        )

        assertThat(refWithDocNumInLocalIdAndEptProject.getLocalId()).isEqualTo("EPT-3")

        val refWithDocNumInLocalIdAndSdProject = updateRecord(
            refWithDocNumInLocalIdAndEptProject,
            "project" to sdProject,
            DbRecordsControlAtts.UPDATE_COUNTER_ATT to "_docNum",
            DbRecordsControlAtts.UPDATE_ID to true
        )

        assertThat(refWithDocNumInLocalIdAndSdProject.getLocalId()).isEqualTo("SD-4")
        assertThat(records.getAtt(refWithDocNumInLocalIdAndSdProject, "_docNum?num").asInt()).isEqualTo(4)
        assertThat(records.getAtt(refWithDocNumInLocalIdAndSdProject, "attWithCounter?num").asInt()).isEqualTo(4)

        updateRecord(
            refWithDocNumInLocalIdAndSdProject,
            DbRecordsControlAtts.UPDATE_COUNTER_ATT to listOf("_docNum", "attWithCounter")
        )

        assertThat(records.getAtt(refWithDocNumInLocalIdAndSdProject, "_docNum?num").asInt()).isEqualTo(5)
        assertThat(records.getAtt(refWithDocNumInLocalIdAndSdProject, "attWithCounter?num").asInt()).isEqualTo(5)
    }

    @Test
    fun testWithCustomIdAndLocalIdTemplate() {

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
        assertThat(records.getAtt(ref, "_movedToRef?id").asText()).isEqualTo(newRef.toString())
        assertThat(records.getAtt(ref, "_movedToRef.scope").asText()).isEqualTo("uiserv")

        assertThat(records.getAtt(newRef, "id").asText()).isEqualTo("function-enabled-new")
        val json2 = records.getAtt(newRef, "?json")
        assertThat(json2).isEqualTo(
            DataValue.createObj()
                .set("id", "function-enabled-new")
                .set("scope", "uiserv")
        )

        val migrationRecords = mainCtx.selectAllFromTable("ed_record_ref_move_history")
        assertThat(migrationRecords).hasSize(1)
        val migrationRecord = migrationRecords[0]

        val migratedFrom = (migrationRecord["__moved_from"] as String).toEntityRef()
        val migratedTo = (migrationRecord["__moved_to"] as String).toEntityRef()
        assertThat(migratedFrom.getLocalId()).isEqualTo("uiserv\$function-enabled")
        assertThat(migratedTo.getLocalId()).isEqualTo("uiserv\$function-enabled-new")

        val newRef2 = records.mutateAtt(newRef, DbRecordsControlAtts.UPDATE_ID, "qwerty")
        assertThat(records.getAtt(newRef2, "id").asText()).isEqualTo("qwerty")

        val migrationRecords2 = mainCtx.selectAllFromTable("ed_record_ref_move_history")
        assertThat(migrationRecords2).hasSize(2)
    }

    @Test
    fun testWithChangeLocalIdToPrevValue() {

        fun regTypeWithIdTemplate(template: String) {
            registerType()
                .withLocalIdTemplate(template)
                .withAttributes(
                    AttributeDef.create().withId("scope"),
                    AttributeDef.create().withId("assoc").withType(AttributeType.ASSOC)
                )
                .register()
        }

        regTypeWithIdTemplate("\${scope}$\${id}")

        val initialRef = createRecord("scope" to "uiserv", "id" to "custom-id")
        assertThat(initialRef.getLocalId()).isEqualTo("uiserv\$custom-id")

        regTypeWithIdTemplate("\${scope}___\${id}")

        assertThat(records.getAtt(initialRef, "scope").asText()).isEqualTo("uiserv")
        val newRef = records.mutateAtt(initialRef, DbRecordsControlAtts.UPDATE_ID, true)

        assertThat(records.getAtt(initialRef, "scope").asText()).isEqualTo("")
        assertThat(records.getAtt(newRef, "scope").asText()).isEqualTo("uiserv")

        regTypeWithIdTemplate("\${scope}$\${id}")

        assertThat(records.getAtt(initialRef, "scope").asText()).isEqualTo("")
        assertThat(records.getAtt(newRef, "scope").asText()).isEqualTo("uiserv")

        val oldRef = records.mutateAtt(newRef, DbRecordsControlAtts.UPDATE_ID, true)
        assertThat(oldRef).isEqualTo(initialRef)

        assertThat(records.getAtt(initialRef, "scope").asText()).isEqualTo("uiserv")
        assertThat(records.getAtt(newRef, "scope").asText()).isEqualTo("")

        assertThat(records.getAtt(newRef, "_movedToRef?id").asText()).isEqualTo(initialRef.toString())

        // assocs to moved refs should be created with movedTo ref
        val refWithAssoc = createRecord("assoc" to newRef, "scope" to "test", "id" to "test")
        assertThat(records.getAtt(refWithAssoc, "assoc?id").asText()).isEqualTo(initialRef.toString())
    }
}
