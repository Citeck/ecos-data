package ru.citeck.ecos.data.sql.test.records

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import ru.citeck.ecos.commons.data.MLText
import ru.citeck.ecos.commons.data.ObjectData
import ru.citeck.ecos.context.lib.auth.AuthContext
import ru.citeck.ecos.data.sql.records.DbRecordsControlAtts
import ru.citeck.ecos.data.sql.records.listener.DbRecordChangedEvent
import ru.citeck.ecos.data.sql.records.listener.DbRecordStatusChangedEvent
import ru.citeck.ecos.data.sql.records.listener.DbRecordTypeChangedEvent
import ru.citeck.ecos.data.sql.records.listener.DbRecordsListenerAdapter
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType
import ru.citeck.ecos.model.lib.attributes.dto.computed.ComputedAttDef
import ru.citeck.ecos.model.lib.attributes.dto.computed.ComputedAttStoringType
import ru.citeck.ecos.model.lib.attributes.dto.computed.ComputedAttType
import ru.citeck.ecos.model.lib.num.dto.NumTemplateDef
import ru.citeck.ecos.model.lib.procstages.dto.ProcStageDef
import ru.citeck.ecos.model.lib.status.dto.StatusDef
import ru.citeck.ecos.model.lib.type.dto.TypeInfo
import ru.citeck.ecos.model.lib.type.dto.TypeModelDef
import ru.citeck.ecos.model.lib.type.dto.WorkspaceScope
import ru.citeck.ecos.model.lib.utils.ModelUtils
import ru.citeck.ecos.records2.RecordConstants
import ru.citeck.ecos.webapp.api.entity.EntityRef

class DbRecordsUpdateTypeTest : DbRecordsTestBase() {

    companion object {
        private const val TYPE_A = "type-a"
        private const val TYPE_B = "type-b"
    }

    @BeforeEach
    fun beforeEach() {
        // base type of the records DAO. Types used in tests are registered as its subtypes
        registerType().register()
    }

    @Test
    fun typeChangeWithinSameSourceIdTest() {

        registerTestType(TYPE_A, attributes = listOf("attA", "common"))
        registerTestType(TYPE_B, attributes = listOf("attB", "common"))

        val rec = createRecordWithType(TYPE_A, "attA" to "a-value", "common" to "common-value")
        assertThat(getTypeId(rec)).isEqualTo(TYPE_A)

        records.mutateAtt(rec, DbRecordsControlAtts.UPDATE_TYPE, TYPE_B)

        assertThat(getTypeId(rec)).isEqualTo(TYPE_B)
        assertThat(records.getAtt(rec, "common").asText()).isEqualTo("common-value")
        // value of the attribute which is not a part of the new type model is kept as is
        assertThat(records.getAtt(rec, "attA").asText()).isEqualTo("a-value")

        updateRecord(rec, "attB" to "b-value")
        assertThat(records.getAtt(rec, "attB").asText()).isEqualTo("b-value")

        records.mutateAtt(rec, DbRecordsControlAtts.UPDATE_TYPE, ModelUtils.getTypeRef(TYPE_A).toString())

        assertThat(getTypeId(rec)).isEqualTo(TYPE_A)
        assertThat(records.getAtt(rec, "attA").asText()).isEqualTo("a-value")
        assertThat(records.getAtt(rec, "attB").asText()).isEqualTo("b-value")
        assertThat(records.getAtt(rec, "common").asText()).isEqualTo("common-value")
    }

    @Test
    fun typeChangeToAnotherSourceIdIsDeniedTest() {

        registerTestType(TYPE_A)
        registerTestType(TYPE_B, sourceId = "another-source-id")

        val rec = createRecordWithType(TYPE_A)

        val ex = assertThrows<Exception> {
            records.mutateAtt(rec, DbRecordsControlAtts.UPDATE_TYPE, TYPE_B)
        }
        assertThat(ex.message).contains("Type can be changed only to the type with the same sourceId")
        assertThat(getTypeId(rec)).isEqualTo(TYPE_A)
    }

    @Test
    fun typeChangeToUnknownTypeIsDeniedTest() {

        registerTestType(TYPE_A)
        val rec = createRecordWithType(TYPE_A)

        val ex = assertThrows<Exception> {
            records.mutateAtt(rec, DbRecordsControlAtts.UPDATE_TYPE, "unknown-type")
        }
        assertThat(ex.message).contains("unknown-type")
        assertThat(getTypeId(rec)).isEqualTo(TYPE_A)
    }

    @Test
    fun emptyTypeValueIsDeniedTest() {

        registerTestType(TYPE_A)
        val rec = createRecordWithType(TYPE_A)

        val ex = assertThrows<Exception> {
            records.mutateAtt(rec, DbRecordsControlAtts.UPDATE_TYPE, "")
        }
        assertThat(ex.message).contains("${DbRecordsControlAtts.UPDATE_TYPE} attribute is empty")
        assertThat(getTypeId(rec)).isEqualTo(TYPE_A)
    }

    @Test
    fun typeChangeForNewRecordIsDeniedTest() {

        registerTestType(TYPE_A)
        registerTestType(TYPE_B)

        val ex = assertThrows<Exception> {
            records.create(
                recordsDao.getId(),
                mapOf(
                    RecordConstants.ATT_TYPE to ModelUtils.getTypeRef(TYPE_A),
                    DbRecordsControlAtts.UPDATE_TYPE to TYPE_B
                )
            )
        }
        assertThat(ex.message).contains("Type can't be changed for a new record")
    }

    @Test
    fun sameTypeIsNoopTest() {

        registerTestType(
            TYPE_A,
            attributes = listOf("attA"),
            dispNameTemplate = MLText("A: \${attA}")
        )

        val events = registerTypeChangedListener()
        val rec = createRecordWithType(TYPE_A, "attA" to "a-value")
        val modifiedBefore = records.getAtt(rec, "_modified").asText()
        assertThat(records.getAtt(rec, "?disp").asText()).isEqualTo("A: a-value")

        // nothing should be recalculated and saved when neither type nor status is changed
        registerTestType(
            TYPE_A,
            attributes = listOf("attA"),
            dispNameTemplate = MLText("Updated: \${attA}")
        )

        records.mutateAtt(rec, DbRecordsControlAtts.UPDATE_TYPE, TYPE_A)

        assertThat(getTypeId(rec)).isEqualTo(TYPE_A)
        assertThat(records.getAtt(rec, "?disp").asText()).isEqualTo("A: a-value")
        assertThat(records.getAtt(rec, "_modified").asText()).isEqualTo(modifiedBefore)
        assertThat(events).isEmpty()
    }

    @Test
    fun statusSupportedByNewTypeTest() {

        registerTestType(TYPE_A, statuses = listOf("common", "only-a"))
        registerTestType(TYPE_B, statuses = listOf("common", "only-b"))

        val rec = createRecordWithType(TYPE_A)
        updateRecord(rec, "_status" to "common")

        records.mutateAtt(rec, DbRecordsControlAtts.UPDATE_TYPE, TYPE_B)

        assertThat(getTypeId(rec)).isEqualTo(TYPE_B)
        assertThat(records.getAtt(rec, "_status?localId").asText()).isEqualTo("common")
    }

    @Test
    fun statusNotSupportedByNewTypeIsDeniedTest() {

        registerTestType(TYPE_A, statuses = listOf("common", "only-a"))
        registerTestType(TYPE_B, statuses = listOf("common", "only-b"))

        val rec = createRecordWithType(TYPE_A)
        updateRecord(rec, "_status" to "only-a")

        val ex = assertThrows<Exception> {
            records.mutateAtt(rec, DbRecordsControlAtts.UPDATE_TYPE, TYPE_B)
        }
        assertThat(ex.message).contains("Status 'only-a' is not supported by type '$TYPE_B'")

        assertThat(getTypeId(rec)).isEqualTo(TYPE_A)
        assertThat(records.getAtt(rec, "_status?localId").asText()).isEqualTo("only-a")
    }

    @Test
    fun statusMayBeChangedWithTypeTest() {

        registerTestType(TYPE_A, statuses = listOf("common", "only-a"))
        registerTestType(TYPE_B, statuses = listOf("common", "only-b"))

        val rec = createRecordWithType(TYPE_A)
        updateRecord(rec, "_status" to "only-a")

        val statusEvents = ArrayList<Pair<String, String>>()
        recordsDao.addListener(
            object : DbRecordsListenerAdapter() {
                override fun onStatusChanged(event: DbRecordStatusChangedEvent) {
                    statusEvents.add(event.before.id to event.after.id)
                }
            }
        )

        records.mutate(
            rec,
            mapOf(
                DbRecordsControlAtts.UPDATE_TYPE to TYPE_B,
                "_status" to "only-b"
            )
        )

        assertThat(getTypeId(rec)).isEqualTo(TYPE_B)
        assertThat(records.getAtt(rec, "_status?localId").asText()).isEqualTo("only-b")
        assertThat(statusEvents).containsExactly("only-a" to "only-b")
    }

    @Test
    fun statusFromMutationIsValidatedTest() {

        registerTestType(TYPE_A, statuses = listOf("common", "only-a"))
        registerTestType(TYPE_B, statuses = listOf("common", "only-b"))

        val rec = createRecordWithType(TYPE_A)
        updateRecord(rec, "_status" to "common")

        val ex = assertThrows<Exception> {
            records.mutate(
                rec,
                mapOf(
                    DbRecordsControlAtts.UPDATE_TYPE to TYPE_B,
                    "_status" to "only-a"
                )
            )
        }
        assertThat(ex.message).contains("Status 'only-a' is not supported by type '$TYPE_B'")
        assertThat(getTypeId(rec)).isEqualTo(TYPE_A)
    }

    @Test
    fun statusMayBeResetToChangeTypeWithoutStatusesTest() {

        registerTestType(
            TYPE_A,
            statuses = listOf("only-a"),
            stages = listOf("stage-a" to listOf("only-a"))
        )
        registerTestType(TYPE_B)

        val rec = createRecordWithType(TYPE_A)
        updateRecord(rec, "_status" to "only-a")
        assertThat(records.getAtt(rec, "_stage?localId").asText()).isEqualTo("stage-a")

        // without an explicit status reset the mutation is denied
        assertThrows<Exception> {
            records.mutateAtt(rec, DbRecordsControlAtts.UPDATE_TYPE, TYPE_B)
        }

        records.mutate(
            rec,
            mapOf(
                DbRecordsControlAtts.UPDATE_TYPE to TYPE_B,
                "_status" to ""
            )
        )

        assertThat(getTypeId(rec)).isEqualTo(TYPE_B)
        assertThat(records.getAtt(rec, "_status?localId").asText()).isEqualTo("")

        // null value of the status attribute resets the status the same way
        records.mutate(
            rec,
            ObjectData.create()
                .set(DbRecordsControlAtts.UPDATE_TYPE, TYPE_A)
                .set("_status", "only-a")
        )
        assertThat(records.getAtt(rec, "_status?localId").asText()).isEqualTo("only-a")

        records.mutate(
            rec,
            ObjectData.create()
                .set(DbRecordsControlAtts.UPDATE_TYPE, TYPE_B)
                .set("_status", null)
        )

        assertThat(getTypeId(rec)).isEqualTo(TYPE_B)
        assertThat(records.getAtt(rec, "_status?localId").asText()).isEqualTo("")
    }

    @Test
    fun stageIsResetWithStatusTest() {

        val stages = listOf("stage-common" to listOf("common"))
        registerTestType(TYPE_A, statuses = listOf("common"), stages = stages)
        registerTestType(TYPE_B, statuses = listOf("common"), stages = stages)

        val rec = createRecordWithType(TYPE_A)
        updateRecord(rec, "_status" to "common")
        assertThat(records.getAtt(rec, "_stage?localId").asText()).isEqualTo("stage-common")

        records.mutate(
            rec,
            mapOf(
                DbRecordsControlAtts.UPDATE_TYPE to TYPE_B,
                "_status" to ""
            )
        )

        assertThat(getTypeId(rec)).isEqualTo(TYPE_B)
        assertThat(records.getAtt(rec, "_status?localId").asText()).isEqualTo("")
        // stage is calculated by the status, so a blank status should reset it too
        assertThat(records.getAtt(rec, "_stage?localId").asText()).isEqualTo("")
    }

    @Test
    fun statusIsChangedWithoutTypeChangingTest() {

        registerTestType(TYPE_A, statuses = listOf("first", "second"))

        val rec = createRecordWithType(TYPE_A)
        updateRecord(rec, "_status" to "first")

        val events = registerTypeChangedListener()

        // type is the same, but the status from the same mutation should not be lost
        records.mutate(
            rec,
            mapOf(
                DbRecordsControlAtts.UPDATE_TYPE to TYPE_A,
                "_status" to "second"
            )
        )

        assertThat(getTypeId(rec)).isEqualTo(TYPE_A)
        assertThat(records.getAtt(rec, "_status?localId").asText()).isEqualTo("second")
        assertThat(events).isEmpty()
    }

    @Test
    fun auditFieldsAreUpdatedTest() {

        registerTestType(TYPE_A, statuses = listOf("common"))
        registerTestType(TYPE_B, statuses = listOf("common"))

        val rec = createRecordWithType(TYPE_A)
        updateRecord(rec, "_status" to "common")

        val modifiedBefore = records.getAtt(rec, "_modified").asText()
        val statusModifiedBefore = records.getAtt(rec, "_statusModified").asText()

        AuthContext.runAs("modifier-user") {
            records.mutate(
                rec,
                mapOf(
                    DbRecordsControlAtts.UPDATE_TYPE to TYPE_B,
                    "_status" to ""
                )
            )
        }

        assertThat(getTypeId(rec)).isEqualTo(TYPE_B)
        assertThat(records.getAtt(rec, "_modified").asText()).isNotEqualTo(modifiedBefore)
        assertThat(records.getAtt(rec, "_modifier?localId").asText()).isEqualTo("modifier-user")
        assertThat(records.getAtt(rec, "_statusModified").asText()).isNotEqualTo(statusModifiedBefore)
    }

    @Test
    fun auditFieldsAreNotUpdatedWithDisableAuditTest() {

        registerTestType(TYPE_A, statuses = listOf("common"))
        registerTestType(TYPE_B, statuses = listOf("common"))

        val rec = createRecordWithType(TYPE_A)
        updateRecord(rec, "_status" to "common")

        val modifiedBefore = records.getAtt(rec, "_modified").asText()
        val modifierBefore = records.getAtt(rec, "_modifier?localId").asText()
        val statusModifiedBefore = records.getAtt(rec, "_statusModified").asText()

        AuthContext.runAsSystem {
            records.mutate(
                rec,
                mapOf(
                    DbRecordsControlAtts.UPDATE_TYPE to TYPE_B,
                    "_status" to "",
                    DbRecordsControlAtts.DISABLE_AUDIT to true
                )
            )
        }

        assertThat(getTypeId(rec)).isEqualTo(TYPE_B)
        assertThat(records.getAtt(rec, "_status?localId").asText()).isEqualTo("")
        assertThat(records.getAtt(rec, "_modified").asText()).isEqualTo(modifiedBefore)
        assertThat(records.getAtt(rec, "_modifier?localId").asText()).isEqualTo(modifierBefore)
        assertThat(records.getAtt(rec, "_statusModified").asText()).isEqualTo(statusModifiedBefore)
    }

    @Test
    fun anotherWorkspaceScopeIsDeniedTest() {

        registerTestType(TYPE_A)
        registerTestType(TYPE_B, workspaceScope = WorkspaceScope.PRIVATE)

        val rec = createRecordWithType(TYPE_A)

        val ex = assertThrows<Exception> {
            records.mutateAtt(rec, DbRecordsControlAtts.UPDATE_TYPE, TYPE_B)
        }
        assertThat(ex.message).contains("Type can't be changed to the type with another workspace scope")
        assertThat(getTypeId(rec)).isEqualTo(TYPE_A)
    }

    @Test
    fun writePermissionsRequiredTest() {

        registerTestType(TYPE_A)
        registerTestType(TYPE_B)

        val rec = createRecordWithType(TYPE_A)
        setAuthoritiesWithReadPerms(rec, "user-with-write", "user-without-write")
        setAuthoritiesWithWritePerms(rec, setOf("user-with-write"))

        AuthContext.runAs("user-without-write") {
            val ex = assertThrows<Exception> {
                records.mutateAtt(rec, DbRecordsControlAtts.UPDATE_TYPE, TYPE_B)
            }
            assertThat(ex.message).containsIgnoringCase("Permissions Denied")
        }
        assertThat(getTypeId(rec)).isEqualTo(TYPE_A)

        AuthContext.runAs("user-with-write") {
            records.mutateAtt(rec, DbRecordsControlAtts.UPDATE_TYPE, TYPE_B)
        }
        assertThat(getTypeId(rec)).isEqualTo(TYPE_B)
    }

    @Test
    fun typeChangedEventTest() {

        registerTestType(TYPE_A)
        registerTestType(TYPE_B)

        val events = registerTypeChangedListener()

        val rec = createRecordWithType(TYPE_A)
        assertThat(events).isEmpty()

        records.mutateAtt(rec, DbRecordsControlAtts.UPDATE_TYPE, TYPE_B)
        assertThat(events).containsExactly(TYPE_A to TYPE_B)

        events.clear()

        AuthContext.runAsSystem {
            records.mutate(
                rec,
                mapOf(
                    DbRecordsControlAtts.UPDATE_TYPE to TYPE_A,
                    DbRecordsControlAtts.DISABLE_EVENTS to true
                )
            )
        }
        assertThat(getTypeId(rec)).isEqualTo(TYPE_A)
        assertThat(events).isEmpty()
    }

    @Test
    fun calculatedAttsRecomputedForNewTypeTest() {

        registerTestType(
            TYPE_A,
            attributes = listOf("common"),
            dispNameTemplate = MLText("A: \${common}")
        )
        registerTestType(
            TYPE_B,
            attributes = listOf("common"),
            dispNameTemplate = MLText("B: \${common}")
        )

        val changedEvents = ArrayList<DbRecordChangedEvent>()
        recordsDao.addListener(
            object : DbRecordsListenerAdapter() {
                override fun onChanged(event: DbRecordChangedEvent) {
                    changedEvents.add(event)
                }
            }
        )

        val rec = createRecordWithType(TYPE_A, "common" to "value")
        assertThat(records.getAtt(rec, "?disp").asText()).isEqualTo("A: value")

        changedEvents.clear()
        records.mutateAtt(rec, DbRecordsControlAtts.UPDATE_TYPE, TYPE_B)

        assertThat(records.getAtt(rec, "?disp").asText()).isEqualTo("B: value")
        assertThat(changedEvents).isEmpty()
    }

    @Test
    fun otherAttributesAreIgnoredTest() {

        registerTestType(TYPE_A, attributes = listOf("common"))
        registerTestType(TYPE_B, attributes = listOf("common"))

        val rec = createRecordWithType(TYPE_A, "common" to "value")

        records.mutate(
            rec,
            mapOf(
                DbRecordsControlAtts.UPDATE_TYPE to TYPE_B,
                "common" to "ignored-value"
            )
        )

        assertThat(getTypeId(rec)).isEqualTo(TYPE_B)
        assertThat(records.getAtt(rec, "common").asText()).isEqualTo("value")
    }

    @Test
    fun otherControlOperationsAreDeniedTest() {

        registerTestType(TYPE_A)
        registerTestType(TYPE_B)

        val rec = createRecordWithType(TYPE_A)

        listOf(
            DbRecordsControlAtts.UPDATE_ID,
            DbRecordsControlAtts.UPDATE_WORKSPACE,
            DbRecordsControlAtts.UPDATE_PERMISSIONS,
            DbRecordsControlAtts.UPDATE_CALCULATED_ATTS
        ).forEach { controlAtt ->
            val ex = assertThrows<Exception>(controlAtt) {
                records.mutate(
                    rec,
                    mapOf(
                        DbRecordsControlAtts.UPDATE_TYPE to TYPE_B,
                        controlAtt to true
                    )
                )
            }
            assertThat(ex.message).contains(
                "${DbRecordsControlAtts.UPDATE_TYPE} can't be used with '$controlAtt'"
            )
            assertThat(getTypeId(rec)).isEqualTo(TYPE_A)
        }
    }

    @Test
    fun counterAttUpdatingIsDeniedWithTypeTest() {

        registerNumTemplate(NumTemplateDef.create().withId("test-counter").build())

        val counterAtt = AttributeDef.create()
            .withId("attWithCounter")
            .withType(AttributeType.NUMBER)
            .withComputed(
                ComputedAttDef.create()
                    .withType(ComputedAttType.COUNTER)
                    .withConfig(ObjectData.create().set("numTemplateRef", "test-counter"))
                    .withStoringType(ComputedAttStoringType.ON_CREATE)
                    .build()
            ).build()

        registerTestType(TYPE_A, attributeDefs = listOf(counterAtt))
        registerTestType(TYPE_B, attributeDefs = listOf(counterAtt))

        val rec = createRecordWithType(TYPE_A)
        assertThat(records.getAtt(rec, "attWithCounter?num").asInt()).isEqualTo(1)

        val ex = assertThrows<Exception> {
            records.mutate(
                rec,
                mapOf(
                    DbRecordsControlAtts.UPDATE_TYPE to TYPE_B,
                    DbRecordsControlAtts.UPDATE_COUNTER_ATT to "attWithCounter"
                )
            )
        }
        assertThat(ex.message).contains(
            "${DbRecordsControlAtts.UPDATE_TYPE} can't be used with " +
                "'${DbRecordsControlAtts.UPDATE_COUNTER_ATT}'"
        )

        assertThat(getTypeId(rec)).isEqualTo(TYPE_A)
        assertThat(records.getAtt(rec, "attWithCounter?num").asInt()).isEqualTo(1)
    }

    private fun registerTypeChangedListener(): MutableList<Pair<String, String>> {
        val events = ArrayList<Pair<String, String>>()
        recordsDao.addListener(
            object : DbRecordsListenerAdapter() {
                override fun onTypeChanged(event: DbRecordTypeChangedEvent) {
                    events.add(event.before.id to event.typeDef.id)
                }
            }
        )
        return events
    }

    private fun getTypeId(rec: EntityRef): String {
        return records.getAtt(rec, "${RecordConstants.ATT_TYPE}?localId").asText()
    }

    private fun createRecordWithType(typeId: String, vararg atts: Pair<String, Any?>): EntityRef {
        return createRecord(
            RecordConstants.ATT_TYPE to ModelUtils.getTypeRef(typeId),
            *atts
        )
    }

    private fun registerTestType(
        typeId: String,
        attributes: List<String> = emptyList(),
        attributeDefs: List<AttributeDef> = emptyList(),
        statuses: List<String> = emptyList(),
        stages: List<Pair<String, List<String>>> = emptyList(),
        sourceId: String = "",
        workspaceScope: WorkspaceScope = WorkspaceScope.PUBLIC,
        dispNameTemplate: MLText = MLText.EMPTY
    ) {
        registerType(
            TypeInfo.create {
                withId(typeId)
                withParentRef(REC_TEST_TYPE_REF)
                withSourceId(sourceId)
                withWorkspaceScope(workspaceScope)
                withDispNameTemplate(dispNameTemplate)
                withModel(
                    TypeModelDef.create {
                        withAttributes(
                            attributes.map {
                                AttributeDef.create().withId(it).build()
                            } + attributeDefs
                        )
                        withStatuses(
                            statuses.map {
                                StatusDef.create().withId(it).build()
                            }
                        )
                        withStages(
                            stages.map { (stageId, stageStatuses) ->
                                ProcStageDef.create()
                                    .withId(stageId)
                                    .withStatuses(stageStatuses)
                                    .build()
                            }
                        )
                    }
                )
            }
        )
    }
}
