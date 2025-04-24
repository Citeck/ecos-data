package ru.citeck.ecos.data.sql.pg.records

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource
import org.junit.jupiter.params.provider.ValueSource
import ru.citeck.ecos.commons.data.DataValue
import ru.citeck.ecos.commons.data.ObjectData
import ru.citeck.ecos.context.lib.auth.AuthContext
import ru.citeck.ecos.context.lib.auth.AuthRole
import ru.citeck.ecos.context.lib.auth.data.SimpleAuthData
import ru.citeck.ecos.data.sql.pg.records.commons.DbRecordsTestBase
import ru.citeck.ecos.data.sql.records.dao.atts.DbRecord
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType
import ru.citeck.ecos.model.lib.type.dto.*
import ru.citeck.ecos.model.lib.utils.ModelUtils
import ru.citeck.ecos.records2.RecordConstants
import ru.citeck.ecos.records2.predicate.model.Predicates
import ru.citeck.ecos.webapp.api.constants.AppName
import ru.citeck.ecos.webapp.api.entity.EntityRef
import ru.citeck.ecos.webapp.api.entity.toEntityRef

class DbRecordsWorkspaceTest : DbRecordsTestBase() {

    companion object {
        @JvmStatic
        fun getRecCreateTestVariants(): List<Array<*>> {
            return listOf(
                *WorkspaceScope.entries.map { arrayOf(it, true) }.toTypedArray(),
                *WorkspaceScope.entries.map { arrayOf(it, false) }.toTypedArray()
            )
        }
    }

    @Test
    fun updateWorkspaceTest() {

        val dao = registerType()
            .withAttributes(
                AttributeDef.create()
                    .withId("children")
                    .withType(AttributeType.ASSOC)
                    .withMultiple(true)
                    .withConfig(ObjectData.create().set("child", true)),
                AttributeDef.create()
                    .withId("content")
                    .withType(AttributeType.CONTENT)
            )
            .addAspect(DbRecord.ASPECT_VERSIONABLE)
            .withWorkspaceScope(WorkspaceScope.PRIVATE).register()

        val ws0 = "workspace-0"
        val attWs = RecordConstants.ATT_WORKSPACE

        fun assertWs(rec: EntityRef, expectedWs: String) {
            assertThat(records.getAtt(rec, "$attWs?localId").asText()).isEqualTo(expectedWs)
        }

        val parent = dao.createRecord(attWs to ws0)
        assertWs(parent, ws0)
        val child0 = dao.createRecord(
            attWs to ws0,
            RecordConstants.ATT_PARENT to parent,
            RecordConstants.ATT_PARENT_ATT to "children"
        )
        assertWs(child0, ws0)
        val child1 = dao.createRecord(
            attWs to ws0,
            RecordConstants.ATT_PARENT to parent,
            RecordConstants.ATT_PARENT_ATT to "children"
        )
        assertWs(child1, ws0)

        val ws1 = "workspace-1"
        workspaceService.setUserWorkspaces("admin", setOf(ws0, ws1))
        workspaceService.setUserWorkspaces("user", setOf(ws0, ws1))
        dao.setAuthoritiesWithWritePerms(parent, AuthRole.ADMIN, "user")

        updateRecord(parent, "__updateWorkspace" to ws1)
        listOf(parent, child0, child1).forEach { assertWs(it, ws1) }

        AuthContext.runAs("admin", listOf(AuthRole.ADMIN)) {
            updateRecord(parent, "__updateWorkspace" to ws0)
            listOf(parent, child0, child1).forEach { assertWs(it, ws0) }
        }

        AuthContext.runAs("user") {
            assertThrows<RuntimeException> {
                updateRecord(parent, "__updateWorkspace" to ws1)
            }
        }
        listOf(parent, child0, child1).forEach { assertWs(it, ws0) }

        val contentData0 = createTempRecord(content = "content-0".toByteArray())
        val contentData1 = createTempRecord(content = "content-1".toByteArray())

        updateRecord(child0, "content" to contentData0)
        updateRecord(child0, "content" to contentData1)

        updateRecord(parent, "__updateWorkspace" to ws1)

        listOf(parent, child0, child1).forEach { assertWs(it, ws1) }
    }

    @ParameterizedTest
    @ValueSource(strings = [DbRecord.WS_DEFAULT, "other"])
    fun defaultWorkspaceTest(workspace: String) {

        registerType()
            .withWorkspaceScope(WorkspaceScope.PRIVATE)
            .withAttributes(AttributeDef.create().withId("text"))
            .register()

        assertThrows<Exception> {
            createRecord("text" to "abc")
        }
        fun assertWs(rec: EntityRef, expected: String) {
            assertThat(records.getAtt(rec, "_workspace?localId").asText()).isEqualTo(expected)
        }

        val rec0 = createRecord("text" to "abc", "_workspace" to "abc")
        assertWs(rec0, "abc")

        registerType()
            .withWorkspaceScope(WorkspaceScope.PRIVATE)
            .withAttributes(AttributeDef.create().withId("text"))
            .withDefaultWorkspace(workspace)
            .register()

        val rec1 = createRecord("text" to "abc")
        assertWs(rec1, workspace)
    }

    @Test
    fun changeWsScopeTest() {

        fun registerType(privateWsScope: Boolean) {
            registerType(
                TypeInfo.create()
                    .withId(REC_TEST_TYPE_ID)
                    .withWorkspaceScope(if (privateWsScope) WorkspaceScope.PRIVATE else WorkspaceScope.PUBLIC)
                    .withModel(
                        TypeModelDef.create()
                            .withAttributes(
                                listOf(
                                    AttributeDef.create()
                                        .withId("text")
                                        .build()
                                )
                            ).build()
                    ).build()
            )
        }

        registerType(false)

        fun assertWs(rec: EntityRef, expected: String?) {
            val ws = records.getAtt(rec, RecordConstants.ATT_WORKSPACE + "?id")
            if (expected == null) {
                assertThat(ws).isEqualTo(DataValue.NULL)
            } else {
                assertThat(ws.isTextual()).isTrue()
                val wsRef = EntityRef.valueOf(ws.asText())
                assertThat(wsRef.getAppName()).isEqualTo(AppName.EMODEL)
                assertThat(wsRef.getSourceId()).isEqualTo("workspace")
                assertThat(wsRef.getLocalId()).isEqualTo(expected)
            }
        }

        val rec0 = createRecord("text" to "abc")

        assertWs(rec0, null)
        assertThat(records.query(baseQuery).getRecords()).contains(rec0)
        assertThat(records.query(baseQuery.copy().withWorkspaces(listOf("default")).build()).getRecords()).contains(rec0)
        assertThat(records.query(baseQuery.copy().withWorkspaces(listOf("unknown")).build()).getRecords()).contains(rec0)

        registerType(true)

        assertWs(rec0, "default")
        assertThat(records.query(baseQuery).getRecords()).contains(rec0)
        assertThat(records.query(baseQuery.copy().withWorkspaces(listOf("default")).build()).getRecords()).contains(rec0)
        assertThat(records.query(baseQuery.copy().withWorkspaces(listOf("unknown")).build()).getRecords()).isEmpty()
    }

    @Test
    fun createChildTest() {

        registerType(
            TypeInfo.create()
                .withId(REC_TEST_TYPE_ID)
                .withWorkspaceScope(WorkspaceScope.PRIVATE)
                .withModel(
                    TypeModelDef.create()
                        .withAttributes(
                            listOf(
                                AttributeDef.create()
                                    .withId("text")
                                    .build(),
                                AttributeDef.create()
                                    .withId("children")
                                    .withType(AttributeType.ASSOC)
                                    .withConfig(ObjectData.create().set("child", true))
                                    .build()
                            )
                        ).build()
                ).build()
        )

        val privateWsChildTypeId = "private-ws-child-type"

        registerType(
            TypeInfo.create()
                .withId(privateWsChildTypeId)
                .withSourceId(privateWsChildTypeId)
                .withWorkspaceScope(WorkspaceScope.PRIVATE)
                .withModel(
                    TypeModelDef.create()
                        .withAttributes(listOf(AttributeDef.create().withId("text").build()))
                        .build()
                ).build()
        )

        val privateWsChildCtx = createRecordsDao(
            DEFAULT_TABLE_REF.withTable("private_ws_child_nodes"),
            ModelUtils.getTypeRef(privateWsChildTypeId),
            privateWsChildTypeId
        )

        val publicWsChildTypeId = "public-ws-child-type"

        registerType(
            TypeInfo.create()
                .withId(publicWsChildTypeId)
                .withSourceId(publicWsChildTypeId)
                .withWorkspaceScope(WorkspaceScope.PUBLIC)
                .withModel(
                    TypeModelDef.create()
                        .withAttributes(listOf(AttributeDef.create().withId("text").build()))
                        .build()
                ).build()
        )

        val publicWsChildCtx = createRecordsDao(
            DEFAULT_TABLE_REF.withTable("public_ws_child_nodes"),
            ModelUtils.getTypeRef(publicWsChildTypeId),
            publicWsChildTypeId
        )

        fun getAtt(rec: EntityRef, att: String) = records.getAtt(rec, att)
        fun getWsId(rec: EntityRef) = getAtt(rec, "_workspace?localId").asText()
        fun getWsRef(rec: EntityRef) = getAtt(rec, "_workspace?id").asText().toEntityRef()

        val testWsId = "test-ws"

        val mainRec = createRecord(
            "text" to "mainRec",
            "_workspace" to testWsId
        )
        assertThat(getWsId(mainRec)).isEqualTo(testWsId)
        assertThat(getWsRef(mainRec)).isEqualTo(EntityRef.create("emodel", "workspace", testWsId))

        val publicChildWithoutParent = publicWsChildCtx.createRecord("text" to "test")
        assertThat(getAtt(publicChildWithoutParent, "_workspace?id")).isEqualTo(DataValue.NULL)
        assertThat(getAtt(publicChildWithoutParent, "_workspace?localId")).isEqualTo(DataValue.NULL)

        val publicChildWithParent = publicWsChildCtx.createRecord(
            "text" to "test",
            "_parent" to mainRec,
            "_parentAtt" to "children"
        )

        assertThat(getWsId(publicChildWithParent)).isEqualTo("")

        assertThrows<RuntimeException> {
            privateWsChildCtx.createRecord("text" to "test")
        }
        val pwsChild0 = privateWsChildCtx.createRecord("text" to "test", "_workspace" to "child-ws")
        assertThat(getWsId(pwsChild0)).isEqualTo("child-ws")

        assertThrows<RuntimeException> {
            privateWsChildCtx.createRecord(
                "text" to "test",
                "_workspace" to "child-ws",
                "_parent" to mainRec,
                "_parentAtt" to "children"
            )
        }

        val pwsChild1 = privateWsChildCtx.createRecord(
            "text" to "test",
            "_workspace" to testWsId,
            "_parent" to mainRec,
            "_parentAtt" to "children"
        )
        assertThat(getWsId(pwsChild1)).isEqualTo(testWsId)

        val pwsChild2 = privateWsChildCtx.createRecord(
            "text" to "test",
            "_parent" to mainRec,
            "_parentAtt" to "children"
        )
        assertThat(getWsId(pwsChild2)).isEqualTo(testWsId)
    }

    @ParameterizedTest
    @MethodSource("getRecCreateTestVariants")
    fun recCreateTest(scope: WorkspaceScope, asSystem: Boolean) {
        registerType(
            TypeInfo.create {
                withId(REC_TEST_TYPE_ID)
                withWorkspaceScope(scope)
                withModel(
                    TypeModelDef.create {
                        withAttributes(
                            listOf(
                                AttributeDef.create()
                                    .withId("text")
                                    .build()
                            )
                        )
                    }
                )
            }
        )
        setQueryPermsPolicy(QueryPermsPolicy.OWN)

        val auth = if (asSystem) {
            AuthContext.SYSTEM_AUTH
        } else {
            workspaceService.setUserWorkspaces("user0", setOf("custom-ws"))
            SimpleAuthData("user0", listOf("GROUP_accountants"))
        }
        fun String.asWsRef(): EntityRef = EntityRef.create(AppName.EMODEL, "workspace", this)
        fun EntityRef.getTextAtt() = records.getAtt(this, "text").asText()
        fun EntityRef.getWsId() = records.getAtt(this, RecordConstants.ATT_WORKSPACE + "?localId").asText()

        fun assertQuery(workspaces: Set<String>, vararg expected: EntityRef) {
            val recs = records.query(
                baseQuery.copy()
                    .withWorkspaces(workspaces.toList())
                    .build()
            ).getRecords()
            assertThat(recs).containsExactlyInAnyOrder(*expected)
        }

        AuthContext.runAs(auth) {
            val isPrivate = scope == WorkspaceScope.PRIVATE
            if (isPrivate) {
                assertThrows<RuntimeException> {
                    // entities with private ws scope can't be created without ws
                    createRecord("text" to "abc")
                }
                if (!asSystem) {
                    assertThrows<RuntimeException> {
                        createRecord(
                            "text" to "abc",
                            // user doesn't included to "new-ws"
                            RecordConstants.ATT_WORKSPACE to "new-ws".asWsRef()
                        )
                    }
                    val rec = createRecord(
                        "text" to "abc",
                        RecordConstants.ATT_WORKSPACE to "custom-ws".asWsRef()
                    )
                    assertThat(rec.getTextAtt()).isEqualTo("abc")
                    assertThat(rec.getWsId()).isEqualTo("custom-ws")

                    assertQuery(setOf("custom-ws"), rec)
                    assertQuery(setOf("other-ws"))
                    assertQuery(emptySet(), rec)

                    val rec1 = AuthContext.runAsSystem {
                        createRecord(
                            "text" to "def",
                            RecordConstants.ATT_WORKSPACE to "other-ws".asWsRef()
                        )
                    }

                    assertQuery(setOf("custom-ws"), rec)
                    assertQuery(setOf("other-ws"))
                    assertQuery(emptySet(), rec)

                    AuthContext.runAsSystem {
                        assertQuery(setOf("custom-ws"), rec)
                        assertQuery(setOf("other-ws"), rec1)
                        assertQuery(setOf("other-ws", "custom-ws"), rec, rec1)
                        assertQuery(emptySet(), rec, rec1)
                    }
                } else {
                    val rec0 = createRecord(
                        "text" to "abc",
                        RecordConstants.ATT_WORKSPACE to "ws1".asWsRef()
                    )
                    assertThat(rec0.getTextAtt()).isEqualTo("abc")
                    val rec1 = createRecord(
                        "text" to "abc",
                        RecordConstants.ATT_WORKSPACE to "ws2".asWsRef()
                    )
                    assertQuery(setOf(), rec0, rec1)
                    assertQuery(setOf("ws1"), rec0)
                    assertQuery(setOf("ws2"), rec1)
                    assertQuery(setOf("ws1", "ws2"), rec0, rec1)
                }
            } else {
                val rec0 = createRecord("text" to "abc")
                assertThat(rec0.getTextAtt()).isEqualTo("abc")
                assertThat(rec0.getWsId()).isEqualTo("")
                val rec1 = createRecord(
                    "text" to "abc",
                    RecordConstants.ATT_WORKSPACE to "new-ws".asWsRef()
                )
                assertThat(rec1.getTextAtt()).isEqualTo("abc")
                assertThat(rec1.getWsId()).isEqualTo("")
            }
        }
    }

    @Test
    fun countTest() {

        fun registerType(scope: WorkspaceScope) {
            registerType(
                TypeInfo.create {
                    withId(REC_TEST_TYPE_ID)
                    withWorkspaceScope(scope)
                    withModel(
                        TypeModelDef.create {
                            withAttributes(
                                listOf(
                                    AttributeDef.create()
                                        .withId("text")
                                        .build()
                                )
                            )
                        }
                    )
                }
            )
        }
        registerType(WorkspaceScope.PUBLIC)
        setQueryPermsPolicy(QueryPermsPolicy.OWN)

        val rec0 = createRecord()
        val rec1 = createRecord()
        val rec2 = createRecord()

        fun assertQueryRes(workspaces: List<String>, vararg expected: EntityRef) {
            val records = records.query(
                baseQuery.copy()
                    .withQuery(Predicates.alwaysTrue())
                    .withWorkspaces(workspaces)
                    .build()
            )

            assertThat(records.getRecords()).containsExactlyElementsOf(expected.toList())
            assertThat(records.getTotalCount()).isEqualTo(expected.size.toLong())
        }

        assertQueryRes(emptyList(), rec0, rec1, rec2)
        assertQueryRes(listOf("test"), rec0, rec1, rec2)
        AuthContext.runAs("user0") {
            assertQueryRes(emptyList(), rec0, rec1, rec2)
            assertQueryRes(listOf("test"), rec0, rec1, rec2)
        }

        registerType(WorkspaceScope.PRIVATE)

        AuthContext.runAs("user0") {
            assertQueryRes(listOf("test"))
        }

        assertThrows<Exception> { createRecord() }

        val rec3 = createRecord("_workspace" to "test")

        AuthContext.runAs("user0") {
            assertQueryRes(emptyList(), rec0, rec1, rec2)
            assertQueryRes(listOf("test"))
        }

        workspaceService.setUserWorkspaces("user0", setOf("test"))

        AuthContext.runAs("user0") {
            assertQueryRes(emptyList(), rec0, rec1, rec2, rec3)
            assertQueryRes(listOf("test"), rec3)
        }
    }
}
