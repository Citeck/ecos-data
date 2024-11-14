package ru.citeck.ecos.data.sql.pg.records

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource
import ru.citeck.ecos.context.lib.auth.AuthContext
import ru.citeck.ecos.context.lib.auth.data.SimpleAuthData
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.type.dto.QueryPermsPolicy
import ru.citeck.ecos.model.lib.type.dto.TypeInfo
import ru.citeck.ecos.model.lib.type.dto.TypeModelDef
import ru.citeck.ecos.model.lib.type.dto.WorkspaceScope
import ru.citeck.ecos.records2.RecordConstants
import ru.citeck.ecos.records2.predicate.model.Predicates
import ru.citeck.ecos.webapp.api.constants.AppName
import ru.citeck.ecos.webapp.api.entity.EntityRef

class DbRecordsWorkspaceTest : DbRecordsTestBase() {

    companion object {
        @JvmStatic
        fun getTestVariants(): List<Array<*>> {
            return listOf(
                *WorkspaceScope.entries.map { arrayOf(it, true) }.toTypedArray(),
                *WorkspaceScope.entries.map { arrayOf(it, false) }.toTypedArray()
            )
        }
    }

    @ParameterizedTest
    @MethodSource("getTestVariants")
    fun test(scope: WorkspaceScope, asSystem: Boolean) {
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
