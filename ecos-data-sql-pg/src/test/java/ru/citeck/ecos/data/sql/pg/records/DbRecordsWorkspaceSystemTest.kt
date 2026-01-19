package ru.citeck.ecos.data.sql.pg.records

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import ru.citeck.ecos.context.lib.auth.AuthContext
import ru.citeck.ecos.data.sql.pg.records.commons.DbRecordsTestBase
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.type.dto.QueryPermsPolicy
import ru.citeck.ecos.model.lib.type.dto.WorkspaceScope
import ru.citeck.ecos.webapp.api.entity.EntityRef

class DbRecordsWorkspaceSystemTest : DbRecordsTestBase() {

    @Test
    fun test() {

        registerType()
            .withAttributes(
                AttributeDef.create().withId("text").build()
            ).withSysAttributes(
                AttributeDef.create().withId("sys_text").build()
            ).withDefaultWorkspace("ws0")
            .withWorkspaceScope(WorkspaceScope.PRIVATE)
            .register()

        setQueryPermsPolicy(QueryPermsPolicy.OWN)

        val ref0 = createRecord("text" to "abc0", "sys_text" to "sys_abc0")
        val ref1 = createRecord("text" to "abc1", "sys_text" to "sys_abc1")

        listOf(ref0, ref1).forEachIndexed { idx, it ->
            assertThat(records.getAtt(it, "_workspace?localId!").asText()).isEqualTo("ws0")
            assertThat(records.getAtt(it, "text").asText()).isEqualTo("abc$idx")
            assertThat(records.getAtt(it, "sys_text").asText()).isEqualTo("sys_abc$idx")
        }

        setAuthoritiesWithReadPerms(ref0, "user0")
        workspaceService.setUserWorkspaces("user0", setOf("ws0"))
        workspaceService.setUserWorkspaces("user1", setOf("ws0"))

        fun checkQueryRes(vararg expected: EntityRef) {
            assertThat(records.query(baseQuery).getRecords()).containsExactlyInAnyOrder(*expected)
        }
        checkQueryRes(ref0, ref1)

        AuthContext.runAs("user0", listOf("ROLE_USER")) {
            checkQueryRes(ref0, ref1)
        }
        AuthContext.runAs("user1", listOf("ROLE_USER")) {
            checkQueryRes(ref1)
        }

        val ref2 = createRecord("text" to "ref2", "sys_text" to "ref2sys")

        workspaceService.impl.runAsWsSystem("ws0") {

            val auth = AuthContext.getCurrentRunAsAuth()
            assertThat(auth.getUser()).isEqualTo("ws_system_ws0-sid")
            assertThat(auth.getAuthorities()).contains("ROLE_WS_SYSTEM")

            listOf(ref0, ref1).forEachIndexed { idx, it ->
                assertThat(records.getAtt(it, "_workspace?localId!").asText()).isEqualTo("ws0")
                assertThat(records.getAtt(it, "text").asText()).isEqualTo("abc$idx")
                assertThat(records.getAtt(it, "sys_text").asText()).isEqualTo("sys_abc$idx")
            }

            val ref3 = createRecord("text" to "ws-system", "sys_text" to "value")
            assertThat(records.getAtt(ref3, "_workspace?localId!").asText()).isEqualTo("ws0")
            assertThat(records.getAtt(ref3, "text").asText()).isEqualTo("ws-system")
            assertThat(records.getAtt(ref3, "sys_text").asText()).isEqualTo("value")

            listOf(ref2, ref3).forEach {
                records.mutateAtt(it, "sys_text", "value2")
                assertThat(records.getAtt(it, "sys_text").asText()).isEqualTo("value2")
                records.mutateAtt(it, "text", "value22")
                assertThat(records.getAtt(it, "text").asText()).isEqualTo("value22")
            }

            checkQueryRes(ref0, ref1, ref2, ref3)

            records.delete(ref3)

            checkQueryRes(ref0, ref1, ref2)
        }

        checkQueryRes(ref0, ref1, ref2)

        val ref4 = createRecord()

        checkQueryRes(ref0, ref1, ref2, ref4)

        workspaceService.impl.runAsWsSystem("ws1") {

            val auth = AuthContext.getCurrentRunAsAuth()
            assertThat(auth.getUser()).isEqualTo("ws_system_ws1-sid")
            assertThat(auth.getAuthorities()).contains("ROLE_WS_SYSTEM")

            listOf(ref0, ref1).forEach {
                assertThat(records.getAtt(it, "_workspace?localId").asJavaObj()).isNull()
                assertThat(records.getAtt(it, "text").asJavaObj()).isNull()
                assertThat(records.getAtt(it, "sys_text").asJavaObj()).isNull()
            }

            assertThrows<Exception> {
                createRecord("text" to "ws-system")
            }
            assertThrows<Exception> {
                records.mutateAtt(ref2, "sys_text", "value2")
            }

            checkQueryRes()

            assertThrows<Exception> {
                records.delete(ref0)
            }
            assertThrows<Exception> {
                records.delete(listOf(ref0, ref1, ref2, ref4))
            }

            checkQueryRes()
        }

        checkQueryRes(ref0, ref1, ref2, ref4)

        workspaceService.impl.runAsWsSystemBySystemId("ws1-sid") {
            val auth = AuthContext.getCurrentRunAsAuth()
            assertThat(auth.getUser()).isEqualTo("ws_system_ws1-sid")
            assertThat(auth.getAuthorities()).contains("ROLE_WS_SYSTEM")
        }
    }

    @Test
    fun accessToPublicRecordsFromWsSystem() {

        registerType()
            .withAttributes(
                AttributeDef.create().withId("text").build()
            ).withWorkspaceScope(WorkspaceScope.PUBLIC).register()

        val publicRef = createRecord("text" to "abc")

        assertThat(records.getAtt(publicRef, "text").asText()).isEqualTo("abc")
        workspaceService.impl.runAsWsSystem("any-ws") {
            assertThat(records.getAtt(publicRef, "text").asText()).isEqualTo("abc")
        }
    }
}
