package ru.citeck.ecos.data.sql.pg.records

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import ru.citeck.ecos.context.lib.auth.AuthContext
import ru.citeck.ecos.data.sql.pg.records.commons.DbRecordsTestBase
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType
import ru.citeck.ecos.model.lib.type.dto.QueryPermsPolicy
import ru.citeck.ecos.model.lib.type.dto.WorkspaceScope

class DbRecordsManagedWorkspacePermsTest : DbRecordsTestBase() {

    private fun registerTypeWithManagedWorkspace() {
        registerType()
            .withAttributes(
                AttributeDef.create().withId("text").build(),
                AttributeDef.create {
                    withId("workspaceManagedBy")
                    withType(AttributeType.ASSOC)
                }
            )
            .withDefaultWorkspace("ws-portfolio")
            .withWorkspaceScope(WorkspaceScope.PRIVATE)
            .register()
        setQueryPermsPolicy(QueryPermsPolicy.OWN)
    }

    @Test
    fun managedWorkspaceAccessTest() {

        registerTypeWithManagedWorkspace()

        val projectRec = createRecord("text" to "my-project")

        // wsRecord represents a workspace entity that manages projectRec via source association
        val wsRecord = createRecord("workspaceManagedBy" to projectRec)
        val managedWsId = wsRecord.getLocalId()

        setAuthoritiesWithReadPerms(projectRec, "user0", "user1", "user2")

        // user0: member of managed workspace -> should have access
        workspaceService.setUserWorkspaces("user0", setOf(managedWsId))
        AuthContext.runAs("user0", listOf("ROLE_USER")) {
            assertThat(records.getAtt(projectRec, "text").asText()).isEqualTo("my-project")
        }

        // user1: member of unrelated workspace -> should NOT have access
        workspaceService.setUserWorkspaces("user1", setOf("some-other-ws"))
        AuthContext.runAs("user1", listOf("ROLE_USER")) {
            assertThat(records.getAtt(projectRec, "text").asJavaObj()).isNull()
        }

        // user2: direct member of record's workspace -> should have access (existing behavior)
        workspaceService.setUserWorkspaces("user2", setOf("ws-portfolio"))
        AuthContext.runAs("user2", listOf("ROLE_USER")) {
            assertThat(records.getAtt(projectRec, "text").asText()).isEqualTo("my-project")
        }
    }

    @Test
    fun noManagedWorkspaceAssocShouldNotGrantAccess() {

        registerTypeWithManagedWorkspace()

        // Record without any workspaceManagedBy association
        val projectRec = createRecord("text" to "my-project")

        workspaceService.setUserWorkspaces("user0", setOf("any-ws"))
        setAuthoritiesWithReadPerms(projectRec, "user0")

        AuthContext.runAs("user0", listOf("ROLE_USER")) {
            assertThat(records.getAtt(projectRec, "text").asJavaObj()).isNull()
        }
    }
}
