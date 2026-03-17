package ru.citeck.ecos.data.sql.pg.records

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import ru.citeck.ecos.context.lib.auth.AuthContext
import ru.citeck.ecos.context.lib.auth.AuthRole
import ru.citeck.ecos.data.sql.pg.records.commons.DbRecordsTestBase
import ru.citeck.ecos.data.sql.records.dao.atts.DbRecord
import ru.citeck.ecos.data.sql.records.workspace.DbWorkspaceDesc
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.type.dto.QueryPermsPolicy
import ru.citeck.ecos.model.lib.type.dto.WorkspaceScope
import ru.citeck.ecos.records2.predicate.model.Predicates
import ru.citeck.ecos.webapp.api.entity.EntityRef

class DbRecordsManagedWorkspacePermsTest : DbRecordsTestBase() {

    private fun registerTypeWithVisibleInWorkspaces() {
        registerType()
            .withAttributes(
                AttributeDef.create().withId("text").build()
            )
            .withDefaultWorkspace("ws-portfolio")
            .withWorkspaceScope(WorkspaceScope.PRIVATE)
            .register()
        setQueryPermsPolicy(QueryPermsPolicy.OWN)
    }

    private fun readVisibleWs(rec: EntityRef): List<String> {
        return AuthContext.runAsSystem {
            records.getAtt(rec, "${DbRecord.ATT_VISIBLE_IN_WORKSPACES}[]?localId").asList(String::class.java)
        }
    }

    // === Mutation permission tests ===

    @Test
    fun systemCanCreateAndUpdateVisibleInWorkspaces() {
        registerTypeWithVisibleInWorkspaces()

        val rec = AuthContext.runAsSystem {
            createRecord(
                "text" to "project",
                DbRecord.ATT_VISIBLE_IN_WORKSPACES to listOf(DbWorkspaceDesc.getRef("ws-a"))
            )
        }
        assertThat(readVisibleWs(rec)).containsExactly("ws-a")

        AuthContext.runAsSystem {
            updateRecord(
                rec,
                DbRecord.ATT_VISIBLE_IN_WORKSPACES to listOf(
                    DbWorkspaceDesc.getRef("ws-a"),
                    DbWorkspaceDesc.getRef("ws-b")
                )
            )
        }
        assertThat(readVisibleWs(rec)).containsExactlyInAnyOrder("ws-a", "ws-b")
    }

    @Test
    fun nonSystemContextCannotSetVisibleInWorkspaces() {
        registerTypeWithVisibleInWorkspaces()

        workspaceService.setUserWorkspaces("admin", setOf("ws-portfolio"))
        workspaceService.setUserWorkspaces("user0", setOf("ws-portfolio"))

        // admin cannot create with _visibleInWorkspaces
        assertThrows<RuntimeException> {
            AuthContext.runAs("admin", listOf(AuthRole.ADMIN)) {
                createRecord(
                    "text" to "project",
                    DbRecord.ATT_VISIBLE_IN_WORKSPACES to listOf(DbWorkspaceDesc.getRef("ws-a")),
                    "_workspace" to "ws-portfolio"
                )
            }
        }

        // regular user cannot create with _visibleInWorkspaces
        assertThrows<RuntimeException> {
            AuthContext.runAs("user0", listOf("ROLE_USER")) {
                createRecord(
                    "text" to "project",
                    DbRecord.ATT_VISIBLE_IN_WORKSPACES to listOf(DbWorkspaceDesc.getRef("ws-a")),
                    "_workspace" to "ws-portfolio"
                )
            }
        }

        // regular user cannot update _visibleInWorkspaces
        val rec = AuthContext.runAsSystem { createRecord("text" to "project") }
        setAuthoritiesWithWritePerms(rec, "user0")

        assertThrows<RuntimeException> {
            AuthContext.runAs("user0", listOf("ROLE_USER")) {
                updateRecord(
                    rec,
                    DbRecord.ATT_VISIBLE_IN_WORKSPACES to listOf(DbWorkspaceDesc.getRef("ws-a"))
                )
            }
        }
    }

    // === att_add_ / att_rem_ operation tests ===

    @Test
    fun addVisibleInWorkspacesOperation() {
        registerTypeWithVisibleInWorkspaces()

        val rec = AuthContext.runAsSystem {
            createRecord(
                "text" to "project",
                DbRecord.ATT_VISIBLE_IN_WORKSPACES to listOf(DbWorkspaceDesc.getRef("ws-a"))
            )
        }

        AuthContext.runAsSystem {
            updateRecord(rec, "att_add_${DbRecord.ATT_VISIBLE_IN_WORKSPACES}" to DbWorkspaceDesc.getRef("ws-b"))
        }

        assertThat(readVisibleWs(rec)).containsExactlyInAnyOrder("ws-a", "ws-b")
    }

    @Test
    fun removeVisibleInWorkspacesOperation() {
        registerTypeWithVisibleInWorkspaces()

        val rec = AuthContext.runAsSystem {
            createRecord(
                "text" to "project",
                DbRecord.ATT_VISIBLE_IN_WORKSPACES to listOf(
                    DbWorkspaceDesc.getRef("ws-a"),
                    DbWorkspaceDesc.getRef("ws-b")
                )
            )
        }

        AuthContext.runAsSystem {
            updateRecord(rec, "att_rem_${DbRecord.ATT_VISIBLE_IN_WORKSPACES}" to DbWorkspaceDesc.getRef("ws-a"))
        }

        assertThat(readVisibleWs(rec)).containsExactlyInAnyOrder("ws-b")
    }

    // === Read permission tests ===

    @Test
    fun visibleInWorkspacesGrantsReadAccess() {
        registerTypeWithVisibleInWorkspaces()

        val rec = AuthContext.runAsSystem {
            createRecord(
                "text" to "my-project",
                DbRecord.ATT_VISIBLE_IN_WORKSPACES to listOf(
                    DbWorkspaceDesc.getRef("managed-ws"),
                    DbWorkspaceDesc.getRef("ws-c")
                )
            )
        }

        setAuthoritiesWithReadPerms(rec, "user0", "user1", "user2")

        // user0: member of one of the workspaces listed in _visibleInWorkspaces -> access granted
        workspaceService.setUserWorkspaces("user0", setOf("managed-ws"))
        AuthContext.runAs("user0", listOf("ROLE_USER")) {
            assertThat(records.getAtt(rec, "text").asText()).isEqualTo("my-project")
        }

        // user1: member of unrelated workspace -> no access
        workspaceService.setUserWorkspaces("user1", setOf("some-other-ws"))
        AuthContext.runAs("user1", listOf("ROLE_USER")) {
            assertThat(records.getAtt(rec, "text").asJavaObj()).isNull()
        }

        // user2: direct member of record's own workspace -> access granted
        workspaceService.setUserWorkspaces("user2", setOf("ws-portfolio"))
        AuthContext.runAs("user2", listOf("ROLE_USER")) {
            assertThat(records.getAtt(rec, "text").asText()).isEqualTo("my-project")
        }
    }

    @Test
    fun noVisibleInWorkspacesShouldNotGrantAccess() {
        registerTypeWithVisibleInWorkspaces()

        val rec = createRecord("text" to "my-project")

        workspaceService.setUserWorkspaces("user0", setOf("any-ws"))
        setAuthoritiesWithReadPerms(rec, "user0")

        AuthContext.runAs("user0", listOf("ROLE_USER")) {
            assertThat(records.getAtt(rec, "text").asJavaObj()).isNull()
        }
    }

    // === Query filtering tests ===

    @Test
    fun queryReturnsRecordsVisibleInUserWorkspace() {
        registerTypeWithVisibleInWorkspaces()

        val recInWs = createRecord("text" to "in-ws")
        val recVisibleInOther = AuthContext.runAsSystem {
            createRecord(
                "text" to "visible-in-other",
                DbRecord.ATT_VISIBLE_IN_WORKSPACES to listOf(DbWorkspaceDesc.getRef("other-ws"))
            )
        }
        createRecord("text" to "invisible")

        workspaceService.setUserWorkspaces("user0", setOf("ws-portfolio", "other-ws"))
        setAuthoritiesWithReadPerms(recInWs, "user0")
        setAuthoritiesWithReadPerms(recVisibleInOther, "user0")

        AuthContext.runAs("user0", listOf("ROLE_USER")) {
            val result = records.query(
                baseQuery.copy()
                    .withWorkspaces(listOf("other-ws"))
                    .build()
            ).getRecords()

            // recVisibleInOther is in ws-portfolio but visible in other-ws
            assertThat(result).contains(recVisibleInOther)
            // recInWs is in ws-portfolio, not visible in other-ws
            assertThat(result).doesNotContain(recInWs)
        }
    }

    @Test
    fun queryInOwnWorkspaceStillWorks() {
        registerTypeWithVisibleInWorkspaces()

        val rec = createRecord("text" to "own-ws-rec")
        setAuthoritiesWithReadPerms(rec, "user0")

        workspaceService.setUserWorkspaces("user0", setOf("ws-portfolio"))
        AuthContext.runAs("user0", listOf("ROLE_USER")) {
            val result = records.query(
                baseQuery.copy()
                    .withWorkspaces(listOf("ws-portfolio"))
                    .build()
            ).getRecords()

            assertThat(result).contains(rec)
        }
    }

    @Test
    fun queryByVisibleInWorkspacesPredicate() {
        registerTypeWithVisibleInWorkspaces()

        val rec0 = AuthContext.runAsSystem {
            createRecord(
                "text" to "rec0",
                DbRecord.ATT_VISIBLE_IN_WORKSPACES to listOf(DbWorkspaceDesc.getRef("ws-x"))
            )
        }
        AuthContext.runAsSystem {
            createRecord(
                "text" to "rec1",
                DbRecord.ATT_VISIBLE_IN_WORKSPACES to listOf(DbWorkspaceDesc.getRef("ws-y"))
            )
        }
        createRecord("text" to "rec2")

        val result = records.query(
            baseQuery.copy()
                .withQuery(
                    Predicates.eq(
                        DbRecord.ATT_VISIBLE_IN_WORKSPACES,
                        DbWorkspaceDesc.getRef("ws-x").toString()
                    )
                )
                .build()
        ).getRecords()

        assertThat(result).containsExactly(rec0)
    }
}
