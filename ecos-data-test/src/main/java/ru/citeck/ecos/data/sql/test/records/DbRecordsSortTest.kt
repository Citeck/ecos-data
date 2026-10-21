package ru.citeck.ecos.data.sql.test.records

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import ru.citeck.ecos.context.lib.auth.AuthContext
import ru.citeck.ecos.context.lib.auth.AuthGroup
import ru.citeck.ecos.data.sql.records.DbRecordsControlAtts
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.type.dto.QueryPermsPolicy
import ru.citeck.ecos.model.lib.type.dto.WorkspaceScope
import ru.citeck.ecos.model.lib.utils.ModelUtils
import ru.citeck.ecos.records2.RecordConstants
import ru.citeck.ecos.records2.predicate.model.Predicates
import ru.citeck.ecos.records3.record.dao.query.dto.query.SortBy
import ru.citeck.ecos.webapp.api.entity.EntityRef
import java.time.Instant

class DbRecordsSortTest : DbRecordsTestBase() {

    @Test
    fun test() {

        registerAtts(
            listOf(
                AttributeDef.create()
                    .withId("test")
                    .build()
            )
        )
        setQueryPermsPolicy(QueryPermsPolicy.OWN)

        AuthContext.runAs("admin", listOf(AuthGroup.EVERYONE)) {

            val ref0 = createRecord("test" to "abc")
            Thread.sleep(100)
            val ref1 = createRecord("test" to "def")

            val refsFromQueryAsc = records.query(
                baseQuery.copy().withSortBy(
                    SortBy("_created", true)
                ).build()
            )

            assertThat(refsFromQueryAsc.getRecords()).hasSize(2)
            assertThat(refsFromQueryAsc.getRecords()[0]).isEqualTo(ref0)
            assertThat(refsFromQueryAsc.getRecords()[1]).isEqualTo(ref1)

            val refsFromQueryDesc = records.query(
                baseQuery.copy().withSortBy(
                    SortBy("_created", false)
                ).build()
            )

            assertThat(refsFromQueryDesc.getRecords()).hasSize(2)
            assertThat(refsFromQueryDesc.getRecords()[0]).isEqualTo(ref1)
            assertThat(refsFromQueryDesc.getRecords()[1]).isEqualTo(ref0)
        }
    }

    @Test
    fun sortByTypeGroupsRecordsInBothDirections() {

        registerType().asSubTypeWithId("sort-type-a").register()
        registerType().asSubTypeWithId("sort-type-b").register()
        val typeA = ModelUtils.getTypeRef("sort-type-a")
        val typeB = ModelUtils.getTypeRef("sort-type-b")
        val a1 = createRecord("id" to "a1", RecordConstants.ATT_TYPE to typeA)
        val b1 = createRecord("id" to "b1", RecordConstants.ATT_TYPE to typeB)
        val a0 = createRecord("id" to "a0", RecordConstants.ATT_TYPE to typeA)
        val b0 = createRecord("id" to "b0", RecordConstants.ATT_TYPE to typeB)
        val groups = if (dbRecordRefService.getIdByEntityRef(typeA) < dbRecordRefService.getIdByEntityRef(typeB)) {
            listOf(listOf(a0, a1), listOf(b0, b1))
        } else {
            listOf(listOf(b0, b1), listOf(a0, a1))
        }

        for (ascending in listOf(true, false)) {
            val expected = (if (ascending) groups else groups.reversed()).flatten()
            val result = records.query(
                baseQuery.copy {
                    withSortBy(listOf(SortBy(RecordConstants.ATT_TYPE, ascending), SortBy("id", true)))
                }
            )
            assertThat(result.getRecords()).containsExactlyElementsOf(expected)
            val filtered = records.query(
                baseQuery.copy {
                    withQuery(Predicates.eq(RecordConstants.ATT_TYPE, typeA))
                    withSortBy(listOf(SortBy(RecordConstants.ATT_TYPE, ascending), SortBy("id", true)))
                }
            )
            assertThat(filtered.getRecords()).containsExactly(a0, a1)
        }
    }

    @Test
    fun sortByWorkspaceGroupsRecordsInBothDirections() {

        registerType().withWorkspaceScope(WorkspaceScope.PRIVATE).register()
        val a1 = createRecord("id" to "a1", RecordConstants.ATT_WORKSPACE to "workspace-a")
        val b1 = createRecord("id" to "b1", RecordConstants.ATT_WORKSPACE to "workspace-b")
        val a0 = createRecord("id" to "a0", RecordConstants.ATT_WORKSPACE to "workspace-a")
        val b0 = createRecord("id" to "b0", RecordConstants.ATT_WORKSPACE to "workspace-b")
        val workspaceIds = getTableCtx().getWorkspaceService()
        val groups = if (workspaceIds.getIdByExtId("workspace-a") < workspaceIds.getIdByExtId("workspace-b")) {
            listOf(listOf(a0, a1), listOf(b0, b1))
        } else {
            listOf(listOf(b0, b1), listOf(a0, a1))
        }

        for (ascending in listOf(true, false)) {
            val expected = (if (ascending) groups else groups.reversed()).flatten()
            val result = records.query(
                baseQuery.copy {
                    withSortBy(listOf(SortBy(RecordConstants.ATT_WORKSPACE, ascending), SortBy("id", true)))
                }
            )
            assertThat(result.getRecords()).containsExactlyElementsOf(expected)
            val scoped = records.query(
                baseQuery.copy {
                    withWorkspaces(listOf("workspace-a"))
                    withSortBy(listOf(SortBy(RecordConstants.ATT_WORKSPACE, ascending), SortBy("id", true)))
                }
            )
            assertThat(scoped.getRecords()).containsExactly(a0, a1)
        }
    }

    @ParameterizedTest
    @ValueSource(strings = ["id", "_id", "_str", "_localId"])
    fun sortByIdentityAliases(attribute: String) {

        registerType().register()
        val z = createRecord("id" to "z-last")
        val a = createRecord("id" to "a-first")
        assertSortOrder(attribute, listOf(a, z))
    }

    @ParameterizedTest
    @ValueSource(strings = ["_created", "_modified", "cm:created", "cm:modified"])
    fun sortByAuditDateAliases(attribute: String) {

        registerType().register()
        val later = createRecord(
            DbRecordsControlAtts.DISABLE_AUDIT to true,
            RecordConstants.ATT_CREATED to Instant.parse("2026-02-01T00:00:00Z"),
            RecordConstants.ATT_MODIFIED to Instant.parse("2026-02-01T00:00:00Z")
        )
        val earlier = createRecord(
            DbRecordsControlAtts.DISABLE_AUDIT to true,
            RecordConstants.ATT_CREATED to Instant.parse("2026-01-01T00:00:00Z"),
            RecordConstants.ATT_MODIFIED to Instant.parse("2026-01-01T00:00:00Z")
        )
        assertSortOrder(attribute, listOf(earlier, later))
    }

    private fun assertSortOrder(attribute: String, ascending: List<EntityRef>) {
        for (direction in listOf(true, false)) {
            val result = records.query(baseQuery.copy { withSortBy(SortBy(attribute, direction)) })
            assertThat(result.getRecords()).containsExactlyElementsOf(if (direction) ascending else ascending.reversed())
        }
    }
}
