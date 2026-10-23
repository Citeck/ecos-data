package ru.citeck.ecos.data.sql.test.records

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import ru.citeck.ecos.commons.data.ObjectData
import ru.citeck.ecos.context.lib.auth.AuthContext
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType
import ru.citeck.ecos.model.lib.type.dto.TypeInfo
import ru.citeck.ecos.model.lib.type.dto.TypeModelDef
import ru.citeck.ecos.model.lib.utils.ModelUtils
import ru.citeck.ecos.webapp.api.entity.EntityRef

class DbRecordsChildDetachTest : DbRecordsTestBase() {

    @Test
    fun unlinkFromParentPreservesChildAndDescendants() {
        registerChildren()
        val grandchild = createRecord()
        val child = createRecord("children" to listOf(grandchild))
        val parent = createRecord("children" to listOf(child))

        records.mutateAtt(parent, "att_rem_children", child)

        assertDetached(child)
        assertThat(children(parent)).isEmpty()
        assertThat(children(child)).containsExactly(grandchild)
        assertThat(records.getAtt(grandchild, "_parent?id").asText()).isEqualTo(child.toString())
    }

    @Test
    fun replacingFullChildrenListOnlyDetachesRemovedChildren() {
        registerChildren()
        val removed = createRecord()
        val retained = createRecord()
        val added = createRecord()
        val parent = createRecord("children" to listOf(removed, retained))

        records.mutateAtt(parent, "children", listOf(retained, added))

        assertDetached(removed)
        assertThat(children(parent)).containsExactly(retained, added)
        for (child in listOf(retained, added)) {
            assertThat(records.getAtt(child, "_parent?id").asText()).isEqualTo(parent.toString())
        }
    }

    @Test
    fun clearingSingleValuedChildPreservesRecord() {
        registerChildren(multiple = false)
        val child = createRecord()
        val parent = createRecord("children" to child)

        records.mutateAtt(parent, "children", null)

        assertDetached(child)
        assertThat(children(parent)).isEmpty()
    }

    @Test
    fun unlinkFromChildPreservesRecordAndCleansParent() {
        registerChildren()
        val parent = createRecord()
        val child = createRecord("_parent" to parent, "_parentAtt" to "children")

        records.mutate(child, mapOf("_parent" to null, "_parentAtt" to null))

        assertDetached(child)
        assertThat(children(parent)).isEmpty()
    }

    @Test
    fun deletingParentPreservesChildrenAcrossPagesAndTheirData() {
        registerChildren()
        val parent = createRecord()
        val children = (0..104).map { createRecord("_parent" to parent, "_parentAtt" to "children") }

        records.delete(parent)

        assertThat(records.getAtt(parent, "_notExists?bool").asBoolean()).isTrue()
        children.forEach { assertDetached(it) }
        // Retained records remain editable and deletable without a stale parent notification.
        records.mutateAtt(children.first(), "name", "retained")
        assertThat(records.getAtt(children.first(), "name").asText()).isEqualTo("retained")
        records.delete(children.last())
        assertThat(records.getAtt(children.last(), "_notExists?bool").asBoolean()).isTrue()
    }

    @Test
    fun deletingParentKeepsChildSubtree() {
        registerChildren()
        val grandchild = createRecord()
        val child = createRecord("children" to listOf(grandchild))
        val parent = createRecord("children" to listOf(child))

        records.delete(parent)

        assertDetached(child)
        assertThat(children(child)).containsExactly(grandchild)
        assertThat(records.getAtt(grandchild, "_parent?id").asText()).isEqualTo(child.toString())
    }

    @Test
    fun deletionUsesEachAssociationsPolicy() {
        registerAtts(listOf(association(), association("owned", false), association("ordinary", true, child = false)))
        val detached = createRecord()
        val deleted = createRecord()
        val ordinary = createRecord()
        val parent = createRecord("children" to listOf(detached), "owned" to listOf(deleted), "ordinary" to listOf(ordinary))

        records.delete(parent)

        assertDetached(detached)
        assertDetached(ordinary)
        assertThat(records.getAtt(deleted, "_notExists?bool").asBoolean()).isTrue()
    }

    @Test
    fun missingOrFalseFlagKeepsLegacyUnlinkAndCascade() {
        for (allowDetach in listOf(null, false)) {
            registerChildren(allowDetach)
            val unlinked = createRecord()
            val cascaded = createRecord()
            val parent = createRecord("children" to listOf(unlinked, cascaded))

            records.mutateAtt(parent, "att_rem_children", unlinked)
            assertThat(records.getAtt(unlinked, "_notExists?bool").asBoolean()).isTrue()
            records.delete(parent)
            assertThat(records.getAtt(cascaded, "_notExists?bool").asBoolean()).isTrue()
        }
    }

    @Test
    fun deletionUsesActualParentTypeInsteadOfDaoDefault() {
        registerChildren(false)
        registerType(
            TypeInfo.create {
                withId("detachable-parent")
                withParentRef(REC_TEST_TYPE_REF)
                withModel(TypeModelDef.create { withAttributes(listOf(association())) })
            }
        )
        val child = createRecord()
        val parent = createRecord("_type" to ModelUtils.getTypeRef("detachable-parent"), "children" to listOf(child))

        records.delete(parent)

        assertDetached(child)
    }

    @Test
    fun enablingDetachAppliesToExistingLinks() {
        registerChildren(false)
        val child = createRecord()
        val parent = createRecord("children" to listOf(child))
        registerChildren(true)

        records.delete(parent)

        assertDetached(child)
    }

    @Test
    fun deletingParentDetachesChildrenInAnotherDao() {
        registerChildren()
        val typeRef = ModelUtils.getTypeRef("detachable-child")
        registerType(
            TypeInfo.create {
                withId(typeRef.getLocalId())
                withSourceId("detachable-child")
                withParentRef(REC_TEST_TYPE_REF)
                withModel(TypeModelDef.create { withAttributes(listOf(AttributeDef.create { withId("name") })) })
            }
        )
        val childCtx = createRecordsDao(DEFAULT_TABLE_REF.withTable("detachable_children"), typeRef, "detachable-child")
        val parent = createRecord()
        val child = childCtx.createRecord("_parent" to parent, "_parentAtt" to "children")

        records.delete(parent)

        assertDetached(child)
        records.mutateAtt(child, "name", "retained")
        assertThat(records.getAtt(child, "name").asText()).isEqualTo("retained")
    }

    @Test
    fun reparentFromEitherSidePreservesChildAndRemovesOldLink() {
        registerChildren()
        val first = createRecord()
        val second = createRecord()
        val third = createRecord()
        val child = createRecord("_parent" to first, "_parentAtt" to "children")

        records.mutateAtt(second, "att_add_children", child)

        assertThat(children(first)).isEmpty()
        assertThat(children(second)).containsExactly(child)
        records.mutate(child, mapOf("_parent" to third, "_parentAtt" to "children"))
        assertThat(children(second)).isEmpty()
        assertThat(children(third)).containsExactly(child)
        assertThat(records.getAtt(child, "_parent?id").asText()).isEqualTo(third.toString())
    }

    @Test
    fun detachDoesNotAllowSelfParentOrCycles() {
        registerChildren()
        val child = createRecord()
        val parent = createRecord("children" to listOf(child))

        assertThrows<Exception> { records.mutateAtt(parent, "att_add_children", parent) }
        assertThrows<Exception> { records.mutateAtt(child, "att_add_children", parent) }

        assertThat(children(parent)).containsExactly(child)
        assertThat(children(child)).isEmpty()
    }

    @Test
    fun parentDeletionCanDetachChildWithoutChildWritePermission() {
        registerChildren()
        val child = createRecord()
        val parent = createRecord("children" to listOf(child))
        setAuthoritiesWithWritePerms(parent, "parent-owner")
        setAuthoritiesWithWritePerms(child, "child-owner")

        AuthContext.runAs("unknown") {
            assertThrows<Exception> { records.delete(parent) }
            assertThrows<Exception> { records.mutateAtt(parent, "att_rem_children", child) }
        }
        AuthContext.runAs("parent-owner") { records.delete(parent) }

        assertDetached(child)
        AuthContext.runAs("unknown") { assertThrows<Exception> { records.mutateAtt(child, "name", "denied") } }
        AuthContext.runAs("child-owner") { records.mutateAtt(child, "name", "allowed") }
        assertThat(records.getAtt(child, "name").asText()).isEqualTo("allowed")
    }

    private fun registerChildren(allowDetach: Boolean? = true, multiple: Boolean = true) {
        registerAtts(
            listOf(
                AttributeDef.create { withId("name") },
                association(allowDetach = allowDetach, multiple = multiple)
            )
        )
    }

    private fun association(
        id: String = "children",
        allowDetach: Boolean? = true,
        multiple: Boolean = true,
        child: Boolean = true
    ): AttributeDef = AttributeDef.create {
        withId(id)
        withType(AttributeType.ASSOC)
        withMultiple(multiple)
        withConfig(
            ObjectData.create().set("child", child).also {
                if (allowDetach != null) it.set("allowChildDetach", allowDetach)
            }
        )
    }

    private fun children(parent: EntityRef): List<EntityRef> = records.getAtt(parent, "children[]?id").asList(EntityRef::class.java)

    private fun assertDetached(child: EntityRef) {
        assertThat(records.getAtt(child, "_notExists?bool").asBoolean()).describedAs(child.toString()).isFalse()
        assertThat(records.getAtt(child, "_parent?id").asText()).isEmpty()
        assertThat(records.getAtt(child, "_parentAtt?str").asText()).isEmpty()
    }
}
