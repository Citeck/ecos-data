package ru.citeck.ecos.data.sql.test.records

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import ru.citeck.ecos.commons.data.ObjectData
import ru.citeck.ecos.context.lib.auth.AuthContext
import ru.citeck.ecos.data.sql.records.DbRecordsControlAtts
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType
import ru.citeck.ecos.records2.RecordConstants
import ru.citeck.ecos.webapp.api.entity.EntityRef
import java.lang.Exception
import java.time.Instant

class DbRecordsAuditTest : DbRecordsTestBase() {

    companion object {
        private val PARENT_MODIFIED = Instant.parse("2020-01-01T00:00:00Z")
        private val CHILD_MODIFIED = Instant.parse("2019-01-01T00:00:00Z")

        private const val CHILDREN = "children"
        private const val OTHER_CHILDREN = "otherChildren"
        private const val DETACHABLE_CHILDREN = "detachableChildren"
    }

    @BeforeEach
    fun beforeEach() {
        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("text")
                }
            ),
            listOf(
                AttributeDef.create {
                    withId("systemText")
                }
            )
        )
    }

    @Test
    fun test() {
        val beforeCreation = Instant.now()

        val ref = AuthContext.runAs("admin") {
            createRecord("text" to "abc")
        }
        val modified0 = getModified(ref)
        val created0 = getCreated(ref)
        val modifier0 = getModifier(ref)
        val creator0 = getCreator(ref)

        assertThat(modifier0).isEqualTo("admin")
        assertThat(creator0).isEqualTo("admin")
        assertThat(modified0).isAfter(beforeCreation)
        assertThat(created0).isAfter(beforeCreation)
        assertThat(created0).isEqualTo(modified0)

        updateRecord(ref, "systemText" to "system-text")
        assertThat(records.getAtt(ref, "systemText").asText()).isEqualTo("system-text")

        assertThat(getModified(ref)).isEqualTo(modified0)
        assertThat(getCreated(ref)).isEqualTo(created0)
        assertThat(getModifier(ref)).isEqualTo(modifier0)
        assertThat(getCreator(ref)).isEqualTo(creator0)

        val customCreator = "custom-creator"
        val customModifier = "custom-modifier"
        val customCreated = Instant.parse("2021-01-01T00:00:00Z")
        val customModified = Instant.parse("2022-01-01T00:00:00Z")
        val mutAtts = mapOf(
            DbRecordsControlAtts.DISABLE_AUDIT to true,
            RecordConstants.ATT_CREATED to customCreated,
            RecordConstants.ATT_CREATOR to customCreator,
            RecordConstants.ATT_MODIFIED to customModified,
            RecordConstants.ATT_MODIFIER to customModifier
        )
        fun assertCustomAtts(ref: EntityRef) {
            assertThat(getModifier(ref)).isEqualTo(customModifier)
            assertThat(getCreator(ref)).isEqualTo(customCreator)
            assertThat(getModified(ref)).isEqualTo(customModified)
            assertThat(getCreated(ref)).isEqualTo(customCreated)
        }

        assertThrows<Exception> {
            AuthContext.runAs("admin") {
                records.mutate(ref, mutAtts)
            }
        }

        records.mutate(ref, mutAtts)
        assertCustomAtts(ref)

        val ref2 = createRecord(ObjectData.create(mutAtts))
        assertCustomAtts(ref2)
    }

    @Test
    fun childCreationWithDisabledAuditKeepsParentAudit() {
        registerChildAssocs()
        val parent = createWithAudit(PARENT_MODIFIED, "parent-modifier")

        val child = createChild(parent, CHILDREN)

        assertThat(children(parent, CHILDREN)).containsExactly(child)
        assertAudit(parent, PARENT_MODIFIED, "parent-modifier")
        assertAudit(child, CHILD_MODIFIED, "child-modifier")
    }

    @Test
    fun childCreationWithoutDisabledAuditUpdatesParentAudit() {
        registerChildAssocs()
        val parent = createWithAudit(PARENT_MODIFIED, "parent-modifier")

        createRecord(RecordConstants.ATT_PARENT to parent, RecordConstants.ATT_PARENT_ATT to CHILDREN)

        assertThat(getModified(parent)).isAfter(PARENT_MODIFIED)
        assertThat(getModifier(parent)).isNotEqualTo("parent-modifier")
    }

    @Test
    fun childMoveWithDisabledAuditKeepsBothParentsAudit() {
        registerChildAssocs()
        val oldParent = createWithAudit(PARENT_MODIFIED, "old-parent-modifier")
        val newParent = createWithAudit(PARENT_MODIFIED, "new-parent-modifier")
        val child = createChild(oldParent, CHILDREN)

        records.mutate(
            child,
            withDisabledAudit(
                RecordConstants.ATT_PARENT to newParent,
                RecordConstants.ATT_PARENT_ATT to CHILDREN
            )
        )

        assertThat(children(oldParent, CHILDREN)).isEmpty()
        assertThat(children(newParent, CHILDREN)).containsExactly(child)
        assertAudit(oldParent, PARENT_MODIFIED, "old-parent-modifier")
        assertAudit(newParent, PARENT_MODIFIED, "new-parent-modifier")
        assertAudit(child, CHILD_MODIFIED, "child-modifier")
    }

    @Test
    fun parentAttChangeWithDisabledAuditKeepsParentAudit() {
        registerChildAssocs()
        val parent = createWithAudit(PARENT_MODIFIED, "parent-modifier")
        val child = createChild(parent, CHILDREN)

        records.mutate(child, withDisabledAudit(RecordConstants.ATT_PARENT_ATT to OTHER_CHILDREN))

        assertThat(children(parent, CHILDREN)).isEmpty()
        assertThat(children(parent, OTHER_CHILDREN)).containsExactly(child)
        assertAudit(parent, PARENT_MODIFIED, "parent-modifier")
        assertAudit(child, CHILD_MODIFIED, "child-modifier")
    }

    @Test
    fun childAddedByParentWithDisabledAuditKeepsChildAudit() {
        registerChildAssocs()
        val parent = createWithAudit(PARENT_MODIFIED, "parent-modifier")
        val child = createWithAudit(CHILD_MODIFIED, "child-modifier")

        records.mutate(parent, withDisabledAudit("att_add_$CHILDREN" to child))

        assertThat(children(parent, CHILDREN)).containsExactly(child)
        assertThat(records.getAtt(child, "_parent?id").asText()).isEqualTo(parent.toString())
        assertAudit(parent, PARENT_MODIFIED, "parent-modifier")
        assertAudit(child, CHILD_MODIFIED, "child-modifier")
    }

    @Test
    fun childTakenByAnotherParentWithDisabledAuditKeepsAllAudit() {
        registerChildAssocs()
        val oldParent = createWithAudit(PARENT_MODIFIED, "old-parent-modifier")
        val newParent = createWithAudit(PARENT_MODIFIED, "new-parent-modifier")
        val child = createChild(oldParent, CHILDREN)

        records.mutate(newParent, withDisabledAudit("att_add_$CHILDREN" to child))

        assertThat(children(oldParent, CHILDREN)).isEmpty()
        assertThat(records.getAtt(child, "_parent?id").asText()).isEqualTo(newParent.toString())
        assertAudit(oldParent, PARENT_MODIFIED, "old-parent-modifier")
        assertAudit(newParent, PARENT_MODIFIED, "new-parent-modifier")
        assertAudit(child, CHILD_MODIFIED, "child-modifier")
    }

    @Test
    fun childDetachedByParentWithDisabledAuditKeepsChildAudit() {
        registerChildAssocs()
        val parent = createWithAudit(PARENT_MODIFIED, "parent-modifier")
        val child = createChild(parent, DETACHABLE_CHILDREN)

        records.mutate(parent, withDisabledAudit("att_rem_$DETACHABLE_CHILDREN" to child))

        assertThat(records.getAtt(child, "_notExists?bool").asBoolean()).isFalse()
        assertThat(records.getAtt(child, "_parent?id").asText()).isEmpty()
        assertAudit(parent, PARENT_MODIFIED, "parent-modifier")
        assertAudit(child, CHILD_MODIFIED, "child-modifier")
    }

    @Test
    fun childDeletedByParentWithDisabledAuditKeepsParentAudit() {
        registerChildAssocs()
        val parent = createWithAudit(PARENT_MODIFIED, "parent-modifier")
        val child = createChild(parent, CHILDREN)

        records.mutate(parent, withDisabledAudit("att_rem_$CHILDREN" to child))

        assertThat(records.getAtt(child, "_notExists?bool").asBoolean()).isTrue()
        assertThat(children(parent, CHILDREN)).isEmpty()
        assertAudit(parent, PARENT_MODIFIED, "parent-modifier")
    }

    private fun registerChildAssocs() {
        registerAtts(
            listOf(
                childAssoc(CHILDREN),
                childAssoc(OTHER_CHILDREN),
                childAssoc(DETACHABLE_CHILDREN, allowDetach = true)
            )
        )
    }

    private fun childAssoc(id: String, allowDetach: Boolean = false): AttributeDef = AttributeDef.create {
        withId(id)
        withType(AttributeType.ASSOC)
        withMultiple(true)
        withConfig(ObjectData.create().set("child", true).set("allowChildDetach", allowDetach))
    }

    private fun withDisabledAudit(vararg atts: Pair<String, Any?>): Map<String, Any?> {
        return mapOf(DbRecordsControlAtts.DISABLE_AUDIT to true, *atts)
    }

    private fun createWithAudit(modified: Instant, modifier: String, vararg atts: Pair<String, Any?>): EntityRef {
        return createRecord(
            ObjectData.create(
                withDisabledAudit(
                    RecordConstants.ATT_MODIFIED to modified,
                    RecordConstants.ATT_MODIFIER to modifier,
                    *atts
                )
            )
        )
    }

    private fun createChild(parent: EntityRef, parentAtt: String): EntityRef {
        return createWithAudit(
            CHILD_MODIFIED,
            "child-modifier",
            RecordConstants.ATT_PARENT to parent,
            RecordConstants.ATT_PARENT_ATT to parentAtt
        )
    }

    private fun children(parent: EntityRef, att: String): List<EntityRef> {
        return records.getAtt(parent, "$att[]?id").asList(EntityRef::class.java)
    }

    private fun assertAudit(ref: EntityRef, modified: Instant, modifier: String) {
        assertThat(getModified(ref)).describedAs("_modified of $ref").isEqualTo(modified)
        assertThat(getModifier(ref)).describedAs("_modifier of $ref").isEqualTo(modifier)
    }

    private fun getModifier(ref: EntityRef): String {
        return records.getAtt(ref, RecordConstants.ATT_MODIFIER + "?localId").asText()
    }

    private fun getCreator(ref: EntityRef): String {
        return records.getAtt(ref, RecordConstants.ATT_CREATOR + "?localId").asText()
    }

    private fun getCreated(ref: EntityRef): Instant {
        return records.getAtt(ref, RecordConstants.ATT_CREATED).getAsInstantOrEpoch()
    }

    private fun getModified(ref: EntityRef): Instant {
        return records.getAtt(ref, RecordConstants.ATT_MODIFIED).getAsInstantOrEpoch()
    }
}
