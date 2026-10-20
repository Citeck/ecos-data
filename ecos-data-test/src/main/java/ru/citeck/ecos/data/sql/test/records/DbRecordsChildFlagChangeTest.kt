package ru.citeck.ecos.data.sql.test.records

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import ru.citeck.ecos.commons.data.ObjectData
import ru.citeck.ecos.context.lib.auth.AuthContext
import ru.citeck.ecos.data.sql.records.DbRecordsControlAtts
import ru.citeck.ecos.data.sql.records.dao.delete.DbRecordsDeleteDao
import ru.citeck.ecos.data.sql.records.dao.mutate.RecMutAssocHandler
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType
import ru.citeck.ecos.model.lib.type.dto.QueryPermsPolicy
import ru.citeck.ecos.model.lib.type.dto.TypeInfo
import ru.citeck.ecos.model.lib.type.dto.TypeModelDef
import ru.citeck.ecos.model.lib.utils.ModelUtils
import ru.citeck.ecos.webapp.api.entity.EntityRef

class DbRecordsChildFlagChangeTest : DbRecordsTestBase() {

    @Test
    fun removingChildFlagPreventsCascadeOnExistingLinks() {
        registerAtts(listOf(association(true)))
        val child = createRecord()
        val parent = createRecord("children" to listOf(child))

        registerAtts(listOf(association(false)))
        records.delete(parent)

        assertExists(parent, false)
        assertExists(child, true)
    }

    @Test
    fun removingChildFlagAllowsDeletingExistingChild() {
        registerAtts(listOf(association(true)))
        val child = createRecord()
        val parent = createRecord("children" to listOf(child))

        registerAtts(listOf(association(false)))
        records.delete(child)

        assertExists(parent, true)
        assertExists(child, false)
        assertThat(records.getAtt(parent, "children[]?id").toStrList()).isEmpty()
    }

    @Test
    fun addingChildFlagDoesNotCascadeExistingNonChildLinks() {
        registerAtts(listOf(association(false)))
        val child = createRecord()
        val parent = createRecord("children" to listOf(child))

        registerAtts(listOf(association(true)))
        records.delete(parent)

        assertExists(parent, false)
        assertExists(child, true)
    }

    @Test
    fun missingAttributeKeepsStoredChildCascade() {
        registerAtts(listOf(association(true)))
        val child = createRecord()
        val parent = createRecord("children" to listOf(child))

        registerAtts(emptyList())
        records.delete(parent)

        assertExists(parent, false)
        assertExists(child, false)
    }

    @Test
    fun missingAttributeKeepsStoredNonChildLink() {
        registerAtts(listOf(association(false)))
        val child = createRecord()
        val parent = createRecord("children" to listOf(child))

        registerAtts(emptyList())
        records.delete(parent)

        assertExists(parent, false)
        assertExists(child, true)
    }

    @Test
    fun cascadeUsesRecordTypeInsteadOfDaoDefault() {
        registerAtts(listOf(association(true)))
        registerParentType(true)
        val child = createRecord()
        val parent = createRecord("_type" to ModelUtils.getTypeRef("other-parent"), "children" to listOf(child))

        registerParentType(false)
        records.delete(parent)

        assertExists(parent, false)
        assertExists(child, true)
    }

    @Test
    fun childCascadeUsesRecordTypeWhenDaoDefaultDisablesIt() {
        registerAtts(listOf(association(false)))
        registerParentType(true)
        val child = createRecord()
        val parent = createRecord("_type" to ModelUtils.getTypeRef("other-parent"), "children" to listOf(child))

        records.delete(parent)

        assertExists(parent, false)
        assertExists(child, false)
    }

    @Test
    fun addingFromChildStillRequiresChildAssociation() {
        registerAtts(listOf(association(false)))
        val parent = createRecord()
        val child = createRecord()

        assertThrows<Exception> {
            records.mutate(
                parent,
                mapOf(
                    RecMutAssocHandler.MUTATION_FROM_CHILD_FLAG to true,
                    "att_add_children" to child
                )
            )
        }
        assertThat(records.getAtt(parent, "children[]?id").toStrList()).isEmpty()
    }

    @Test
    fun removalDoesNotAllowAddingToNonChildAssociationInSameMutation() {
        registerAtts(listOf(association(false)))
        val oldTarget = createRecord()
        val newTarget = createRecord()
        val parent = createRecord("children" to listOf(oldTarget))

        assertThrows<Exception> {
            records.mutate(
                parent,
                mapOf(
                    RecMutAssocHandler.MUTATION_FROM_CHILD_FLAG to true,
                    "att_rem_children" to oldTarget,
                    "att_add_children" to newTarget
                )
            )
        }
        assertThat(records.getAtt(parent, "children[]?id").toStrList()).containsExactly(oldTarget.toString())
    }

    @Test
    fun directAttributeValueDoesNotQualifyAsRemovalOnly() {
        registerAtts(listOf(association(false)))
        val oldTarget = createRecord()
        val newTarget = createRecord()
        val parent = createRecord("children" to listOf(oldTarget))
        val ctx = recordsDao.getRecordsDaoCtx()
        val columns = ctx.ecosTypeService.getColumnsForTypes(
            listOf(ctx.ecosTypeService.getTypeInfoNotNull(REC_TEST_TYPE_REF.getLocalId()))
        )
        val attributes = ObjectData.create()
            .set(RecMutAssocHandler.MUTATION_FROM_CHILD_FLAG, true)
            .set("att_rem_children", oldTarget)

        ctx.mutAssocHandler.validateChildAssocs(attributes, setOf("children"), parent.getLocalId(), columns)
        for (directValue in listOf(null, listOf(newTarget))) {
            val error = assertThrows<IllegalStateException> {
                ctx.mutAssocHandler.validateChildAssocs(
                    attributes.deepCopy().set("children", directValue),
                    setOf("children"),
                    parent.getLocalId(),
                    columns
                )
            }
            assertThat(error).hasMessageContaining("'children' is not a child association")
        }
    }

    @Test
    fun preservedChildCanBeDeletedAfterParent() {
        registerAtts(listOf(association(true)))
        val child = createRecord()
        val parent = createRecord("children" to listOf(child))

        registerAtts(listOf(association(false)))
        records.delete(parent)
        records.delete(child)

        assertExists(parent, false)
        assertExists(child, false)
    }

    @Test
    fun childNotificationsToMissingParentAreIgnored() {
        registerAtts(listOf(association(true)))
        val parent = createRef("missing-parent")
        val child = createRecord()

        val mutations = listOf(
            emptyMap(),
            mapOf("att_rem_children" to child),
            mapOf("att_add_children" to child),
            mapOf("att_rem_children" to child, "att_add_children" to child),
            mapOf("children" to listOf(child)),
            mapOf(
                "att_rem_children" to child,
                DbRecordsDeleteDao.ASSOC_FORCE_DELETION_FLAG to true,
                DbRecordsControlAtts.DISABLE_EVENTS to true,
                "__futureNotificationOption" to true
            )
        )
        for (mutation in mutations) {
            records.mutate(
                parent,
                mutation + mapOf(RecMutAssocHandler.MUTATION_FROM_CHILD_FLAG to true)
            )
        }

        assertExists(parent, false)
        assertExists(child, true)
    }

    @Test
    fun mutationsToMissingParentWithoutChildFlagStillFail() {
        registerAtts(listOf(association(true)))
        val parent = createRef("missing-parent")
        val child = createRecord()
        val mutations = listOf(
            mapOf("att_rem_children" to child),
            mapOf(RecMutAssocHandler.MUTATION_FROM_CHILD_FLAG to false, "att_rem_children" to child),
            mapOf("att_add_children" to child),
            mapOf("att_rem_children" to child, "att_add_children" to child),
            mapOf("children" to listOf(child))
        )
        for (mutation in mutations) {
            val error = assertThrows<Exception> { records.mutate(parent, mutation) }
            assertThat(error).hasStackTraceContaining("was not found")
        }
        assertExists(parent, false)
        assertExists(child, true)
    }

    @Test
    fun removalNotificationToUnreadableParentStillFails() {
        registerAtts(listOf(association(true)))
        setQueryPermsPolicy(QueryPermsPolicy.OWN)
        val child = createRecord()
        val parent = createRecord("children" to listOf(child))
        setAuthoritiesWithReadPerms(parent, "parent-owner")

        AuthContext.runAs("other-user") {
            val error = assertThrows<Exception> {
                records.mutate(
                    parent,
                    mapOf(
                        RecMutAssocHandler.MUTATION_FROM_CHILD_FLAG to true,
                        "att_rem_children" to child
                    )
                )
            }
            assertThat(error).hasStackTraceContaining("Access denied")
        }
        assertThat(records.getAtt(parent, "children[]?id").toStrList()).containsExactly(child.toString())
        assertExists(child, true)
    }

    private fun registerParentType(child: Boolean) {
        registerType(
            TypeInfo.create()
                .withId("other-parent")
                .withParentRef(REC_TEST_TYPE_REF)
                .withModel(TypeModelDef.create().withAttributes(listOf(association(child))).build())
                .build()
        )
    }

    private fun association(child: Boolean): AttributeDef {
        return AttributeDef.create()
            .withId("children")
            .withType(AttributeType.ASSOC)
            .withMultiple(true)
            .withConfig(ObjectData.create().set("child", child))
            .build()
    }

    private fun assertExists(ref: EntityRef, expected: Boolean) {
        assertThat(records.getAtt(ref, "_notExists?bool").asBoolean()).isEqualTo(!expected)
    }
}
