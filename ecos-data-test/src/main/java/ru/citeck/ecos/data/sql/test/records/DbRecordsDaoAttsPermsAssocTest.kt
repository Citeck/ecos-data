package ru.citeck.ecos.data.sql.test.records

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import ru.citeck.ecos.commons.data.ObjectData
import ru.citeck.ecos.context.lib.auth.AuthContext
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType
import ru.citeck.ecos.model.lib.type.dto.QueryPermsPolicy
import ru.citeck.ecos.records2.RecordConstants
import ru.citeck.ecos.webapp.api.entity.EntityRef

/**
 * A write to an attribute the current user has no write permissions for must be refused for
 * every attribute type. Attributes stored in the assocs table used to be an exception: the
 * denial was logged, but the value was applied anyway, because association values are applied
 * from a container map which was replayed later in a system context (computed atts stage).
 *
 * The container may be registered either from the attribute value itself or from an
 * att_add_/att_rem_ operation, so both sources are checked here, for plain, multiple and child
 * associations. Every test repeats the same mutation as a user who does have the permissions,
 * to be sure the denial is not applied to everyone.
 */
class DbRecordsDaoAttsPermsAssocTest : DbRecordsTestBase() {

    companion object {
        private const val USER_DENIED = "userWithoutAttPerms"
        private const val USER_ALLOWED = "userWithAttPerms"

        private const val ATT_TEXT = "textAtt"
        private const val ATT_ASSOC = "assocAtt"
        private const val ATT_MULTI_ASSOC = "multiAssocAtt"
        private const val ATT_CHILD_ASSOC = "childAssocAtt"

        private val ALL_ATTS = listOf(ATT_TEXT, ATT_ASSOC, ATT_MULTI_ASSOC, ATT_CHILD_ASSOC)

        private const val NOT_EXISTS_ATT = RecordConstants.ATT_NOT_EXISTS + "?bool"
    }

    private lateinit var target0: EntityRef
    private lateinit var target1: EntityRef
    private lateinit var target2: EntityRef
    private lateinit var child0: EntityRef
    private lateinit var child1: EntityRef
    private lateinit var ref: EntityRef

    @Test
    fun deniedValueResetShouldNotBeAppliedTest() {

        initTestData()

        AuthContext.runAs(USER_DENIED) {

            for (att in ALL_ATTS) {
                assertThat(records.getAtt(ref, "_edge.$att.protected?bool").asBoolean())
                    .describedAs(att)
                    .isTrue()
            }

            records.mutate(
                ref,
                mapOf(
                    ATT_TEXT to null,
                    ATT_ASSOC to null,
                    ATT_MULTI_ASSOC to null
                )
            )
        }
        assertNothingChanged()

        AuthContext.runAs(USER_ALLOWED) {
            records.mutate(
                ref,
                mapOf(
                    ATT_TEXT to null,
                    ATT_ASSOC to null,
                    ATT_MULTI_ASSOC to null
                )
            )
        }
        assertThat(getTextAtt()).isEmpty()
        assertThat(getAssocAtt()).isEmpty()
        assertThat(getMultiAssocAtt()).isEmpty()
    }

    @Test
    fun deniedValueReplacementShouldNotBeAppliedTest() {

        initTestData()

        val newValues = mapOf(
            ATT_TEXT to "changed-text",
            ATT_ASSOC to target2,
            ATT_MULTI_ASSOC to listOf(target1, target2)
        )

        AuthContext.runAs(USER_DENIED) {
            records.mutate(ref, newValues)
        }
        assertNothingChanged()

        AuthContext.runAs(USER_ALLOWED) {
            records.mutate(ref, newValues)
        }
        assertThat(getTextAtt()).isEqualTo("changed-text")
        assertThat(getAssocAtt()).isEqualTo(target2.toString())
        assertThat(getMultiAssocAtt()).containsExactly(target1, target2)
    }

    @Test
    fun deniedAddRemOperationsShouldNotBeAppliedTest() {

        initTestData()

        // att_add_/att_rem_ registers the values container by a path of its own,
        // but the container ends up in the same shared map
        AuthContext.runAs(USER_DENIED) {
            records.mutate(ref, mapOf("att_add_$ATT_MULTI_ASSOC" to target2))
            records.mutate(ref, mapOf("att_rem_$ATT_MULTI_ASSOC" to target0))
            records.mutate(ref, mapOf("att_rem_$ATT_ASSOC" to target0))
        }
        assertNothingChanged()

        AuthContext.runAs(USER_ALLOWED) {
            records.mutate(ref, mapOf("att_add_$ATT_MULTI_ASSOC" to target2))
            records.mutate(ref, mapOf("att_rem_$ATT_MULTI_ASSOC" to target0))
            records.mutate(ref, mapOf("att_rem_$ATT_ASSOC" to target0))
        }
        assertThat(getAssocAtt()).isEmpty()
        assertThat(getMultiAssocAtt()).containsExactly(target1, target2)
    }

    @Test
    fun deniedChildAssocMutationShouldNotBeAppliedTest() {

        initTestData()

        // unlinking a child from a child association deletes it, so an applied denied mutation
        // would not only change the association, but destroy the children as well
        AuthContext.runAs(USER_DENIED) {
            records.mutate(ref, mapOf(ATT_CHILD_ASSOC to null))
            records.mutate(ref, mapOf("att_rem_$ATT_CHILD_ASSOC" to child0))
        }
        assertChildrenExists(child0, child1)
        assertNothingChanged()

        AuthContext.runAs(USER_ALLOWED) {
            records.mutate(ref, mapOf("att_rem_$ATT_CHILD_ASSOC" to child0))
        }
        assertThat(getChildAssocAtt()).containsExactly(child1)
    }

    private fun initTestData() {

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId(ATT_TEXT)
                    withType(AttributeType.TEXT)
                },
                AttributeDef.create {
                    withId(ATT_ASSOC)
                    withType(AttributeType.ASSOC)
                },
                AttributeDef.create {
                    withId(ATT_MULTI_ASSOC)
                    withType(AttributeType.ASSOC)
                    withMultiple(true)
                },
                AttributeDef.create {
                    withId(ATT_CHILD_ASSOC)
                    withType(AttributeType.ASSOC)
                    withMultiple(true)
                    withConfig(ObjectData.create("""{"child":true}"""))
                }
            )
        )
        setQueryPermsPolicy(QueryPermsPolicy.PUBLIC)

        AuthContext.runAsSystem {

            target0 = createRecord(ATT_TEXT to "target0")
            target1 = createRecord(ATT_TEXT to "target1")
            target2 = createRecord(ATT_TEXT to "target2")

            ref = createRecord(
                ATT_TEXT to "initial-text",
                ATT_ASSOC to target0,
                ATT_MULTI_ASSOC to listOf(target0, target1)
            )

            child0 = createRecord(
                RecordConstants.ATT_PARENT to ref,
                RecordConstants.ATT_PARENT_ATT to ATT_CHILD_ASSOC
            )
            child1 = createRecord(
                RecordConstants.ATT_PARENT to ref,
                RecordConstants.ATT_PARENT_ATT to ATT_CHILD_ASSOC
            )
        }

        setAuthoritiesWithWritePerms(ref, USER_DENIED, USER_ALLOWED)
        for (att in ALL_ATTS) {
            setAuthoritiesWithAttWritePerms(ref, att, USER_ALLOWED)
        }

        assertNothingChanged()
        assertChildrenExists(child0, child1)
    }

    private fun assertNothingChanged() {
        assertThat(getTextAtt()).isEqualTo("initial-text")
        assertThat(getAssocAtt()).isEqualTo(target0.toString())
        assertThat(getMultiAssocAtt()).containsExactly(target0, target1)
        assertThat(getChildAssocAtt()).containsExactly(child0, child1)
    }

    private fun assertChildrenExists(vararg children: EntityRef) {
        AuthContext.runAsSystem {
            for (child in children) {
                assertThat(records.getAtt(child, NOT_EXISTS_ATT).asBoolean())
                    .describedAs(child.toString())
                    .isFalse()
            }
        }
    }

    private fun getTextAtt(): String {
        return AuthContext.runAsSystem { records.getAtt(ref, ATT_TEXT).asText() }
    }

    private fun getAssocAtt(): String {
        return AuthContext.runAsSystem { records.getAtt(ref, "$ATT_ASSOC?id").asText() }
    }

    private fun getMultiAssocAtt(): List<EntityRef> {
        return AuthContext.runAsSystem {
            records.getAtt(ref, "$ATT_MULTI_ASSOC[]?id").asList(EntityRef::class.java)
        }
    }

    private fun getChildAssocAtt(): List<EntityRef> {
        return AuthContext.runAsSystem {
            records.getAtt(ref, "$ATT_CHILD_ASSOC[]?id").asList(EntityRef::class.java)
        }
    }
}
