package ru.citeck.ecos.data.sql.pg.records

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import ru.citeck.ecos.commons.data.ObjectData
import ru.citeck.ecos.context.lib.auth.AuthContext
import ru.citeck.ecos.context.lib.auth.AuthGroup
import ru.citeck.ecos.context.lib.auth.AuthUser
import ru.citeck.ecos.context.lib.auth.data.EmptyAuth
import ru.citeck.ecos.data.sql.pg.records.commons.DbRecordsTestBase
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType
import ru.citeck.ecos.model.lib.type.dto.QueryPermsPolicy
import ru.citeck.ecos.records2.predicate.model.Predicates
import ru.citeck.ecos.txn.lib.TxnContext
import ru.citeck.ecos.webapp.api.entity.EntityRef
import java.util.UUID

class RecordsDaoPermsTest : DbRecordsTestBase() {

    @Test
    fun createWithParentPermsTest() {

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("children")
                    withType(AttributeType.ASSOC)
                    withMultiple(true)
                    withConfig(ObjectData.create().set("child", true))
                }
            )
        )

        setQueryPermsPolicy(QueryPermsPolicy.OWN)

        val root = createRecord()
        setAuthoritiesWithReadPerms(root, AuthGroup.EVERYONE)
        val childrenRecs = (1..10).map {
            TxnContext.doInTxn {
                createRecord("_parent" to root, "_parentAtt" to "children")
            }
        }

        val queryRes = AuthContext.runAs("user", listOf(AuthGroup.EVERYONE)) {
            records.query(
                baseQuery.copy {
                    withQuery(Predicates.eq("_parent", root.toString()))
                }
            ).getRecords()
        }

        assertThat(queryRes).containsExactlyInAnyOrderElementsOf(childrenRecs)
    }

    @Test
    fun createInTxnPermsTest() {

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("test")
                }
            )
        )
        setQueryPermsPolicy(QueryPermsPolicy.OWN)

        val ref = EntityRef.create(recordsDao.getId(), "test")
        setAuthoritiesWithWritePerms(ref, "user0")

        AuthContext.runAs("user1") {
            TxnContext.doInTxn {
                createRecord(
                    "id" to ref.getLocalId(),
                    "test" to "abc"
                )
                assertThat(records.getAtt(ref, "test").asText()).isEqualTo("abc")
                records.mutateAtt(ref, "test", "def")
                assertThat(records.getAtt(ref, "test").asText()).isEqualTo("def")
            }
            TxnContext.doInTxn {
                assertThrows<Exception> {
                    records.mutateAtt(ref, "test", "def")
                }
            }
        }
    }

    @Test
    fun `write permissions test`() {
        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("test")
                }
            )
        )
        setQueryPermsPolicy(QueryPermsPolicy.OWN)

        val ref = EntityRef.create(recordsDao.getId(), "test-rec")
        setAuthoritiesWithWritePerms(ref, listOf("user0"))
        addAuthoritiesWithReadPerms(ref, listOf("user1"))

        AuthContext.runAs("user3") {
            createRecord("id" to ref.getLocalId())
        }
        AuthContext.runAs(EmptyAuth) {
            assertThat(records.getAtt(ref, "permissions._has.Write?bool!false").asText()).isEqualTo("false")
        }
        AuthContext.runAs("user1") {
            assertThat(records.getAtt(ref, "permissions._has.Write?bool").asText()).isEqualTo("false")
        }
        AuthContext.runAs("user0") {
            assertThat(records.getAtt(ref, "permissions._has.Write?bool").asText()).isEqualTo("true")
        }

        val usersWithoutWritePerms = listOf(
            "user1",
            "user2",
            "user3"
        )
        val usersWithWritePerms = listOf(
            "user0",
            AuthUser.SYSTEM
        )

        usersWithoutWritePerms.forEach {
            AuthContext.runAs(it) {
                assertThrows<Exception> {
                    records.mutateAtt(ref, "test", "abcd")
                }
            }
        }
        usersWithWritePerms.forEach {
            AuthContext.runAs(it) {
                val value = UUID.randomUUID().toString()
                records.mutateAtt(ref, "test", value)
                assertThat(records.getAtt(ref, "test").asText()).isEqualTo(value)
            }
        }
    }

    @Test
    fun `custom permissions exists test`() {
        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("test")
                }
            )
        )
        setQueryPermsPolicy(QueryPermsPolicy.OWN)

        val user = "user0"
        val permission = "createProcessInstance"

        val ref = EntityRef.create(recordsDao.getId(), "test-rec")
        AuthContext.runAs(user) {
            createRecord("id" to ref.getLocalId())
        }
        addAdditionalPermission(ref, user, permission)

        val createProcessInstancePermsValue = AuthContext.runAs(user) {
            records.getAtt(ref, "permissions._has.$permission?bool").asBoolean()
        }

        assertTrue(createProcessInstancePermsValue)
    }

    @Test
    fun `custom permissions not exists test`() {
        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("test")
                }
            )
        )
        setQueryPermsPolicy(QueryPermsPolicy.OWN)

        val user = "user0"
        val permission = "createProcessInstance"

        val ref = EntityRef.create(recordsDao.getId(), "test-rec")
        AuthContext.runAs(user) {
            createRecord("id" to ref.getLocalId())
        }

        val createProcessInstancePermsValue = AuthContext.runAs(user) {
            records.getAtt(ref, "permissions._has.$permission?bool").asBoolean()
        }

        assertFalse(createProcessInstancePermsValue)
    }
}
