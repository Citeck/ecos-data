package ru.citeck.ecos.data.sql.pg.records

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import ru.citeck.ecos.context.lib.auth.AuthContext
import ru.citeck.ecos.context.lib.auth.AuthUser
import ru.citeck.ecos.context.lib.auth.data.EmptyAuth
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.records2.RecordRef
import ru.citeck.ecos.txn.lib.TxnContext
import java.util.UUID

class RecordsDaoWritePermsTest : DbRecordsTestBase() {

    @Test
    fun createInTxnPermsTest() {

        initServices(authEnabled = true)
        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("test")
                }
            )
        )

        val ref = RecordRef.create(recordsDao.getId(), "test")
        setAuthoritiesWithWritePerms(ref, "user0")

        AuthContext.runAs("user1") {
            TxnContext.doInTxn {
                createRecord(
                    "id" to ref.id,
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
    fun test() {
        initServices(authEnabled = true)
        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("test")
                }
            )
        )

        val ref = RecordRef.create(recordsDao.getId(), "test-rec")
        setAuthoritiesWithWritePerms(ref, listOf("user0"))
        addAuthoritiesWithReadPerms(ref, listOf("user1"))

        AuthContext.runAs("user3") {
            createRecord("id" to ref.id)
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
}
