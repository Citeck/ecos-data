package ru.citeck.ecos.data.sql.pg.records

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import ru.citeck.ecos.context.lib.auth.AuthContext
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.type.dto.TypePermsPolicy
import ru.citeck.ecos.records2.RecordRef

class DbRecordsDaoAttsPermsTest : DbRecordsTestBase() {

    @Test
    fun test() {

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("test")
                }
            )
        )
        setPermsPolicy(TypePermsPolicy.OWN)

        val ref = RecordRef.create(recordsDao.getId(), "test")

        AuthContext.runAs("admin") {
            createRecord("id" to ref.id, "test" to "abc")
        }

        val allUsers = setOf("user0", "user1", "user2")
        val usersWithAttReadPerms = setOf("user0, user1")
        val usersWithAttWritePerms = setOf("user1, user2")

        setAuthoritiesWithWritePerms(ref, allUsers)
        setAuthoritiesWithAttReadPerms(ref, "test", *usersWithAttReadPerms.toTypedArray())
        setAuthoritiesWithAttWritePerms(ref, "test", *usersWithAttWritePerms.toTypedArray())

        for (user in allUsers) {
            AuthContext.runAs(user) {
                val protected = records.getAtt(ref, "_edge.test.protected?bool").asText()
                val unreadable = records.getAtt(ref, "_edge.test.unreadable?bool").asText()
                if (usersWithAttReadPerms.contains(user)) {
                    assertTrue(unreadable == "false", user)
                } else {
                    assertTrue(unreadable == "true", user)
                }
                if (usersWithAttWritePerms.contains(user)) {
                    assertTrue(protected == "false", user)
                } else {
                    assertTrue(protected == "true", user)
                }
                records.mutateAtt(ref, "test", user)
                val valueAfterMutation = AuthContext.runAsSystem {
                    records.getAtt(ref, "test").asText()
                }
                if (usersWithAttWritePerms.contains(user)) {
                    assertThat(valueAfterMutation).isEqualTo(user)
                } else {
                    assertThat(valueAfterMutation).isNotEqualTo(user)
                }
            }
        }
    }
}
