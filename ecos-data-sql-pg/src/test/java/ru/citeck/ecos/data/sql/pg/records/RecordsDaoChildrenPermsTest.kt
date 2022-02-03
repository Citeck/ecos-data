package ru.citeck.ecos.data.sql.pg.records

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import ru.citeck.ecos.commons.data.ObjectData
import ru.citeck.ecos.context.lib.auth.AuthContext
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType
import ru.citeck.ecos.records2.RecordRef

class RecordsDaoChildrenPermsTest : DbRecordsTestBase() {

    @ParameterizedTest
    @ValueSource(strings = ["true", "false"])
    fun test(inheritParentPerms: Boolean) {

        initServices(authEnabled = true, inheritParentPerms = inheritParentPerms)

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("childAssocs")
                    withType(AttributeType.ASSOC)
                    withMultiple(true)
                    withConfig(ObjectData.create().set("child", true))
                }
            )
        )

        val checkReadPerms = { ref: RecordRef, isReadPermsExpected: Boolean ->
            if (isReadPermsExpected) {
                assertThat(records.getAtt(ref, "_notExists?bool").asBoolean()).isFalse
            } else {
                assertThat(records.getAtt(ref, "_notExists?bool").asBoolean()).isTrue
            }
        }

        val childRecord = AuthContext.runAs("user") { createRecord() }
        checkReadPerms(childRecord, false)
        AuthContext.runAs("user") {
            checkReadPerms(childRecord, true)
        }
        setPerms(childRecord, "user", "parent-user")

        val parentRecord = AuthContext.runAsFull("parent-user") {
            createRecord("childAssocs" to childRecord)
        }
        setPerms(parentRecord, "parent-user", "other-user")

        checkReadPerms(parentRecord, false)
        AuthContext.runAs("parent-user") {
            checkReadPerms(parentRecord, true)
        }
        AuthContext.runAs("other-user") {
            checkReadPerms(parentRecord, true)
        }
        AuthContext.runAs("user") {
            checkReadPerms(parentRecord, false)
        }
        AuthContext.runAs("other-user") {
            checkReadPerms(childRecord, inheritParentPerms)
        }
    }
}
