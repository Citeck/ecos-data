package ru.citeck.ecos.data.sql.pg.records

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import ru.citeck.ecos.commons.data.ObjectData
import ru.citeck.ecos.context.lib.auth.AuthContext
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType
import ru.citeck.ecos.records2.RecordRef
import ru.citeck.ecos.records3.record.atts.dto.RecordAtts

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
        setAuthoritiesWithReadPerms(childRecord, "user", "parent-user")

        val parentRecord = AuthContext.runAsFull("parent-user") {
            createRecord("childAssocs" to childRecord)
        }
        setAuthoritiesWithReadPerms(parentRecord, "parent-user", "other-user")

        val checkReadPermsForAll = {
            checkReadPerms(childRecord, false)
            AuthContext.runAs("user") {
                checkReadPerms(childRecord, true)
            }
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

        checkReadPermsForAll()

        val additionalChildren = RecordAtts(RecordRef.create(RECS_DAO_ID, ""))
        additionalChildren.setAtt("_alias", "alias-1")
        additionalChildren.setAtt("_type", REC_TEST_TYPE_REF)

        val mainRecord = RecordAtts(parentRecord)
        mainRecord.setAtt("childAssocs", listOf(childRecord, "alias-1"))

        AuthContext.runAs("parent-user") {
            records.mutate(listOf(mainRecord, additionalChildren))
        }

        checkReadPermsForAll()
    }
}
