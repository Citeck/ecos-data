package ru.citeck.ecos.data.sql.pg.records

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import ru.citeck.ecos.commons.data.ObjectData
import ru.citeck.ecos.context.lib.auth.AuthContext
import ru.citeck.ecos.context.lib.auth.data.EmptyAuth
import ru.citeck.ecos.data.sql.pg.ContentUtils
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType
import ru.citeck.ecos.model.lib.type.dto.QueryPermsPolicy
import ru.citeck.ecos.model.lib.type.dto.TypeInfo
import ru.citeck.ecos.model.lib.type.dto.TypeModelDef
import ru.citeck.ecos.records2.QueryContext.withAttributes
import ru.citeck.ecos.records2.RecordRef
import ru.citeck.ecos.records3.record.atts.dto.RecordAtts
import ru.citeck.ecos.webapp.api.entity.EntityRef
import java.util.*

class RecordsDaoChildrenPermsTest : DbRecordsTestBase() {

    @Test
    fun test() {

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
        setQueryPermsPolicy(QueryPermsPolicy.OWN)

        val checkReadPerms = { ref: RecordRef, isReadPermsExpected: Boolean ->
            if (isReadPermsExpected) {
                assertThat(records.getAtt(ref, "_notExists?bool").asBoolean()).isFalse
            } else {
                assertThat(records.getAtt(ref, "_notExists?bool").asBoolean()).isTrue
            }
        }

        val childRecord = AuthContext.runAs("user") { createRecord() }
        // setAuthoritiesWithReadPerms(childRecord, "user", "parent-user")

        val parentRecord = AuthContext.runAsFull("parent-user") {
            createRecord("childAssocs" to childRecord)
        }
        setAuthoritiesWithReadPerms(parentRecord, "parent-user", "other-user")

        val checkReadPermsForAll = {
            AuthContext.runAs(EmptyAuth) {
                checkReadPerms(childRecord, false)
                AuthContext.runAs("user") {
                    checkReadPerms(childRecord, false)
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
                    checkReadPerms(childRecord, true)
                }
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

    @Test
    fun createWithChildrenAsContent() {

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
        setQueryPermsPolicy(QueryPermsPolicy.OWN)

        registerType(
            TypeInfo.create {
                withId("child-id")
                withQueryPermsPolicy(QueryPermsPolicy.PARENT)
                withModel(
                    TypeModelDef.create().withAttributes(
                        listOf(
                            AttributeDef.create {
                                withId("content")
                                withType(AttributeType.CONTENT)
                            }
                        )
                    ).build()
                )
            }
        )

        setAuthoritiesWithWritePerms(EntityRef.create(APP_NAME, RECS_DAO_ID, "parent-rec-id"), "user-1")
        val record = AuthContext.runAs("test") {
            createRecord(
                "id" to "parent-rec-id",
                "childAssocs" to listOf(
                    ContentUtils.createContentObjFromText("test-text-1", fileType = "child-id"),
                    ContentUtils.createContentObjFromText("test-text-2", fileType = "child-id")
                )
            )
        }
        fun checkContent(expected: List<String>?) {
            val childAssocsContentBytesRaw = records.getAtt(record, "childAssocs[]._content.bytes")
            if (expected == null) {
                assertThat(childAssocsContentBytesRaw.isEmpty()).isTrue
            } else {
                val contents = childAssocsContentBytesRaw.toStrList().map {
                    String(Base64.getDecoder().decode(it))
                }
                assertThat(contents).containsExactlyElementsOf(expected)
            }
        }
        AuthContext.runAsSystem {
            checkContent(listOf("test-text-1", "test-text-2"))
        }
        AuthContext.runAs("test") {
            checkContent(null)
        }
    }
}
