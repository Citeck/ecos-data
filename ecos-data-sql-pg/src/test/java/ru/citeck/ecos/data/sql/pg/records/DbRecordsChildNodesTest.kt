package ru.citeck.ecos.data.sql.pg.records

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import ru.citeck.ecos.commons.data.ObjectData
import ru.citeck.ecos.context.lib.auth.AuthContext
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType
import ru.citeck.ecos.model.lib.type.dto.QueryPermsPolicy
import ru.citeck.ecos.records3.record.atts.dto.RecordAtts
import ru.citeck.ecos.records3.record.request.RequestContext
import ru.citeck.ecos.txn.lib.TxnContext
import ru.citeck.ecos.webapp.api.entity.EntityRef
import java.lang.RuntimeException

class DbRecordsChildNodesTest : DbRecordsTestBase() {

    @Test
    fun createWithChildrenTest() {

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("strField")
                },
                AttributeDef.create {
                    withId("childAssoc")
                    withType(AttributeType.ASSOC)
                    withConfig(ObjectData.create("""{"child":true}"""))
                }
            )
        )

        val mainRec = RecordAtts(EntityRef.create(mainCtx.dao.getId(), ""))
        mainRec.setAtt("strField", "parent-test")
        mainRec.setAtt("childAssoc?assoc", "alias-123")
        mainRec.setAtt("_type", REC_TEST_TYPE_REF)

        val childRec = RecordAtts(EntityRef.create(mainCtx.dao.getId(), ""))
        childRec.setAtt("_alias", "alias-123")
        childRec.setAtt("strField", "child-test")
        childRec.setAtt("_type", REC_TEST_TYPE_REF)

        val mutatedRecords = records.mutate(listOf(mainRec, childRec))
        assertThat(mutatedRecords).hasSize(2)

        val childAssocs = records.getAtt(mutatedRecords[0], "childAssoc[]?id").asList(EntityRef::class.java)
        assertThat(childAssocs).containsExactly(mutatedRecords[1])

        printQueryRes("SELECT * FROM ${tableRef.fullName}")

        val parentId = records.getAtt(mutatedRecords[1], "_parent?id").getAs(EntityRef::class.java)
        assertThat(parentId).isEqualTo(mutatedRecords[0])

        // test with sourceId mapping

        val otherSourceId = "other-source-id"
        val mutatedRecords2 = RequestContext.doWithCtx({ ctxData ->
            ctxData.withSourceIdMapping(mapOf(mainCtx.dao.getId() to otherSourceId))
        }) {
            records.mutate(listOf(mainRec, childRec))
        }
        val parentId2 = records.getAtt(mutatedRecords2[1], "_parent?id").getAs(EntityRef::class.java)
        assertThat(parentId2?.getSourceId()).isEqualTo(otherSourceId)
    }

    @Test
    fun createNewChildrenTest() {

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("childAssoc")
                    withType(AttributeType.ASSOC)
                    withConfig(ObjectData.create().set("child", true))
                    withMultiple(true)
                }
            )
        )
        setQueryPermsPolicy(QueryPermsPolicy.OWN)

        val refs = mutableListOf<EntityRef>()

        AuthContext.runAs("user") {
            val parentRec = TxnContext.doInTxn {
                createRecord()
            }

            TxnContext.doInTxn {
                val childRec1 = createRecord("_parent" to parentRec, "_parentAtt" to "childAssoc")
                val parentRefFromChild1 = records.getAtt(childRec1, "_parent?id").asText()
                assertThat(parentRefFromChild1).isEqualTo(parentRec.toString())
                refs.add(childRec1)
/*            }

            TxnContext.doInTxn {*/
                val childRec2 = createRecord("_parent" to parentRec, "_parentAtt" to "childAssoc")
                val parentRefFromChild2 = records.getAtt(childRec2, "_parent?id").asText()
                assertThat(parentRefFromChild2).isEqualTo(parentRec.toString())
                refs.add(childRec2)

                println("ORIG_TABLE")
                printQueryRes("SELECT * FROM ${tableRef.fullName}")
                println("REFS")
                printQueryRes("SELECT * from \"${tableRef.schema}\".\"ecos_record_ref\";")
            }

            println("===================== AFTER COMMIT ===============================")

            println("ORIG_TABLE")
            printQueryRes("SELECT * FROM ${tableRef.fullName}")
            println("REFS")
            printQueryRes("SELECT * from \"${tableRef.schema}\".\"ecos_record_ref\";")

            printQueryRes("SELECT * FROM ${tableRef.fullName}")
            val childRefs = records.getAtt(parentRec, "childAssoc[]?id").asList(EntityRef::class.java)
            assertThat(childRefs).containsExactlyElementsOf(refs)
        }
    }

    @Test
    fun notExistentAssocTest() {
        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("notAssoc")
                },
                AttributeDef.create {
                    withId("notChildAssoc")
                    withType(AttributeType.ASSOC)
                },
                AttributeDef.create {
                    withId("childAssoc")
                    withType(AttributeType.ASSOC)
                    withConfig(ObjectData.create().set("child", true))
                }
            )
        )

        val parent = createRecord()
        fun check(attribute: String, errorExpected: Boolean) {
            if (errorExpected) {
                assertThrows<RuntimeException> {
                    createRecord("_parent" to parent, "_parentAtt" to attribute)
                }
            } else {
                createRecord("_parent" to parent, "_parentAtt" to attribute)
            }
        }
        check("unknown", true)
        check("notAssoc", true)
        check("notChildAssoc", true)
        check("childAssoc", false)
    }
}
