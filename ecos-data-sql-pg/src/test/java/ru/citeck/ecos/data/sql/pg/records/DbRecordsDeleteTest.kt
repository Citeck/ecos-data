package ru.citeck.ecos.data.sql.pg.records

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import ru.citeck.ecos.commons.data.ObjectData
import ru.citeck.ecos.context.lib.auth.AuthContext
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType
import ru.citeck.ecos.model.lib.type.dto.QueryPermsPolicy
import ru.citeck.ecos.model.lib.type.dto.TypeInfo
import ru.citeck.ecos.model.lib.utils.ModelUtils
import ru.citeck.ecos.records3.record.dao.delete.DelStatus
import ru.citeck.ecos.txn.lib.TxnContext
import ru.citeck.ecos.webapp.api.entity.EntityRef

class DbRecordsDeleteTest : DbRecordsTestBase() {

    @Test
    fun deleteWithAssoc() {

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("targetAssoc")
                    withType(AttributeType.ASSOC)
                },
                AttributeDef.create {
                    withId("childAssoc")
                    withType(AttributeType.ASSOC)
                    withConfig(ObjectData.create("""{"child": true}"""))
                }
            )
        )
        registerType(
            TypeInfo.create {
                withId("other")
                withSourceId("$APP_NAME/other")
            }
        )

        val otherCtx = createRecordsDao(
            tableRef.withTable("other"),
            ModelUtils.getTypeRef("other"),
            sourceId = "other"
        )

        val otherTargetRef0 = otherCtx.createRecord()
        val otherChildRef0 = otherCtx.createRecord()

        val mainRec = mainCtx.createRecord(
            "targetAssoc" to otherTargetRef0,
            "childAssoc" to otherChildRef0
        )

        fun assertAssoc(att: String, expected: List<EntityRef>) {
            assertThat(records.getAtt(mainRec, "$att[]?id").asList(EntityRef::class.java))
                .describedAs("$att: $expected")
                .containsExactlyElementsOf(expected)
        }
        assertAssoc("targetAssoc", listOf(otherTargetRef0))
        assertAssoc("childAssoc", listOf(otherChildRef0))

        records.delete(otherChildRef0)
        assertAssoc("childAssoc", emptyList())

        records.delete(otherTargetRef0)
        assertAssoc("targetAssoc", emptyList())

        val otherTargetRef1 = otherCtx.createRecord()
        updateRecord(mainRec, "targetAssoc" to otherTargetRef1)
        assertAssoc("targetAssoc", listOf(otherTargetRef1))

        records.delete(mainRec)
        records.delete(otherTargetRef1)

        assertThat(records.getAtt(mainRec, "_notExists?bool").asBoolean()).isEqualTo(true)
        assertThat(records.getAtt(otherTargetRef1, "_notExists?bool").asBoolean()).isEqualTo(true)
    }

    @Test
    fun test() {

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("textAtt")
                }
            )
        )
        setQueryPermsPolicy(QueryPermsPolicy.OWN)

        AuthContext.runAs("admin") {
            testAsAdmin()
        }
    }

    private fun testAsAdmin() {

        val elements = createElements()

        TxnContext.doInTxn {
            records.delete(elements)
        }

        val recordsFromDao1 = records.query(baseQuery)
        assertThat(recordsFromDao1.getRecords()).isEmpty()

        val elements2 = createElements()
        elements2.forEach {
            TxnContext.doInTxn {
                records.delete(it)
            }
        }
        val recordsFromDao2 = records.query(baseQuery)
        assertThat(recordsFromDao2.getRecords()).isEmpty()
    }

    @Test
    fun testDeleteAndCreateWithSameId() {

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("textAtt")
                }
            )
        )

        val customId = "custom-id"

        val recRef = createRecord(
            "id" to customId,
            "textAtt" to "value"
        )
        assertThat(recRef.getLocalId()).isEqualTo(customId)

        val delRes = records.delete(recRef)

        assertThat(delRes).isEqualTo(DelStatus.OK)

        val recRef2 = createRecord(
            "id" to customId,
            "textAtt" to "value2"
        )
        assertThat(recRef2.getLocalId()).isEqualTo(customId)
        assertThat(records.getAtt(recRef2, "textAtt").asText()).isEqualTo("value2")
    }

    private fun createElements(): List<EntityRef> {

        val recordsList = ArrayList<EntityRef>()
        for (i in 0..5) {
            recordsList.add(createRecord("textAtt" to "idx-$i"))
        }

        recordsList.forEach {
            setAuthoritiesWithReadPerms(it, "admin")
        }

        val recordsFromDao0 = records.query(baseQuery)
        assertThat(recordsFromDao0.getRecords()).containsExactlyElementsOf(recordsList)

        return recordsList
    }
}
