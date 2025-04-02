package ru.citeck.ecos.data.sql.pg.records

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import ru.citeck.ecos.data.sql.pg.records.commons.DbRecordsTestBase
import ru.citeck.ecos.model.lib.aspect.dto.AspectInfo
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType
import ru.citeck.ecos.webapp.api.entity.EntityRef
import ru.citeck.ecos.webapp.api.entity.toEntityRef

class DbRecordsAddRemAssocTest : DbRecordsTestBase() {

    @Test
    fun test() {

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("single_assoc")
                    withType(AttributeType.ASSOC)
                },
                AttributeDef.create {
                    withId("multi_assoc")
                    withType(AttributeType.ASSOC)
                    withMultiple(true)
                }
            )
        )

        val extRef0 = EntityRef.valueOf("emodel/sourceId@localId0")
        val extRef1 = EntityRef.valueOf("emodel/sourceId@localId1")
        val extRef2 = EntityRef.valueOf("emodel/sourceId@localId2")

        val assocsList = listOf(extRef0)
        val rec0 = createRecord("single_assoc" to assocsList)

        val getAssocsList = { name: String ->
            records.getAtt(rec0, "$name[]?id").asStrList().map { EntityRef.valueOf(it) }
        }

        assertThat(getAssocsList("single_assoc")).isEqualTo(listOf(extRef0))
        updateRecord(rec0, "att_rem_single_assoc" to extRef1)
        assertThat(getAssocsList("single_assoc")).isEqualTo(listOf(extRef0))
        updateRecord(rec0, "att_rem_single_assoc" to extRef0)
        assertThat(getAssocsList("single_assoc")).isEqualTo(emptyList<EntityRef>())
        updateRecord(rec0, "att_add_single_assoc" to extRef2)
        assertThat(getAssocsList("single_assoc")).isEqualTo(listOf(extRef2))

        // new record will be ignored because assoc is not multiple
        updateRecord(rec0, "att_add_single_assoc" to extRef1)
        assertThat(getAssocsList("single_assoc")).isEqualTo(listOf(extRef2))

        assertThat(getAssocsList("multi_assoc")).isEqualTo(emptyList<EntityRef>())
        updateRecord(rec0, "att_add_multi_assoc" to extRef0)
        assertThat(getAssocsList("multi_assoc")).isEqualTo(listOf(extRef0))
        updateRecord(rec0, "att_add_multi_assoc" to extRef0)
        assertThat(getAssocsList("multi_assoc")).isEqualTo(listOf(extRef0))
        updateRecord(rec0, "att_add_multi_assoc" to extRef1)
        assertThat(getAssocsList("multi_assoc")).isEqualTo(listOf(extRef0, extRef1))
        updateRecord(rec0, "att_add_multi_assoc" to extRef0)
        assertThat(getAssocsList("multi_assoc")).isEqualTo(listOf(extRef0, extRef1))
        updateRecord(rec0, "att_add_multi_assoc" to extRef2)
        assertThat(getAssocsList("multi_assoc")).isEqualTo(listOf(extRef0, extRef1, extRef2))
        updateRecord(rec0, "att_rem_multi_assoc" to extRef1)
        assertThat(getAssocsList("multi_assoc")).isEqualTo(listOf(extRef0, extRef2))
        updateRecord(rec0, "att_rem_multi_assoc" to listOf(extRef0, extRef2))
        assertThat(getAssocsList("multi_assoc")).isEqualTo(emptyList<EntityRef>())
    }

    @Test
    fun createWithAdd() {

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("multi_assoc")
                    withType(AttributeType.ASSOC)
                    withMultiple(true)
                }
            )
        )

        val extRef0 = EntityRef.valueOf("emodel/sourceId@localId0")
        val extRef1 = EntityRef.valueOf("emodel/sourceId@localId1")

        val assocsList = listOf(extRef0, extRef1)
        val rec0 = createRecord("att_add_multi_assoc" to assocsList)

        val getAssocsList = { name: String ->
            records.getAtt(rec0, "$name[]?id").asStrList().map { EntityRef.valueOf(it) }
        }

        assertThat(getAssocsList("multi_assoc")).containsExactly(extRef0, extRef1)
    }

    @Test
    fun mutateWithoutExistingAssoc() {

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("multi_assoc")
                    withType(AttributeType.ASSOC)
                    withMultiple(true)
                },
                AttributeDef.create {
                    withId("text")
                }
            )
        )
        registerAspect(
            AspectInfo.create {
                withId("aspect")
                withPrefix("aspect")
                withAttributes(
                    listOf(
                        AttributeDef.create {
                            withId("aspect:multi_assoc")
                            withType(AttributeType.ASSOC)
                            withMultiple(true)
                        }
                    )
                )
            }
        )

        val ref0 = "emodel/custom-0@abc".toEntityRef()
        val ref1 = "emodel/custom-1@abc".toEntityRef()

        listOf("multi_assoc", "aspect:multi_assoc").forEach { assocName ->

            val desc = "assoc: '$assocName'"

            val ref = createRecord(
                assocName to listOf(ref0, ref1)
            )
            val assocs0 = records.getAtt(ref, "$assocName[]?id").toList(EntityRef::class.java)
            assertThat(assocs0).describedAs(desc).containsExactly(ref0, ref1)

            printQueryRes("SELECT * FROM ${tableRef.fullName};")
            updateRecord(ref, "text" to "abc")
            printQueryRes("SELECT * FROM ${tableRef.fullName};")

            val assocs1 = records.getAtt(ref, "$assocName[]?id").toList(EntityRef::class.java)
            assertThat(assocs1).describedAs(desc).containsExactly(ref0, ref1)
        }
    }
}
