package ru.citeck.ecos.data.sql.pg.records

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType
import ru.citeck.ecos.records2.RecordRef

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

        val extRef0 = RecordRef.valueOf("emodel/sourceId@localId0")
        val extRef1 = RecordRef.valueOf("emodel/sourceId@localId1")
        val extRef2 = RecordRef.valueOf("emodel/sourceId@localId2")

        val assocsList = listOf(extRef0)
        val rec0 = createRecord("single_assoc" to assocsList)

        val getAssocsList = { name: String ->
            records.getAtt(rec0, "$name[]?id").asStrList().map { RecordRef.valueOf(it) }
        }

        assertThat(getAssocsList("single_assoc")).isEqualTo(listOf(extRef0))
        updateRecord(rec0, "att_rem_single_assoc" to extRef1)
        assertThat(getAssocsList("single_assoc")).isEqualTo(listOf(extRef0))
        updateRecord(rec0, "att_rem_single_assoc" to extRef0)
        assertThat(getAssocsList("single_assoc")).isEqualTo(emptyList<RecordRef>())
        updateRecord(rec0, "att_add_single_assoc" to extRef2)
        assertThat(getAssocsList("single_assoc")).isEqualTo(listOf(extRef2))

        // new record will be ignored because assoc is not multiple
        updateRecord(rec0, "att_add_single_assoc" to extRef1)
        assertThat(getAssocsList("single_assoc")).isEqualTo(listOf(extRef2))

        assertThat(getAssocsList("multi_assoc")).isEqualTo(emptyList<RecordRef>())
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
        assertThat(getAssocsList("multi_assoc")).isEqualTo(emptyList<RecordRef>())
    }
}
