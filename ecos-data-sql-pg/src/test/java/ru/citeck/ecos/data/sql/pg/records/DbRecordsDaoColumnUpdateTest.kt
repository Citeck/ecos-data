package ru.citeck.ecos.data.sql.pg.records

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import ru.citeck.ecos.commons.data.MLText
import ru.citeck.ecos.data.sql.ecostype.DbEcosTypeInfo
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType
import ru.citeck.ecos.records2.RecordRef

class DbRecordsDaoColumnUpdateTest : DbRecordsTestBase() {

    @Test
    fun convertToArrayTest() {

        val testTypeId = "test-type"
        val registerTypeWithAtt = { attId: String, multiple: Boolean ->
            registerType(
                DbEcosTypeInfo(
                    testTypeId, MLText(), MLText(), RecordRef.EMPTY,
                    listOf(
                        AttributeDef.create()
                            .withId(attId)
                            .withType(AttributeType.TEXT)
                            .withMultiple(multiple)
                    ).map { it.build() },
                    emptyList()
                )
            )
        }

        registerTypeWithAtt.invoke("textAtt", false)

        val simpleValue = "value"
        val recId = getRecords().create(RECS_DAO_ID, mapOf("textAtt" to simpleValue, "_type" to testTypeId))
        assertThat(getRecords().getAtt(recId, "textAtt").asText()).isEqualTo(simpleValue)

        registerTypeWithAtt.invoke("textAtt", true)

        val valuesList = listOf("value0", "value1")
        getRecords().mutate(recId, mapOf("textAtt" to valuesList))
        assertThat(getRecords().getAtt(recId, "textAtt[]").asStrList()).containsExactlyElementsOf(valuesList)

        registerTypeWithAtt.invoke("textAtt", false)

        val valuesList2 = listOf("value2", "value3")
        getRecords().mutate(recId, mapOf("textAtt" to valuesList2))
        val att2 = getRecords().getAtt(recId, "textAtt[]").asStrList()
        assertThat(att2).containsExactlyElementsOf(listOf(valuesList2.first()))
    }
}