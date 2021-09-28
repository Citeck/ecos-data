package ru.citeck.ecos.data.sql.pg.records

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import ru.citeck.ecos.data.sql.service.DbDataServiceConfig
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType
import ru.citeck.ecos.model.lib.type.dto.TypeInfo
import ru.citeck.ecos.model.lib.type.dto.TypeModelDef

class DbRecordsDaoColumnUpdateTest : DbRecordsTestBase() {

    @Test
    fun dateTimeTest() {

        val attId = "att-id"
        val registerTypeWithAtt = { type: AttributeType ->
            registerAtts(
                listOf(
                    AttributeDef.create()
                        .withId(attId)
                        .withType(type)
                        .build()
                )
            )
        }

        registerTypeWithAtt(AttributeType.DATETIME)

        val dateTimeValue = "2021-01-01T00:00:00Z"
        val rec = createRecord(attId to dateTimeValue)
        assertThat(records.getAtt(rec, attId).asText()).isEqualTo(dateTimeValue)

        registerTypeWithAtt(AttributeType.DATE)

        sqlUpdate("ALTER TABLE ${tableRef.fullName} ALTER COLUMN \"$attId\" TYPE DATE USING \"$attId\"::date;")
        sqlUpdate("ALTER TABLE ${tableRef.fullName.removeSuffix("\"")}__ext_txn\" ALTER COLUMN \"$attId\" TYPE DATE USING \"$attId\"::date;")

        val rec2 = createRecord(attId to dateTimeValue)
        assertThat(records.getAtt(rec2, attId).asText()).isEqualTo(dateTimeValue)
/*
        //automatic conversion is not allowed yet because when we convert 2021-01-01

        registerTypeWithAtt(AttributeType.DATETIME)
        val rec3 = createRecord(attId to dateTimeValue)

        var idx = 0
        listOf(rec, rec2, rec3).forEach {
            assertThat(records.getAtt(it, attId).asText())
                .describedAs(it.toString() + "-" + (idx++))
                .isEqualTo(dateTimeValue)
        }*/
    }

    @Test
    fun convertToArrayTest() {

        val testTypeId = "test-type"
        val registerTypeWithAtt = { attId: String, multiple: Boolean ->
            registerType(
                TypeInfo.create {
                    withId(testTypeId)
                    withModel(
                        TypeModelDef.create()
                            .withAttributes(
                                listOf(
                                    AttributeDef.create()
                                        .withId(attId)
                                        .withType(AttributeType.TEXT)
                                        .withMultiple(multiple)
                                        .build()
                                )
                            )
                            .build()
                    )
                }
            )
        }

        registerTypeWithAtt.invoke("textAtt", false)

        val simpleValue = "value"
        val recId = records.create(RECS_DAO_ID, mapOf("textAtt" to simpleValue, "_type" to testTypeId))
        assertThat(records.getAtt(recId, "textAtt").asText()).isEqualTo(simpleValue)

        registerTypeWithAtt.invoke("textAtt", true)

        val valuesList = listOf("value0", "value1")
        records.mutate(recId, mapOf("textAtt" to valuesList))
        assertThat(records.getAtt(recId, "textAtt[]").asStrList()).containsExactlyElementsOf(valuesList)

        registerTypeWithAtt.invoke("textAtt", false)

        val valuesList2 = listOf("value2", "value3")
        records.mutate(recId, mapOf("textAtt" to valuesList2))
        val att2 = records.getAtt(recId, "textAtt[]").asStrList()
        assertThat(att2).containsExactlyElementsOf(listOf(valuesList2.first()))
    }

    @Test
    fun maxItemsSchemaMigrationTest() {

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("testAtt")
                }
            )
        )

        repeat(DbDataServiceConfig.EMPTY.maxItemsToAllowSchemaMigration.toInt() + 1) {
            createRecord("textAtt" to "val")
        }

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("testAtt")
                    withMultiple(true)
                }
            )
        )

        assertThrows<Exception> {
            createRecord("textAtt" to "val")
        }
    }
}
