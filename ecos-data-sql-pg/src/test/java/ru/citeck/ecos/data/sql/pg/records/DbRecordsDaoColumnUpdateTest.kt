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

        val rec2 = createRecord(attId to dateTimeValue)
        assertThat(records.getAtt(rec2, attId).asText()).isEqualTo(dateTimeValue)
    }

    @Test
    fun dateToDateTimeTest() {

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("strField")
                    withType(AttributeType.TEXT)
                },
                AttributeDef.create {
                    withId("dateField")
                    withType(AttributeType.DATE)
                },
                AttributeDef.create {
                    withId("dateTimeField")
                    withType(AttributeType.DATETIME)
                }
            )
        )

        val dateFields = Array(5) { "2021-01-0${it + 1}" }.toList()
        val recordsList = dateFields.map { createRecord("dateField" to it, "dateTimeField" to it) }

        val checkDateTime = { field: String ->
            assertThat(
                records.getAtts(recordsList, listOf(field)).map {
                    it.getAtt(field).asText()
                }
            ).containsExactlyElementsOf(dateFields.map { it + "T00:00:00Z" })
        }
        checkDateTime("dateField")
        checkDateTime("dateTimeField")

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("strField")
                    withType(AttributeType.TEXT)
                },
                AttributeDef.create {
                    withId("dateField")
                    withType(AttributeType.DATETIME)
                },
                AttributeDef.create {
                    withId("dateTimeField")
                    withType(AttributeType.DATETIME)
                }
            )
        )

        printQueryRes("select * from pg_timezone_names")

        updateRecord(recordsList[0], "strField" to "test")

        printQueryRes("select * from ${tableRef.fullName}")

        checkDateTime("dateTimeField")
        checkDateTime("dateField")
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
