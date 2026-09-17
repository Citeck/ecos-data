package ru.citeck.ecos.data.sql.test.records

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType
import ru.citeck.ecos.model.lib.type.dto.TypeInfo
import ru.citeck.ecos.model.lib.type.dto.TypeModelDef

class DbRecordsDaoColumnUpdateTest : DbRecordsTestBase() {

    companion object {
        // the historical default of the now-deprecated DbDataServiceConfig.maxItemsToAllowSchemaMigration -
        // named here instead of read off the deprecated property, which this test must not touch
        private const val OLD_LIMIT = 1000
    }

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

        // The contract above is unchanged, but what carries it has changed: narrowing an array is
        // class C, so the array column is moved aside and a fresh scalar column takes
        // its place - which is where the write above landed. It used to be ignored outright,
        // leaving the column an array for ever while the model said scalar.
        val ctx = getTableCtx()
        assertThat(ctx.getAllPhysicalColumns().first { it.name == "__backup_textAtt_text_multiple" }.multiple)
            .describedAs("the values the narrowing dropped are kept, not destroyed")
            .isTrue()
        assertThat(ctx.getColumns().first { it.name == "textAtt" }.multiple)
            .describedAs("and the schema finally agrees with the model")
            .isFalse()
    }

    /**
     * The old behaviour this replaces: a table with more than
     * `DbDataServiceConfig.maxItemsToAllowSchemaMigration` (1000) rows refused every type change,
     * safe or not, and the refusal took down the whole mutation. That limit is exactly what made
     * customers edit column types by hand.
     */
    @Test
    fun safeConversionRunsOnATableAboveTheOldLimitTest() {

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("testAtt")
                }
            )
        )

        repeat(OLD_LIMIT + 1) {
            createRecord("testAtt" to "val")
        }

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("testAtt")
                    withMultiple(true)
                }
            )
        )

        val record = createRecord("testAtt" to listOf("val"))
        assertThat(records.getAtt(record, "testAtt[]").asStrList()).containsExactly("val")
        assertThat(getColumns().first { it.name == "testAtt" }.multiple)
            .describedAs("the conversion is safe and the table size must not block it")
            .isTrue()
    }
}
