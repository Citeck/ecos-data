package ru.citeck.ecos.data.sql.pg.records

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import ru.citeck.ecos.commons.data.DataValue
import ru.citeck.ecos.commons.json.Json
import ru.citeck.ecos.data.sql.dto.DbColumnDef
import ru.citeck.ecos.data.sql.dto.DbColumnType
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType

class DbRecordsDaoJsonArrayTest : DbRecordsTestBase() {

    @Test
    fun test() {

        val jsonAttSingle = "json_att_single"
        val jsonAttMultiple = "json_att_multiple"

        val attsDef = mapOf(
            jsonAttSingle to AttributeDef.create {
                withId(jsonAttSingle)
                withType(AttributeType.JSON)
            },
            jsonAttMultiple to AttributeDef.create {
                withId(jsonAttMultiple)
                withType(AttributeType.JSON)
                withMultiple(true)
            }
        )

        registerAtts(attsDef.values.toList())

        val jsonSingleValue = DataValue.create(
            """
            {
                "someKey": "someValue"
            }
            """.trimIndent()
        )

        val rec = createRecord(
            jsonAttSingle to jsonSingleValue,
            jsonAttMultiple to jsonSingleValue
        )

        dbDataSource.withTransaction(true) {
            assertThat(dbSchemaDao.getColumns()).contains(
                // check that json columns created with multiple: false
                DbColumnDef(jsonAttSingle, DbColumnType.JSON, false, emptyList()),
                DbColumnDef(jsonAttMultiple, DbColumnType.JSON, false, emptyList())
            )
        }

        val singleValues = records.getAtts(
            rec,
            mapOf(
                jsonAttSingle to "$jsonAttSingle?json",
                jsonAttMultiple to "$jsonAttMultiple?json",
            )
        )

        assertThat(singleValues.getAtt(jsonAttSingle)).isEqualTo(jsonSingleValue)
        assertThat(singleValues.getAtt(jsonAttMultiple)).isEqualTo(jsonSingleValue)

        listOf(jsonAttSingle, jsonAttMultiple).forEach { att ->
            dbDataSource.withTransaction(true) {
                dbDataSource.query("SELECT $att::text as res FROM ${tableRef.fullName}", emptyList()) { res ->
                    res.next()
                    val value = Json.mapper.read(res.getString("res"), DataValue::class.java)
                    if (attsDef[att]!!.multiple) {
                        assertThat(value).isEqualTo(jsonSingleValue.asSingletonList())
                    } else {
                        assertThat(value).isEqualTo(jsonSingleValue)
                    }
                }
            }
        }

        val arrayValue = DataValue.createArr()
        arrayValue.add(jsonSingleValue)
        arrayValue.add(jsonSingleValue)
        arrayValue.add(jsonSingleValue)

        updateRecord(
            rec,
            jsonAttSingle to arrayValue,
            jsonAttMultiple to arrayValue
        )

        val multiValues = records.getAtts(
            rec,
            mapOf(
                jsonAttSingle to "$jsonAttSingle[]?json",
                jsonAttMultiple to "$jsonAttMultiple[]?json",
            )
        )

        val arrayWithFirstElement = DataValue.createArr()
        arrayWithFirstElement.add(arrayValue.get(0))
        assertThat(multiValues.getAtt(jsonAttSingle)).isEqualTo(arrayWithFirstElement)
        assertThat(multiValues.getAtt(jsonAttMultiple)).isEqualTo(arrayValue)

        listOf(jsonAttSingle, jsonAttMultiple).forEach { att ->
            dbDataSource.withTransaction(true) {
                dbDataSource.query("SELECT $att::text as res FROM ${tableRef.fullName}", emptyList()) { res ->
                    res.next()
                    val value = Json.mapper.read(res.getString("res"), DataValue::class.java)
                    if (attsDef[att]!!.multiple) {
                        assertThat(value).isEqualTo(arrayValue)
                    } else {
                        assertThat(value).isEqualTo(jsonSingleValue)
                    }
                }
            }
        }
    }

    private fun DataValue.asSingletonList(): DataValue {
        val res = DataValue.createArr()
        res.add(this)
        return res
    }
}
