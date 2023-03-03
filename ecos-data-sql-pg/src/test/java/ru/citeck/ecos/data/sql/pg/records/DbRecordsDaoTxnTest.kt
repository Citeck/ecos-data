package ru.citeck.ecos.data.sql.pg.records

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType
import ru.citeck.ecos.model.lib.type.dto.TypeInfo
import ru.citeck.ecos.model.lib.type.dto.TypeModelDef
import ru.citeck.ecos.model.lib.utils.ModelUtils
import ru.citeck.ecos.records2.RecordConstants
import ru.citeck.ecos.records2.RecordRef
import ru.citeck.ecos.txn.lib.TxnContext

class DbRecordsDaoTxnTest : DbRecordsTestBase() {

    @Test
    fun deleteTest2() {

        registerAtts(
            listOf(
                AttributeDef.create()
                    .withId("textAtt")
                    .withType(AttributeType.TEXT)
            ).map { it.build() }
        )

        val newRecId = TxnContext.doInTxn { createRecord("textAtt" to "value") }
        TxnContext.doInTxn {
            assertThat(records.getAtt(newRecId, "textAtt").asText()).isEqualTo("value")
            records.delete(newRecId)
            assertThat(records.getAtt(newRecId, "textAtt").asText()).isEqualTo("")
        }
    }

    @Test
    fun doubleUpdateTest() {

        registerAtts(
            listOf(
                AttributeDef.create()
                    .withId("textAtt")
                    .withType(AttributeType.TEXT)
            ).map { it.build() }
        )

        var newRecRef = RecordRef.EMPTY
        TxnContext.doInTxn(false) {
            newRecRef = createRecord("textAtt" to "Value")
            assertThat(records.getAtt(newRecRef, "textAtt").asText()).isEqualTo("Value")
            updateRecord(newRecRef, "textAtt" to "Value2")
            assertThat(records.getAtt(newRecRef, "textAtt").asText()).isEqualTo("Value2")
        }
        assertThat(records.getAtt(newRecRef, "textAtt").asText()).isEqualTo("Value2")

        TxnContext.doInTxn(false) {
            updateRecord(newRecRef, "textAtt" to "Value3")
            assertThat(records.getAtt(newRecRef, "textAtt").asText()).isEqualTo("Value3")
            updateRecord(newRecRef, "textAtt" to "Value4")
            assertThat(records.getAtt(newRecRef, "textAtt").asText()).isEqualTo("Value4")
        }
        assertThat(records.getAtt(newRecRef, "textAtt").asText()).isEqualTo("Value4")
    }

    @Test
    fun deleteTest() {

        registerAtts(
            listOf(
                AttributeDef.create()
                    .withId("textAtt")
                    .withType(AttributeType.TEXT)
            ).map { it.build() }
        )

        var newRecRef = RecordRef.EMPTY
        TxnContext.doInTxn(false) {
            newRecRef = createRecord("textAtt" to "Value")
            assertThat(records.getAtt(newRecRef, "textAtt").asText()).isEqualTo("Value")
            assertThat(records.getAtt(newRecRef, "_notExists?bool").asBoolean()).isFalse

            records.delete(newRecRef)
            assertThat(records.getAtt(newRecRef, "textAtt").asText()).isEmpty()
            assertThat(records.getAtt(newRecRef, "_notExists?bool").asBoolean()).isTrue
        }

        assertThat(records.getAtt(newRecRef, "textAtt").asText()).isEmpty()
        assertThat(records.getAtt(newRecRef, "_notExists?bool").asBoolean()).isTrue

        TxnContext.doInTxn(false) {
            newRecRef = createRecord("textAtt" to "Value")
            assertThat(records.getAtt(newRecRef, "textAtt").asText()).isEqualTo("Value")
            assertThat(records.getAtt(newRecRef, "_notExists?bool").asBoolean()).isFalse
        }

        try {
            TxnContext.doInTxn(false) {
                records.delete(newRecRef)
                assertThat(records.getAtt(newRecRef, "textAtt").asText()).isEmpty()
                assertThat(records.getAtt(newRecRef, "_notExists?bool").asBoolean()).isTrue

                error("error")
            }
        } catch (e: Exception) {
            // expected error
        }

        assertThat(records.getAtt(newRecRef, "textAtt").asText()).isEqualTo("Value")
        assertThat(records.getAtt(newRecRef, "_notExists?bool").asBoolean()).isFalse()
    }

    @Test
    fun test() {

        val testTypeId = "test-type"
        registerType(
            TypeInfo.create {
                withId(testTypeId)
                withModel(
                    TypeModelDef.create()
                        .withAttributes(
                            listOf(
                                AttributeDef.create()
                                    .withId("textAtt")
                                    .withType(AttributeType.TEXT),
                                AttributeDef.create()
                                    .withId("numAtt")
                                    .withType(AttributeType.NUMBER)
                            ).map { it.build() }
                        )
                        .build()
                )
            }
        )

        var newRecRef: RecordRef = RecordRef.EMPTY
        TxnContext.doInTxn(false) {
            newRecRef = records.create(
                RECS_DAO_ID,
                mapOf(
                    "textAtt" to "value",
                    RecordConstants.ATT_TYPE to ModelUtils.getTypeRef(testTypeId)
                )
            )
            assertThat(records.getAtt(newRecRef, "textAtt").asText()).isEqualTo("value")
        }
        printAllColumns()
        assertThat(records.getAtt(newRecRef, "textAtt").asText()).isEqualTo("value")

        TxnContext.doInTxn(false) {
            records.mutate(newRecRef, mapOf("textAtt" to "changedInTxn"))
            assertThat(records.getAtt(newRecRef, "textAtt").asText()).isEqualTo("changedInTxn")
        }
        assertThat(records.getAtt(newRecRef, "textAtt").asText()).isEqualTo("changedInTxn")

        try {
            TxnContext.doInTxn(false) {
                records.mutate(newRecRef, mapOf("textAtt" to "changedInTxnAndRolledBack"))
                assertThat(records.getAtt(newRecRef, "textAtt").asText()).isEqualTo("changedInTxnAndRolledBack")
                error("error")
            }
        } catch (e: Exception) {
            // exception is expected
        }
        assertThat(records.getAtt(newRecRef, "textAtt").asText()).isEqualTo("changedInTxn")

        try {
            TxnContext.doInTxn(false) {
                records.mutate(newRecRef, mapOf("textAtt" to "changedInTxnAndRolledBack"))
                records.mutate(newRecRef, mapOf("numAtt" to 1234))

                val atts = records.getAtts(
                    newRecRef,
                    mapOf(
                        "num" to "numAtt?num",
                        "txt" to "textAtt"
                    )
                )
                assertThat(atts.getAtt("num").asDouble()).isEqualTo(1234.0)
                assertThat(atts.getAtt("txt").asText()).isEqualTo("changedInTxnAndRolledBack")

                error("error")
            }
        } catch (e: Exception) {
            // exception is expected
        }
        val atts = records.getAtts(
            newRecRef,
            mapOf(
                "num" to "numAtt?num",
                "txt" to "textAtt"
            )
        )
        assertThat(atts.getAtt("num").asDouble()).isEqualTo(0.0)
        assertThat(atts.getAtt("txt").asText()).isEqualTo("changedInTxn")

        TxnContext.doInTxn(false) {
            records.mutate(newRecRef, mapOf("textAtt" to "changedInTxn2"))
            records.mutate(newRecRef, mapOf("numAtt" to 1234))
        }

        val atts2 = records.getAtts(
            newRecRef,
            mapOf(
                "num" to "numAtt?num",
                "txt" to "textAtt"
            )
        )
        assertThat(atts2.getAtt("num").asDouble()).isEqualTo(1234.0)
        assertThat(atts2.getAtt("txt").asText()).isEqualTo("changedInTxn2")
    }
}
