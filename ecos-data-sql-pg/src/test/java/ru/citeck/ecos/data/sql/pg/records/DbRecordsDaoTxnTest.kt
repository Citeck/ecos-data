package ru.citeck.ecos.data.sql.pg.records

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import ru.citeck.ecos.commons.data.MLText
import ru.citeck.ecos.data.sql.ecostype.DbEcosTypeInfo
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType
import ru.citeck.ecos.model.lib.type.service.utils.TypeUtils
import ru.citeck.ecos.records2.RecordRef
import ru.citeck.ecos.records3.record.request.RequestContext

class DbRecordsDaoTxnTest : DbRecordsTestBase() {

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
        RequestContext.doWithTxn(false) {
            newRecRef = createRecord("textAtt" to "Value")
            assertThat(getRecords().getAtt(newRecRef, "textAtt").asText()).isEqualTo("Value")
            assertThat(getRecords().getAtt(newRecRef, "_notExists?bool").asBoolean()).isFalse()

            getRecords().delete(newRecRef)
            assertThat(getRecords().getAtt(newRecRef, "textAtt").asText()).isEmpty()
            assertThat(getRecords().getAtt(newRecRef, "_notExists?bool").asBoolean()).isTrue()
        }

        assertThat(getRecords().getAtt(newRecRef, "textAtt").asText()).isEmpty()
        assertThat(getRecords().getAtt(newRecRef, "_notExists?bool").asBoolean()).isTrue()

        RequestContext.doWithTxn(false) {
            newRecRef = createRecord("textAtt" to "Value")
            assertThat(getRecords().getAtt(newRecRef, "textAtt").asText()).isEqualTo("Value")
            assertThat(getRecords().getAtt(newRecRef, "_notExists?bool").asBoolean()).isFalse()
        }

        try {
            RequestContext.doWithTxn(false) {
                getRecords().delete(newRecRef)
                assertThat(getRecords().getAtt(newRecRef, "textAtt").asText()).isEmpty()
                assertThat(getRecords().getAtt(newRecRef, "_notExists?bool").asBoolean()).isTrue()

                error("error")
            }
        } catch (e: Exception) {
            // expected error
        }

        assertThat(getRecords().getAtt(newRecRef, "textAtt").asText()).isEqualTo("Value")
        assertThat(getRecords().getAtt(newRecRef, "_notExists?bool").asBoolean()).isFalse()
    }

    @Test
    fun test() {

        val testTypeId = "test-type"
        registerType(
            DbEcosTypeInfo(
                testTypeId, MLText(), MLText(), RecordRef.EMPTY,
                listOf(
                    AttributeDef.create()
                        .withId("textAtt")
                        .withType(AttributeType.TEXT),
                    AttributeDef.create()
                        .withId("numAtt")
                        .withType(AttributeType.NUMBER)
                ).map { it.build() },
                emptyList()
            )
        )

        var newRecRef: RecordRef = RecordRef.EMPTY
        RequestContext.doWithTxn(false) {
            newRecRef = getRecords().create(
                RECS_DAO_ID,
                mapOf(
                    "textAtt" to "value",
                    "_type" to TypeUtils.getTypeRef(testTypeId)
                )
            )
            assertThat(getRecords().getAtt(newRecRef, "textAtt").asText()).isEqualTo("value")
        }
        printAllColumns()
        assertThat(getRecords().getAtt(newRecRef, "textAtt").asText()).isEqualTo("value")

        RequestContext.doWithTxn(false) {
            getRecords().mutate(newRecRef, mapOf("textAtt" to "changedInTxn"))
            assertThat(getRecords().getAtt(newRecRef, "textAtt").asText()).isEqualTo("changedInTxn")
        }
        assertThat(getRecords().getAtt(newRecRef, "textAtt").asText()).isEqualTo("changedInTxn")

        try {
            RequestContext.doWithTxn(false) {
                getRecords().mutate(newRecRef, mapOf("textAtt" to "changedInTxnAndRolledBack"))
                assertThat(getRecords().getAtt(newRecRef, "textAtt").asText()).isEqualTo("changedInTxnAndRolledBack")
                error("error")
            }
        } catch (e: Exception) {
            // exception is expected
        }
        assertThat(getRecords().getAtt(newRecRef, "textAtt").asText()).isEqualTo("changedInTxn")

        try {
            RequestContext.doWithTxn(false) {
                getRecords().mutate(newRecRef, mapOf("textAtt" to "changedInTxnAndRolledBack"))
                getRecords().mutate(newRecRef, mapOf("numAtt" to 1234))

                val atts = getRecords().getAtts(
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
        val atts = getRecords().getAtts(
            newRecRef,
            mapOf(
                "num" to "numAtt?num",
                "txt" to "textAtt"
            )
        )
        assertThat(atts.getAtt("num").asDouble()).isEqualTo(0.0)
        assertThat(atts.getAtt("txt").asText()).isEqualTo("changedInTxn")

        RequestContext.doWithTxn(false) {
            getRecords().mutate(newRecRef, mapOf("textAtt" to "changedInTxn2"))
            getRecords().mutate(newRecRef, mapOf("numAtt" to 1234))
        }

        val atts2 = getRecords().getAtts(
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
