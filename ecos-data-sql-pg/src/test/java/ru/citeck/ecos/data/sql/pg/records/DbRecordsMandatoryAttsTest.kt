package ru.citeck.ecos.data.sql.pg.records

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource
import ru.citeck.ecos.commons.data.MLText
import ru.citeck.ecos.data.sql.pg.ContentUtils
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType
import ru.citeck.ecos.txn.lib.TxnContext
import ru.citeck.ecos.webapp.api.entity.EntityRef
import ru.citeck.ecos.webapp.api.entity.toEntityRef
import java.lang.Exception
import java.time.Instant

class DbRecordsMandatoryAttsTest : DbRecordsTestBase() {

    @ParameterizedTest
    @EnumSource(AttributeType::class)
    fun differentTypesTest(type: AttributeType) {

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("notMandatory")
                    withType(type)
                },
                AttributeDef.create {
                    withId("mandatory")
                    withType(type)
                    withMandatory(true)
                }
            )
        )

        val notEmptyValue: Any = when (type) {
            AttributeType.ASSOC -> EntityRef.valueOf("abc@def")
            AttributeType.PERSON -> EntityRef.valueOf("person@admin")
            AttributeType.AUTHORITY_GROUP -> EntityRef.valueOf("authority-group@ECOS_ADMINISTRATORS")
            AttributeType.AUTHORITY -> EntityRef.valueOf("person@admin")
            AttributeType.TEXT -> "text"
            AttributeType.MLTEXT -> MLText("abcd")
            AttributeType.NUMBER -> 123
            AttributeType.BOOLEAN -> false
            AttributeType.DATE -> Instant.ofEpochMilli(100000)
            AttributeType.DATETIME -> Instant.ofEpochMilli(100000)
            AttributeType.CONTENT -> ContentUtils.createContentObjFromText("abc")
            AttributeType.JSON -> """{"aa":"bb"}"""
            AttributeType.BINARY -> ByteArray(10) { it.toByte() }
        }
        val emptyValues = when (type) {
            AttributeType.TEXT -> listOf(null, "")
            AttributeType.MLTEXT -> listOf(null, MLText())
            AttributeType.JSON -> listOf(null, emptyMap<Any, Any>(), emptyArray<Any>())
            AttributeType.BINARY -> listOf(null, ByteArray(0))
            else -> listOf(null)
        }

        emptyValues.forEach { emptyValue ->
            assertThrows<Exception>("emptyValue: $emptyValue") {
                createRecord(
                    "mandatory" to emptyValue,
                    "notMandatory" to null
                )
            }
        }

        val record = createRecord(
            "mandatory" to notEmptyValue,
            "notMandatory" to null
        )

        emptyValues.forEach { emptyValue ->
            assertThrows<Exception> {
                updateRecord(
                    record,
                    "mandatory" to emptyValue
                )
            }
        }

        emptyValues.forEach { emptyValue ->
            updateRecord(record, "notMandatory" to notEmptyValue)
            updateRecord(record, "notMandatory" to emptyValue)
        }
    }

    @Test
    fun textAndAssocTest() {
        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("notMandatory")
                },
                AttributeDef.create {
                    withId("mandatory")
                    withMandatory(true)
                },
                AttributeDef.create {
                    withId("mandatoryAssoc")
                    withType(AttributeType.ASSOC)
                    withMultiple(true)
                    withMandatory(true)
                }
            )
        )
        val refs = listOf("abc@def", "hij@klm").map { "$$APP_NAME/$it".toEntityRef() }
        assertThrows<Exception> {
            createRecord("notMandatory" to "abc", "mandatoryAssoc" to refs)
        }
        val rec = TxnContext.doInTxn {
            createRecord("mandatory" to "abc", "mandatoryAssoc" to refs)
        }
        assertThrows<Exception> {
            updateRecord(rec, "mandatory" to null)
        }
        assertThrows<Exception> {
            updateRecord(rec, "mandatoryAssoc" to null)
        }
        updateRecord(rec, "att_rem_mandatoryAssoc" to refs[0])
        assertThat(
            records.getAtt(rec, "mandatoryAssoc[]?id")
                .asStrList()
                .map { it.toEntityRef() }
        ).containsExactly(refs[1])

        assertThrows<Exception> {
            updateRecord(rec, "att_rem_mandatoryAssoc" to refs[1])
        }

        val newRec = createRecord("notMandatory" to "abc", "_state" to "draft")
        assertThat(records.getAtt(newRec, "notMandatory").asText()).isEqualTo("abc")
    }
}
