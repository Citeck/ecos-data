package ru.citeck.ecos.data.sql.pg.records

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType
import ru.citeck.ecos.txn.lib.TxnContext
import ru.citeck.ecos.webapp.api.entity.toEntityRef
import java.lang.Exception

class DbRecordsMandatoryAttsTest : DbRecordsTestBase() {

    @Test
    fun test() {
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
