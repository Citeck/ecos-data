package ru.citeck.ecos.data.sql.pg.records

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import ru.citeck.ecos.commons.data.DataValue
import ru.citeck.ecos.context.lib.auth.AuthContext
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.records3.record.dao.impl.mem.InMemDataRecordsDao
import ru.citeck.ecos.webapp.api.entity.EntityRef

class DbRecordsRecordMetaTest : DbRecordsTestBase() {

    companion object {
        private const val CREATOR_LOCAL_ID = "_creator?localId"
        private const val MODIFIER_LOCAL_ID = "_modifier?localId"
        private const val CREATED = "_created"
        private const val MODIFIED = "_modified"
    }

    @Test
    fun test() {
        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("text")
                }
            )
        )
        fun assertAttValues(rec: EntityRef, atts: Map<String, Any?>) {
            for ((att, expected) in atts) {
                val value = records.getAtt(rec, att)
                assertThat(value)
                    .describedAs("ref: $rec att: $att")
                    .isEqualTo(DataValue.create(expected))
            }
        }

        val rec = createRecord("text" to "abc")
        assertAttValues(
            rec,
            mapOf(
                "text?str" to "abc",
                CREATOR_LOCAL_ID to "system",
                MODIFIER_LOCAL_ID to "system"
            )
        )
        val recAtts0 = records.getAtts(rec, listOf(CREATED, MODIFIED))
        assertThat(recAtts0[CREATED]).isEqualTo(recAtts0[CREATED])
        Thread.sleep(1)
        AuthContext.runAs("user") {
            updateRecord(rec, "text" to "abcd")
        }
        val recAtts1 = records.getAtts(rec, listOf(CREATED, MODIFIED))
        assertThat(recAtts0[CREATED]).isEqualTo(recAtts1[CREATED])
        assertThat(recAtts1[MODIFIED].getAsInstant()).isAfter(recAtts0[MODIFIED].getAsInstant())

        assertAttValues(
            rec,
            mapOf(
                "text?str" to "abcd",
                CREATOR_LOCAL_ID to "system",
                MODIFIER_LOCAL_ID to "user"
            )
        )

        val rec1 = AuthContext.runAsFull("user") {
            createRecord("text" to "def")
        }
        assertAttValues(
            rec1,
            mapOf(
                "text?str" to "def",
                CREATOR_LOCAL_ID to "user",
                MODIFIER_LOCAL_ID to "user"
            )
        )

        val personsDao = InMemDataRecordsDao("emodel/person")
        records.register(personsDao)
        records.create(
            personsDao.getId(),
            mapOf(
                "id" to "user",
                "personAtt" to "personAttValue"
            )
        )
        assertThat(records.getAtt(rec1, "_creator.personAtt").asText()).isEqualTo("personAttValue")
        assertThat(records.getAtt(rec1, "_modifier.personAtt").asText()).isEqualTo("personAttValue")
    }
}
