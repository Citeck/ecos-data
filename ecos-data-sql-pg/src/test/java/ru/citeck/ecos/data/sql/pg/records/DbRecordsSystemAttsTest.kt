package ru.citeck.ecos.data.sql.pg.records

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import ru.citeck.ecos.context.lib.auth.AuthContext
import ru.citeck.ecos.context.lib.auth.data.EmptyAuth
import ru.citeck.ecos.data.sql.pg.records.commons.DbRecordsTestBase
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType
import ru.citeck.ecos.webapp.api.entity.EntityRef

class DbRecordsSystemAttsTest : DbRecordsTestBase() {

    @Test
    fun mutationPermissionsTest() {

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("att-0")
                }
            ),
            listOf(
                AttributeDef.create {
                    withId("system-att-0")
                }
            )
        )

        AuthContext.runAs(EmptyAuth) {
            val exception = assertThrows<Exception> {
                createRecord("system-att-0" to "value")
            }
            assertThat(exception.message).contains("Permission denied")
        }

        val record = AuthContext.runAsSystem {
            createRecord("system-att-0" to "value")
        }

        assertThat(records.getAtt(record, "system-att-0").asText()).isEqualTo("value")
    }

    @Test
    fun systemAssocTest() {

        registerAtts(
            listOf(),
            listOf(
                AttributeDef.create {
                    withId("att-0")
                    withType(AttributeType.ASSOC)
                }
            )
        )

        val ref = EntityRef.valueOf("aa/bb@cc")
        val record = AuthContext.runAsSystem {
            createRecord("att-0" to ref)
        }

        val refFromRecord = records.getAtt(record, "att-0?id").getAs(EntityRef::class.java)
        assertThat(refFromRecord).isEqualTo(ref)
    }
}
