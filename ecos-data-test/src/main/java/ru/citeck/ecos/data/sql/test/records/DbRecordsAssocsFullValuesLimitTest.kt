package ru.citeck.ecos.data.sql.test.records

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import ru.citeck.ecos.data.sql.props.DbEcosDataProps
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType
import ru.citeck.ecos.webapp.api.entity.EntityRef
import ru.citeck.ecos.webapp.api.entity.toEntityRef

/**
 * Assocs with values count greater than
 * [DbEcosDataProps.AssocsProps.maxCountToEditByFullValuesList]
 * can't be edited by providing full values list.
 */
class DbRecordsAssocsFullValuesLimitTest : DbRecordsTestBase() {

    @Test
    fun defaultLimitTest() {
        assertThat(DbEcosDataProps().assocs.maxCountToEditByFullValuesList).isEqualTo(150)
    }

    @Test
    fun test() {

        val limit = DbEcosDataProps().assocs.maxCountToEditByFullValuesList

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("assocs")
                    withType(AttributeType.ASSOC)
                    withMultiple(true)
                }
            )
        )

        val refs = (0..limit).map { "emodel/sourceId@localId-$it".toEntityRef() }

        val rec = createRecord("att_add_assocs" to refs.subList(0, limit))
        assertThat(getAssocs(rec)).hasSize(limit)

        // assoc with values count equal to limit still may be edited by full values list
        updateRecord(rec, "assocs" to refs.subList(0, limit - 1))
        assertThat(getAssocs(rec)).hasSize(limit - 1)

        updateRecord(rec, "att_add_assocs" to refs.subList(limit - 1, limit + 1))
        assertThat(getAssocs(rec)).hasSize(limit + 1)

        val error = assertThrows<Exception> {
            updateRecord(rec, "assocs" to refs.subList(0, 10))
        }
        assertThat(error.stackTraceToString())
            .contains("You can't edit large associations by providing full values list")

        // values count is not changed after failed mutation
        assertThat(getAssocs(rec)).hasSize(limit + 1)

        // add/rem operations are still allowed
        updateRecord(rec, "att_rem_assocs" to refs[0])
        assertThat(getAssocs(rec)).hasSize(limit)
    }

    private fun getAssocs(rec: EntityRef): List<EntityRef> {
        return records.getAtt(rec, "assocs[]?id").asStrList().map { EntityRef.valueOf(it) }
    }
}
