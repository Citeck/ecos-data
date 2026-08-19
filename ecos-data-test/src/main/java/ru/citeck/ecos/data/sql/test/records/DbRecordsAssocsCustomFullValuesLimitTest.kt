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
 * Max assocs count to edit by full values list may be changed by
 * 'ecos.webapp.data.assocs.maxCountToEditByFullValuesList' property.
 */
class DbRecordsAssocsCustomFullValuesLimitTest : DbRecordsTestBase() {

    companion object {
        private const val LIMIT = 5
    }

    init {
        dataProps = DbEcosDataProps(
            assocs = DbEcosDataProps.AssocsProps(
                maxCountToEditByFullValuesList = LIMIT
            )
        )
    }

    @Test
    fun test() {

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("assocs")
                    withType(AttributeType.ASSOC)
                    withMultiple(true)
                }
            )
        )

        val refs = (0..LIMIT).map { "emodel/sourceId@localId-$it".toEntityRef() }

        val rec = createRecord("assocs" to refs.subList(0, LIMIT))
        assertThat(getAssocs(rec)).hasSize(LIMIT)

        updateRecord(rec, "assocs" to refs.subList(0, LIMIT - 1))
        assertThat(getAssocs(rec)).hasSize(LIMIT - 1)

        updateRecord(rec, "att_add_assocs" to refs.subList(LIMIT - 1, LIMIT + 1))
        assertThat(getAssocs(rec)).hasSize(LIMIT + 1)

        val error = assertThrows<Exception> {
            updateRecord(rec, "assocs" to refs.subList(0, 2))
        }
        assertThat(error.stackTraceToString())
            .contains("You can't edit large associations by providing full values list")
        assertThat(error.stackTraceToString())
            .contains("Max values count to edit by full values list: $LIMIT")

        assertThat(getAssocs(rec)).hasSize(LIMIT + 1)
    }

    private fun getAssocs(rec: EntityRef): List<EntityRef> {
        return records.getAtt(rec, "assocs[]?id").asStrList().map { EntityRef.valueOf(it) }
    }
}
