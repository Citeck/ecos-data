package ru.citeck.ecos.data.sql.pg.records

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import ru.citeck.ecos.commons.data.DataValue
import ru.citeck.ecos.commons.data.ObjectData
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType

class DbRecordsDtoTest : DbRecordsTestBase() {

    @Test
    fun test() {

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("dataValue")
                    withType(AttributeType.JSON)
                }
            )
        )

        val dtoWithNull = DtoWithDataValue(DataValue.NULL)
        val ref = createRecord(ObjectData.create(dtoWithNull))

        val dtoFromRecs = records.getAtts(ref, DtoWithDataValue::class.java)
        assertThat(dtoFromRecs).isEqualTo(dtoWithNull)
    }

    data class DtoWithDataValue(
        val dataValue: DataValue
    )
}
