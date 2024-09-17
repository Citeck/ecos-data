package ru.citeck.ecos.data.sql.pg.records

import org.junit.jupiter.api.Test
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType

class DbRecordsJsonValueTest : DbRecordsTestBase() {

    @Test
    fun test() {

        registerAtts(
            listOf(
                AttributeDef.create()
                    .withId("json")
                    .withType(AttributeType.JSON)
                    .build()
            )
        )

        createRecord("json" to "{\"aa\":\"bb\"}")
    }
}
