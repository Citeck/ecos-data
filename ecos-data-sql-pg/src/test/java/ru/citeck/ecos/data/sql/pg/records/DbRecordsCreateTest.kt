package ru.citeck.ecos.data.sql.pg.records

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import ru.citeck.ecos.commons.data.ObjectData
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.records2.RecordRef

class DbRecordsCreateTest : DbRecordsTestBase() {

    @Test
    fun test() {

        registerAtts(
            listOf(
                AttributeDef.create { withId("textAtt") }
            )
        )

        val ref = RecordRef.create(recordsDao.getId(), "unknown-id")
        val exception = assertThrows<Exception> {
            val data = ObjectData.create()
            data.set("_type", REC_TEST_TYPE_REF)
            records.mutate(ref, data)
        }
        assertThat(exception.message).contains("Record doesn't found")
    }
}
