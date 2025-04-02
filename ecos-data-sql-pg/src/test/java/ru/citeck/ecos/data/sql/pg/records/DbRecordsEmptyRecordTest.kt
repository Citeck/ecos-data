package ru.citeck.ecos.data.sql.pg.records

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import ru.citeck.ecos.commons.data.MLText
import ru.citeck.ecos.data.sql.pg.records.commons.DbRecordsTestBase
import ru.citeck.ecos.model.lib.type.dto.TypeInfo
import ru.citeck.ecos.model.lib.utils.ModelUtils
import ru.citeck.ecos.webapp.api.entity.EntityRef
import java.util.*

class DbRecordsEmptyRecordTest : DbRecordsTestBase() {

    @Test
    fun test() {
        val name = "TestName"
        val testTypeRef = ModelUtils.getTypeRef("some-type")
        registerType(
            TypeInfo.create {
                withId(testTypeRef.getLocalId())
                withName(MLText(Locale.ENGLISH to name))
            }
        )
        createRecordsDao(typeRef = testTypeRef)

        val emptyRef = EntityRef.create(recordsDao.getId(), "")
        val emptyRecDisp = records.getAtt(emptyRef, "?disp").asText()

        assertThat(emptyRecDisp).isEqualTo(name)
    }
}
