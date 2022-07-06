package ru.citeck.ecos.data.sql.pg.records

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import ru.citeck.ecos.commons.data.MLText
import ru.citeck.ecos.model.lib.num.dto.NumTemplateDef
import ru.citeck.ecos.model.lib.type.dto.TypeInfo
import ru.citeck.ecos.records2.RecordRef

class DbRecordsDisplayNameTest : DbRecordsTestBase() {

    @Test
    fun test() {

        registerType(TypeInfo.create {
            withId(REC_TEST_TYPE_ID)
            withNumTemplateRef(RecordRef.create("num-template", "test-template"))
            withDispNameTemplate(MLText(
                "App №\${_docNum}"
            ))
        })

        registerNumTemplate(NumTemplateDef("test-template", "", "123", emptyList()))

        assertThat(records.getAtt(createRecord(), "?disp").asText()).isEqualTo("App №1")
        assertThat(records.getAtt(createRecord(), "?disp").asText()).isEqualTo("App №2")
        assertThat(records.getAtt(createRecord(), "?disp").asText()).isEqualTo("App №3")
    }
}
