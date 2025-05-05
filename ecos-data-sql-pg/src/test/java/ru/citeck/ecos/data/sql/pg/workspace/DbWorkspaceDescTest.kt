package ru.citeck.ecos.data.sql.pg.workspace

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource
import ru.citeck.ecos.data.sql.records.workspace.DbWorkspaceDesc
import ru.citeck.ecos.webapp.api.entity.EntityRef

class DbWorkspaceDescTest {

    @ParameterizedTest
    @CsvSource(
        "emodel/workspace@abc, emodel/workspace@abc",
        "emodel/workspace@, emodel/workspace@",
        "aaa@bbb, emodel/workspace@bbb",
        "aaa, emodel/workspace@aaa",
    )
    fun workspaceDescGetRefTest(wsId: String, expected: String) {
        assertThat(DbWorkspaceDesc.getRef(wsId)).isEqualTo(EntityRef.valueOf(expected))
    }
}
