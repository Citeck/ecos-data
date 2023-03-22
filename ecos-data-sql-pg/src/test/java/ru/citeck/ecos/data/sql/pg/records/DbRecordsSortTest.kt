package ru.citeck.ecos.data.sql.pg.records

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import ru.citeck.ecos.context.lib.auth.AuthContext
import ru.citeck.ecos.context.lib.auth.AuthGroup
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.type.dto.QueryPermsPolicy
import ru.citeck.ecos.records3.record.dao.query.dto.query.SortBy

class DbRecordsSortTest : DbRecordsTestBase() {

    @Test
    fun test() {

        registerAtts(
            listOf(
                AttributeDef.create()
                    .withId("test")
                    .build()
            )
        )
        setQueryPermsPolicy(QueryPermsPolicy.OWN)

        AuthContext.runAs("admin", listOf(AuthGroup.EVERYONE)) {

            val ref0 = createRecord("test" to "abc")
            Thread.sleep(100)
            val ref1 = createRecord("test" to "def")

            val refsFromQueryAsc = records.query(
                baseQuery.copy().withSortBy(
                    SortBy("_created", true)
                ).build()
            )

            assertThat(refsFromQueryAsc.getRecords()).hasSize(2)
            assertThat(refsFromQueryAsc.getRecords()[0]).isEqualTo(ref0)
            assertThat(refsFromQueryAsc.getRecords()[1]).isEqualTo(ref1)

            val refsFromQueryDesc = records.query(
                baseQuery.copy().withSortBy(
                    SortBy("_created", false)
                ).build()
            )

            assertThat(refsFromQueryDesc.getRecords()).hasSize(2)
            assertThat(refsFromQueryDesc.getRecords()[0]).isEqualTo(ref1)
            assertThat(refsFromQueryDesc.getRecords()[1]).isEqualTo(ref0)
        }
    }
}
