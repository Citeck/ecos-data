package ru.citeck.ecos.data.sql.pg.records

import org.assertj.core.api.Assertions
import org.junit.jupiter.api.Test
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.records2.predicate.model.Predicates

class DbRecordsParentTest : DbRecordsTestBase() {

    @Test
    fun test() {

        registerAtts(
            listOf(
                AttributeDef.create { withId("textAtt") }
            )
        )

        val ref = createRecord(
            "_parent" to "alfresco/@abc"
        )

        val result = records.query(baseQuery.copy()
            .withQuery(
                Predicates.eq("_parent", "alfresco/@abc")
            ).build()
        )
        Assertions.assertThat(result.getRecords()).hasSize(1)
        Assertions.assertThat(result.getRecords()[0]).isEqualTo(ref)
    }
}
