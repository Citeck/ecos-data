package ru.citeck.ecos.data.sql.pg.records

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import ru.citeck.ecos.context.lib.auth.AuthContext
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.records2.RecordConstants
import ru.citeck.ecos.records2.predicate.model.Predicates
import ru.citeck.ecos.webapp.api.constants.AppName
import ru.citeck.ecos.webapp.api.entity.EntityRef

class DbRecordsMetaFieldsTest : DbRecordsTestBase() {

    @Test
    fun test() {

        registerAtts(
            listOf(
                AttributeDef.create { withId("text") }
            )
        )

        val ref = AuthContext.runAsFull("user") {
            createRecord("text" to "abc")
        }

        listOf(RecordConstants.ATT_CREATOR, RecordConstants.ATT_MODIFIER).forEach { metaAtt ->

            val queryRes = records.query(
                baseQuery.copy()
                    .withQuery(
                        Predicates.eq(
                            metaAtt,
                            EntityRef.create(AppName.EMODEL, "person", "user")
                        )
                    )
                    .build()
            ).getRecords()

            assertThat(queryRes).describedAs("att: $metaAtt").containsExactly(ref)
        }
    }
}
