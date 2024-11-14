package ru.citeck.ecos.data.sql.pg.records

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.records2.predicate.model.Predicate
import ru.citeck.ecos.records2.predicate.model.Predicates
import ru.citeck.ecos.webapp.api.entity.EntityRef

class DbRecordsColumnNotExistTest : DbRecordsTestBase() {

    @Test
    fun test() {

        registerAtts(
            listOf(
                AttributeDef.create()
                    .withId("text")
                    .build(),
                AttributeDef.create()
                    .withId("bool")
                    .build()
            )
        )

        val rec0 = createRecord()

        fun queryTest(predicate: Predicate, vararg expected: EntityRef) {

            val recs = records.query(
                baseQuery.copy()
                    .withQuery(predicate)
                    .build()
            ).getRecords()

            assertThat(recs).containsExactlyElementsOf(expected.toList())
        }

        queryTest(Predicates.empty("text"), rec0)
        queryTest(Predicates.eq("text", null), rec0)
        queryTest(Predicates.notEmpty("text"))
        queryTest(Predicates.notEq("text", null))

        queryTest(Predicates.empty("bool"), rec0)
        queryTest(Predicates.eq("bool", null), rec0)
        queryTest(Predicates.notEmpty("bool"))
        queryTest(Predicates.notEq("bool", null))

        registerAtts(
            listOf(
                AttributeDef.create()
                    .withId("text")
                    .build(),
                AttributeDef.create()
                    .withId("bool")
                    .build(),
                AttributeDef.create()
                    .withId("text2")
                    .build(),
                AttributeDef.create()
                    .withId("bool2")
                    .build()
            )
        )

        queryTest(Predicates.empty("text2"), rec0)
        queryTest(Predicates.eq("text2", null), rec0)
        queryTest(Predicates.notEmpty("text2"))
        queryTest(Predicates.notEq("text2", null))

        queryTest(Predicates.empty("bool2"), rec0)
        queryTest(Predicates.eq("bool2", null), rec0)
        queryTest(Predicates.notEmpty("bool2"))
        queryTest(Predicates.notEq("bool2", null))
    }
}
