package ru.citeck.ecos.data.sql.pg.records

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import ru.citeck.ecos.data.sql.pg.records.commons.DbRecordsTestBase
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType
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
                    .withId("multiText2")
                    .withMultiple(true)
                    .build(),
                AttributeDef.create()
                    .withId("nonExistentAuth")
                    .withType(AttributeType.AUTHORITY)
                    .withMultiple(true)
                    .build(),
                AttributeDef.create()
                    .withId("bool2")
                    .build()
            )
        )

        fun assertNotExistentAtt(att: String) {
            queryTest(Predicates.empty(att), rec0)
            queryTest(Predicates.eq(att, null), rec0)
            queryTest(Predicates.notEmpty(att))
            queryTest(Predicates.notEq(att, null))
        }
        assertNotExistentAtt("text2")
        assertNotExistentAtt("multiText2")
        assertNotExistentAtt("bool2")
        assertNotExistentAtt("nonExistentAuth")
    }
}
