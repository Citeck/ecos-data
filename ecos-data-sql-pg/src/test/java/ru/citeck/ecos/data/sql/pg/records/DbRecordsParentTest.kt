package ru.citeck.ecos.data.sql.pg.records

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import ru.citeck.ecos.commons.data.DataValue
import ru.citeck.ecos.commons.data.ObjectData
import ru.citeck.ecos.data.sql.pg.records.commons.DbRecordsTestBase
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType
import ru.citeck.ecos.records2.RecordConstants
import ru.citeck.ecos.records2.predicate.model.Predicate
import ru.citeck.ecos.records2.predicate.model.Predicates
import ru.citeck.ecos.webapp.api.entity.EntityRef

class DbRecordsParentTest : DbRecordsTestBase() {

    @Test
    fun compareNonExistentParentAttWithCurrentRecAttTest() {
        registerAtts(
            listOf(
                // AttributeDef.create { withId("text") },
                AttributeDef.create {
                    withId("childAssoc")
                    withType(AttributeType.ASSOC)
                    withConfig(ObjectData.create().set("child", true))
                }
            )
        )

        val rec0 = createRecord("text" to "")
        val rec1 = createRecord(
            "text" to "",
            "_parent" to rec0,
            "_parentAtt" to "childAssoc"
        )

        val query = baseQuery.copy()
            .withSortBy(emptyList())
            .withQuery(
                Predicates.and(
                    Predicates.eq("_parent._type", REC_TEST_TYPE_REF),
                    Predicates.or(
                        Predicates.eq("(_parent.text = text)", false),
                        Predicates.and(
                            Predicates.notEmpty(RecordConstants.ATT_PARENT),
                            Predicates.empty("unknownAtt")
                        )
                    )
                )
            ).build()

        assertThat(records.query(query).getRecords()).containsExactly(rec1)
    }

    @ParameterizedTest
    @ValueSource(strings = ["text", "att-with-special"])
    fun compareParentAttWithCurrentRecAttTest(textAttId: String) {

        registerAtts(
            listOf(
                AttributeDef.create { withId(textAttId) },
                AttributeDef.create {
                    withId("childAssoc")
                    withType(AttributeType.ASSOC)
                    withConfig(ObjectData.create().set("child", true))
                }
            )
        )

        val rec0 = createRecord(textAttId to "abc")
        val rec1 = createRecord(textAttId to "abc", "_parent" to rec0, "_parentAtt" to "childAssoc")
        val rec2 = createRecord(textAttId to "abc", "_parent" to rec0, "_parentAtt" to "childAssoc")
        val rec3 = createRecord(textAttId to "def", "_parent" to rec0, "_parentAtt" to "childAssoc")
        val rec4 = createRecord(textAttId to "hij", "_parent" to rec0, "_parentAtt" to "childAssoc")
        val rec5 = createRecord("_parent" to rec0, "_parentAtt" to "childAssoc")

        fun assertParentQuery(query: Predicate, vararg expected: EntityRef) {
            val records = records.query(
                baseQuery.copy()
                    .withSortBy(emptyList())
                    .withQuery(
                        Predicates.and(
                            Predicates.eq("_parent._type", REC_TEST_TYPE_REF),
                            query
                        )
                    ).build()
            ).getRecords()
            assertThat(records).containsExactlyInAnyOrderElementsOf(expected.asList())
        }
        assertParentQuery(Predicates.eq("(_parent.\"$textAttId\" = \"$textAttId\")", true), rec1, rec2)
        assertParentQuery(Predicates.eq("(_parent.\"$textAttId\" = coalesce(\"$textAttId\", ''))", true), rec1, rec2)
        assertParentQuery(Predicates.eq("(_parent.\"$textAttId\" <> coalesce(\"$textAttId\", ''))", true), rec3, rec4, rec5)
        assertParentQuery(Predicates.eq("(_parent.\"$textAttId\" = coalesce(\"$textAttId\", ''))", false), rec3, rec4, rec5)
        assertParentQuery(Predicates.eq("(_parent.\"$textAttId\" = \"$textAttId\")", false), rec3, rec4)
        assertParentQuery(Predicates.eq("(_parent.\"$textAttId\" <> \"$textAttId\")", false), rec1, rec2)
    }

    @Test
    fun test() {

        registerAtts(
            listOf(
                AttributeDef.create { withId("textAtt") },
                AttributeDef.create {
                    withId("childAssoc")
                    withType(AttributeType.ASSOC)
                    withConfig(ObjectData.create().set("child", true))
                }
            )
        )

        val ref = createRecord(
            "_parent" to "alfresco/@abc",
            "_parentAtt" to "childAssoc"
        )

        fun queryTest() {
            val result = records.query(
                baseQuery.copy()
                    .withQuery(
                        Predicates.eq("_parent", "alfresco/@abc")
                    ).build()
            )
            assertThat(result.getRecords()).hasSize(1)
            assertThat(result.getRecords()[0]).isEqualTo(ref)
        }
        queryTest()
        assocsService.createTableIfNotExists()
        queryTest()
    }

    @Test
    fun recursiveParentTest() {

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("childAssoc")
                    withType(AttributeType.ASSOC)
                    withConfig(ObjectData.create().set("child", true))
                }
            )
        )

        val ref0 = createRecord()
        val ref1 = createRecord(
            "_parent" to ref0,
            "_parentAtt" to "childAssoc"
        )
        val ref2 = createRecord(
            "_parent" to ref1,
            "_parentAtt" to "childAssoc"
        )
        val exception = assertThrows<RuntimeException> {
            records.mutate(
                ref0,
                DataValue.createObj()
                    .set("_parent", ref2)
                    .set("_parentAtt", "childAssoc")
            )
        }
        assertThat(exception.message).containsIgnoringCase("Recursive")
        println(exception.message)
    }
}
