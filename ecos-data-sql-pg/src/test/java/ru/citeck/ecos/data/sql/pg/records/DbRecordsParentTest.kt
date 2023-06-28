package ru.citeck.ecos.data.sql.pg.records

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import ru.citeck.ecos.commons.data.DataValue
import ru.citeck.ecos.commons.data.ObjectData
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType
import ru.citeck.ecos.records2.predicate.model.Predicates

class DbRecordsParentTest : DbRecordsTestBase() {

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
