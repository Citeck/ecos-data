package ru.citeck.ecos.data.sql.pg.records

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import ru.citeck.ecos.commons.data.MLText
import ru.citeck.ecos.data.sql.pg.records.commons.DbRecordsTestBase
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType
import ru.citeck.ecos.model.lib.type.dto.TypeInfo
import ru.citeck.ecos.model.lib.type.dto.TypeModelDef
import ru.citeck.ecos.records2.predicate.model.Predicates
import java.util.*

class DbRecordsNameTest : DbRecordsTestBase() {

    @Test
    fun testWithoutCustomNameWith() {

        registerType(
            TypeInfo.create {
                withId(REC_TEST_TYPE_ID)
            }
        )

        val newRecRef = createRecord()
        val dispAtt = records.getAtt(newRecRef, "_disp")
        val nameAtt = records.getAtt(newRecRef, "_name")

        assertThat(dispAtt.asText())
            .isEqualTo(nameAtt.asText())
            .isEqualTo(REC_TEST_TYPE_ID)

        val customNameAtt = records.getAtt(newRecRef, "name")
        assertThat(customNameAtt.asText()).isEqualTo("")

        val mlName = MLText(
            Locale.ENGLISH to "en-name",
            Locale.FRANCE to "fr-name"
        )

        registerType(
            TypeInfo.create {
                withId(REC_TEST_TYPE_ID)
                withName(mlName)
            }
        )

        updateRecord(newRecRef)

        val checkDispIsTypeName = {
            val dispAtt2 = records.getAtt(newRecRef, "_disp?json")
            val nameAtt2 = records.getAtt(newRecRef, "_name?json")

            assertThat(dispAtt2.getAs(MLText::class.java))
                .isEqualTo(nameAtt2.getAs(MLText::class.java))
                .isEqualTo(mlName)
        }
        checkDispIsTypeName()
        updateRecord(newRecRef, "_name" to "123")
        checkDispIsTypeName()
        updateRecord(newRecRef, "_disp" to "123")
        checkDispIsTypeName()

        registerType(
            TypeInfo.create {
                withId(REC_TEST_TYPE_ID)
                withName(mlName)
                withModel(
                    TypeModelDef.create {
                        withAttributes(
                            listOf(
                                AttributeDef.create {
                                    withId("name")
                                    withType(AttributeType.TEXT)
                                }
                            )
                        )
                    }
                )
            }
        )

        val mutateNameCheck = { att: String, value: String ->

            updateRecord(newRecRef, att to value)

            val dispAtt3 = records.getAtt(newRecRef, "_disp")
            val nameAtt3 = records.getAtt(newRecRef, "_name")
            val customNameAtt3 = records.getAtt(newRecRef, "name")

            assertThat(dispAtt3.asText())
                .isEqualTo(nameAtt3.asText())
                .isEqualTo(customNameAtt3.asText())
                .isEqualTo(value)
        }

        mutateNameCheck("_disp", "abc")
        mutateNameCheck("_name", "def")
        mutateNameCheck("name", "ghi")

        val nameToSearch = "custom-name"
        updateRecord(newRecRef, "_name" to nameToSearch)

        listOf("_disp", "_name", "name").forEach { att ->

            listOf(Predicates.contains(att, nameToSearch), Predicates.eq(att, nameToSearch)).forEach { predicate ->

                val res = records.query(baseQuery.copy { withQuery(predicate) })
                assertThat(res.getRecords()).hasSize(1)
                assertThat(res.getRecords()[0]).isEqualTo(newRecRef)

                val res2 = records.queryOne(baseQuery.copy { withQuery(predicate) }, listOf("name"))
                assertThat(res2?.getAtt("name")?.asText()).isEqualTo(nameToSearch)
            }
        }

        createRecord("_name" to "$nameToSearch-postfix")

        val queryRes = records.query(
            baseQuery.copy {
                withQuery(Predicates.eq("_name", nameToSearch))
            }
        )

        assertThat(queryRes.getRecords()).hasSize(1)
    }
}
