package ru.citeck.ecos.data.sql.pg.records

import com.github.javafaker.Faker
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import ru.citeck.ecos.commons.data.DataValue
import ru.citeck.ecos.commons.data.ObjectData
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType
import ru.citeck.ecos.records2.RecordRef
import ru.citeck.ecos.records2.predicate.model.Predicate
import ru.citeck.ecos.records2.predicate.model.Predicates
import ru.citeck.ecos.records2.predicate.model.ValuePredicate
import ru.citeck.ecos.records3.record.dao.atts.RecordAttsDao
import ru.citeck.ecos.records3.record.dao.query.dto.query.RecordsQuery
import ru.citeck.ecos.webapp.api.entity.EntityRef
import ru.citeck.ecos.webapp.api.entity.toEntityRef
import java.net.URLEncoder
import java.util.*

class DbRecordsAssocTest : DbRecordsTestBase() {

    @Test
    fun test() {

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("textAtt")
                    withType(AttributeType.TEXT)
                },
                AttributeDef.create {
                    withId("assocAtt")
                    withType(AttributeType.ASSOC)
                },
                AttributeDef.create {
                    withId("multiAssocAtt")
                    withType(AttributeType.ASSOC)
                    withMultiple(true)
                }
            )
        )

        val txtValue = "txt-value"

        val rec0 = createRecord("textAtt" to txtValue)
        val rec1 = createRecord("assocAtt" to rec0)

        assertThat(records.getAtt(rec1, "assocAtt?id").asText()).isEqualTo(rec0.toString())
        assertThat(records.getAtt(rec1, "assocAtt.textAtt").asText()).isEqualTo(txtValue)

        val execQuery = { condition: Predicate ->
            records.query(
                baseQuery.copy {
                    withQuery(
                        Predicates.and(
                            condition,
                            Predicates.eq("_type", REC_TEST_TYPE_REF)
                        )
                    )
                }
            )
        }

        val queryRes = execQuery(Predicates.eq("assocAtt", rec0))
        assertThat(queryRes.getRecords()).hasSize(1)
        assertThat(queryRes.getRecords()).containsExactly(rec1)

        assertThat(execQuery(Predicates.eq("assocAtt", rec1)).getRecords()).isEmpty()

        val rec2TxtVal = "bbb"
        val rec3TxtVal = "ccc"
        val rec4TxtVal = "aaa"

        val rec2 = createRecord("textAtt" to rec2TxtVal)
        val rec3 = createRecord("textAtt" to rec3TxtVal)
        val rec4 = createRecord("textAtt" to rec4TxtVal)

        updateRecord(rec1, "multiAssocAtt" to listOf(rec2, rec3, rec4))

        val multiAssocRefValue = records.getAtt(rec1, "multiAssocAtt[]?id").asStrList()
        assertThat(multiAssocRefValue).containsExactly(rec2.toString(), rec3.toString(), rec4.toString())

        val multiAssocTxtValue = records.getAtt(rec1, "multiAssocAtt[].textAtt").asStrList()
        assertThat(multiAssocTxtValue).containsExactly(rec2TxtVal, rec3TxtVal, rec4TxtVal)

        val extIdRef = EntityRef.create("ext-src-id", "ext-record-id")
        val extRefRecValue = ObjectData.create("""{"aa":"bb"}""")
        records.register(object : RecordAttsDao {
            override fun getId() = extIdRef.getSourceId()
            override fun getRecordAtts(recordId: String): Any? {
                if (recordId == extIdRef.getLocalId()) {
                    return extRefRecValue
                }
                return null
            }
        })

        val rec5 = createRecord("assocAtt" to extIdRef)
        assertThat(records.getAtt(rec5, "assocAtt.aa").asText()).isEqualTo("bb")

        val rec6 = createRecord()
        updateRecord(rec6, "assocAtt" to rec5)
        assertThat(records.getAtt(rec6, "assocAtt?id").toEntityRef()).isEqualTo(rec5)
        updateRecord(rec6, "assocAtt" to rec4)
        assertThat(records.getAtt(rec6, "assocAtt?id").toEntityRef()).isEqualTo(rec4)

        // printQueryRes("SELECT * FROM ${tableRef.withTable("ecos_record_ref").fullName}")
        // printQueryRes("SELECT * FROM ${tableRef.fullName}")
    }

    @Test
    fun testWithAlfRecords() {

        mainCtx = createRecordsDao()

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("assoc")
                    withType(AttributeType.ASSOC)
                }
            )
        )

        val testWithRef = { ref: String, alfresco: Boolean ->

            val ref0 = createRecord("assoc" to ref)
            val ref1 = createRecord("assoc" to ref)

            val recRef = if (alfresco) {
                EntityRef.create("alfresco", "", ref).toString()
            } else {
                EntityRef.create(APP_NAME, "", ref).toString()
            }

            assertThat(records.getAtt(ref0, "assoc?id").asText()).isEqualTo(recRef)
            assertThat(records.getAtt(ref1, "assoc?id").asText()).isEqualTo(recRef)
        }

        testWithRef("workspace://SpacesStore/123123", true)
        testWithRef("1234567890", false)
    }

    @Test
    fun searchTest() {

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("assocAtt")
                    withType(AttributeType.ASSOC)
                },
                AttributeDef.create {
                    withId("multiAssocAtt")
                    withType(AttributeType.ASSOC)
                    withMultiple(true)
                }
            )
        )

        val refs = Array(5) { "some/assoc@value-$it" }

        val record = createRecord(
            "assocAtt" to refs[0],
            "multiAssocAtt" to refs.toList().take(3)
        )

        val queryTest = { query: Predicate, expected: List<RecordRef> ->
            val result = records.query(
                RecordsQuery.create {
                    withSourceId(mainCtx.dao.getId())
                    withQuery(
                        Predicates.and(
                            Predicates.eq("_type", REC_TEST_TYPE_REF),
                            query
                        )
                    )
                }
            )
            assertThat(result.getRecords()).describedAs(query.toString()).hasSize(expected.size)
            assertThat(result.getRecords()).describedAs(query.toString()).containsExactlyElementsOf(expected)
        }

        listOf(ValuePredicate.Type.EQ, ValuePredicate.Type.CONTAINS).forEach { predType ->

            queryTest(ValuePredicate("assocAtt", predType, refs[0]), listOf(record))
            queryTest(ValuePredicate("multiAssocAtt", predType, refs[0]), listOf(record))
            queryTest(ValuePredicate("multiAssocAtt", predType, refs[1]), listOf(record))

            queryTest(ValuePredicate("assocAtt", predType, refs[3]), listOf())
            queryTest(ValuePredicate("multiAssocAtt", predType, refs[3]), listOf())

            queryTest(ValuePredicate("multiAssocAtt", predType, listOf(refs[3], refs[4])), listOf())
            queryTest(ValuePredicate("multiAssocAtt", predType, listOf(refs[3], refs[0])), listOf(record))
        }
    }

    @Test
    fun assocAsContentTest() {

        registerType(
            """
            ---
            id: $REC_TEST_TYPE_ID
            model:
              attributes:
                  - id: assoc
                    type: ASSOC
                    multiple: true
                    config:
                      child: true
                      typeRef: emodel/type@user-base
            """.trimIndent()
        )

        registerType(
            """
            ---
            id: user-base
            """.trimIndent()
        )

        registerType(
            """
            ---
            id: document
            parentRef: emodel/type@user-base
            model:
              attributes:
                  - id: name
                  - id: content
                    type: CONTENT
            """.trimIndent()
        )

        val contentData = Faker.instance().chuckNorris().fact()
        val contentDataBase64 = Base64.getEncoder().encodeToString(contentData.toByteArray())

        val contentAttValue = DataValue.create(
            """
            [
              {
                "storage": "base64",
                "name": "Assoc test.-ad3e182c-3aac-4d7d-b145-880dae799698.txt",
                "url": "data:text/plain;base64,$contentDataBase64",
                "size": ${contentData.toByteArray().size},
                "type": "text/plain",
                "fileType": "document",
                "originalName": "Assoc test.txt"
              }
            ]
            """.trimIndent()
        )

        val record = createRecord("assoc" to contentAttValue)

        val assocRef = records.getAtt(record, "assoc?id").toEntityRef()
        assertThat(assocRef.isEmpty()).isFalse
        val parentRef = records.getAtt(assocRef, "_parent?id").toEntityRef()
        assertThat(parentRef.getLocalId()).isEqualTo(record.getLocalId())
        val parentAtt = records.getAtt(assocRef, "_parentAtt").asText()
        assertThat(parentAtt).isEqualTo("assoc")

        val assocContentBase64 = records.getAtt(record, "assoc._content.bytes").asText()
        val assocContent = String(Base64.getDecoder().decode(assocContentBase64))
        assertThat(assocContent).isEqualTo(contentData)

        val assocAsContentData = records.getAtt(record, "assoc[]._as.content-data?json").asList(DataValue::class.java)

        val refArg = "ref=${URLEncoder.encode(assocRef.withoutAppName().toString(), "UTF-8")}"
        assertThat(assocAsContentData.size).isEqualTo(1)
        assertThat(assocAsContentData[0]["url"].asText()).contains(refArg)

        val newAssocContent = Faker.instance().chuckNorris().fact()
        val newAssocContentBase64 = Base64.getEncoder().encodeToString(newAssocContent.toByteArray())

        val currentContentValue = records.getAtt(record, "assoc[]._as.content-data?json")
        currentContentValue.add(
            DataValue.of(
                """
              {
                "storage": "base64",
                "name": "Assoc test2.-ad3e182c-3aac-4d7d-b145-880dae799698.txt",
                "url": "data:text/plain;base64,$newAssocContentBase64",
                "size": ${newAssocContent.toByteArray().size},
                "type": "text/plain",
                "fileType": "document",
                "originalName": "Assoc test2.txt"
              }
                """.trimIndent()
            )
        )
        records.mutateAtt(record, "assoc", currentContentValue)

        val assocsRefs = records.getAtt(record, "assoc[]?id").asList(EntityRef::class.java)
        assertThat(assocsRefs).hasSize(2)
        assertThat(assocsRefs[0]).isEqualTo(assocRef)
    }
}
