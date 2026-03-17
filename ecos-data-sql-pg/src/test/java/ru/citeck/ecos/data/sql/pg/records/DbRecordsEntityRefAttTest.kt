package ru.citeck.ecos.data.sql.pg.records

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import ru.citeck.ecos.data.sql.pg.records.commons.DbRecordsTestBase
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType
import ru.citeck.ecos.records2.predicate.model.Predicates
import ru.citeck.ecos.records3.record.dao.atts.RecordAttsDao
import ru.citeck.ecos.webapp.api.entity.EntityRef

/**
 * Tests for AttributeType.ENTITY_REF - stores EntityRefs as LONG IDs
 * directly in the record table column (no ed_associations overhead).
 */
class DbRecordsEntityRefAttTest : DbRecordsTestBase() {

    @Test
    fun singleEntityRefCrud() {

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("refAtt")
                    withType(AttributeType.ENTITY_REF)
                },
                AttributeDef.create {
                    withId("textAtt")
                    withType(AttributeType.TEXT)
                }
            )
        )

        val targetRef = EntityRef.create("some-src", "target-1")

        val rec = createRecord("refAtt" to targetRef, "textAtt" to "hello")

        // read back
        assertThat(records.getAtt(rec, "refAtt?localId").asText()).isEqualTo("target-1")

        // update
        val newTarget = EntityRef.create("some-src", "target-2")
        updateRecord(rec, "refAtt" to newTarget)

        assertThat(records.getAtt(rec, "refAtt?localId").asText()).isEqualTo("target-2")

        // clear
        updateRecord(rec, "refAtt" to null)
        assertThat(records.getAtt(rec, "refAtt?localId").asText()).isEqualTo("")
    }

    @Test
    fun multipleEntityRefCrud() {

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("refs")
                    withType(AttributeType.ENTITY_REF)
                    withMultiple(true)
                }
            )
        )

        val ref1 = EntityRef.create("src", "a")
        val ref2 = EntityRef.create("src", "b")
        val ref3 = EntityRef.create("src", "c")

        val rec = createRecord("refs" to listOf(ref1, ref2))
        val readRefs = records.getAtt(rec, "refs[]?localId").asStrList()
        assertThat(readRefs).containsExactly("a", "b")

        // update: replace list
        updateRecord(rec, "refs" to listOf(ref2, ref3))
        val updatedRefs = records.getAtt(rec, "refs[]?localId").asStrList()
        assertThat(updatedRefs).containsExactly("b", "c")
    }

    @Test
    fun entityRefAndAssocCoexist() {

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("entityRefAtt")
                    withType(AttributeType.ENTITY_REF)
                    withMultiple(true)
                },
                AttributeDef.create {
                    withId("assocAtt")
                    withType(AttributeType.ASSOC)
                    withMultiple(true)
                }
            )
        )

        val ref1 = EntityRef.create("src", "r1")
        val ref2 = EntityRef.create("src", "r2")

        val rec = createRecord(
            "entityRefAtt" to listOf(ref1, ref2),
            "assocAtt" to listOf(ref1)
        )

        val entityRefVals = records.getAtt(rec, "entityRefAtt[]?localId").asStrList()
        assertThat(entityRefVals).containsExactly("r1", "r2")

        val assocVals = records.getAtt(rec, "assocAtt[]?localId").asStrList()
        assertThat(assocVals).containsExactly("r1")
    }

    @Test
    fun queryByEntityRefAttribute() {

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("refAtt")
                    withType(AttributeType.ENTITY_REF)
                },
                AttributeDef.create {
                    withId("text")
                    withType(AttributeType.TEXT)
                }
            )
        )

        val target1 = EntityRef.create("src", "target-1")
        val target2 = EntityRef.create("src", "target-2")

        val rec0 = createRecord("refAtt" to target1, "text" to "rec0")
        val rec1 = createRecord("refAtt" to target2, "text" to "rec1")
        val rec2 = createRecord("refAtt" to null, "text" to "rec2")

        // query by EQ
        val eqResult = records.query(
            baseQuery.copy {
                withQuery(Predicates.eq("refAtt", target1.toString()))
            }
        ).getRecords()
        assertThat(eqResult).containsExactly(rec0)

        // query by IN
        val inResult = records.query(
            baseQuery.copy {
                withQuery(Predicates.inVals("refAtt", listOf(target1.toString(), target2.toString())))
            }
        ).getRecords()
        assertThat(inResult).containsExactlyInAnyOrder(rec0, rec1)

        // query EMPTY
        val emptyResult = records.query(
            baseQuery.copy {
                withQuery(Predicates.empty("refAtt"))
            }
        ).getRecords()
        assertThat(emptyResult).containsExactly(rec2)
    }

    @Test
    fun entityRefInnerAttributeResolution() {

        records.register(object : RecordAttsDao {
            override fun getId() = "ext-src"
            override fun getRecordAtts(recordId: String): Any {
                return mapOf("name" to "ext-$recordId")
            }
        })

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("refAtt")
                    withType(AttributeType.ENTITY_REF)
                }
            )
        )

        val targetRef = EntityRef.create("ext-src", "rec-42")
        val rec = createRecord("refAtt" to targetRef)

        // inner attribute resolution through ref
        val name = records.getAtt(rec, "refAtt.name").asText()
        assertThat(name).isEqualTo("ext-rec-42")
    }

    @Test
    fun queryByMultipleEntityRef() {

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("refs")
                    withType(AttributeType.ENTITY_REF)
                    withMultiple(true)
                }
            )
        )

        val refA = EntityRef.create("src", "a")
        val refB = EntityRef.create("src", "b")
        val refC = EntityRef.create("src", "c")

        val rec0 = createRecord("refs" to listOf(refA, refB))
        val rec1 = createRecord("refs" to listOf(refB, refC))
        createRecord("refs" to listOf(refC))

        // query by CONTAINS on multiple entity ref
        val result = records.query(
            baseQuery.copy {
                withQuery(Predicates.contains("refs", refB.toString()))
            }
        ).getRecords()
        assertThat(result).containsExactlyInAnyOrder(rec0, rec1)
    }
}
