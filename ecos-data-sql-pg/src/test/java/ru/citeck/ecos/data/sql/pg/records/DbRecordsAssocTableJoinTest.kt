package ru.citeck.ecos.data.sql.pg.records

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import ru.citeck.ecos.commons.data.ObjectData
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType
import ru.citeck.ecos.model.lib.type.dto.TypeInfo
import ru.citeck.ecos.model.lib.type.dto.TypeModelDef
import ru.citeck.ecos.model.lib.utils.ModelUtils
import ru.citeck.ecos.records2.RecordRef
import ru.citeck.ecos.records2.predicate.model.Predicate
import ru.citeck.ecos.records2.predicate.model.Predicates
import ru.citeck.ecos.records3.record.dao.query.dto.query.SortBy
import ru.citeck.ecos.webapp.api.entity.EntityRef

class DbRecordsAssocTableJoinTest : DbRecordsTestBase() {

    private lateinit var targetDao: RecordsDaoTestCtx
    private lateinit var childDao: RecordsDaoTestCtx

    @Test
    fun testWithMultipleAssoc() {

        val targetRec0 = targetDao.createRecord("targetText" to "abc", "targetNum" to 10)
        val targetRec1 = targetDao.createRecord("targetText" to "def", "targetNum" to 100)

        val targetRec2 = targetDao.createRecord("targetText" to "hij", "targetNum" to 5)
        val targetRec3 = targetDao.createRecord("targetText" to "klm", "targetNum" to 50)

        val record0 = createRecord("multiAssocAtt" to listOf(targetRec0, targetRec1))
        val record1 = createRecord("multiAssocAtt" to listOf(targetRec2, targetRec3))

        val queryRes0 = records.query(baseQuery.copy{
            withQuery(Predicates.eq("multiAssocAtt.targetText", "klm" ))
        }).getRecords()

        assertThat(queryRes0).containsExactly(record1)

        val queryRes1 = records.query(baseQuery.copy{
            withQuery(Predicates.inVals("multiAssocAtt.targetText", listOf("klm", "abc") ))
        }).getRecords()

        assertThat(queryRes1).containsExactlyInAnyOrder(record0, record1)
    }

    @Test
    fun groupByParentTest() {

        val targetRec0 = targetDao.createRecord("targetText" to "abc", "targetNum" to 10)
        val targetRec1 = targetDao.createRecord("targetText" to "def", "targetNum" to 100)

        val srcRec0 = createRecord("assocAtt" to targetRec0)
        val srcRec1 = createRecord("assocAtt" to targetRec1)

        childDao.createRecord("_parent" to srcRec0, "_parentAtt" to "childAssocAtt")
        childDao.createRecord("_parent" to srcRec1, "_parentAtt" to "childAssocAtt")
        childDao.createRecord("_parent" to srcRec1, "_parentAtt" to "childAssocAtt")

        val recsResult = records.query(
            baseQuery.copy {
                withSourceId(childDao.dao.getId())
                withQuery(Predicates.eq("_parent._type", REC_TEST_TYPE_REF))
                withSortBy(listOf(SortBy("_parent.assocAtt", true)))
                withGroupBy(listOf("_parent.assocAtt"))
            },
            mapOf(
                "assocAtt" to "_parent.assocAtt?id",
                "count" to "count(*)"
            )
        ).getRecords()

        assertThat(recsResult).hasSize(2)
        assertThat(recsResult.map { it["assocAtt"].asText() }).containsExactly(targetRec0.toString(), targetRec1.toString())
        assertThat(recsResult.map { it["count"].asInt() }).containsExactly(1, 2)
    }

    @Test
    fun test() {

        val targetRec0 = targetDao.createRecord("targetText" to "abc", "targetNum" to 10)
        val targetRec1 = targetDao.createRecord("targetText" to "def", "targetNum" to 100)

        val srcRec0 = createRecord("assocAtt" to targetRec0)
        val srcRec1 = createRecord("assocAtt" to targetRec1)

        fun queryTest(predicate: Predicate, expected: List<EntityRef>) {
            val result = records.query(
                baseQuery.copy {
                    withQuery(predicate)
                }
            ).getRecords()
            assertThat(result).describedAs("Predicate: $predicate")
                .containsExactlyInAnyOrderElementsOf(expected.map { RecordRef.valueOf(it) })
        }

        queryTest(Predicates.eq("assocAtt.targetText", "abc"), listOf(srcRec0))
        queryTest(Predicates.eq("assocAtt.targetText", "def"), listOf(srcRec1))

        queryTest(Predicates.not(Predicates.eq("assocAtt.targetText", "def")), listOf(srcRec0))
        queryTest(Predicates.not(Predicates.contains("assocAtt.targetText", "de")), listOf(srcRec0))
        queryTest(Predicates.contains("assocAtt.targetText", "de"), listOf(srcRec1))

        fun sortByTest(ascending: Boolean) {
            val result = records.query(
                baseQuery.copy {
                    withSortBy(SortBy("assocAtt.targetNum", ascending))
                }
            ).getRecords()
            val expected = if (ascending) {
                listOf(srcRec0, srcRec1)
            } else {
                listOf(srcRec1, srcRec0)
            }
            assertThat(result).describedAs("Ascending: $ascending")
                .containsExactlyElementsOf(expected.map { RecordRef.valueOf(it) })
        }
        sortByTest(true)
        sortByTest(false)

        val result = records.query(
            baseQuery.copy {
                withGroupBy(listOf("assocAtt.targetText"))
                withSortBy(listOf(SortBy("assocAtt.targetText", true)))
            },
            mapOf(
                "text" to "assocAtt.targetText",
                "count" to "count(*)"
            )
        ).getRecords()

        assertThat(result).hasSize(2)
        assertThat(result[0]["text"].asText()).isEqualTo("abc")
        assertThat(result[0]["count"].asInt()).isEqualTo(1)
        assertThat(result[1]["text"].asText()).isEqualTo("def")
        assertThat(result[1]["count"].asInt()).isEqualTo(1)
    }

    @BeforeEach
    fun prepare() {

        val targetTypeRef = ModelUtils.getTypeRef("target-type")
        val childTypeRef = ModelUtils.getTypeRef("child-type")

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("textAtt")
                },
                AttributeDef.create {
                    withId("assocAtt")
                    withType(AttributeType.ASSOC)
                    withConfig(ObjectData.create().set("typeRef", targetTypeRef.toString()))
                },
                AttributeDef.create {
                    withId("multiAssocAtt")
                    withType(AttributeType.ASSOC)
                    withMultiple(true)
                    withConfig(ObjectData.create().set("typeRef", targetTypeRef.toString()))
                },
                AttributeDef.create {
                    withId("childAssocAtt")
                    withType(AttributeType.ASSOC)
                    withConfig(
                        ObjectData.create()
                            .set("typeRef", childTypeRef.toString())
                            .set("child", true)
                    )
                }
            )
        )

        targetDao = createRecordsDao(
            DEFAULT_TABLE_REF.withTable(targetTypeRef.getLocalId()),
            targetTypeRef,
            targetTypeRef.getLocalId()
        )

        registerType(
            TypeInfo.create {
                withId(targetTypeRef.getLocalId())
                withSourceId(targetTypeRef.getLocalId())
                withModel(
                    TypeModelDef.create()
                        .withAttributes(
                            listOf(
                                AttributeDef.create()
                                    .withId("targetText")
                                    .build(),
                                AttributeDef.create()
                                    .withId("targetNum")
                                    .withType(AttributeType.NUMBER)
                                    .build()
                            )
                        )
                        .build()
                )
            }
        )

        childDao = createRecordsDao(
            DEFAULT_TABLE_REF.withTable(childTypeRef.getLocalId()),
            childTypeRef,
            childTypeRef.getLocalId()
        )

        registerType(
            TypeInfo.create {
                withId(childTypeRef.getLocalId())
                withSourceId(childTypeRef.getLocalId())
                withModel(
                    TypeModelDef.create()
                        .withAttributes(
                            listOf(
                                AttributeDef.create()
                                    .withId("childText")
                                    .build(),
                                AttributeDef.create()
                                    .withId("childNum")
                                    .withType(AttributeType.NUMBER)
                                    .build()
                            )
                        )
                        .build()
                )
            }
        )
    }
}
