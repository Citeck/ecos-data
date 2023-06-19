package ru.citeck.ecos.data.sql.pg.records

import org.assertj.core.api.Assertions.assertThat
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
import ru.citeck.ecos.webapp.api.entity.EntityRef

class DbRecordsAssocJoinTest : DbRecordsTestBase() {

    @Test
    fun test() {

        val targetTypeRef = ModelUtils.getTypeRef("target-type")

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("textAtt")
                },
                AttributeDef.create {
                    withId("assocAtt")
                    withType(AttributeType.ASSOC)
                    withConfig(ObjectData.create().set("typeRef", targetTypeRef.toString()))
                }
            )
        )

        val targetDao = createRecordsDao(
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
                                    .build()
                            )
                        )
                        .build()
                )
            }
        )

        val targetRec0 = targetDao.createRecord("targetText" to "abc")
        val targetRec1 = targetDao.createRecord("targetText" to "def")

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
    }
}
