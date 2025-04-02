package ru.citeck.ecos.data.sql.pg.records

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import ru.citeck.ecos.data.sql.pg.records.commons.DbRecordsTestBase
import ru.citeck.ecos.model.lib.aspect.dto.AspectInfo
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType
import ru.citeck.ecos.model.lib.type.dto.TypeAspectDef
import ru.citeck.ecos.model.lib.type.dto.TypeInfo
import ru.citeck.ecos.model.lib.type.dto.TypeModelDef
import ru.citeck.ecos.model.lib.utils.ModelUtils
import ru.citeck.ecos.records2.predicate.model.ValuePredicate
import ru.citeck.ecos.webapp.api.entity.EntityRef
import ru.citeck.ecos.webapp.api.entity.toEntityRef
import java.util.concurrent.atomic.AtomicInteger

class DbRecordsAspectTest : DbRecordsTestBase() {

    @Test
    fun test() {

        val record = createRecord()

        fun hasAspects(rec: EntityRef, vararg aspects: String): Boolean {
            return aspects.all { records.getAtt(rec, "_aspects._has.$it?bool").asBoolean() }
        }
        fun getAspectsList(rec: EntityRef): List<String> {
            return records.getAtt(rec, "_aspects.list[]?localId").asStrList()
        }

        assertThat(hasAspects(record, "aspect0")).isTrue
        assertThat(hasAspects(record, "aspect1")).isFalse
        assertThat(getAspectsList(record)).containsExactly("aspect0")

        updateRecord(record, "aspect1:text" to "aspect-1-text-value")

        assertThat(hasAspects(record, "aspect0", "aspect1")).isTrue
        assertThat(getAspectsList(record)).containsExactly("aspect0", "aspect1")

        // should not be removed because this aspect defined in type
        updateRecord(record, "att_rem__aspects" to ModelUtils.getAspectRef("aspect0"))

        assertThat(hasAspects(record, "aspect0", "aspect1")).isTrue
        assertThat(getAspectsList(record)).containsExactly("aspect0", "aspect1")

        updateRecord(record, "att_add__aspects" to ModelUtils.getAspectRef("aspect2"))

        assertThat(hasAspects(record, "aspect0", "aspect1", "aspect2")).isTrue
        assertThat(getAspectsList(record)).containsExactly("aspect0", "aspect1", "aspect2")

        updateRecord(record, "att_rem__aspects" to ModelUtils.getAspectRef("aspect2"))

        assertThat(hasAspects(record, "aspect0", "aspect1")).isTrue
        assertThat(getAspectsList(record)).containsExactly("aspect0", "aspect1")

        fun checkQueryByAtt(att: String, value: Any, vararg expected: EntityRef) {
            val valuePredTypes = listOf(
                ValuePredicate.Type.EQ,
                ValuePredicate.Type.CONTAINS
            )
            for (predType in valuePredTypes) {
                val query = baseQuery.copy()
                    .withEcosType(REC_TEST_TYPE_ID)
                    .withQuery(ValuePredicate(att, predType, value))
                    .build()
                val result = records.query(query).getRecords() as List<EntityRef>
                assertThat(result).describedAs("$att $predType $value")
                    .containsExactly(*expected)
            }
        }
        fun checkQueryByAspect(aspect: String, vararg expected: EntityRef) {
            checkQueryByAtt("_aspects", ModelUtils.getAspectRef(aspect), *expected)
        }

        checkQueryByAspect("aspect0", record)
        checkQueryByAspect("aspect1", record)
        checkQueryByAspect("aspect2")

        val mutateCounter = AtomicInteger()
        fun mutateTest(record: EntityRef, att: String) {
            val isAssoc = att.contains("assoc")
            var value = "att-value-${mutateCounter.getAndIncrement()}"
            if (isAssoc) {
                value = "emodel/assoc@$value"
            }
            records.mutateAtt(record, att, value)
            val attToLoad = if (isAssoc) {
                "$att?id"
            } else {
                att
            }
            assertThat(records.getAtt(record, attToLoad).asText())
                .describedAs("$record $att")
                .isEqualTo(value)
        }

        listOf("typeText", "aspect0:text", "aspect1:text", "aspect2:text", "aspect2:assoc").forEach {
            mutateTest(record, it)
        }

        fun checkAssociatedAssocs(vararg expected: String) {
            val assocs = records.getAtt(record, "assoc:associatedWith[]?id").asStrList()
            assertThat(assocs).containsExactly(*expected)
        }
        val ref0 = "emodel/some-type@ref0"
        val ref1 = "emodel/some-type@ref1"
        val ref2 = "emodel/some-type@ref2"

        updateRecord(record, "att_add_assoc:associatedWith" to ref0)
        checkAssociatedAssocs(ref0)
        updateRecord(record, "att_add_assoc:associatedWith" to ref1)
        checkAssociatedAssocs(ref0, ref1)
        updateRecord(record, "att_add_assoc:associatedWith" to ref2)
        checkAssociatedAssocs(ref0, ref1, ref2)
        updateRecord(record, "att_rem_assoc:associatedWith" to ref0)
        checkAssociatedAssocs(ref1, ref2)
        updateRecord(record, "att_rem_assoc:associatedWith" to ref1)
        checkAssociatedAssocs(ref2)
        updateRecord(record, "att_rem_assoc:associatedWith" to ref2)
        checkAssociatedAssocs()
    }

    @Test
    fun test2() {

        val record = createRecord(
            "typeText" to "typeText",
            "aspect0:text" to "test"
        )
        assertThat(records.getAtt(record, "aspect0:text").asText()).isEqualTo("test")
        assertThat(records.getAtt(record, "typeText").asText()).isEqualTo("typeText")
    }

    @BeforeEach
    fun beforeEach() {

        registerType(
            TypeInfo.create {
                withId(REC_TEST_TYPE_ID)
                withModel(
                    TypeModelDef.create {
                        withAttributes(
                            listOf(
                                AttributeDef.create()
                                    .withId("typeText")
                                    .build(),
                                AttributeDef.create()
                                    .withId("aspect0:text")
                                    .build()
                            )
                        ).build()
                    }
                )
                withAspects(
                    listOf(
                        TypeAspectDef.create()
                            .withRef("emodel/aspect@aspect0".toEntityRef())
                            .build()
                    )
                )
            }
        )

        registerAspect(
            AspectInfo.create {
                withId("aspect0")
                withAttributes(
                    listOf(
                        AttributeDef.create {
                            withId("aspect0:text")
                        }
                    )
                )
            }
        )

        registerAspect(
            AspectInfo.create {
                withId("aspect1")
                withAttributes(
                    listOf(
                        AttributeDef.create {
                            withId("aspect1:text")
                        }
                    )
                )
            }
        )

        registerAspect(
            AspectInfo.create {
                withId("aspect2")
                withAttributes(
                    listOf(
                        AttributeDef.create {
                            withId("aspect2:text")
                        },
                        AttributeDef.create {
                            withId("aspect2:assoc")
                            withType(AttributeType.ASSOC)
                        }
                    )
                )
            }
        )

        registerAspect(
            AspectInfo.create {
                withId("associated")
                withAttributes(
                    listOf(
                        AttributeDef.create {
                            withId("assoc:associatedWith")
                            withType(AttributeType.ASSOC)
                            withMultiple(true)
                        }
                    )
                )
            }
        )
    }
}
