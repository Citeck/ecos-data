package ru.citeck.ecos.data.sql.pg.records

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import ru.citeck.ecos.context.lib.auth.AuthContext
import ru.citeck.ecos.data.sql.pg.records.commons.DbRecordsTestBase
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType
import ru.citeck.ecos.model.lib.type.dto.QueryPermsPolicy
import ru.citeck.ecos.records2.predicate.model.Predicate
import ru.citeck.ecos.records2.predicate.model.Predicates
import ru.citeck.ecos.records2.predicate.model.ValuePredicate
import ru.citeck.ecos.webapp.api.entity.EntityRef
import ru.citeck.ecos.webapp.api.entity.toEntityRef

class DbRecordsAssocQueryTest : DbRecordsTestBase() {

    @Test
    fun testEqualToNull() {

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("assoc0")
                    withType(AttributeType.ASSOC)
                }
            )
        )

        val rec0 = createRecord("assoc0" to "emodel/type@abc")
        val rec1 = createRecord("assoc0" to null)

        val res0 = records.query(
            baseQuery.copy {
                withQuery(Predicates.eq("assoc0", "emodel/type@abc"))
            }
        ).getRecords()
        assertThat(res0).containsExactly(rec0)

        val res1 = records.query(
            baseQuery.copy {
                withQuery(Predicates.eq("assoc0", null))
            }
        ).getRecords()
        assertThat(res1).containsExactly(rec1)
    }

    @Test
    fun test() {

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("name")
                    withType(AttributeType.TEXT)
                },
                AttributeDef.create {
                    withId("assoc0")
                    withType(AttributeType.ASSOC)
                    withMultiple(true)
                },
                AttributeDef.create {
                    withId("assoc1")
                    withType(AttributeType.ASSOC)
                    withMultiple(true)
                },
                AttributeDef.create {
                    withId("assoc2")
                    withType(AttributeType.ASSOC)
                    withMultiple(true)
                }
            )
        )

        val targetRecords = (1..30).map { createRecord("name" to "rec#$it") }
        val otherTargetRecord = createRecord("name" to "rec-other")

        val record0 = createRecord(
            "assoc0" to targetRecords,
            "assoc1" to targetRecords
        )
        testQuery(Predicates.eq("assoc0", targetRecords[0]), listOf(record0))
        testQuery(
            Predicates.and(
                Predicates.eq("assoc0", targetRecords[0]),
                Predicates.eq("assoc1", targetRecords[0])
            ),
            listOf(record0)
        )
        testQuery(
            Predicates.and(
                Predicates.empty("name"),
                Predicates.empty("assoc2")
            ),
            listOf(record0)
        )

        testQuery(Predicates.notEmpty("assoc2"), emptyList())

        val record1 = createRecord("assoc2" to otherTargetRecord)

        testQuery(Predicates.notEmpty("assoc2"), listOf(record1))
        testQuery(
            Predicates.and(
                Predicates.notEmpty("assoc1"),
                Predicates.notEmpty("assoc2")
            ),
            emptyList()
        )
        testQuery(
            Predicates.or(
                Predicates.notEmpty("assoc1"),
                Predicates.notEmpty("assoc2")
            ),
            listOf(record1, record0)
        )
        testQuery(Predicates.eq("assoc1", targetRecords[20]), listOf(record0))

        val record2 = createRecord("assoc0" to targetRecords[0])

        testQuery(Predicates.eq("assoc0", targetRecords[0]), listOf(record0, record2))
        testQuery(ValuePredicate.contains("assoc0", targetRecords[0]), listOf(record0, record2))

        testQuery(
            Predicates.and(
                Predicates.eq("assoc0", targetRecords[0]),
                Predicates.eq("assoc0", targetRecords[15])
            ),
            listOf(record0)
        )

        testQuery(
            ValuePredicate(
                "assoc0",
                ValuePredicate.Type.CONTAINS,
                listOf(
                    targetRecords[0],
                    targetRecords[15]
                )
            ),
            listOf(record0, record2)
        )

        testQuery(
            Predicates.and(
                ValuePredicate.contains("assoc0", targetRecords[0]),
                ValuePredicate.contains("assoc0", targetRecords[15])
            ),
            listOf(record0)
        )

        testQuery(
            Predicates.or(
                Predicates.eq("assoc0", targetRecords[0]),
                Predicates.eq("assoc0", targetRecords[15])
            ),
            listOf(record0, record2)
        )
    }

    private fun testQuery(predicate: Predicate, expected: List<EntityRef>) {
        val result = records.query(
            baseQuery.copy {
                withQuery(predicate)
            }
        ).getRecords()
            .map { EntityRef.valueOf(it) }
        assertThat(result)
            .describedAs("$predicate - $expected")
            .containsExactlyInAnyOrderElementsOf(expected)
    }

    @Test
    fun test2() {

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("assoc")
                    withType(AttributeType.ASSOC)
                    withMultiple(true)
                }
            )
        )
        setQueryPermsPolicy(QueryPermsPolicy.OWN)

        val targets = (1..30).map {
            createRecord("id" to "id-$it")
        }

        val rec0 = createRecord("assoc" to targets.subList(0, 15))
        val rec1 = createRecord("assoc" to targets.subList(10, 30))

        setAuthoritiesWithReadPerms(rec0, "user0", "admin")
        setAuthoritiesWithReadPerms(rec1, "user1", "admin")

        val assocPred = Predicates.eq("assoc", targets[11])
        testQuery(assocPred, listOf(rec0, rec1))
        AuthContext.runAs("user0") {
            testQuery(assocPred, listOf(rec0))
        }
        AuthContext.runAs("user1") {
            testQuery(assocPred, listOf(rec1))
        }
        AuthContext.runAs("user1") {
            testQuery(assocPred, listOf(rec1))
        }
        AuthContext.runAs("unknown") {
            testQuery(assocPred, emptyList())
        }
        AuthContext.runAs("admin") {
            testQuery(assocPred, listOf(rec0, rec1))
        }
    }

    @ParameterizedTest
    @ValueSource(strings = ["ASSOC", "PERSON", "AUTHORITY_GROUP", "AUTHORITY"])
    fun attTypesTest(attType: String) {

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("assoc")
                    withType(AttributeType.valueOf(attType))
                    withMultiple(true)
                }
            )
        )
        assocsService.createTableIfNotExists()

        val anyRef = "emodel/person@admin".toEntityRef()
        val record = createRecord("assoc" to anyRef)
        testQuery(Predicates.eq("assoc", anyRef), listOf(record))
        testQuery(ValuePredicate.contains("assoc", anyRef), listOf(record))

        val currentAssocValue0 = records.getAtt(record, "assoc[]?id")
            .toList(EntityRef::class.java)
        assertThat(currentAssocValue0).containsExactly(anyRef)

        updateRecord(record, "assoc" to emptyList<Any>())

        val currentAssocValue1 = records.getAtt(record, "assoc[]?id")
            .toList(EntityRef::class.java)
        assertThat(currentAssocValue1.isEmpty()).isTrue
    }
}
