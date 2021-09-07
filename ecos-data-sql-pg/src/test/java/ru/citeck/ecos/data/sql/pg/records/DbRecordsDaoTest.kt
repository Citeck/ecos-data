package ru.citeck.ecos.data.sql.pg.records

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import ru.citeck.ecos.commons.data.MLText
import ru.citeck.ecos.commons.json.Json
import ru.citeck.ecos.context.lib.auth.AuthContext
import ru.citeck.ecos.data.sql.dto.DbTableRef
import ru.citeck.ecos.data.sql.ecostype.DbEcosTypeInfo
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType
import ru.citeck.ecos.model.lib.type.service.utils.TypeUtils
import ru.citeck.ecos.records2.RecordRef
import ru.citeck.ecos.records2.predicate.model.Predicates
import ru.citeck.ecos.records2.predicate.model.VoidPredicate
import ru.citeck.ecos.records3.record.dao.query.dto.query.RecordsQuery
import java.time.Instant
import java.time.temporal.ChronoUnit

class DbRecordsDaoTest : DbRecordsTestBase() {

    @Test
    fun testWithChangedSchema() {

        val ref = RecordRef.create(RECS_DAO_ID, "test")

        val firstTableRef = DbTableRef("ecos-data", "test-data")
        initWithTable(firstTableRef)

        val testTypeId = "test-type"
        registerType(
            testTypeId,
            listOf(
                AttributeDef.create()
                    .withId("textAtt")
                    .withType(AttributeType.TEXT)
            ).map { it.build() }
        )

        assertThat(records.getAtt(ref, "textAtt").asText()).isEmpty()
        recordsDao.runMigrations(TypeUtils.getTypeRef(testTypeId), mock = false)
        assertThat(records.getAtt(ref, "textAtt").asText()).isEmpty()

        val rec0Id = records.create(
            RECS_DAO_ID,
            mapOf(
                "textAtt" to "value",
                "_type" to TypeUtils.getTypeRef(testTypeId)
            )
        )
        assertThat(records.getAtt(rec0Id, "textAtt").asText()).isEqualTo("value")

        val secondTableRef = DbTableRef("ecos_data", "test_data")
        initWithTable(secondTableRef)

        assertThat(records.getAtt(rec0Id, "textAtt").asText()).isEmpty()

        val rec1Id = records.create(
            RECS_DAO_ID,
            mapOf(
                "textAtt" to "value2",
                "_type" to TypeUtils.getTypeRef(testTypeId)
            )
        )
        assertThat(records.getAtt(rec1Id, "textAtt").asText()).isEqualTo("value2")

        initWithTable(firstTableRef)

        assertThat(records.getAtt(rec0Id, "textAtt").asText()).isEqualTo("value")
    }

    @Test
    fun schemaMockTest() {

        val testTypeId = "test-type"
        registerType(
            DbEcosTypeInfo(
                testTypeId, MLText(), MLText(), RecordRef.EMPTY,
                listOf(
                    AttributeDef.create()
                        .withId("textAtt")
                        .withType(AttributeType.TEXT)
                ).map { it.build() },
                emptyList()
            )
        )

        val typeRef = TypeUtils.getTypeRef(testTypeId)
        val commands0 = recordsDao.runMigrations(typeRef, diff = true)
        val commands1 = recordsDao.runMigrations(typeRef, diff = false)

        assertThat(commands0).isEqualTo(commands1)

        records.create(RECS_DAO_ID, mapOf("textAtt" to "value", "_type" to testTypeId))

        registerType(
            DbEcosTypeInfo(
                testTypeId, MLText(), MLText(), RecordRef.EMPTY,
                listOf(
                    AttributeDef.create()
                        .withId("textAtt")
                        .withType(AttributeType.TEXT),
                    AttributeDef.create()
                        .withId("textAtt2")
                        .withType(AttributeType.TEXT)
                ).map { it.build() },
                emptyList()
            )
        )

        assertThat(recordsDao.runMigrations(typeRef, diff = true))
            .allMatch { !it.contains("CREATE TABLE") }
            .anyMatch { it.contains("ALTER TABLE") }

        assertThat(recordsDao.runMigrations(typeRef, diff = false))
            .anyMatch { it.contains("CREATE TABLE") }
    }

    @Test
    fun testWithArrays() {

        val testTypeId = "test-type"
        registerType(
            DbEcosTypeInfo(
                testTypeId, MLText(), MLText(), RecordRef.EMPTY,
                listOf(
                    AttributeDef.create()
                        .withId("textAtt")
                        .withType(AttributeType.TEXT),
                    AttributeDef.create()
                        .withId("textArrayAtt")
                        .withType(AttributeType.TEXT)
                        .withMultiple(true),
                    AttributeDef.create()
                        .withId("numArrayAtt")
                        .withType(AttributeType.NUMBER)
                        .withMultiple(true),
                    AttributeDef.create()
                        .withId("dateTimeArrayAtt")
                        .withType(AttributeType.DATETIME)
                        .withMultiple(true)
                ).map { it.build() },
                emptyList()
            )
        )

        val attsMap = mapOf(
            "textArrayAtt" to listOf("text value"),
            "numArrayAtt" to listOf(123),
            "dateTimeArrayAtt" to listOf(Instant.now().truncatedTo(ChronoUnit.MILLIS)),
            "_type" to "emodel/type@$testTypeId"
        )
        val newRecId = records.create(RECS_DAO_ID, attsMap)

        val attsToReq = listOf(
            "textArrayAtt",
            "numArrayAtt?num",
            "dateTimeArrayAtt"
        )

        val result = records.getAtts(newRecId, attsToReq)
        assertThat(result.getAtt("textArrayAtt").asText()).isEqualTo((attsMap["textArrayAtt"] as List<*>)[0])
        assertThat(result.getAtt("numArrayAtt?num").asInt()).isEqualTo((attsMap["numArrayAtt"] as List<*>)[0])
        assertThat(Instant.parse(result.getAtt("dateTimeArrayAtt").asText())).isEqualTo((attsMap["dateTimeArrayAtt"] as List<*>)[0])

        val strList = arrayListOf("aaa", "bbb", "ccc")
        records.mutateAtt(newRecId, "textArrayAtt", strList)
        val attRes = records.getAtt(newRecId, "textArrayAtt[]?str").asStrList()
        assertThat(attRes).containsExactlyElementsOf(strList)

        val simpleStr = "simple-str"
        records.mutateAtt(newRecId, "textArrayAtt", simpleStr)
        val attRes2 = records.getAtt(newRecId, "textArrayAtt[]?str").asStrList()
        assertThat(attRes2).containsExactlyElementsOf(listOf(simpleStr))
    }

    @Test
    fun test() {

        val testTypeId = "test-type"
        registerType(
            DbEcosTypeInfo(
                testTypeId,
                MLText(),
                MLText("Имя #\${numAtt}"),
                RecordRef.EMPTY,
                listOf(
                    AttributeDef.create()
                        .withId("textAtt")
                        .withType(AttributeType.TEXT),
                    AttributeDef.create()
                        .withId("numAtt")
                        .withType(AttributeType.NUMBER),
                    AttributeDef.create()
                        .withId("dateTimeAtt")
                        .withType(AttributeType.DATETIME)
                ).map { it.build() },
                emptyList()
            )
        )

        val timeBeforeCreated = Instant.now().truncatedTo(ChronoUnit.MILLIS)

        val dateTimeAttValueStr = "2021-07-29T17:21:29+07:00"
        val dateTimeAttValue = Json.mapper.read("\"$dateTimeAttValueStr\"", Instant::class.java)!!
        val attsMap = mapOf(
            "textAtt" to "text value",
            "numAtt" to 123,
            "dateTimeAtt" to dateTimeAttValueStr,
            "unknown" to "value",
            "_type" to "emodel/type@$testTypeId"
        )
        val newRecId = records.create("test", attsMap)
        val creator = AuthContext.getCurrentUser()

        val attsToCheck = listOf(
            "textAtt",
            "numAtt?num",
            "unknown",
            "_modified",
            "_modifier?localId",
            "_created",
            "_creator?localId",
            "dateTimeAtt?str",
            "?disp"
        )

        val attsFromDao = records.getAtts(newRecId, attsToCheck)

        assertThat(attsFromDao.getAtt("?disp").asText()).isEqualTo("Имя #${attsMap["numAtt"]}")
        assertThat(attsFromDao.getAtt("textAtt").asText()).isEqualTo(attsMap["textAtt"])
        assertThat(attsFromDao.getAtt("numAtt?num").asInt()).isEqualTo(attsMap["numAtt"])
        assertThat(attsFromDao.getAtt("unknown").isNull()).isTrue()
        assertThat(attsFromDao.getAtt("_modified").getAs(Instant::class.java)!!.isAfter(timeBeforeCreated))
        assertThat(attsFromDao.getAtt("_created").getAs(Instant::class.java)!!.isAfter(timeBeforeCreated))
        assertThat(attsFromDao.getAtt("_modifier?localId").asText()).isEqualTo(AuthContext.getCurrentUser())
        assertThat(attsFromDao.getAtt("_creator?localId").asText()).isEqualTo(creator)
        assertThat(attsFromDao.getAtt("dateTimeAtt?str").asText()).isEqualTo(dateTimeAttValue.toString())

        val newTextValue = attsMap["textAtt"].toString() + "-postfix"

        AuthContext.runAs("new-user") {
            records.mutate(newRecId, mapOf("textAtt" to newTextValue))

            val attsFromDao2 = records.getAtts(newRecId, attsToCheck)

            assertThat(attsFromDao2.getAtt("textAtt").asText()).isEqualTo(newTextValue)
            assertThat(attsFromDao2.getAtt("numAtt?num").asInt()).isEqualTo(attsMap["numAtt"])
            assertThat(attsFromDao2.getAtt("unknown").isNull()).isTrue
            assertThat(attsFromDao2.getAtt("_modified").getAs(Instant::class.java)!!.isAfter(timeBeforeCreated))
            assertThat(attsFromDao2.getAtt("_created").getAs(Instant::class.java)!!.isAfter(timeBeforeCreated))
            assertThat(attsFromDao2.getAtt("_modifier?localId").asText()).isEqualTo(AuthContext.getCurrentUser())
            assertThat(attsFromDao2.getAtt("_creator?localId").asText()).isEqualTo(creator)
        }

        val res = records.query(
            RecordsQuery.create {
                withSourceId("test")
                withQuery(Predicates.eq("textAtt", newTextValue))
            }
        )
        assertThat(res.getRecords()).hasSize(1)
        assertThat(res.getRecords()[0].sourceId).isEqualTo("test")
        assertThat(res.getRecords()[0].id).isEqualTo(newRecId.id)
        assertThat(res.getTotalCount()).isEqualTo(1)

        val fullResWithUnknownFieldPred = records.query(
            RecordsQuery.create {
                withSourceId("test")
                withQuery(Predicates.eq("textAtt123", newTextValue))
            }
        )
        assertThat(fullResWithUnknownFieldPred.getTotalCount()).isEqualTo(0)
        assertThat(fullResWithUnknownFieldPred.getRecords()).hasSize(0)

        val emptyRes = records.query(
            RecordsQuery.create {
                withSourceId("test")
                withQuery(Predicates.eq("textAtt", "unknown-value"))
            }
        )
        assertThat(emptyRes.getTotalCount()).isZero()
        assertThat(emptyRes.getRecords()).isEmpty()

        for (i in 0 until 30) {
            records.create(
                "test",
                mapOf(
                    "numAtt" to i,
                    "_type" to "emodel/type@$testTypeId"
                )
            )
        }

        val fullRes = records.query(
            RecordsQuery.create {
                withSourceId("test")
                withQuery(VoidPredicate.INSTANCE)
            }
        )
        assertThat(fullRes.getTotalCount()).isEqualTo(31)
        assertThat(fullRes.getRecords()).hasSize(31)

        val resWithPage = records.query(
            RecordsQuery.create {
                withSourceId("test")
                withQuery(VoidPredicate.INSTANCE)
                withMaxItems(10)
            }
        )
        assertThat(resWithPage.getTotalCount()).isEqualTo(31)
        assertThat(resWithPage.getRecords()).hasSize(10)

        val resWithSkip = records.query(
            RecordsQuery.create {
                withSourceId("test")
                withQuery(VoidPredicate.INSTANCE)
                withSkipCount(25)
            }
        )
        assertThat(resWithSkip.getTotalCount()).isEqualTo(31)
        assertThat(resWithSkip.getRecords()).hasSize(31 - 25)

        val resWithGt = records.query(
            RecordsQuery.create {
                withSourceId("test")
                withQuery(Predicates.gt("numAtt", 15.0))
            }
        )
        assertThat(resWithGt.getTotalCount()).isEqualTo(15)
        assertThat(resWithGt.getRecords()).hasSize(15)

        val recId = records.queryOne(
            RecordsQuery.create {
                withSourceId("test")
                withQuery(Predicates.eq("numAtt", 15))
            }
        )!!
        records.delete(recId)

        assertThat(
            records.queryOne(
                RecordsQuery.create {
                    withSourceId("test")
                    withQuery(Predicates.eq("numAtt", 15))
                }
            )
        ).isNull()

        assertThat(records.getAtt(recId, "_notExists?bool").asBoolean()).isTrue()

        val fullQueryAfterDeleteRes = records.query(
            RecordsQuery.create {
                withSourceId("test")
                withQuery(VoidPredicate.INSTANCE)
            }
        )
        assertThat(fullQueryAfterDeleteRes.getTotalCount()).isEqualTo(30)
        assertThat(fullQueryAfterDeleteRes.getRecords()).hasSize(30)
    }
}
