package ru.citeck.ecos.data.sql.pg.records

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import ru.citeck.ecos.commons.data.MLText
import ru.citeck.ecos.commons.json.Json
import ru.citeck.ecos.data.sql.ecostype.DbEcosTypeInfo
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType
import ru.citeck.ecos.records2.RecordRef
import ru.citeck.ecos.records2.predicate.model.Predicates
import ru.citeck.ecos.records2.predicate.model.VoidPredicate
import ru.citeck.ecos.records3.record.dao.query.dto.query.RecordsQuery
import java.time.Instant
import java.time.temporal.ChronoUnit

class DbRecordsDaoTest : DbRecordsTestBase() {

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
        val newRecId = getRecords().create(RECS_DAO_ID, attsMap)

        val attsToReq = listOf(
            "textArrayAtt",
            "numArrayAtt?num",
            "dateTimeArrayAtt"
        )

        val result = getRecords().getAtts(newRecId, attsToReq)
        assertThat(result.getAtt("textArrayAtt").asText()).isEqualTo((attsMap["textArrayAtt"] as List<*>)[0])
        assertThat(result.getAtt("numArrayAtt?num").asInt()).isEqualTo((attsMap["numArrayAtt"] as List<*>)[0])
        assertThat(Instant.parse(result.getAtt("dateTimeArrayAtt").asText())).isEqualTo((attsMap["dateTimeArrayAtt"] as List<*>)[0])

        val strList = arrayListOf("aaa", "bbb", "ccc")
        getRecords().mutate(newRecId, "textArrayAtt", strList)
        val attRes = getRecords().getAtt(newRecId, "textArrayAtt[]?str").asStrList()
        assertThat(attRes).containsExactlyElementsOf(strList)

        val simpleStr = "simple-str"
        getRecords().mutate(newRecId, "textArrayAtt", simpleStr)
        val attRes2 = getRecords().getAtt(newRecId, "textArrayAtt[]?str").asStrList()
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
        val newRecId = getRecords().create("test", attsMap)
        val creator = getCurrentUser()

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

        val attsFromDao = getRecords().getAtts(newRecId, attsToCheck)

        assertThat(attsFromDao.getAtt("?disp").asText()).isEqualTo("Имя #${attsMap["numAtt"]}")
        assertThat(attsFromDao.getAtt("textAtt").asText()).isEqualTo(attsMap["textAtt"])
        assertThat(attsFromDao.getAtt("numAtt?num").asInt()).isEqualTo(attsMap["numAtt"])
        assertThat(attsFromDao.getAtt("unknown").isNull()).isTrue()
        assertThat(attsFromDao.getAtt("_modified").getAs(Instant::class.java)!!.isAfter(timeBeforeCreated))
        assertThat(attsFromDao.getAtt("_created").getAs(Instant::class.java)!!.isAfter(timeBeforeCreated))
        assertThat(attsFromDao.getAtt("_modifier?localId").asText()).isEqualTo(getCurrentUser())
        assertThat(attsFromDao.getAtt("_creator?localId").asText()).isEqualTo(creator)
        assertThat(attsFromDao.getAtt("dateTimeAtt?str").asText()).isEqualTo(dateTimeAttValue.toString())

        setCurrentUser("new-user")
        val newTextValue = attsMap["textAtt"].toString() + "-postfix"
        getRecords().mutate(newRecId, mapOf("textAtt" to newTextValue))

        val attsFromDao2 = getRecords().getAtts(newRecId, attsToCheck)

        assertThat(attsFromDao2.getAtt("textAtt").asText()).isEqualTo(newTextValue)
        assertThat(attsFromDao2.getAtt("numAtt?num").asInt()).isEqualTo(attsMap["numAtt"])
        assertThat(attsFromDao2.getAtt("unknown").isNull()).isTrue()
        assertThat(attsFromDao2.getAtt("_modified").getAs(Instant::class.java)!!.isAfter(timeBeforeCreated))
        assertThat(attsFromDao2.getAtt("_created").getAs(Instant::class.java)!!.isAfter(timeBeforeCreated))
        assertThat(attsFromDao2.getAtt("_modifier?localId").asText()).isEqualTo(getCurrentUser())
        assertThat(attsFromDao2.getAtt("_creator?localId").asText()).isEqualTo(creator)

        val res = getRecords().query(
            RecordsQuery.create {
                withSourceId("test")
                withQuery(Predicates.eq("textAtt", newTextValue))
            }
        )
        assertThat(res.getRecords()).hasSize(1)
        assertThat(res.getRecords()[0].sourceId).isEqualTo("test")
        assertThat(res.getRecords()[0].id).isEqualTo(newRecId.id)
        assertThat(res.getTotalCount()).isEqualTo(1)

        val fullResWithUnknownFieldPred = getRecords().query(
            RecordsQuery.create {
                withSourceId("test")
                withQuery(Predicates.eq("textAtt123", newTextValue))
            }
        )
        assertThat(fullResWithUnknownFieldPred.getTotalCount()).isEqualTo(1)
        assertThat(fullResWithUnknownFieldPred.getRecords()).hasSize(1)

        val emptyRes = getRecords().query(
            RecordsQuery.create {
                withSourceId("test")
                withQuery(Predicates.eq("textAtt", "unknown-value"))
            }
        )
        assertThat(emptyRes.getTotalCount()).isZero()
        assertThat(emptyRes.getRecords()).isEmpty()

        for (i in 0 until 30) {
            getRecords().create(
                "test",
                mapOf(
                    "numAtt" to i,
                    "_type" to "emodel/type@$testTypeId"
                )
            )
        }

        val fullRes = getRecords().query(
            RecordsQuery.create {
                withSourceId("test")
                withQuery(VoidPredicate.INSTANCE)
            }
        )
        assertThat(fullRes.getTotalCount()).isEqualTo(31)
        assertThat(fullRes.getRecords()).hasSize(31)

        val resWithPage = getRecords().query(
            RecordsQuery.create {
                withSourceId("test")
                withQuery(VoidPredicate.INSTANCE)
                withMaxItems(10)
            }
        )
        assertThat(resWithPage.getTotalCount()).isEqualTo(31)
        assertThat(resWithPage.getRecords()).hasSize(10)

        val resWithSkip = getRecords().query(
            RecordsQuery.create {
                withSourceId("test")
                withQuery(VoidPredicate.INSTANCE)
                withSkipCount(25)
            }
        )
        assertThat(resWithSkip.getTotalCount()).isEqualTo(31)
        assertThat(resWithSkip.getRecords()).hasSize(31 - 25)

        val resWithGt = getRecords().query(
            RecordsQuery.create {
                withSourceId("test")
                withQuery(Predicates.gt("numAtt", 15.0))
            }
        )
        assertThat(resWithGt.getTotalCount()).isEqualTo(15)
        assertThat(resWithGt.getRecords()).hasSize(15)

        val recId = getRecords().queryOne(
            RecordsQuery.create {
                withSourceId("test")
                withQuery(Predicates.eq("numAtt", 15))
            }
        )!!
        getRecords().delete(recId)

        assertThat(
            getRecords().queryOne(
                RecordsQuery.create {
                    withSourceId("test")
                    withQuery(Predicates.eq("numAtt", 15))
                }
            )
        ).isNull()

        assertThat(getRecords().getAtt(recId, "_notExists?bool").asBoolean()).isTrue()

        val fullQueryAfterDeleteRes = getRecords().query(
            RecordsQuery.create {
                withSourceId("test")
                withQuery(VoidPredicate.INSTANCE)
            }
        )
        assertThat(fullQueryAfterDeleteRes.getTotalCount()).isEqualTo(30)
        assertThat(fullQueryAfterDeleteRes.getRecords()).hasSize(30)
    }
}
