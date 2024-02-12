package ru.citeck.ecos.data.sql.pg.records

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import ru.citeck.ecos.commons.data.MLText
import ru.citeck.ecos.commons.json.Json
import ru.citeck.ecos.context.lib.auth.AuthContext
import ru.citeck.ecos.data.sql.dto.DbTableRef
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType
import ru.citeck.ecos.model.lib.type.dto.QueryPermsPolicy
import ru.citeck.ecos.model.lib.type.dto.TypeInfo
import ru.citeck.ecos.model.lib.type.dto.TypeModelDef
import ru.citeck.ecos.model.lib.utils.ModelUtils
import ru.citeck.ecos.records2.RecordConstants
import ru.citeck.ecos.records2.predicate.model.Predicates
import ru.citeck.ecos.records2.predicate.model.VoidPredicate
import ru.citeck.ecos.records3.record.dao.impl.proxy.RecordsDaoProxy
import ru.citeck.ecos.records3.record.dao.query.dto.query.RecordsQuery
import ru.citeck.ecos.txn.lib.TxnContext
import ru.citeck.ecos.webapp.api.entity.EntityRef
import java.time.Instant
import java.time.temporal.ChronoUnit

class DbRecordsDaoTest : DbRecordsTestBase() {

    @Test
    fun testMetaFields() {

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("testStr")
                }
            )
        )

        val instantBeforeCreate = Instant.now()
        Thread.sleep(1)

        val testAttValue = "abc"
        val ref = TxnContext.doInTxn {
            val ref = createRecord("testStr" to testAttValue)
            ref
        }

        assertThat(records.getAtt(ref, "testStr").asText()).isEqualTo(testAttValue)

        val getCreatedModified = {
            val created = Instant.parse(records.getAtt(ref, "_created").asText())
            val modified = Instant.parse(records.getAtt(ref, "_modified").asText())
            created to modified
        }

        val (created1, modified1) = getCreatedModified()

        assertThat(modified1).isAfterOrEqualTo(created1)
        assertThat(created1).isAfter(instantBeforeCreate)
        assertThat(modified1).isAfter(instantBeforeCreate)

        updateRecord(ref, "testStr" to testAttValue + 1)

        val (created2, modified2) = getCreatedModified()

        assertThat(created2).isEqualTo(created1)
        assertThat(modified2).isAfter(modified1)

        val (created3, modified3) = TxnContext.doInTxn {
            updateRecord(ref, "testStr" to testAttValue + 2)
            getCreatedModified()
        }

        assertThat(created3).isEqualTo(created1)
        assertThat(modified3).isAfter(modified2)

        val (created4, modified4) = getCreatedModified()

        assertThat(created4).isEqualTo(created1)
        assertThat(modified4).isEqualTo(modified3)

        var updatedTime: Instant? = null
        TxnContext.doInTxn {
            updateRecord(ref, "testStr" to testAttValue + 3)
            // Without this sleep sometimes modified5 is equal to updatedTime
            Thread.sleep(10)
            updatedTime = Instant.now()
            // check that modified time doesn't change on commit
            Thread.sleep(1_000)
        }
        val (created5, modified5) = getCreatedModified()
        assertThat(created5).isEqualTo(created1)
        assertThat(modified5).isBefore(updatedTime)
    }

    @Test
    fun testWithChangedSchema() {

        val ref = EntityRef.create(RECS_DAO_ID, "test")

        val firstTableRef = DbTableRef("ecos-data", "test-data")
        mainCtx = createRecordsDao(tableRef = firstTableRef)

        val testTypeId = "test-type"
        registerAttributes(
            testTypeId,
            listOf(
                AttributeDef.create()
                    .withId("textAtt")
                    .withType(AttributeType.TEXT)
            ).map { it.build() }
        )

        assertThat(records.getAtt(ref, "textAtt").asText()).isEmpty()
        recordsDao.runMigrations(ModelUtils.getTypeRef(testTypeId), mock = false)
        assertThat(records.getAtt(ref, "textAtt").asText()).isEmpty()

        val rec0Id = records.create(
            RECS_DAO_ID,
            mapOf(
                "textAtt" to "value",
                RecordConstants.ATT_TYPE to ModelUtils.getTypeRef(testTypeId)
            )
        )
        assertThat(records.getAtt(rec0Id, "textAtt").asText()).isEqualTo("value")

        val secondTableRef = DbTableRef("ecos_data", "test_data")
        mainCtx = createRecordsDao(tableRef = secondTableRef)

        assertThat(records.getAtt(rec0Id, "textAtt").asText()).isEmpty()

        val rec1Id = records.create(
            RECS_DAO_ID,
            mapOf(
                "textAtt" to "value2",
                RecordConstants.ATT_TYPE to ModelUtils.getTypeRef(testTypeId)
            )
        )
        assertThat(records.getAtt(rec1Id, "textAtt").asText()).isEqualTo("value2")

        mainCtx = createRecordsDao(tableRef = firstTableRef)

        assertThat(records.getAtt(rec0Id, "textAtt").asText()).isEqualTo("value")
    }

    @Test
    fun schemaMockTest() {

        val testTypeId = "test-type"
        registerType(
            TypeInfo.create {
                withId(testTypeId)
                withModel(
                    TypeModelDef.create()
                        .withAttributes(
                            listOf(
                                AttributeDef.create()
                                    .withId("textAtt")
                                    .withType(AttributeType.TEXT)
                            ).map { it.build() }
                        )
                        .build()
                )
            }
        )

        val typeRef = ModelUtils.getTypeRef(testTypeId)
        val commands0 = recordsDao.runMigrations(typeRef, diff = true)
        val commands1 = recordsDao.runMigrations(typeRef, diff = false)

        assertThat(commands0).isEqualTo(commands1)

        records.create(RECS_DAO_ID, mapOf("textAtt" to "value", "_type" to testTypeId))

        recordsDao.runMigrations(typeRef, mock = false, diff = true)

        registerType(
            TypeInfo.create {
                withId(testTypeId)
                withModel(
                    TypeModelDef.create {
                        withAttributes(
                            listOf(
                                AttributeDef.create()
                                    .withId("textAtt")
                                    .withType(AttributeType.TEXT),
                                AttributeDef.create()
                                    .withId("textAtt2")
                                    .withType(AttributeType.TEXT)
                            ).map { it.build() }
                        )
                    }
                )
            }
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
            TypeInfo.create {
                withId(testTypeId)
                withModel(
                    TypeModelDef.create {
                        withAttributes(
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
                            ).map { it.build() }
                        )
                    }
                )
            }
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
        assertThat(
            Instant.parse(
                result.getAtt("dateTimeArrayAtt").asText()
            )
        ).isEqualTo((attsMap["dateTimeArrayAtt"] as List<*>)[0])

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
            TypeInfo.create {
                withId(testTypeId)
                withDispNameTemplate(MLText("Имя #\${numAtt}"))
                withModel(
                    TypeModelDef.create {
                        withAttributes(
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
                            ).map { it.build() }
                        )
                    }
                )
                withQueryPermsPolicy(QueryPermsPolicy.PUBLIC)
            }
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
        val creatorUser = "user0"
        val newRecId = AuthContext.runAs(creatorUser) {
            records.create("test", attsMap)
        }

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
        assertThat(attsFromDao.getAtt("_modifier?localId").asText()).isEqualTo(creatorUser)
        assertThat(attsFromDao.getAtt("_creator?localId").asText()).isEqualTo(creatorUser)
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
            assertThat(attsFromDao2.getAtt("_creator?localId").asText()).isEqualTo(creatorUser)
        }

        val res = records.query(
            RecordsQuery.create {
                withSourceId("test")
                withQuery(Predicates.eq("textAtt", newTextValue))
            }
        )
        assertThat(res.getRecords()).hasSize(1)
        assertThat(res.getRecords()[0].getSourceId()).isEqualTo("test")
        assertThat(res.getRecords()[0].getLocalId()).isEqualTo(newRecId.getLocalId())
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

    @Test
    fun recIdProxyTest() {

        registerAtts(
            listOf(
                AttributeDef.create()
                    .withId("att")
                    .build()
            )
        )

        records.register(RecordsDaoProxy("proxy-dao", recordsDao.getId(), null))

        val atts = mapOf(
            "_type" to REC_TEST_TYPE_REF,
            "att" to "value"
        )
        val record = records.create("proxy-dao", atts)
        val refId = selectRecFromDb(record, "\"__ref_id\"") as Long

        val recordRef = EntityRef.valueOf(
            selectFieldFromDbTable(
                "__ext_id",
                tableRef.withTable("ecos_record_ref").fullName,
                "id=$refId"
            ) as String
        )

        assertThat(recordRef.getSourceId()).isEqualTo("proxy-dao")
    }
}
