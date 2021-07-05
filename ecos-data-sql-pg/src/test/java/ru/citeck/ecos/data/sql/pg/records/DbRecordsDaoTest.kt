package ru.citeck.ecos.data.sql.pg.records

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import ru.citeck.ecos.commons.data.MLText
import ru.citeck.ecos.data.sql.dto.DbTableRef
import ru.citeck.ecos.data.sql.ecostype.DbEcosTypeInfo
import ru.citeck.ecos.data.sql.ecostype.DbEcosTypeRepo
import ru.citeck.ecos.data.sql.pg.PgDataServiceFactory
import ru.citeck.ecos.data.sql.pg.PgUtils
import ru.citeck.ecos.data.sql.records.DbRecordsDao
import ru.citeck.ecos.data.sql.records.DbRecordsDaoConfig
import ru.citeck.ecos.data.sql.repo.DbContextManager
import ru.citeck.ecos.data.sql.repo.entity.DbEntity
import ru.citeck.ecos.data.sql.service.DbDataServiceConfig
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType
import ru.citeck.ecos.records2.RecordRef
import ru.citeck.ecos.records2.predicate.model.Predicates
import ru.citeck.ecos.records2.predicate.model.VoidPredicate
import ru.citeck.ecos.records3.RecordsServiceFactory
import ru.citeck.ecos.records3.record.dao.query.dto.query.RecordsQuery
import java.time.Instant
import java.time.temporal.ChronoUnit

class DbRecordsDaoTest {

    @Test
    fun test() {
        PgUtils.withDbDataSource { dataSource ->

            val testTypeId = "test-type"
            val ecosTypeRepo = object : DbEcosTypeRepo {
                override fun getTypeInfo(typeId: String): DbEcosTypeInfo? {
                    if (testTypeId == typeId) {
                        return DbEcosTypeInfo(
                            testTypeId,
                            MLText(),
                            MLText("Имя #\${numAtt}"),
                            RecordRef.EMPTY,
                            listOf(
                                AttributeDef.create()
                                    .withId("textAtt")
                                    .withType(AttributeType.TEXT)
                                    .build(),
                                AttributeDef.create()
                                    .withId("numAtt")
                                    .withType(AttributeType.NUMBER)
                                    .build()
                            ),
                            emptyList()
                        )
                    }
                    return null
                }
            }
            var currentUser = "user0"
            val contextManager = object : DbContextManager {
                override fun getCurrentUser() = currentUser
                override fun getCurrentUserAuthorities(): List<String> = listOf(getCurrentUser())
            }

            val recordsDao = DbRecordsDao(
                "test",
                DbRecordsDaoConfig(
                    insertable = true,
                    updatable = true,
                    deletable = true
                ),
                ecosTypeRepo,
                PgDataServiceFactory().create(DbEntity::class.java)
                    .withConfig(DbDataServiceConfig(true))
                    .withDataSource(dataSource)
                    .withDbContextManager(contextManager)
                    .withTableRef(DbTableRef("records-test-schema", "test-records-table"))
                    .build()
            )

            val records = RecordsServiceFactory().recordsServiceV1
            records.register(recordsDao)

            val timeBeforeCreated = Instant.now().truncatedTo(ChronoUnit.MILLIS)

            val attsMap = mapOf(
                "textAtt" to "text value",
                "numAtt" to 123,
                "unknown" to "value",
                "_type" to "emodel/type@$testTypeId"
            )
            val newRecId = records.create("test", attsMap)
            val creator = contextManager.getCurrentUser()

            val attsToCheck = listOf(
                "textAtt",
                "numAtt?num",
                "unknown",
                "_modified",
                "_modifier?localId",
                "_created",
                "_creator?localId",
                "?disp"
            )

            val attsFromDao = records.getAtts(newRecId, attsToCheck)

            assertThat(attsFromDao.getAtt("?disp").asText()).isEqualTo("Имя #${attsMap["numAtt"]}")
            assertThat(attsFromDao.getAtt("textAtt").asText()).isEqualTo(attsMap["textAtt"])
            assertThat(attsFromDao.getAtt("numAtt?num").asInt()).isEqualTo(attsMap["numAtt"])
            assertThat(attsFromDao.getAtt("unknown").isNull()).isTrue()
            assertThat(attsFromDao.getAtt("_modified").getAs(Instant::class.java)!!.isAfter(timeBeforeCreated))
            assertThat(attsFromDao.getAtt("_created").getAs(Instant::class.java)!!.isAfter(timeBeforeCreated))
            assertThat(attsFromDao.getAtt("_modifier?localId").asText()).isEqualTo(currentUser)
            assertThat(attsFromDao.getAtt("_creator?localId").asText()).isEqualTo(creator)

            currentUser = "new-user"
            val newTextValue = attsMap["textAtt"].toString() + "-postfix"
            records.mutate(newRecId, mapOf("textAtt" to newTextValue))

            val attsFromDao2 = records.getAtts(newRecId, attsToCheck)

            assertThat(attsFromDao2.getAtt("textAtt").asText()).isEqualTo(newTextValue)
            assertThat(attsFromDao2.getAtt("numAtt?num").asInt()).isEqualTo(attsMap["numAtt"])
            assertThat(attsFromDao2.getAtt("unknown").isNull()).isTrue()
            assertThat(attsFromDao2.getAtt("_modified").getAs(Instant::class.java)!!.isAfter(timeBeforeCreated))
            assertThat(attsFromDao2.getAtt("_created").getAs(Instant::class.java)!!.isAfter(timeBeforeCreated))
            assertThat(attsFromDao2.getAtt("_modifier?localId").asText()).isEqualTo(currentUser)
            assertThat(attsFromDao2.getAtt("_creator?localId").asText()).isEqualTo(creator)

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
            assertThat(fullResWithUnknownFieldPred.getTotalCount()).isEqualTo(1)
            assertThat(fullResWithUnknownFieldPred.getRecords()).hasSize(1)

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
}
