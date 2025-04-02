package ru.citeck.ecos.data.sql.pg.records

import io.github.oshai.kotlinlogging.KotlinLogging
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import ru.citeck.ecos.commons.data.DataValue
import ru.citeck.ecos.data.sql.pg.records.commons.DbRecordsTestBase
import ru.citeck.ecos.model.lib.attributes.dto.AttIndexDef
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType
import ru.citeck.ecos.model.lib.status.constants.StatusConstants
import ru.citeck.ecos.model.lib.status.dto.StatusDef
import ru.citeck.ecos.model.lib.type.dto.TypeInfo
import ru.citeck.ecos.model.lib.type.dto.TypeModelDef
import ru.citeck.ecos.records2.predicate.PredicateService
import ru.citeck.ecos.records2.predicate.model.Predicate
import ru.citeck.ecos.records2.predicate.model.Predicates
import ru.citeck.ecos.records2.predicate.model.ValuePredicate
import ru.citeck.ecos.records2.predicate.model.VoidPredicate
import ru.citeck.ecos.records3.record.dao.query.dto.query.RecordsQuery
import ru.citeck.ecos.webapp.api.entity.EntityRef
import java.time.Instant
import java.time.ZoneOffset
import java.time.temporal.ChronoUnit
import java.util.*
import kotlin.collections.ArrayList

class DbRecordsDaoQueryTest : DbRecordsTestBase() {

    companion object {
        val log = KotlinLogging.logger { }
    }

    @Test
    fun testWithUnknownField() {
        registerAtts(
            listOf(
                AttributeDef.create()
                    .withId("text")
                    .build()
            )
        )

        createRecord("text" to "abc")

        fun testPred(predicate: Predicate) {
            val res = records.query(baseQuery.copy().withQuery(predicate).build())
            assertThat(res.getRecords()).describedAs(predicate.toString()).isEmpty()
        }

        testPred(
            Predicates.or(
                Predicates.eq("unknown", "value"),
                Predicates.like("unknown", "value")
            )
        )
    }

    @Test
    fun test() {

        registerType(
            TypeInfo.create {
                withId(REC_TEST_TYPE_ID)
                withModel(
                    TypeModelDef.create()
                        .withAttributes(
                            listOf(
                                AttributeDef.create()
                                    .withId("textAtt")
                                    .withType(AttributeType.TEXT)
                                    .build(),
                                AttributeDef.create()
                                    .withId("dateTimeAtt")
                                    .withType(AttributeType.DATETIME)
                                    .build()
                            )
                        )
                        .withStatuses(
                            listOf(
                                StatusDef.create {
                                    withId("draft")
                                },
                                StatusDef.create {
                                    withId("new")
                                }
                            )
                        ).build()
                )
            }
        )

        val baseQuery = RecordsQuery.create {
            withSourceId(recordsDao.getId())
            withQuery(VoidPredicate.INSTANCE)
            withLanguage(PredicateService.LANGUAGE_PREDICATE)
        }
        val queryWithEmptyStatus = baseQuery.copy {
            withQuery(Predicates.empty(StatusConstants.ATT_STATUS))
        }

        val result = records.query(queryWithEmptyStatus)
        assertThat(result.getRecords()).isEmpty()

        val rec0 = createRecord("textAtt" to "value")

        val result2 = records.query(queryWithEmptyStatus)
        assertThat(result2.getRecords()).containsExactly(rec0)

        val queryWithNotEmptyStatus = baseQuery.copy {
            withQuery(Predicates.notEmpty(StatusConstants.ATT_STATUS))
        }

        val result3 = records.query(queryWithNotEmptyStatus)
        assertThat(result3.getRecords()).isEmpty()

        val rec1 = createRecord(
            "textAtt" to "value",
            "_status" to "draft"
        )

        val result4 = records.query(queryWithNotEmptyStatus)
        assertThat(result4.getRecords()).containsExactly(rec1)

        val result5 = records.query(queryWithEmptyStatus)
        assertThat(result5.getRecords()).containsExactly(rec0)

        val result6 = records.query(
            baseQuery.copy {
                withQuery(Predicates.eq("unknown", "value"))
            }
        )
        assertThat(result6.getRecords()).isEmpty()
    }

    @Test
    fun test2() {

        registerType(
            TypeInfo.create {
                withId(REC_TEST_TYPE_ID)
                withModel(
                    TypeModelDef.create {
                        withAttributes(
                            listOf(
                                AttributeDef.create()
                                    .withId("textAtt")
                                    .withType(AttributeType.TEXT)
                                    .build(),
                                AttributeDef.create()
                                    .withId("dateTimeAtt")
                                    .withType(AttributeType.DATETIME)
                                    .build(),
                                AttributeDef.create()
                                    .withId("dateAtt")
                                    .withType(AttributeType.DATE)
                                    .build()
                            )
                        )
                            .withStatuses(
                                listOf(
                                    StatusDef.create {
                                        withId("draft")
                                    },
                                    StatusDef.create {
                                        withId("new")
                                    }
                                )
                            )
                    }
                )
            }
        )

        testForDateAtt("dateTimeAtt", true)
        testForDateAtt("dateAtt", true)

        val tzBefore = TimeZone.getDefault()
        try {
            TimeZone.setDefault(TimeZone.getTimeZone("UTC"))

            testForDateAtt("dateTimeAtt", false)
            testForDateAtt("dateAtt", false)

            TimeZone.setDefault(TimeZone.getTimeZone(ZoneOffset.ofHours(12)))

            testForDateAtt("dateTimeAtt", false)
            testForDateAtt("dateAtt", false)

            TimeZone.setDefault(TimeZone.getTimeZone("UTC"))

            testForDateAtt("dateTimeAtt", false)
            testForDateAtt("dateAtt", false)
        } finally {
            TimeZone.setDefault(tzBefore)
        }

        printQueryRes("SELECT __ext_id,\"dateTimeAtt\",\"dateAtt\" FROM ${tableRef.fullName}")
    }

    private fun testForDateAtt(attName: String, withCreate: Boolean) {

        val withTime = attName.contains("Time")

        log.info { "Test attribute $attName. With time: $withTime" }
        printQueryRes("show timezone;")

        val fixTime = { time: String ->
            if (withTime) {
                time
            } else {
                Instant.parse(time).truncatedTo(ChronoUnit.DAYS).toString()
            }
        }

        val recsByTime = mutableMapOf<String, MutableList<EntityRef>>()
        val addRecByTime = { time: String, rec: EntityRef ->
            recsByTime.computeIfAbsent(fixTime(time)) { ArrayList() }.add(rec)
        }

        val time01_00 = "2021-01-01T00:00:00Z"
        val recWithDate0 = EntityRef.create(APP_NAME, recordsDao.getId(), "$attName-01-00")
        if (withCreate) {
            createRecord(
                "_localId" to recWithDate0.getLocalId(),
                attName to time01_00
            )
        }
        addRecByTime(time01_00, recWithDate0)
        assertThat(records.getAtt(recWithDate0, attName).asText()).isEqualTo(fixTime(time01_00))

        val time01_10 = "2021-01-01T10:00:10Z"
        val recWithDate1 = EntityRef.create(APP_NAME, recordsDao.getId(), "$attName-01-10")
        if (withCreate) {
            createRecord(
                "_localId" to recWithDate1.getLocalId(),
                attName to time01_10
            )
        }
        addRecByTime(time01_10, recWithDate1)
        assertThat(records.getAtt(recWithDate1, attName).asText()).isEqualTo(fixTime(time01_10))

        val time02_00 = "2021-01-02T00:00:00Z"
        val recWithDate2 = EntityRef.create(APP_NAME, recordsDao.getId(), "$attName-02-00")
        if (withCreate) {
            createRecord(
                "_localId" to recWithDate2.getLocalId(),
                attName to time02_00
            )
        }
        addRecByTime(time02_00, recWithDate2)
        assertThat(records.getAtt(recWithDate2, attName).asText()).isEqualTo(fixTime(time02_00))

        listOf(time01_00, time01_10, time02_00).forEach { time ->
            val fixedTime = fixTime(time)
            val times = if (withTime) {
                listOf(fixedTime)
            } else {
                val dateWithoutTime = fixedTime.split("T")[0]
                listOf(fixedTime, dateWithoutTime)
            }
            times.forEach { timeToTest ->
                val result = records.query(
                    baseQuery.copy {
                        withQuery(Predicates.eq(attName, timeToTest))
                    }
                )
                assertThat(result.getRecords()).describedAs(timeToTest).containsExactlyElementsOf(recsByTime[fixedTime])
            }
        }

        val result8 = records.query(
            baseQuery.copy {
                withQuery(Predicates.gt(attName, Instant.parse(time01_10)))
            }
        )
        if (withTime) {
            assertThat(result8.getRecords()).containsExactly(recWithDate2)
        } else {
            assertThat(result8.getRecords()).containsExactly(recWithDate2)
        }

        val result9 = records.query(
            baseQuery.copy {
                withQuery(Predicates.ge(attName, Instant.parse(time01_10)))
            }
        )
        if (withTime) {
            assertThat(result9.getRecords()).containsExactly(recWithDate1, recWithDate2)
        } else {
            assertThat(result9.getRecords()).containsExactly(recWithDate0, recWithDate1, recWithDate2)
        }

        val result10 = records.query(
            baseQuery.copy {
                withQuery(Predicates.ge(attName, Instant.parse(time01_00)))
            }
        )
        assertThat(result10.getRecords()).containsExactly(recWithDate0, recWithDate1, recWithDate2)
    }

    @Test
    fun test3() {

        val attName = "textAtt"
        registerAtts(
            listOf(
                AttributeDef.create {
                    withId(attName)
                    withIndex(
                        AttIndexDef.create()
                            .withEnabled(true)
                            .build()
                    )
                }
            )
        )

        val ref = createRecord(attName to "Abcdefg")

        val predicatesWhichShouldMatch = listOf(
            Predicates.eq(attName, "Abcdefg"),
            Predicates.contains(attName, "Abcdefg"),
            Predicates.contains(attName, "abcdefg"),
            Predicates.contains(attName, "cdefg"),
            Predicates.contains(attName, "abcde"),
            Predicates.contains(attName, "cde"),
            ValuePredicate.like(attName, "Abcd%"),
            ValuePredicate.like(attName, "Abcdefg"),
            ValuePredicate.like(attName, "abcdefg"),
            ValuePredicate.like(attName, "abcd%"),
            ValuePredicate.like(attName, "%bcdefg"),
            ValuePredicate.like(attName, "_bcdefg"),
        )

        predicatesWhichShouldMatch.forEach { pred ->
            val queryRes = records.query(baseQuery.withQuery(DataValue.create(pred)))
            assertThat(queryRes.getRecords()).describedAs(pred.toString()).containsExactly(ref)
        }

        val predicatesWhichShouldNotMatch = listOf(
            ValuePredicate.contains(attName, "AAcdefg"),
            ValuePredicate.like(attName, "Abcdee%"),
        )

        predicatesWhichShouldNotMatch.forEach { pred ->
            val queryRes = records.query(baseQuery.withQuery(DataValue.create(pred)))
            assertThat(queryRes.getRecords()).describedAs(pred.toString()).isEmpty()
        }
    }
}
