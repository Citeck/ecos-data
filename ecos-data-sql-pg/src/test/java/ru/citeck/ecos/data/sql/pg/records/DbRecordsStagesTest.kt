package ru.citeck.ecos.data.sql.pg.records

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import ru.citeck.ecos.commons.data.DataValue
import ru.citeck.ecos.commons.data.MLText
import ru.citeck.ecos.data.sql.pg.records.commons.DbRecordsTestBase
import ru.citeck.ecos.data.sql.records.DbRecordsControlAtts
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.procstages.dto.ProcStageDef
import ru.citeck.ecos.model.lib.status.dto.StatusDef
import ru.citeck.ecos.records2.predicate.model.Predicates

class DbRecordsStagesTest : DbRecordsTestBase() {

    companion object {
        private const val TEST_ATT = "testAtt"

        private const val STATUS_0 = "status-0"
        private const val STATUS_1 = "status-1"
        private const val STATUS_2 = "status-2"
        private const val STATUS_3 = "status-3"

        private const val STAGE_0 = "stage-0"
        private const val STAGE_1 = "stage-1"

        private const val STAGE_0_NAME = "Stage0"
        private const val STAGE_1_NAME = "Stage1"
    }

    @Test
    fun test() {

        registerType()
            .withAttributes(AttributeDef.create { withId(TEST_ATT) })
            .withDefaultStatus(STATUS_0)
            .withStatuses(
                listOf(STATUS_0, STATUS_1, STATUS_2, STATUS_3).map {
                    StatusDef.create().withId(it)
                }
            ).withStages(
                ProcStageDef.create().withId(STAGE_0).withName(MLText(STAGE_0_NAME)).withStatuses(listOf(STATUS_0)),
                ProcStageDef.create().withId(STAGE_1).withName(MLText(STAGE_1_NAME)).withStatuses(listOf(STATUS_1, STATUS_2))
            ).register()

        val ref = createRecord()
        fun getStatusId() = records.getAtt(ref, "_status?str").asText()
        fun getStageId() = records.getAtt(ref, "_stage?str").asText()
        fun setStatus(newStatus: String) = records.mutateAtt(ref, "_status", newStatus)
        fun queryStageEq(stage: String) = records.query(
            baseQuery.copy().withQuery(Predicates.eq("_stage", stage)).build()
        ).getRecords().firstOrNull()
        fun queryStageEmpty() = records.query(
            baseQuery.copy().withQuery(Predicates.empty("_stage")).build()
        ).getRecords().firstOrNull()

        assertThat(getStatusId()).isEqualTo(STATUS_0)
        assertThat(getStageId()).isEqualTo(STAGE_0)

        // direct change is not allowed
        records.mutateAtt(ref, "_stage", STAGE_1)
        assertThat(getStageId()).isEqualTo(STAGE_0)

        assertThat(queryStageEq(STAGE_0)).isEqualTo(ref)
        assertThat(queryStageEq(STAGE_1)).isNull()
        assertThat(queryStageEmpty()).isNull()

        setStatus(STATUS_1)
        assertThat(getStatusId()).isEqualTo(STATUS_1)
        assertThat(getStageId()).isEqualTo(STAGE_1)

        assertThat(queryStageEq(STAGE_0)).isNull()
        assertThat(queryStageEq(STAGE_1)).isEqualTo(ref)
        assertThat(queryStageEmpty()).isNull()

        setStatus(STATUS_2)
        assertThat(getStatusId()).isEqualTo(STATUS_2)
        assertThat(getStageId()).isEqualTo(STAGE_1)

        assertThat(queryStageEq(STAGE_0)).isNull()
        assertThat(queryStageEq(STAGE_1)).isEqualTo(ref)
        assertThat(queryStageEmpty()).isNull()

        setStatus(STATUS_3)
        assertThat(getStatusId()).isEqualTo(STATUS_3)
        assertThat(getStageId()).isEmpty()

        assertThat(queryStageEq(STAGE_0)).isNull()
        assertThat(queryStageEq(STAGE_1)).isNull()
        assertThat(queryStageEmpty()).isEqualTo(ref)

        listOf(ref, ref.withoutLocalId()).forEach { refToCheckOptions ->
            val options = records.getAtt(refToCheckOptions, "_edge._stage.options[]{?str,?disp}").asList(DataValue::class.java)
            assertThat(options).describedAs { refToCheckOptions.toString() }.hasSize(2)
            assertThat(options).describedAs { refToCheckOptions.toString() }.containsExactly(
                DataValue.createObj().set("?str", STAGE_0).set("?disp", STAGE_0_NAME),
                DataValue.createObj().set("?str", STAGE_1).set("?disp", STAGE_1_NAME)
            )
        }

        setStatus(STATUS_2)
        assertThat(records.getAtt(ref, "_stage")).isEqualTo(DataValue.createStr(STAGE_1_NAME))

        setStatus(STATUS_3)
        assertThat(records.getAtt(ref, "_stage")).isEqualTo(DataValue.NULL)

        repeat(10) { createRecord("_status" to STATUS_0) }
        repeat(3) { createRecord("_status" to STATUS_2) }

        val groupingRes = records.query(
            baseQuery.copy().withGroupBy(listOf("_stage")).build(),
            mapOf("stage" to "_stage{?str,?disp}", "count" to "count(*)")
        )

        val groupsByStageId = groupingRes.getRecords().associateBy { it["/stage/?str"].asText() }
        assertThat(groupsByStageId[""]?.get("count")?.asInt()).isEqualTo(1)
        assertThat(groupsByStageId[STAGE_0]?.get("count")?.asInt()).isEqualTo(10)
        assertThat(groupsByStageId[STAGE_1]?.get("count")?.asInt()).isEqualTo(3)

        assertThat(groupsByStageId[STAGE_1]?.get("/stage/?disp")?.asText()).isEqualTo(STAGE_1_NAME)
    }

    @Test
    fun testWithTypeChange() {

        fun registerType(stageForStatus2: String) {
            val statusesByStage = HashMap<String, MutableList<String>>()
            statusesByStage.computeIfAbsent(STAGE_0) { ArrayList() }.add(STATUS_0)
            statusesByStage.computeIfAbsent(STAGE_1) { ArrayList() }.add(STATUS_1)
            if (stageForStatus2.isNotEmpty()) {
                statusesByStage.computeIfAbsent(stageForStatus2) { ArrayList() }.add(STATUS_2)
            }

            registerType()
                .withAttributes(AttributeDef.create { withId(TEST_ATT) })
                .withStatuses(
                    listOf(STATUS_0, STATUS_1, STATUS_2).map {
                        StatusDef.create().withId(it)
                    }
                ).withStages(
                    ProcStageDef.create()
                        .withId(STAGE_0)
                        .withName(MLText(STAGE_0_NAME))
                        .withStatuses(statusesByStage[STAGE_0]),
                    ProcStageDef.create()
                        .withId(STAGE_1)
                        .withName(MLText(STAGE_1_NAME))
                        .withStatuses(statusesByStage[STAGE_1])
                ).register()
        }

        registerType("")
        val ref = createRecord()

        fun getStatusId() = records.getAtt(ref, "_status?str").asText()
        fun getStageId() = records.getAtt(ref, "_stage?str").asText()
        fun setStatus(newStatus: String) = records.mutateAtt(ref, "_status", newStatus)

        assertThat(getStageId()).isEmpty()
        assertThat(getStatusId()).isEmpty()

        setStatus(STATUS_1)
        assertThat(getStageId()).isEqualTo(STAGE_1)
        setStatus(STATUS_2)
        assertThat(getStageId()).isEmpty()

        registerType(STAGE_0)
        assertThat(getStageId()).isEmpty()
        records.mutateAtt(ref, DbRecordsControlAtts.UPDATE_CALCULATED_ATTS, true)
        records.mutateAtt(ref, DbRecordsControlAtts.UPDATE_CALCULATED_ATTS, true)
        assertThat(getStageId()).isEqualTo(STAGE_0)
    }
}
