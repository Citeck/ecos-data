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
import ru.citeck.ecos.model.lib.status.constants.StatusConstants
import ru.citeck.ecos.model.lib.status.dto.StatusDef
import ru.citeck.ecos.model.lib.type.service.utils.TypeUtils
import ru.citeck.ecos.records2.RecordRef
import ru.citeck.ecos.records2.predicate.PredicateService
import ru.citeck.ecos.records2.predicate.model.Predicates
import ru.citeck.ecos.records2.predicate.model.VoidPredicate
import ru.citeck.ecos.records3.record.dao.query.dto.query.RecordsQuery
import java.time.Instant
import java.time.temporal.ChronoUnit

class DbRecordsDaoQueryTest : DbRecordsTestBase() {

    @Test
    fun test() {

        registerType(DbEcosTypeInfo(
            REC_TEST_TYPE_ID,
            MLText.EMPTY,
            MLText.EMPTY,
            RecordRef.EMPTY,
            listOf(AttributeDef.create()
                .withId("textAtt")
                .withType(AttributeType.TEXT)
                .build()),
            listOf(
                StatusDef.create {
                    withId("draft")
                },
                StatusDef.create {
                    withId("new")
                }
            )
        ))

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
    }
}
