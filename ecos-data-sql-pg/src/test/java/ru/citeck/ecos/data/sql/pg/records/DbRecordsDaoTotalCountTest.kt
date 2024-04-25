package ru.citeck.ecos.data.sql.pg.records

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
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
import ru.citeck.ecos.records3.record.dao.query.dto.query.SortBy
import ru.citeck.ecos.txn.lib.TxnContext
import ru.citeck.ecos.webapp.api.entity.EntityRef
import java.time.Instant
import java.time.temporal.ChronoUnit

class DbRecordsDaoTotalCountTest : DbRecordsTestBase() {

    companion object {
        private val TYPE_REF = ModelUtils.getTypeRef("total-count-test")
    }

    @ParameterizedTest
    @ValueSource(booleans = [true, false])
    fun test(enableTotalCount: Boolean) {

        val dao = createRecordsDao(
            typeRef = TYPE_REF,
            tableRef = tableRef.withTable("total_count_test"),
            sourceId = "total-count-test",
            enableTotalCount = enableTotalCount
        )
        registerType(TypeInfo.create()
            .withId(TYPE_REF.getLocalId())
            .withSourceId("total-count-test")
            .withModel(TypeModelDef.create()
                .withAttributes(listOf(
                    AttributeDef.create()
                        .withId("text")
                        .build()
                )).build()
            ).build()
        )

        for (i in 0 until 20) {
            dao.createRecord("id" to "$i", "text" to i)
        }

        fun queryTest(skipCount: Int, maxItems: Int, expectedTotalCount: Int, expectedRecords: IntRange?) {

            val queryRes = records.query(dao.createQuery {
                withSkipCount(skipCount)
                withMaxItems(maxItems)
                withSortBy(SortBy("dbid", true))
            })
            assertThat(queryRes.getTotalCount()).isEqualTo(expectedTotalCount.toLong())
            if (expectedRecords != null) {
                assertThat(queryRes.getRecords().map { it.getLocalId().toInt() }).isEqualTo(expectedRecords.toList())
            } else {
                assertThat(queryRes.getRecords()).isEmpty()
            }
        }

        queryTest(0, 10, if (enableTotalCount) { 20 } else { 11 }, 0..9)
        queryTest(5, 10, if (enableTotalCount) { 20 } else { 16 }, 5..14)
        queryTest(0, 0, 20, null)
        queryTest(0, 100, 20, 0..19)
        queryTest(0, 20, 20, 0..19)
        queryTest(20, 1, 20, null)
        queryTest(19, 10, 20, 19..19)

    }
}
