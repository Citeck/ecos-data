package ru.citeck.ecos.data.sql.pg.records

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.records2.predicate.model.Predicates
import java.util.*

class DbRecordsColumnsCacheTest : DbRecordsTestBase() {

    @Test
    fun test() {

        registerAtts(
            Array(10) {
                AttributeDef.create {
                    withId("att-$it")
                }
            }.toList()
        )

        val record = createRecord(
            *Array(10) {
                "att-$it" to "val-$it"
            }
        )

        assertThat(records.getAtt(record, "att-0").asText()).isEqualTo("val-0")

        dbDataSource.withTransaction(false) {
            dbDataSource.updateSchema("ALTER TABLE ${tableRef.fullName} DROP COLUMN \"att-5\"")
        }

        assertThat(records.getAtt(record, "att-0").asText()).isEqualTo("val-0")

        dbDataSource.withTransaction(false) {
            dbDataSource.updateSchema("ALTER TABLE ${tableRef.fullName} DROP COLUMN \"att-6\"")
        }

        val queryRes = records.query(
            baseQuery.copy {
                withQuery(Predicates.eq("att-1", "val-1"))
            }
        ).getRecords()
        assertThat(queryRes).containsExactly(record)
    }
}
