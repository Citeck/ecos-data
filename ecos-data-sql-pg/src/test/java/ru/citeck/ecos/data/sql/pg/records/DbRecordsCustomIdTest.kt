package ru.citeck.ecos.data.sql.pg.records

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.records2.predicate.model.Predicates
import ru.citeck.ecos.records3.record.dao.query.dto.query.SortBy
import ru.citeck.ecos.webapp.api.entity.EntityRef

class DbRecordsCustomIdTest : DbRecordsTestBase() {

    @Test
    fun test() {

        val attName = "textAtt"

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId(attName)
                }
            )
        )

        val customId1 = "custom-id-1"
        val attValue = "att-value"

        val newRecId = createRecord(
            "id" to customId1,
            attName to attValue
        )

        assertThat(newRecId.getLocalId()).isEqualTo(customId1)

        val customId2 = "custom-id-2"

        // for legacy support
        val newRecLocalId = createRecord(
            "_localId" to customId2,
            attName to attValue
        )

        assertThat(newRecLocalId.getLocalId()).isEqualTo(customId2)

        val queryResult = records.query(baseQuery, TestDto::class.java)

        assertThat(queryResult.getTotalCount()).isEqualTo(2)
        assertThat(queryResult.getRecords().map { it.id }).containsExactlyInAnyOrder(customId1, customId2)
        assertThat(queryResult.getRecords().all { it.textAtt == attValue }).isTrue

        val testQueryWithId = { recId: String ->

            val queryResult2 = records.query(
                baseQuery.copy {
                    withQuery(Predicates.eq("id", recId))
                },
                TestDto::class.java
            )

            assertThat(queryResult2.getRecords()).hasSize(1)
            assertThat(queryResult2.getRecords()[0].id).isEqualTo(recId)
        }

        testQueryWithId(customId1)
        testQueryWithId(customId2)

        val testQueryWithSort = { asc: Boolean ->
            {

                val queryResult2 = records.query(
                    baseQuery.copy {
                        withSortBy(SortBy("id", asc))
                    },
                    TestDto::class.java
                )

                assertThat(queryResult2.getRecords()).hasSize(2)
                if (asc) {
                    assertThat(queryResult2.getRecords().map { it.id }).containsExactly(customId1, customId2)
                } else {
                    assertThat(queryResult2.getRecords().map { it.id }).containsExactly(customId2, customId1)
                }
            }
        }

        testQueryWithSort(true)
        testQueryWithSort(false)

        val recRef = EntityRef.create(recordsDao.getId(), customId2)
        val atts = records.getAtts(recRef, TestDto::class.java)
        assertThat(atts.id).isEqualTo(customId2)
        assertThat(atts.textAtt).isEqualTo(attValue)
        atts.textAtt = "new value after mutation"

        records.mutate(recRef, atts)

        val atts2 = records.getAtts(recRef, TestDto::class.java)

        assertThat(atts2).isNotSameAs(atts)
        assertThat(atts2.id).isEqualTo(customId2)
        assertThat(atts2.textAtt).isEqualTo("new value after mutation")

        assertThrows<Exception> {
            updateRecord(EntityRef.create(recordsDao.getId(), "unknown-id"), attName to attValue)
        }

        updateRecord(EntityRef.create(recordsDao.getId(), ""), "id" to customId1, attName to "newValue1")
        assertThat(records.getAtt(newRecId, attName).asText()).isEqualTo("newValue1")
    }

    class TestDto(
        val id: String,
        var textAtt: String
    )
}
