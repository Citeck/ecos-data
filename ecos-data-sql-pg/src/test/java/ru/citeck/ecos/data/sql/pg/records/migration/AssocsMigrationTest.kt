package ru.citeck.ecos.data.sql.pg.records.migration

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import ru.citeck.ecos.commons.data.ObjectData
import ru.citeck.ecos.data.sql.pg.records.DbRecordsTestBase
import ru.citeck.ecos.data.sql.records.migration.AssocsDbMigration
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType

class AssocsMigrationTest : DbRecordsTestBase() {

    @Test
    fun test() {

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("text")
                },
                // use text type to emulate old assocs saved as text
                AttributeDef.create {
                    withId("assoc")
                },
                AttributeDef.create {
                    withId("assocArray")
                    withMultiple(true)
                }
            )
        )

        val assocValues = Array(10) {
            "source-id@localId-$it"
        }
        val assocArrayValues = Array(10) { recIdx ->
            Array(3) { "source-id@assoc-array-$recIdx-$it" }.toList()
        }
        val createdRecords = assocValues.mapIndexed { idx, assocValue ->
            createRecord(
                "text" to "text-value-rec-$idx",
                "assoc" to assocValue,
                "assocArray" to assocArrayValues[idx]
            )
        }

        val checkAssocValues = {
            for ((idx, record) in createdRecords.withIndex()) {
                val assocActualValue = records.getAtt(record, "assoc?id").asText()
                assertThat(assocActualValue).isEqualTo(assocValues[idx])
                val assocArrayActualValue = records.getAtt(record, "assocArray[]?id").asStrList()
                assertThat(assocArrayActualValue).isEqualTo(assocArrayValues[idx])
            }
        }
        checkAssocValues()

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("text")
                },
                AttributeDef.create {
                    withId("assoc")
                    withType(AttributeType.ASSOC)
                },
                AttributeDef.create {
                    withId("assocArray")
                    withType(AttributeType.ASSOC)
                    withMultiple(true)
                }
            )
        )

        printQueryRes("SELECT * from ${tableRef.fullName}")

        recordsDao.runMigrationByType(AssocsDbMigration.TYPE, REC_TEST_TYPE_REF, false, ObjectData.create())

        printQueryRes("SELECT * from ${tableRef.fullName}")

        checkAssocValues()

        val newRecord = createRecord(
            "text" to "text-value2",
            "assoc" to assocValues[0],
            "assocArray" to listOf(assocValues[0], assocValues[1])
        )

        val assocActualValue3 = records.getAtt(newRecord, "assoc?id").asText()
        assertThat(assocActualValue3).isEqualTo(assocValues[0])

        val assocArrayActualValue3 = records.getAtt(newRecord, "assocArray[]?id").asStrList()
        assertThat(assocArrayActualValue3).isEqualTo(listOf(assocValues[0], assocValues[1]))

        // printQueryRes("SELECT * from \"records-test-schema\".\"ecos_record_ref\"")
    }
}
