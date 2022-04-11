package ru.citeck.ecos.data.sql.pg.records.migration

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import ru.citeck.ecos.context.lib.auth.AuthContext
import ru.citeck.ecos.context.lib.auth.AuthRole
import ru.citeck.ecos.data.sql.pg.records.DbRecordsTestBase
import ru.citeck.ecos.data.sql.repo.entity.DbEntity
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType
import ru.citeck.ecos.records2.RecordRef
import ru.citeck.ecos.records3.record.atts.dto.RecordAtts
import ru.citeck.ecos.records3.record.request.RequestContext

class AssocsMigrationTest : DbRecordsTestBase() {

    @Test
    fun test() {

        initServices(typeRef = REC_TEST_TYPE_REF)

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
            when (recIdx) {
                3 -> Array(3) {
                    if (it != 1) {
                        "source-id@assoc-array-$recIdx-$it"
                    } else {
                        null
                    }
                }.toList()
                4 -> null
                else -> {
                    Array(3) { "source-id@assoc-array-$recIdx-$it" }.toList()
                }
            }
        }
        val createdRecords = assocValues.mapIndexed { idx, assocValue ->
            createRecord(
                "text" to "text-value-rec-$idx",
                "assoc" to assocValue,
                "assocArray" to assocArrayValues[idx]
            )
        }

        val deletedRecords = mutableSetOf<RecordRef>()

        val checkAssocValues = {
            for ((idx, record) in createdRecords.withIndex()) {
                val assocActualValue = records.getAtt(record, "assoc?id").asText()
                val assocArrayActualValue = records.getAtt(record, "assocArray[]?id").asStrList()
                if (deletedRecords.contains(record)) {
                    assertThat(assocActualValue).isEmpty()
                    assertThat(assocArrayActualValue).isEmpty()
                } else {
                    assertThat(assocActualValue).isEqualTo(assocValues[idx])
                    assertThat(assocArrayActualValue)
                        .containsExactlyElementsOf(assocArrayValues[idx]?.filterNotNull() ?: emptyList())
                }
            }
        }
        checkAssocValues()

        records.delete(createdRecords[5])
        deletedRecords.add(createdRecords[5])

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

        dbDataSource.withTransaction(false) {
            dbDataSource.updateSchema("ALTER TABLE ${tableRef.fullName} DROP COLUMN ${DbEntity.REF_ID}")
            dbDataSource.updateSchema("ALTER TABLE ${tableRef.fullName.dropLast(1)}__ext_txn\" DROP COLUMN ${DbEntity.REF_ID}")
            dbDataSource.updateSchema("DROP TABLE \"${tableRef.schema}\".\"ecos_record_ref\"")
        }
        dbRecordRefDataService.resetColumnsCache()
        dataService.resetColumnsCache()

        printQueryRes("SELECT * from ${tableRef.fullName}")

        RequestContext.doWithTxn {
            AuthContext.runAs("admin", listOf(AuthRole.ADMIN)) {
                val emptyRef = RecordRef.create(recordsDao.getId(), "")
                val atts = RecordAtts(emptyRef)
                atts.setAtt("__runAssocsMigration", true)
                atts.setAtt("_type", REC_TEST_TYPE_ID)
                records.mutate(atts)
            }
        }

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
