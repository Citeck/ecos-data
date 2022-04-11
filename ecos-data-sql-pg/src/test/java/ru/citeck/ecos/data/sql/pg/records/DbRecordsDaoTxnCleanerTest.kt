package ru.citeck.ecos.data.sql.pg.records

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import ru.citeck.ecos.data.sql.dto.DbColumnDef
import ru.citeck.ecos.data.sql.dto.DbColumnType
import ru.citeck.ecos.data.sql.job.DbJobsProvider
import ru.citeck.ecos.data.sql.repo.entity.DbEntity
import ru.citeck.ecos.data.sql.service.DbDataServiceImpl
import ru.citeck.ecos.data.sql.service.job.txn.TxnDataCleaner
import ru.citeck.ecos.data.sql.txn.ExtTxnContext
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.records3.record.request.RequestContext
import java.util.*

class DbRecordsDaoTxnCleanerTest : DbRecordsTestBase() {

    @Test
    fun testMetaFields() {

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("testStr")
                }
            )
        )

        val ref = createRecord("testStr" to "")

        val columns = listOf(
            DbColumnDef.create {
                withName("testStr")
                withType(DbColumnType.TEXT)
            }
        )

        val entity = DbEntity()
        entity.extId = ref.id
        entity.attributes["testStr"] = "abc"

        val txnId = UUID.randomUUID()
        val newEntity = ExtTxnContext.withExtTxn(txnId, false) {
            dataService.save(entity, columns)
        }

        val newEntity2 = newEntity.copy()
        val newStrValue = "abcdef"
        newEntity2.attributes["testStr"] = newStrValue
        newEntity2.attributes.remove(DbDataServiceImpl.COLUMN_EXT_TXN_ID)

        val assertThrowsAlreadyUpdatedException = { action: () -> Unit ->
            assertThrows<Exception> {
                action.invoke()
            }
        }

        assertThrowsAlreadyUpdatedException {
            ExtTxnContext.withExtTxn(UUID.randomUUID(), false) {
                dataService.save(newEntity2, columns)
            }
        }

        val updateRef = {
            RequestContext.doWithTxn {
                updateRecord(
                    ref,
                    "_type" to REC_TEST_TYPE_REF,
                    "testStr" to newStrValue
                )
            }
        }

        assertThrowsAlreadyUpdatedException {
            updateRef()
        }

        Thread.sleep(1000)

        val txnCleaner = (dataService as DbJobsProvider).getJobs().first {
            it is TxnDataCleaner<*>
        } as TxnDataCleaner<*>

        txnCleaner.execute()

        // We should clean only records that changed earlier than {txnCleaner.getConfig().txnRecordLifeTimeMs} ms ago
        // with default config txnCleaner should not clean our test record
        assertThrowsAlreadyUpdatedException {
            updateRef()
        }

        val customCleaner = txnCleaner.getCleanerWithConfig(
            txnCleaner.getConfig()
                .copy()
                .withTxnRecordLifeTimeMs(0)
                .build()
        )
        // Clean with txnRecordLifeTime == 0
        customCleaner.execute()

        printQueryRes("SELECT * from ${tableRef.fullName};")
        printQueryRes("SELECT * from ${tableRef.fullName.dropLast(1)}__ext_txn\";")

        updateRef()

        val newValue = records.getAtt(ref, "testStr").asText()
        assertThat(newValue).isEqualTo(newStrValue)
    }
}
