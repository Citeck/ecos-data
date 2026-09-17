package ru.citeck.ecos.data.sql.pg.records.commons

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.MethodOrderer
import org.junit.jupiter.api.Order
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestMethodOrder
import ru.citeck.ecos.data.sql.test.records.DbRecordsTestBase
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef

/**
 * The schema cache's own contract, asserted from the outside: every claim below holds both with
 * the cache on and with `-Decos.data.test.reuseSchema=false`, because the cache's entire promise
 * is that a test cannot tell which mode it is running in. A test that passed in only one mode
 * would be a finding, not a reason to exclude it.
 *
 * Ordered on purpose - what is under test is what one test leaves for the next one, which is
 * exactly the thing an unordered suite cannot express.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation::class)
class DbTestSchemaCacheTest : DbRecordsTestBase() {

    private fun createOneRecordAndReturnItsFirstRefId(): Any? {
        registerAtts(listOf(AttributeDef.create { withId("someAtt") }))
        createRecord("someAtt" to "value")
        return selectFieldFromDbTable("id", "\"records-test-schema\".\"ed_record_ref\"", "1=1 ORDER BY id LIMIT 1")
    }

    private fun countInCatalog(query: String): Any? {
        return selectFieldFromDbTable("cnt", "(SELECT count(*) AS cnt FROM $query) t", "1=1")
    }

    @Test
    @Order(1)
    fun aFirstTestSeesAFreshSchema() {
        assertThat(createOneRecordAndReturnItsFirstRefId()).isEqualTo(1L)
    }

    @Test
    @Order(2)
    fun theNextTestSeesTheCountersBackAtTheStart() {
        // The reused schema still holds the ed_record_ref rows of the previous test unless the
        // cache put both the rows and the identity counter back where a fresh schema has them.
        assertThat(createOneRecordAndReturnItsFirstRefId()).isEqualTo(1L)
    }

    @Test
    @Order(3)
    fun aTestMayChangeTheSchemaStructurallyAndLeaveAnOrphanSequenceBehind() {
        createOneRecordAndReturnItsFirstRefId()
        // A column on a cached system table: the cache must notice and stop trusting itself.
        sqlUpdate("ALTER TABLE \"records-test-schema\".\"ed_record_ref\" ADD COLUMN \"probe_col\" VARCHAR")
        // A sequence owned by no column: dropping every table does not remove it, so the drop-based
        // fallback used to hand it to the next test, which then captured it *as the new pristine
        // baseline* and kept it for the rest of the JVM.
        sqlUpdate("CREATE SEQUENCE \"records-test-schema\".\"probe_seq\"")
    }

    @Test
    @Order(4)
    fun andTheTestAfterItStillSeesAFreshSchema() {
        assertThat(createOneRecordAndReturnItsFirstRefId()).isEqualTo(1L)
        assertThat(getColumns().map { it.name }).contains("someAtt")
        assertThat(
            countInCatalog(
                "information_schema.columns WHERE table_name='ed_record_ref' AND column_name='probe_col'"
            )
        ).describedAs("the column the previous test added must be gone").isEqualTo(0L)
        assertThat(countInCatalog("information_schema.sequences WHERE sequence_name='probe_seq'"))
            .describedAs("the sequence the previous test created must be gone").isEqualTo(0L)
    }

    @Test
    @Order(5)
    fun andTheCacheIsUsableAgainAfterItWasDropped() {
        assertThat(createOneRecordAndReturnItsFirstRefId()).isEqualTo(1L)
        assertThat(countInCatalog("information_schema.sequences WHERE sequence_name='probe_seq'"))
            .describedAs("the re-captured baseline must not have adopted the orphan")
            .isEqualTo(0L)
    }
}
