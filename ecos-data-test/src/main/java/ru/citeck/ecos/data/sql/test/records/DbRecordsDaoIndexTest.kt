package ru.citeck.ecos.data.sql.test.records

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import ru.citeck.ecos.webapp.api.entity.EntityRef

/**
 * A model change arrives as a type id, and the only way from there to the tables that store it is
 * the index of live records DAOs. What is at stake here is that the index is filled on the path
 * both production and these tests take - [ru.citeck.ecos.data.sql.records.DbRecordsDao
 * .setRecordsServiceFactory] - rather than in `DbDomainFactory`, which no test of this repository
 * ever builds.
 */
class DbRecordsDaoIndexTest : DbRecordsTestBase() {

    private val index get() = dataSourceCtx.recordsDaoIndex

    @Test
    fun daoRegisteredOnTheTestPathIsIndexedTest() {

        assertThat(index.getByTypeId(REC_TEST_TYPE_ID)).contains(mainCtx.dao)
        assertThat(index.getAll()).contains(mainCtx.dao)
        assertThat(index.getTypeIds()).contains(REC_TEST_TYPE_ID)
    }

    @Test
    fun twoDaoOfTheSameTypeAreBothIndexedTest() {

        val secondCtx = createRecordsDao(
            DEFAULT_TABLE_REF.withTable("index-test-second-table"),
            REC_TEST_TYPE_REF,
            "index-test-second-dao"
        )

        assertThat(index.getByTypeId(REC_TEST_TYPE_ID))
            .containsExactlyInAnyOrder(mainCtx.dao, secondCtx.dao)
    }

    /**
     * The check that matters: a weak reference alone would not drop this entry, because the test
     * factory keeps every DAO it created in a list of its own - exactly as
     * `DbDomainFactory.recordsDaoWithoutDefaultContentStorage` does in production. `dropped` below
     * is a hard reference held for the whole test for the same reason: it makes the weak-reference
     * path provably unable to pass this test, so what is left is the check against the records
     * service.
     */
    @Test
    fun daoUnregisteredFromTheRecordsServiceLeavesTheIndexTest() {

        val secondCtx = createRecordsDao(
            DEFAULT_TABLE_REF.withTable("index-test-dropped-table"),
            REC_TEST_TYPE_REF,
            "index-test-dropped-dao"
        )
        val dropped = secondCtx.dao
        assertThat(index.getByTypeId(REC_TEST_TYPE_ID)).contains(dropped)

        records.unregister(dropped.getId())

        // no System.gc() here on purpose - see the comment above
        assertThat(index.getByTypeId(REC_TEST_TYPE_ID)).containsExactly(mainCtx.dao)
        assertThat(index.getAll()).doesNotContain(dropped)
    }

    @Test
    fun unknownTypeIdGivesAnEmptyListTest() {

        assertThat(index.getByTypeId("unknown-type-id")).isEmpty()
    }

    /**
     * A DAO without a type is unreachable from a model change anyway, and indexing it under the
     * empty id would glue every such DAO into one group.
     */
    @Test
    fun daoWithoutTypeIsNotIndexedTest() {

        val typeLessCtx = createRecordsDao(
            DEFAULT_TABLE_REF.withTable("index-test-type-less-table"),
            EntityRef.EMPTY,
            "index-test-type-less-dao"
        )

        assertThat(index.getAll()).doesNotContain(typeLessCtx.dao)
        assertThat(index.getTypeIds()).doesNotContain("")
    }
}
