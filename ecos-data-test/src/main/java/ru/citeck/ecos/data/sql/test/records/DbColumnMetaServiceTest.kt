package ru.citeck.ecos.data.sql.test.records

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import ru.citeck.ecos.data.sql.columnmeta.DbColumnMetaDto
import ru.citeck.ecos.data.sql.columnmeta.DbColumnSemanticType
import ru.citeck.ecos.data.sql.dto.DbColumnType
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType
import java.time.Instant

/**
 * The column registry itself: it has to exist on every schema, survive a round trip of both
 * semantic type forms, and be scoped per table.
 */
// tableExistsOnAFreshSchemaTest asserts that ed_column_meta is created together with the schema.
// On a cached schema it exists because an earlier test created it, so the assertion would hold
// without the behaviour it is meant to pin down still being there.
@RequiresFreshSchema
class DbColumnMetaServiceTest : DbRecordsTestBase() {

    @Test
    fun savedRowsComeBackPerTableTest() {

        registerAtts(listOf(AttributeDef.create { withId("someAtt") }))
        createRecord("someAtt" to "value")

        val service = getTableCtx().getSchemaCtx().columnMetaService

        // "manualColumn" is not a registered model attribute, so nothing auto-seeds a row for it -
        // unlike "someAtt", which createRecord() above already caused to be seeded (DbColumnMetaSeedTest
        // covers that path; this test is about the round trip and the per-table scoping)
        val first = service.save(
            DbColumnMetaDto(
                id = DbColumnMetaDto.NEW_REC_ID,
                table = tableRef.table,
                columnName = "manualColumn",
                attId = "manualColumn",
                attType = DbColumnSemanticType.Model(AttributeType.TEXT),
                multiple = false,
                backup = false,
                created = Instant.EPOCH,
                creator = "tester"
            )
        )
        assertThat(first.id).isNotEqualTo(DbColumnMetaDto.NEW_REC_ID)

        service.save(
            DbColumnMetaDto(
                id = DbColumnMetaDto.NEW_REC_ID,
                table = "other-table",
                columnName = "manualColumn",
                attId = "manualColumn",
                attType = DbColumnSemanticType.Raw(DbColumnType.LONG),
                multiple = true,
                backup = true,
                created = Instant.EPOCH,
                creator = "tester"
            )
        )

        // two rows now: "someAtt" auto-seeded by createRecord() above, plus the manually-inserted
        // "manualColumn" - this test's job is the round trip and per-table scoping of the latter
        val forMainTable = service.getByTable(tableRef.table)
        assertThat(forMainTable).hasSize(2)
        val manualColumnRow = forMainTable.first { it.columnName == "manualColumn" }
        assertThat(manualColumnRow.attType)
            .isEqualTo(DbColumnSemanticType.Model(AttributeType.TEXT))
        assertThat(manualColumnRow.multiple).isFalse()
        assertThat(manualColumnRow.backup).isFalse()
        assertThat(manualColumnRow.creator).isEqualTo("tester")

        val forOtherTable = service.getByTable("other-table")
        assertThat(forOtherTable).hasSize(1)
        assertThat(forOtherTable[0].attType).isEqualTo(DbColumnSemanticType.Raw(DbColumnType.LONG))
        assertThat(forOtherTable[0].multiple).isTrue()
        assertThat(forOtherTable[0].backup).isTrue()
    }

    @Test
    fun tableExistsOnAFreshSchemaTest() {

        registerAtts(listOf(AttributeDef.create { withId("someAtt") }))
        createRecord("someAtt" to "value")

        // the table is created together with the schema, not on first write. Asserted through the
        // schema dao rather than through an empty read: a read of a missing table comes back empty
        // anyway, because an unknown column turns the predicate into "always false".
        assertThat(
            dbDataSource.withTransaction(true) {
                dbSchemaDao.isTableExists(dbDataSource, tableRef.withTable("ed_column_meta"))
            }
        ).isTrue()

        // per-table scoping: a table nobody has touched has no rows. tableRef.table itself is no
        // longer empty here - createRecord() above just seeded "someAtt" into it, which is exactly
        // the behaviour DbColumnMetaSeedTest exists to cover
        assertThat(getTableCtx().getSchemaCtx().columnMetaService.getByTable("never-touched-table"))
            .isEmpty()
    }
}
