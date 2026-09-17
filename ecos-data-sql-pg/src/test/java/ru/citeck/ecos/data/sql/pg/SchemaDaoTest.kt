package ru.citeck.ecos.data.sql.pg

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import ru.citeck.ecos.context.lib.ctx.GlobalEcosContext
import ru.citeck.ecos.data.sql.context.DbDataSourceContext
import ru.citeck.ecos.data.sql.datasource.DbDataSource
import ru.citeck.ecos.data.sql.domain.migration.DbMigrationService
import ru.citeck.ecos.data.sql.dto.DbColumnConstraint
import ru.citeck.ecos.data.sql.dto.DbColumnDef
import ru.citeck.ecos.data.sql.dto.DbColumnIndexDef
import ru.citeck.ecos.data.sql.dto.DbColumnType
import ru.citeck.ecos.data.sql.dto.DbIndexDef
import ru.citeck.ecos.data.sql.dto.DbTableRef
import ru.citeck.ecos.test.commons.EcosWebAppApiMock
import java.sql.SQLException

class SchemaDaoTest {

    @Test
    fun test() {
        val schemaCommands = PgUtils.withDbDataSource { testImpl(it) }
        schemaCommands.forEach { println(it) }
    }

    private fun testImpl(dataSource: DbDataSource) {

        val dsCtx = DbDataSourceContext(
            dataSource,
            PgDataServiceFactory(),
            DbMigrationService(),
            EcosWebAppApiMock("test"),
            GlobalEcosContext.getContext()
        )
        val tableRef = DbTableRef("some-schema", "test-table")

        val dbSchemaDao = dsCtx.schemaDao
        assertThat(dbSchemaDao.getColumns(dataSource, tableRef)).isEmpty()

        val singleValueColumns = DbColumnType.entries.mapIndexed { idx, value ->
            DbColumnDef.create {
                withName("column_$idx")
                withType(value)
                withMultiple(false)
            }
        }
        dbSchemaDao.createTable(dataSource, tableRef, singleValueColumns)
        val columnsFromDb = dbSchemaDao.getColumns(dataSource, tableRef)
        assertThat(columnsFromDb).containsExactlyInAnyOrderElementsOf(singleValueColumns)

        val arrayColumns = DbColumnType.entries
            .filter { it != DbColumnType.BIGSERIAL }
            .filter { it != DbColumnType.JSON }
            .mapIndexed { idx, value ->
                DbColumnDef.create {
                    withName("column_arr_$idx")
                    withType(value)
                    withMultiple(true)
                }
            }

        dbSchemaDao.addColumns(dataSource, tableRef, arrayColumns)

        val allColumnsFromDb = dbSchemaDao.getColumns(dataSource, tableRef)
        assertThat(allColumnsFromDb).containsExactlyInAnyOrderElementsOf(
            listOf(*singleValueColumns.toTypedArray(), *arrayColumns.toTypedArray())
        )

        val indexCommands = dataSource.watchSchemaCommands {
            dbSchemaDao.addColumns(
                dataSource,
                tableRef,
                listOf(
                    DbColumnDef.create {
                        withName("indexed_str")
                        withType(DbColumnType.TEXT)
                        withIndex(DbColumnIndexDef(true))
                    },
                    DbColumnDef.create {
                        withName("indexed_str_arr")
                        withType(DbColumnType.TEXT)
                        withIndex(DbColumnIndexDef(true))
                        withMultiple(true)
                    }
                )
            )
        }.filter { it.contains("INDEX") }

        assertThat(indexCommands).hasSize(2)
        assertThat(indexCommands).allMatch { it.contains("CREATE INDEX") }
        assertThat(indexCommands).anyMatch { it.contains("indexed_str") && !it.contains("GIN") }
        assertThat(indexCommands).anyMatch { it.contains("indexed_str_arr") && it.contains("GIN") }
    }

    @Test
    fun typeUpdateTest() {
        PgUtils.withDbDataSource { typeUpdateTestImpl(it) }
    }

    /**
     * Pins the constant to the database: PostgreSQL keeps NAMEDATALEN - 1 bytes of an identifier
     * and silently truncates the rest, so a longer column comes back from the metadata under the
     * truncated name. If NAMEDATALEN ever changes, this test tells us to revisit the limit.
     */
    @Test
    fun columnNameLimitMatchesPostgresTest() {
        PgUtils.withDbDataSource { columnNameLimitMatchesPostgresTestImpl(it) }
    }

    private fun columnNameLimitMatchesPostgresTestImpl(dataSource: DbDataSource) {

        val dsCtx = DbDataSourceContext(
            dataSource,
            PgDataServiceFactory(),
            DbMigrationService(),
            EcosWebAppApiMock("test"),
            GlobalEcosContext.getContext()
        )
        val tableRef = DbTableRef("some-schema", "long-column-table")

        val dbSchemaDao = dsCtx.schemaDao
        val limit = dbSchemaDao.getMaxColumnNameBytes()
        assertThat(limit).isEqualTo(DbSchemaDaoPg.MAX_COLUMN_NAME_BYTES)

        val tooLongName = "c".repeat(limit + 1)
        dbSchemaDao.createTable(
            dataSource,
            tableRef,
            listOf(
                DbColumnDef.create {
                    withName(tooLongName)
                    withType(DbColumnType.TEXT)
                }
            )
        )

        val columnNames = dbSchemaDao.getColumns(dataSource, tableRef).map { it.name }
        assertThat(columnNames).containsExactly(tooLongName.substring(0, limit))
    }

    private fun typeUpdateTestImpl(dataSource: DbDataSource) {

        val dsCtx = DbDataSourceContext(
            dataSource,
            PgDataServiceFactory(),
            DbMigrationService(),
            EcosWebAppApiMock("test"),
            GlobalEcosContext.getContext()
        )
        val tableRef = DbTableRef("some-schema", "test-table")

        val dbSchemaDao = dsCtx.schemaDao
        dbSchemaDao.createTable(
            dataSource,
            tableRef,
            listOf(
                DbColumnDef.create {
                    withName("text_column")
                    withType(DbColumnType.TEXT)
                },
                DbColumnDef.create {
                    withName("date_column")
                    withType(DbColumnType.DATE)
                },
                DbColumnDef.create {
                    withName("long_column")
                    withType(DbColumnType.LONG)
                },
                DbColumnDef.create {
                    withName("int_column")
                    withType(DbColumnType.INT)
                },
                DbColumnDef.create {
                    withName("double_column")
                    withType(DbColumnType.DOUBLE)
                }
            )
        )

        // text to json

        val columns0 = dbSchemaDao.getColumns(dataSource, tableRef)
        assertThat(columns0).hasSize(5)
        assertThat(columns0.map { it.type }).containsExactly(
            DbColumnType.TEXT,
            DbColumnType.DATE,
            DbColumnType.LONG,
            DbColumnType.INT,
            DbColumnType.DOUBLE
        )

        dbSchemaDao.setColumnType(dataSource, tableRef, "text_column", false, DbColumnType.JSON)
        assertThat(dbSchemaDao.getColumns(dataSource, tableRef)[0].type).isEqualTo(DbColumnType.JSON)

        dbSchemaDao.setColumnType(dataSource, tableRef, "text_column", false, DbColumnType.TEXT)
        assertThat(dbSchemaDao.getColumns(dataSource, tableRef)[0].type).isEqualTo(DbColumnType.TEXT)

        dbSchemaDao.setColumnType(dataSource, tableRef, "date_column", false, DbColumnType.DATETIME)
        assertThat(dbSchemaDao.getColumns(dataSource, tableRef)[1].type).isEqualTo(DbColumnType.DATETIME)

        dbSchemaDao.setColumnType(dataSource, tableRef, "long_column", false, DbColumnType.TEXT)
        assertThat(dbSchemaDao.getColumns(dataSource, tableRef)[2].type).isEqualTo(DbColumnType.TEXT)
        dbSchemaDao.setColumnType(dataSource, tableRef, "int_column", false, DbColumnType.TEXT)
        assertThat(dbSchemaDao.getColumns(dataSource, tableRef)[3].type).isEqualTo(DbColumnType.TEXT)
        dbSchemaDao.setColumnType(dataSource, tableRef, "double_column", false, DbColumnType.TEXT)
        assertThat(dbSchemaDao.getColumns(dataSource, tableRef)[4].type).isEqualTo(DbColumnType.TEXT)
    }

    @Test
    fun schemaMismatchErrorsAreRecognizedTest() {
        PgUtils.withDbDataSource { schemaMismatchErrorsAreRecognizedTestImpl(it) }
    }

    /**
     * Pins the four PostgreSQL error classes that mean "the cached column set is stale" - the two
     * structural ones the old message regex covered, and the two type ones it did not. The negative
     * cases matter as much: a reset on every SQL error would hide real bugs behind a silent retry.
     */
    private fun schemaMismatchErrorsAreRecognizedTestImpl(dataSource: DbDataSource) {

        val dsCtx = DbDataSourceContext(
            dataSource,
            PgDataServiceFactory(),
            DbMigrationService(),
            EcosWebAppApiMock("test"),
            GlobalEcosContext.getContext()
        )
        val tableRef = DbTableRef("some-schema", "mismatch-table")
        val dbSchemaDao = dsCtx.schemaDao

        // setup runs in its own committed transaction (a new physical connection, not the one this
        // whole test still holds open): the probes below use requiresNew=true to get an isolated
        // transaction each, and a new connection can never see this table if it is only sitting
        // uncommitted in the enclosing one
        dataSource.withTransaction(false, true) {
            dbSchemaDao.createTable(
                dataSource,
                tableRef,
                listOf(
                    DbColumnDef.create {
                        withName("long_column")
                        withType(DbColumnType.LONG)
                    },
                    DbColumnDef.create {
                        withName("not_null_column")
                        withType(DbColumnType.TEXT)
                        withConstraints(listOf(DbColumnConstraint.NOT_NULL))
                    }
                )
            )
        }

        // each probe gets its own transaction: a failed statement poisons the one it ran in, and
        // the next probe must not inherit that state
        val errorOf = { query: String ->
            val error = runCatching {
                dataSource.withTransaction(false, true) { dataSource.update(query, emptyList()) }
            }.exceptionOrNull()
            assertThat(error).describedAs("query was expected to fail: %s", query).isNotNull()
            error!!
        }

        // 42P01 undefined_table
        assertThat(dbSchemaDao.isSchemaMismatchError(errorOf("SELECT 1 FROM \"some-schema\".\"no-such-table\""))).isTrue()
        // 42703 undefined_column
        assertThat(dbSchemaDao.isSchemaMismatchError(errorOf("SELECT \"no_such_column\" FROM ${tableRef.fullName}"))).isTrue()
        // 42804 datatype_mismatch
        assertThat(
            dbSchemaDao.isSchemaMismatchError(
                errorOf("INSERT INTO ${tableRef.fullName} (\"long_column\") VALUES ('not-a-number'::varchar)")
            )
        ).isTrue()
        // 42883 undefined_function - "operator does not exist: bigint = character varying"
        assertThat(
            dbSchemaDao.isSchemaMismatchError(
                errorOf("SELECT 1 FROM ${tableRef.fullName} WHERE \"long_column\" = 'x'::varchar")
            )
        ).isTrue()

        // 42601 syntax_error - a bug in our SQL, not a stale cache
        assertThat(dbSchemaDao.isSchemaMismatchError(errorOf("SELECT FROM WHERE"))).isFalse()
        // 23502 not_null_violation - a data problem, not a stale cache
        assertThat(
            dbSchemaDao.isSchemaMismatchError(
                errorOf("INSERT INTO ${tableRef.fullName} (\"long_column\") VALUES (1)")
            )
        ).isFalse()
        // anything that is not an SQLException at all
        assertThat(dbSchemaDao.isSchemaMismatchError(IllegalStateException("boom"))).isFalse()
        // the SQLState may sit on a cause rather than on the thrown exception
        assertThat(
            dbSchemaDao.isSchemaMismatchError(
                RuntimeException("wrapped", SQLException("undefined column", "42703"))
            )
        ).isTrue()
        // the SQLState may also sit on a chained nextException rather than on the exception itself
        assertThat(
            dbSchemaDao.isSchemaMismatchError(
                SQLException("syntax error", "42601").apply {
                    setNextException(SQLException("undefined column", "42703"))
                }
            )
        ).isTrue()
    }

    @Test
    fun valuePreservingConversionsTest() {
        PgUtils.withDbDataSource { valuePreservingConversionsTestImpl(it) }
    }

    /**
     * Conversions are only worth anything if the values survive them, so this asserts on the data
     * and not on the column type alone. The date formats are pinned on purpose: a bare `::text`
     * would follow the session's DateStyle/TimeZone, and the platform reads these columns back as
     * ISO-8601.
     */
    private fun valuePreservingConversionsTestImpl(dataSource: DbDataSource) {

        val dsCtx = DbDataSourceContext(
            dataSource,
            PgDataServiceFactory(),
            DbMigrationService(),
            EcosWebAppApiMock("test"),
            GlobalEcosContext.getContext()
        )
        val tableRef = DbTableRef("some-schema", "conversion-table")
        val dbSchemaDao = dsCtx.schemaDao

        dbSchemaDao.createTable(
            dataSource,
            tableRef,
            listOf(
                DbColumnDef.create {
                    withName("date_column")
                    withType(DbColumnType.DATE)
                },
                DbColumnDef.create {
                    withName("datetime_column")
                    withType(DbColumnType.DATETIME)
                },
                DbColumnDef.create {
                    withName("datetime_frac_column")
                    withType(DbColumnType.DATETIME)
                },
                DbColumnDef.create {
                    withName("date_arr_column")
                    withType(DbColumnType.DATE)
                    withMultiple(true)
                },
                DbColumnDef.create {
                    withName("long_arr_column")
                    withType(DbColumnType.LONG)
                    withMultiple(true)
                },
                DbColumnDef.create {
                    withName("text_arr_column")
                    withType(DbColumnType.TEXT)
                    withMultiple(true)
                }
            )
        )

        dataSource.withTransaction(false) {
            dataSource.update(
                "INSERT INTO ${tableRef.fullName} " +
                    "(\"date_column\", \"datetime_column\", \"datetime_frac_column\", " +
                    "\"date_arr_column\", \"long_arr_column\", \"text_arr_column\") VALUES " +
                    "('2021-03-04'::date, '2021-03-04T05:06:07Z'::timestamptz, " +
                    "'2021-03-04T05:06:07.123Z'::timestamptz, " +
                    "'{2021-03-04,2021-03-05}'::date[], '{10,20}'::int8[], '{a,b}'::varchar[])",
                emptyList()
            )
        }

        val readText = { column: String ->
            dataSource.withTransaction(true) {
                dataSource.query("SELECT \"$column\"::text AS res FROM ${tableRef.fullName}", emptyList()) { rs ->
                    rs.next()
                    rs.getString("res")
                }
            }
        }

        val epochOf = { column: String, index: Int ->
            dataSource.withTransaction(true) {
                dataSource.query(
                    "SELECT extract(epoch from \"$column\"[$index]) AS res FROM ${tableRef.fullName}",
                    emptyList()
                ) { rs ->
                    rs.next()
                    rs.getDouble("res").toLong()
                }
            }
        }

        // DATE -> TEXT, pinned to YYYY-MM-DD regardless of the session DateStyle
        dbSchemaDao.setColumnType(dataSource, tableRef, "date_column", false, DbColumnType.TEXT)
        assertThat(readText("date_column")).isEqualTo("2021-03-04")

        // DATETIME -> TEXT, pinned to the ISO-8601 form the platform reads back
        dbSchemaDao.setColumnType(dataSource, tableRef, "datetime_column", false, DbColumnType.TEXT)
        assertThat(readText("datetime_column")).isEqualTo("2021-03-04T05:06:07Z")

        // same rule, but with sub-second precision: this is the lossless class, so a whole-second
        // pattern that would silently drop the fractional digits is not an acceptable expression
        dbSchemaDao.setColumnType(dataSource, tableRef, "datetime_frac_column", false, DbColumnType.TEXT)
        assertThat(readText("datetime_frac_column")).isEqualTo("2021-03-04T05:06:07.123Z")

        // this pair is reported as supported today but generates invalid SQL. The session
        // zone is deliberately pinned to something other than UTC first, so this assertion cannot
        // pass by accident on a UTC host: if the conversion's own SET LOCAL TO 'UTC' were removed,
        // the cast would pick up America/New_York instead and the epoch values below would not
        // match midnight UTC.
        dataSource.updateSchema("SET LOCAL TimeZone TO 'America/New_York';")
        dbSchemaDao.setColumnType(dataSource, tableRef, "date_arr_column", true, DbColumnType.DATETIME)
        assertThat(dbSchemaDao.getColumns(dataSource, tableRef).first { it.name == "date_arr_column" })
            .satisfies({ assertThat(it.type).isEqualTo(DbColumnType.DATETIME) })
            .satisfies({ assertThat(it.multiple).isTrue() })
        // zone-agnostic: extract(epoch ...) reads the stored instant, not a display rendering, so
        // this is the assertion that must fail if the conversion's SET LOCAL disappears
        assertThat(epochOf("date_arr_column", 1))
            .describedAs("2021-03-04T00:00:00Z")
            .isEqualTo(1614816000L)
        assertThat(epochOf("date_arr_column", 2))
            .describedAs("2021-03-05T00:00:00Z")
            .isEqualTo(1614902400L)
        // display assertion retained for readability - it only holds because the conversion's own
        // SET LOCAL TO 'UTC' is still in force for the rest of this transaction (see the comment at
        // that call site); by itself it would not catch a regression on a host whose default zone
        // happens to already be UTC, which is exactly why the epoch assertions above exist too
        assertThat(readText("date_arr_column"))
            .describedAs("midnight UTC, not midnight in whatever zone the session happened to use")
            .isEqualTo("{\"2021-03-04 00:00:00+00\",\"2021-03-05 00:00:00+00\"}")

        // plain array cast
        dbSchemaDao.setColumnType(dataSource, tableRef, "long_arr_column", true, DbColumnType.TEXT)
        assertThat(readText("long_arr_column")).isEqualTo("{10,20}")
    }

    @Test
    fun unsupportedConversionsFailWithANamedErrorTest() {
        PgUtils.withDbDataSource { unsupportedConversionsFailWithANamedErrorTestImpl(it) }
    }

    /**
     * A conversion the backend cannot express in place must say so. Before this change the same
     * pairs produced a PostgreSQL syntax error from a malformed `USING` clause, which told an
     * operator nothing about what to do.
     */
    private fun unsupportedConversionsFailWithANamedErrorTestImpl(dataSource: DbDataSource) {

        val dsCtx = DbDataSourceContext(
            dataSource,
            PgDataServiceFactory(),
            DbMigrationService(),
            EcosWebAppApiMock("test"),
            GlobalEcosContext.getContext()
        )
        val tableRef = DbTableRef("some-schema", "unsupported-conversion-table")
        val dbSchemaDao = dsCtx.schemaDao

        dbSchemaDao.createTable(
            dataSource,
            tableRef,
            listOf(
                DbColumnDef.create {
                    withName("text_arr_column")
                    withType(DbColumnType.TEXT)
                    withMultiple(true)
                },
                DbColumnDef.create {
                    withName("text_column")
                    withType(DbColumnType.TEXT)
                },
                DbColumnDef.create {
                    withName("json_column")
                    withType(DbColumnType.JSON)
                }
            )
        )

        val textArr = DbColumnDef.create {
            withName("text_arr_column")
            withType(DbColumnType.TEXT)
            withMultiple(true)
        }
        val jsonTarget = DbColumnDef.create {
            withName("text_arr_column")
            withType(DbColumnType.JSON)
        }
        assertThat(dbSchemaDao.isTypeChangeSupported(textArr, jsonTarget)).isFalse()

        val error = assertThrows<Exception> {
            dataSource.withTransaction(false) {
                dbSchemaDao.setColumnType(dataSource, tableRef, "text_arr_column", false, DbColumnType.JSON)
            }
        }
        assertThat(error.message)
            .contains("text_arr_column")
            .contains("TEXT")
            .contains("JSON")

        // JSON is always physically scalar (see isArrayConversion in DbSchemaDaoPg), so a JSON
        // source has nothing to widen from when the target wants an array. The mutator used to
        // reach for a same-type widening ALTER here and get a type mismatch from PostgreSQL
        // instead of a named error; the probe and the mutator must now agree it is unsupported.
        val jsonSingle = DbColumnDef.create {
            withName("json_column")
            withType(DbColumnType.JSON)
        }
        val textArrTarget = DbColumnDef.create {
            withName("json_column")
            withType(DbColumnType.TEXT)
            withMultiple(true)
        }
        assertThat(dbSchemaDao.isTypeChangeSupported(jsonSingle, textArrTarget)).isFalse()

        val jsonToTextArrError = assertThrows<Exception> {
            dataSource.withTransaction(false) {
                dbSchemaDao.setColumnType(dataSource, tableRef, "json_column", true, DbColumnType.TEXT)
            }
        }
        assertThat(jsonToTextArrError.message)
            .contains("json_column")
            .contains("JSON")
            .contains("TEXT")

        // TEXT -> LONG has never been supported and still is not; the message must name the pair
        val textSingle = DbColumnDef.create {
            withName("text_column")
            withType(DbColumnType.TEXT)
        }
        val longTarget = DbColumnDef.create {
            withName("text_column")
            withType(DbColumnType.LONG)
        }
        assertThat(dbSchemaDao.isTypeChangeSupported(textSingle, longTarget)).isFalse()

        // and the pairs that do work report as supported
        assertThat(
            dbSchemaDao.isTypeChangeSupported(
                textSingle,
                DbColumnDef.create {
                    withName("text_column")
                    withType(DbColumnType.JSON)
                }
            )
        ).isTrue()
    }

    @Test
    fun renameDropsOnlyTheIndexesOfTheRenamedColumnTest() {
        PgUtils.withDbDataSource { renameDropsOnlyTheIndexesOfTheRenamedColumnTestImpl(it) }
    }

    /**
     * A backup column keeps its data and loses its indexes. Composite indexes are not this column's
     * to drop, and neither is the primary key.
     */
    private fun renameDropsOnlyTheIndexesOfTheRenamedColumnTestImpl(dataSource: DbDataSource) {

        val dsCtx = DbDataSourceContext(
            dataSource,
            PgDataServiceFactory(),
            DbMigrationService(),
            EcosWebAppApiMock("test"),
            GlobalEcosContext.getContext()
        )
        val tableRef = DbTableRef("some-schema", "rename-table")
        val dbSchemaDao = dsCtx.schemaDao

        dbSchemaDao.createTable(
            dataSource,
            tableRef,
            listOf(
                DbColumnDef.create {
                    withName("id")
                    withType(DbColumnType.BIGSERIAL)
                    withConstraints(listOf(DbColumnConstraint.PRIMARY_KEY))
                },
                DbColumnDef.create {
                    // single-value TEXT is indexed as LOWER("indexed_text") - an expression index
                    withName("indexed_text")
                    withType(DbColumnType.TEXT)
                    withIndex(DbColumnIndexDef(true))
                },
                DbColumnDef.create {
                    // multiple TEXT is indexed with GIN on the plain column - a key index
                    withName("indexed_arr")
                    withType(DbColumnType.TEXT)
                    withMultiple(true)
                    withIndex(DbColumnIndexDef(true))
                },
                DbColumnDef.create {
                    withName("other_text")
                    withType(DbColumnType.TEXT)
                },
                DbColumnDef.create {
                    // named to collide with the deparsed cast "lower((indexed_text)::text)" - a
                    // regex over that expression text would match "text" inside it and drop
                    // indexed_text's own index when this unrelated column is renamed
                    withName("text")
                    withType(DbColumnType.TEXT)
                    withIndex(DbColumnIndexDef(true))
                }
            )
        )
        dbSchemaDao.createIndexes(
            dataSource,
            tableRef,
            listOf(DbIndexDef.create { withColumns(listOf("indexed_text", "other_text")) })
        )

        dataSource.withTransaction(false) {
            dataSource.update(
                "INSERT INTO ${tableRef.fullName} (\"indexed_text\", \"indexed_arr\", \"other_text\") " +
                    "VALUES ('kept', '{a,b}'::varchar[], 'other')",
                emptyList()
            )
        }

        val indexCount = { column: String ->
            dataSource.withTransaction(true) {
                dataSource.query(
                    "SELECT count(*) AS cnt FROM pg_index i " +
                        "JOIN pg_class c ON c.oid = i.indrelid " +
                        "JOIN pg_namespace n ON n.oid = c.relnamespace " +
                        "JOIN pg_attribute a ON a.attrelid = c.oid AND a.attname = ? " +
                        "WHERE n.nspname = ? AND c.relname = ? AND NOT i.indisprimary AND (" +
                        "  (SELECT count(*) FROM unnest(i.indkey) k WHERE k = a.attnum) > 0" +
                        "  OR coalesce(pg_get_expr(i.indexprs, i.indrelid), '') LIKE '%' || ? || '%'" +
                        ")",
                    listOf(column, tableRef.schema, tableRef.table, column)
                ) { rs ->
                    rs.next()
                    rs.getLong("cnt")
                }
            }
        }

        assertThat(indexCount("indexed_text")).describedAs("own index + composite").isEqualTo(2L)
        assertThat(indexCount("indexed_arr")).isEqualTo(1L)

        // renaming an unrelated column literally named "text" must not touch indexed_text's own
        // index just because the deparsed expression "lower((indexed_text)::text)" contains the
        // substring "text" - only the catalog's own dependency link decides what belongs to what
        dataSource.withTransaction(false) {
            dbSchemaDao.renameColumn(dataSource, tableRef, "text", "__backup_text_text")
        }
        assertThat(indexCount("indexed_text"))
            .describedAs("renaming column \"text\" must not drop indexed_text's own index")
            .isEqualTo(2L)

        dataSource.withTransaction(false) {
            dbSchemaDao.renameColumn(dataSource, tableRef, "indexed_text", "__backup_indexed_text_text")
            dbSchemaDao.renameColumn(dataSource, tableRef, "indexed_arr", "__backup_indexed_arr_text_multiple")
        }

        val columnNames = dbSchemaDao.getColumns(dataSource, tableRef).map { it.name }
        assertThat(columnNames).contains("__backup_indexed_text_text", "__backup_indexed_arr_text_multiple")
        assertThat(columnNames).doesNotContain("indexed_text", "indexed_arr")

        assertThat(indexCount("__backup_indexed_text_text"))
            .describedAs("the composite index survives, the column's own index does not")
            .isEqualTo(1L)
        assertThat(indexCount("__backup_indexed_arr_text_multiple")).isEqualTo(0L)

        val kept = dataSource.withTransaction(true) {
            dataSource.query(
                "SELECT \"__backup_indexed_text_text\" AS res FROM ${tableRef.fullName}",
                emptyList()
            ) { rs ->
                rs.next()
                rs.getString("res")
            }
        }
        assertThat(kept).describedAs("a rename never loses data").isEqualTo("kept")
    }
}
