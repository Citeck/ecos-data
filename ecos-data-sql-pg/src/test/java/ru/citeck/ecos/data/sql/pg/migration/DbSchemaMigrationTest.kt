package ru.citeck.ecos.data.sql.pg.migration

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import ru.citeck.ecos.context.lib.ctx.GlobalEcosContext
import ru.citeck.ecos.data.sql.batch.DbBatchTaskEntity
import ru.citeck.ecos.data.sql.columnmeta.DbColumnMetaEntity
import ru.citeck.ecos.data.sql.context.DbDataSourceContext
import ru.citeck.ecos.data.sql.context.DbSchemaContext
import ru.citeck.ecos.data.sql.datasource.DbDataSource
import ru.citeck.ecos.data.sql.datasource.DbDataSourceImpl
import ru.citeck.ecos.data.sql.domain.migration.DbMigrationService
import ru.citeck.ecos.data.sql.pg.PgDataServiceFactory
import ru.citeck.ecos.data.sql.records.assocs.DbAssocBackupEntity
import ru.citeck.ecos.data.sql.test.records.RequiresFreshSchema
import ru.citeck.ecos.test.commons.EcosWebAppApiMock
import ru.citeck.ecos.test.commons.containers.TestContainers
import ru.citeck.ecos.txn.lib.TxnContext
import ru.citeck.ecos.txn.lib.manager.EcosTxnProps
import ru.citeck.ecos.txn.lib.manager.TransactionManagerImpl
import ru.citeck.ecos.webapp.api.datasource.JdbcDataSource
import java.io.ByteArrayOutputStream
import java.io.PrintStream

/**
 * Upgrade-path tests for [DbMigrationService.runSchemaMigrations] - the first this repository has
 * had for a schema migration (deferred, because nothing existed yet to test an upgrade
 * *into*).
 *
 * Every test here observes whether a [ru.citeck.ecos.data.sql.domain.migration.schema.DbSchemaMigration]
 * actually ran through the one honest, code-owned signal available: `runSchemaMigrations` logs
 * `"Run schema migration: <ClassName> for schema '<schema>'"` at INFO for every migration it runs,
 * so capturing stderr (slf4j-simple, the test binding, writes there by default - the same pattern
 * [ru.citeck.ecos.data.sql.test.records.DbLargeTableMigrationTest] established) and asserting on
 * that line is a real observable, not a guess about internal state.
 *
 * Each test gets its own [TestContainers.getPostgres] key. Today that is belt and braces -
 * `getPostgres(key, "")` only opts out of per-test release when `key == image && image == ""`, so a
 * non-blank key is released (and the container disposed) at the end of every test method anyway -
 * but the fresh-schema test below is an *absence* assertion, and on a database an earlier method
 * already migrated it would pass vacuously (the version would already be
 * [DbSchemaContext.NEW_SCHEMA_VERSION], so `runSchemaMigrations` returns before doing anything).
 * That validity must not hinge on a release-semantics detail of the test framework.
 */
// What is under test here IS schema creation and the upgrade path into it, so a schema the
// record suite already built and cached would make every assertion below vacuous. The class
// happens to use its own container key today and would survive without this, but that is a
// property of the container framework, not a statement of what this test needs.
@RequiresFreshSchema
class DbSchemaMigrationTest {

    companion object {
        /**
         * A real, named schema. The schema name is mandatory - [DbDataSourceContext.getSchemaContext]
         * rejects a blank one - and a named schema is also what makes this test exercise the honest
         * new-schema path: `runSchemaMigrations` asks `isSchemaExists()`, which for a named schema
         * really does answer "no" on a fresh database.
         */
        private const val SCHEMA = "schema_migration_test"
    }

    private fun createDsCtx(containerKey: String): Pair<DbDataSource, DbDataSourceContext> {
        // A transaction manager is required, not decorative: DbSchemaContext registers an
        // onSchemaCreated listener that defers setVersion() through TxnContext.doBeforeCommit, and
        // that call runs the action *inline* when there is no transaction (TxnContext.kt:
        // `getTxnOrNull() ?: return action.invoke()`). Inline means setVersion() re-enters
        // DbSchemaDaoPg.createTableInSync while it is still creating ed_schema_meta, and the second
        // CREATE TABLE fails with "relation already exists". Installing the manager - exactly as
        // ru.citeck.ecos.data.sql.pg.PgUtils does, and as a real webapp always has - defers the hook
        // to commit, which is where it was meant to run.
        val txnManager = TransactionManagerImpl()
        txnManager.init(EcosWebAppApiMock(), EcosTxnProps())
        TxnContext.setManager(txnManager)

        val postgres = TestContainers.getPostgres(containerKey, "")
        val jdbcDataSource = object : JdbcDataSource {
            override fun getKey() = "key"
            override fun getJavaDataSource() = postgres.getDataSource()
            override fun isManaged() = false
        }
        val dataSource = DbDataSourceImpl(jdbcDataSource)
        val dsCtx = DbDataSourceContext(
            dataSource,
            PgDataServiceFactory(),
            DbMigrationService(),
            EcosWebAppApiMock("test"),
            GlobalEcosContext.getContext()
        )
        return dataSource to dsCtx
    }

    private fun captureStderr(action: () -> Unit): String {
        val original = System.err
        val captured = ByteArrayOutputStream()
        System.setErr(PrintStream(captured, true))
        try {
            action()
        } finally {
            System.setErr(original)
        }
        return captured.toString()
    }

    /**
     * A fresh schema is created straight at [DbSchemaContext.NEW_SCHEMA_VERSION] - every system
     * table, `ed_column_meta` included, is created directly through `createTableIfNotExists()`
     * calls inside `runSchemaMigrations`'s `version == 0` branch, which sets the version and
     * *returns before* the `while (version < NEW_SCHEMA_VERSION)` loop that runs the migration
     * list. No [ru.citeck.ecos.data.sql.domain.migration.schema.DbSchemaMigration] is meant to run
     * on a fresh install; this pins that nothing quietly changes that. An absence assertion with no
     * corroborating positive control is worse than no test at all - see
     * [upgradeFrom8To9RecreatesColumnMetaTableAndLogsTheMigrationTest] below for the other half.
     */
    @Test
    fun freshSchemaRunsNoSchemaMigrationTest() {

        val (dataSource, dsCtx) = createDsCtx("schema-migration-test-fresh")

        val log = captureStderr {
            dsCtx.getSchemaContext(SCHEMA)
        }

        assertThat(log)
            .describedAs(
                "a fresh schema is created straight at NEW_SCHEMA_VERSION - no migration in the " +
                    "list should ever run against it"
            )
            .doesNotContain("Run schema migration:")

        val schemaCtx = dsCtx.getSchemaContext(SCHEMA)
        assertThat(schemaCtx.getVersion())
            .describedAs("the version must still land on NEW_SCHEMA_VERSION even though no migration ran")
            .isEqualTo(DbSchemaContext.NEW_SCHEMA_VERSION)

        val columnMetaTableRef = schemaCtx.getTableRef(DbColumnMetaEntity.TABLE)
        dataSource.withTransaction(true) {
            assertThat(schemaCtx.isTableExists(columnMetaTableRef))
                .describedAs("ed_column_meta must exist even though it was never reached by a migration")
                .isTrue()
        }

        val batchTaskTableRef = schemaCtx.getTableRef(DbBatchTaskEntity.TABLE)
        dataSource.withTransaction(true) {
            assertThat(schemaCtx.isTableExists(batchTaskTableRef))
                .describedAs("ed_batch_task must exist on a fresh schema too, created directly and not by a migration")
                .isTrue()
        }

        val assocBackupTableRef = schemaCtx.getTableRef(DbAssocBackupEntity.TABLE)
        dataSource.withTransaction(true) {
            assertThat(schemaCtx.isTableExists(assocBackupTableRef))
                .describedAs(
                    "ed_associations_backup must exist on a fresh schema too - a migration step " +
                        "alone would never reach it, and the first attribute to leave the " +
                        "association group would have nowhere to put its links"
                )
                .isTrue()
        }
    }

    /**
     * The positive control for [freshSchemaRunsNoSchemaMigrationTest]: without this, "no
     * 'Run schema migration:' line captured" could equally mean the capture itself is broken, and
     * this plan has already been bitten more than once by an assertion that passed for the wrong
     * reason. A schema already at version 9 has its version rolled back to 8 and `ed_column_meta`
     * dropped, reproducing what an installation that predates this plan looks like one version
     * short of it, and `runSchemaMigrations` is asked to bring it forward again - which must both
     * recreate the table and log that it ran `EnsureColumnMetaTableExists`, through the exact same
     * capture mechanism the fresh-schema test relies on.
     */
    @Test
    fun upgradeFrom8To9RecreatesColumnMetaTableAndLogsTheMigrationTest() {

        val (dataSource, dsCtx) = createDsCtx("schema-migration-test-upgrade")

        // a brand-new schema is created straight at NEW_SCHEMA_VERSION (version == 0's fast path),
        // so it already has ed_column_meta - roll both back to reproduce an 8-to-9 upgrade.
        val schemaCtx = dsCtx.getSchemaContext(SCHEMA)
        val columnMetaTableRef = schemaCtx.getTableRef(DbColumnMetaEntity.TABLE)

        TxnContext.doInNewTxn {
            dataSource.withTransaction(false) {
                dataSource.updateSchema("DROP TABLE ${columnMetaTableRef.fullName}")
                schemaCtx.setVersion(8)
            }
        }
        schemaCtx.resetColumnsCache()
        dataSource.withTransaction(true) {
            assertThat(schemaCtx.isTableExists(columnMetaTableRef))
                .describedAs("the table was just dropped, so the upgrade below has something real to do")
                .isFalse()
        }

        val log = captureStderr {
            DbMigrationService().runSchemaMigrations(schemaCtx)
        }

        assertThat(log)
            .describedAs(
                "the positive control: this capture mechanism must actually see a migration run " +
                    "when one really does, or the fresh-schema test's absence assertion proves nothing"
            )
            .contains("Run schema migration: EnsureColumnMetaTableExists")
        assertThat(log)
            .describedAs(
                "only the 9th step was pending, so the rollback to 8 really took effect and the " +
                    "loop did not restart from an earlier version"
            )
            .doesNotContain("Run schema migration: EnsureUploadSessionTableExists")

        dataSource.withTransaction(true) {
            assertThat(schemaCtx.isTableExists(columnMetaTableRef))
                .describedAs("the 8-to-9 migration (EnsureColumnMetaTableExists) must recreate it")
                .isTrue()
        }
    }

    /**
     * The 9-to-10 upgrade, written the same way the 8-to-9 one is: roll a fresh schema back to 9
     * with `ed_batch_task` dropped, which is exactly what an installation that predates the batch
     * engine looks like, and require that bringing it forward both recreates the table and logs
     * that it ran [ru.citeck.ecos.data.sql.domain.migration.schema.EnsureBatchTaskTableExists].
     */
    @Test
    fun upgradeFrom9To10RecreatesBatchTaskTableAndLogsTheMigrationTest() {

        val (dataSource, dsCtx) = createDsCtx("schema-migration-test-upgrade-9-10")

        val schemaCtx = dsCtx.getSchemaContext(SCHEMA)
        val batchTaskTableRef = schemaCtx.getTableRef(DbBatchTaskEntity.TABLE)

        TxnContext.doInNewTxn {
            dataSource.withTransaction(false) {
                dataSource.updateSchema("DROP TABLE ${batchTaskTableRef.fullName}")
                schemaCtx.setVersion(9)
            }
        }
        schemaCtx.resetColumnsCache()
        dataSource.withTransaction(true) {
            assertThat(schemaCtx.isTableExists(batchTaskTableRef))
                .describedAs("the table was just dropped, so the upgrade below has something real to do")
                .isFalse()
        }

        val log = captureStderr {
            DbMigrationService().runSchemaMigrations(schemaCtx)
        }

        assertThat(log)
            .describedAs("the 9-to-10 step must actually run")
            .contains("Run schema migration: EnsureBatchTaskTableExists")
        assertThat(log)
            .describedAs(
                "only the 10th step was pending, so the rollback to 9 really took effect and the " +
                    "loop did not restart from an earlier version"
            )
            .doesNotContain("Run schema migration: EnsureColumnMetaTableExists")

        dataSource.withTransaction(true) {
            assertThat(schemaCtx.isTableExists(batchTaskTableRef))
                .describedAs("the 9-to-10 migration must recreate it")
                .isTrue()
        }
    }

    /**
     * The 10-to-11 upgrade, written the same way the two before it are: roll a fresh schema back to
     * 10 with `ed_associations_backup` dropped, which is exactly what an installation that predates
     * the association backup looks like, and require that bringing it forward both recreates the
     * table and logs that it ran
     * [ru.citeck.ecos.data.sql.domain.migration.schema.EnsureAssocBackupTableExists].
     */
    @Test
    fun upgradeFrom10To11RecreatesAssocBackupTableAndLogsTheMigrationTest() {

        val (dataSource, dsCtx) = createDsCtx("schema-migration-test-upgrade-10-11")

        val schemaCtx = dsCtx.getSchemaContext(SCHEMA)
        val assocBackupTableRef = schemaCtx.getTableRef(DbAssocBackupEntity.TABLE)

        TxnContext.doInNewTxn {
            dataSource.withTransaction(false) {
                dataSource.updateSchema("DROP TABLE ${assocBackupTableRef.fullName}")
                schemaCtx.setVersion(10)
            }
        }
        schemaCtx.resetColumnsCache()
        dataSource.withTransaction(true) {
            assertThat(schemaCtx.isTableExists(assocBackupTableRef))
                .describedAs("the table was just dropped, so the upgrade below has something real to do")
                .isFalse()
        }

        val log = captureStderr {
            DbMigrationService().runSchemaMigrations(schemaCtx)
        }

        assertThat(log)
            .describedAs("the 10-to-11 step must actually run")
            .contains("Run schema migration: EnsureAssocBackupTableExists")
        assertThat(log)
            .describedAs(
                "only the 11th step was pending, so the rollback to 10 really took effect and the " +
                    "loop did not restart from an earlier version"
            )
            .doesNotContain("Run schema migration: EnsureBatchTaskTableExists")

        dataSource.withTransaction(true) {
            assertThat(schemaCtx.isTableExists(assocBackupTableRef))
                .describedAs("the 10-to-11 migration must recreate it")
                .isTrue()
        }
    }

    /**
     * The 11-to-12 upgrade: `ed_associations_backup` gets an index on `__source_id`.
     *
     * Not a tidying-up. Every record deletion asks this table two questions keyed on the source
     * alone - which of my children are parked, and drop what I parked - and the indexes the table
     * was created with both lead on `__column_meta_id`, which no such query names. Without one on
     * `__source_id` every delete on the installation reads the whole backup, and that table is as
     * large as the type changes an administrator has made.
     *
     * A schema created fresh gets the index from the entity's own `@Indexes`; a schema that already
     * holds the table - which is every installation of `1.73.0` - can only get it from here, because
     * `DbDataServiceImpl.addIndexesAndConstraintsForNewColumns` adds indexes for **new columns**
     * and returns at once when there are none.
     */
    @Test
    fun upgradeFrom11To12AddsTheSourceIndexToTheAssocBackupTableTest() {

        val (dataSource, dsCtx) = createDsCtx("schema-migration-test-upgrade-11-12")

        val schemaCtx = dsCtx.getSchemaContext(SCHEMA)
        val assocBackupTableRef = schemaCtx.getTableRef(DbAssocBackupEntity.TABLE)

        TxnContext.doInNewTxn {
            dataSource.withTransaction(false) {
                // the table as 1.73.0 created it: the two indexes of the entity as it was then,
                // both leading on __column_meta_id
                dataSource.updateSchema("DROP TABLE ${assocBackupTableRef.fullName}")
                schemaCtx.setVersion(10)
            }
        }
        schemaCtx.resetColumnsCache()
        DbMigrationService().runSchemaMigrations(schemaCtx)

        dataSource.withTransaction(true) {
            assertThat(indexedColumnsOf(dataSource, assocBackupTableRef.table))
                .describedAs("the source id has an index of its own after the upgrade")
                .anyMatch { it.startsWith("__source_id") }
        }
    }

    /**
     * The column lists of every index of one table, in the order each index names them, read from
     * the catalog rather than guessed from the entity - the point of the assertion being what the
     * database really has.
     */
    private fun indexedColumnsOf(dataSource: DbDataSource, table: String): List<String> {
        return dataSource.query(
            "SELECT indexdef FROM pg_indexes WHERE schemaname = ? AND tablename = ?",
            listOf(SCHEMA, table)
        ) { rs ->
            val result = ArrayList<String>()
            while (rs.next()) {
                result.add(
                    rs.getString(1).substringAfterLast('(').substringBefore(')').replace("\"", "")
                )
            }
            result
        }
    }
}
