# ecos-data-inmem

An **in-memory backend** for ecos-data's storage SPI. It implements the same
`DbDataServiceFactory` / `DbEntityRepo` / `DbSchemaDao` / `DbDataSource` abstractions as the
PostgreSQL backend (`ecos-data-sql-pg`), but stores rows in plain JVM data structures and evaluates
queries with `ecos-records` `PredicateService` instead of SQL.

It exists so that consumers (e.g. citeck-forge) get **fast, Docker-free, Spring-free** storage with
real ecos-data semantics (typed attributes, ext-id/long-id mapping, optimistic locking,
transactional rollback). See `docs/architecture/decisions/0001-...` in citeck-forge (ADR-0001).

## How to use

The backend is selected purely by the factory + data source passed to `DbDataSourceContext` —
nothing above the storage SPI (`DbDataServiceImpl`, `DbRecordsDao`, ...) changes:

```kotlin
val dataSource = InMemDataSource()
val dsCtx = DbDataSourceContext(
    dataSource,
    InMemDataServiceFactory(),   // <- the only difference from the PG wiring
    DbMigrationService(),
    webAppApi,
    ecosContext
)
val schemaCtx = dsCtx.getSchemaContext("my-schema")
val service = DbDataServiceImpl(DbEntity::class.java, config, schemaCtx)
```

A `TxnContext` manager must be initialized (as in any platform runtime). See
`InMemDataServiceTestUtils` for the full no-Spring construction recipe.

## Components

| Class | Responsibility |
|-------|----------------|
| `InMemDataServiceFactory` | The backend seam: creates `InMemSchemaDao` + `InMemEntityRepo`. |
| `InMemDataSource` | In-memory `DbDataSource`. Holds the store; provides `withTransaction` with snapshot-on-begin / discard-on-rollback semantics, and the `watchSchemaCommands`/`withSchemaMock` machinery the migration flow needs. Never executes SQL. |
| `InMemSchemaDao` | Mutates table/column structure directly in the store; registers a schema command per change so migrations are detected. |
| `InMemEntityRepo` | save / insertIfNoConflictByExtId / find / delete over the store. `find` builds the predicate + sorting + paging and evaluates them via `PredicateService.filterAndSort` over `RowElement` wrappers. |
| `InMemStore` / `InMemTable` | The data: ordered columns + rows keyed by long id + an ext-id index. Copyable for transaction snapshots. |
| `RowElements` / `RowElement` / `RowElementAttributes` | Adapt rows to `ecos-records` `Element`s so the platform's predicate engine filters/sorts them (dates normalized to ISO-8601, arrays to lists). |

## Parity with the PG backend & scope limits

`find` is implemented by a dedicated **`InMemQueryEngine`** (package `…inmem.query`) that mirrors the
observable behaviour of `DbEntityRepoPg.find` and honours the **full `DbFindQuery`**, not just the
predicate. What it reproduces in memory:

- **association table joins** (`assocTableJoins`) and **assoc joins with predicate**
  (`assocJoinsWithPredicate`) as EXISTS over the `ed_associations` table; **assoc select joins**
  (`assocAtt.targetAtt`) and **raw table joins** (`rawTableJoins`) — all resolved against the SAME
  in-memory store, since associations reference records by `__ref_id` exactly as in PG;
- **computed `expressions`**: arithmetic, comparison (`=`,`<>`,`<`,…), `IS [NOT] NULL`, `CASE`, cast,
  the numeric functions (`floor/ceil/round/abs/sign/sqrt/mod/power/trunc`), string functions
  (`length/upper/lower/trim/concat/concat_ws/substring/substringBefore/replace/position/…`),
  `coalesce/nullif/greatest/least`, and per-row assoc aggregation (`sum(multiAssoc.targetNum)`);
- **`groupBy` + aggregation** (`count/sum/min/max/avg`) with PG's grouped-SELECT column rules and
  numeric-type preservation;
- **PG NULL/empty fidelity** (the part `DefaultValueComparator` gets wrong): `eq(null)` → `IS NULL`;
  `empty()` matches NULL/`''`/empty-array but **not** `0`; `not(eq)` uses `IS DISTINCT FROM` (NULL
  included) for TEXT/INT/DOUBLE/LONG/DATE and three-valued `NOT` otherwise; boolean `IS TRUE/IS FALSE`;
  `LIKE` wildcard matching;
- **read-perms filtering**: `QueryPermsPolicy` OWN/PARENT/PUBLIC/NONE against the `ed_read_perms` table
  using `DbFindQuery.userAuthorities` plus type-scoped `delegatedAuthorities`.

`delete(predicate)` uses the same `PredicateEvaluator` and removes rows by their internal store key,
so it works for id-less tables (`ed_associations`, `ed_read_perms`).

**Deliberately NOT reproduced** (genuinely PostgreSQL-internal — would mean re-implementing the PG
engine; tests that need them are documented `Assumptions` skips, not failures):

- PG text-formatting / timezone / interval expressions: `to_char(date, 'YYYY.MM')`,
  `date_part('epoch', …)`, `date_trunc`, `interval` arithmetic
  (`supportsSqlInternalExpressions = false`);
- raw-SQL / DDL assertions (`selectRecFromDb`, `sqlUpdate`, `::text` casts, `ALTER TABLE DROP COLUMN`)
  (`supportsRawSql = false`).

**Mid-transaction rollback IS reproduced** (`supportsTransactionRollback = true`). The in-mem source
enlists a `TransactionResource` with the active platform transaction the first time it durably writes:
the resource captures the durable store as it was *before* that write, and the platform transaction
manager calls `rollback()` (restore the pre-image) or commit (a no-op — the writes are already applied)
when the `doInTxn { … }` block ends. Two details make this correct under nesting: the pre-image is
captured at the first **write** (not the first access), so a `requiresNew` id-mapping insert — which
runs in its own platform transaction and commits before the parent's first write — is already in the
captured pre-image and survives a parent commit; and the per-table id sequence (`InMemTable.idCounter`)
is shared by reference across store snapshots, so — like a PostgreSQL `SEQUENCE` — it is
**non-transactional** and never reissues an id a rolled-back transaction already consumed.

## Record-level contract suite (cross-backend)

Beyond the storage-SPI contract test (`InMemDataServiceContractTest`), the in-mem backend re-runs the
**full record-level suite** of `DbRecordsTestBase` subclasses exercising the `DbRecordsDao` /
Records API. The suite is **backend-neutral** and now lives in `ecos-data-test` (package
`ru.citeck.ecos.data.sql.test.records`). The in-mem module depends on **`ecos-data-test`** (not on
`ecos-data-sql-pg`) and re-runs the same classes via Surefire `dependenciesToScan` with
`-Decos.data.test.backend=inmem` — no test classes are copied, and there is **no dependency on
`ecos-data-sql-pg`**.

Run it: `mvn -pl ecos-data-inmem test` (after `mvn -pl ecos-data-sql,ecos-data-test -DskipTests install`). Run the PG side: `mvn -pl ecos-data-sql-pg test` (needs Docker/Testcontainers).

A small set of cases are skipped on in-mem as documented JUnit *assumptions* (not failures), keyed off
the `DbRecordsTestBackend` capability flags `supportsRawSql` and `supportsSqlInternalExpressions` — the
raw-SQL/DDL and PG-internal expression behaviours described under "Parity with the PG backend & scope
limits" above, which a SQL-free backend cannot reproduce.
