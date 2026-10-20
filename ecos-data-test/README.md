# ecos-data-test

Backend-**agnostic** contract tests for the ecos-data storage SPI, shared across backends.

The contract test base classes live in `src/main/java` so that each backend module depends on this
module at `scope=test` and provides the backend behind an abstract hook. This is the test-module
pattern borrowed from `ecos-webapp-commons`'s `ecos-webapp-lib-spring-test`.

## Pattern

`DbDataServiceContractTest` is an abstract JUnit5 test that drives the storage SPI through
`DbDataServiceImpl` / `DbDataSourceContext` and asserts the observable behaviour every backend must
provide (save, find by id / ext-id, predicate filtering, sorting, paging + total count,
multiple-value columns, optimistic locking, delete by predicate / id). The backend is supplied by
two abstract hooks:

```kotlin
protected abstract fun createDataServiceFactory(): DbDataServiceFactory
protected abstract fun createDataSource(): DbDataSource
```

A concrete subclass per backend wires the real backend and runs the SAME tests:

- `ecos-data-sql-pg`  -> `PgDataServiceContractTest` (PostgreSQL via Testcontainers)
- `ecos-data-inmem`   -> `InMemDataServiceContractTest` (in-memory backend)

## Record-level contract suite

Besides the storage-SPI contract above, this module owns the **full record-level test suite** for
the `DbRecordsDao` / Records API layer. The suite lives in `src/main/java` (package
`ru.citeck.ecos.data.sql.test.records`) so that it is reusable infrastructure — this module does
NOT run the suite itself (no src/test entry point here).

### Suite contents

The suite is the set of `DbRecordsTestBase` subclasses plus the shared base class itself
(`DbRecordsTestBase`) and the helpers `TypeRegistration` and `ContentUtils` — all in package
`ru.citeck.ecos.data.sql.test.records`.

`DbRecordsChildFlagChangeTest` covers deletion after an association's `child` flag changes.
Live cascade queries retain the stored `child=true` filter. The deleted record's resolved attribute
model can suppress cascade for those candidates, including overrides on record types sharing a
DAO; an undefined attribute falls back to the stored association flag. Changing `false` to `true`
does not retroactively cascade existing non-child links.
Removing an existing link is permitted after `child` becomes false. For an existing parent, adding
a child link still requires a child association, including mutations that combine addition and removal.
The removal-only exception requires the direct attribute key to be absent, even when its value is null.
Mutations to a missing parent are ignored when `MUTATION_FROM_CHILD_FLAG` is true, regardless of
the other attributes. Missing-record errors without this flag and access-denied errors for existing
parents still fail. Deletion does not pre-read the parent's existence.

### Global ecos-data props in tests

Global library properties (`DbEcosDataProps`, in a webapp loaded from the `ecos.webapp.data`
configuration section) are exposed by `DataMockFactory.dataProps`. Assign it **before** `setUp()`
runs — i.e. from the `init` block of the test class — because `DbDataSourceContext` is created in
`setUp()`:

```kotlin
class MyTest : DbRecordsTestBase() {
    init {
        dataProps = DbEcosDataProps(
            assocs = DbEcosDataProps.AssocsProps(maxCountToEditByFullValuesList = 5)
        )
    }
}
```

### Backend SPI

The suite delegates all backend-specific concerns to a pluggable SPI:

| Type | Role |
|------|------|
| `DbRecordsTestBackend` | Abstracts backend construction (`dbDataSource` + `dataServiceFactory`) and the DB-direct test helpers (`selectRecFromDb`, `sqlUpdate`, `dropAllTables`, ...). `supportsRawSql` lets a backend without a SQL engine turn raw-SQL helpers into skipped assumptions. |
| `DbRecordsTestBackendFactory` | `ServiceLoader` SPI, one impl per backend, keyed by `id` (`pg`, `inmem`). |
| `DbRecordsTestBackends` | Resolves the active factory from the `ecos.data.test.backend` system property (default `inmem`). |

### Equal consumers

Both `ecos-data-sql-pg` and `ecos-data-inmem` are **equal consumers** of this module: each depends
on `ecos-data-test` at `scope=test` and re-runs the entire suite via Surefire
`dependenciesToScan` with a different `ecos.data.test.backend` system property:

| Module | Property | Backend impl |
|--------|----------|--------------|
| `ecos-data-sql-pg` | `-Decos.data.test.backend=pg` | `PgRecordsTestBackend` (Testcontainers / DBCP / XA) |
| `ecos-data-inmem` | `-Decos.data.test.backend=inmem` | `InMemRecordsTestBackend` |

Only the backend implementation classes (`PgRecordsTestBackend`, `InMemRecordsTestBackend`) and any
genuinely PG-only engine tests remain in their respective modules. There is **no dependency from
`ecos-data-inmem` to `ecos-data-sql-pg`**. See `ecos-data-inmem/README.md` for the capability-based
skips (raw-SQL / PG-internal expressions) that a SQL-free backend cannot reproduce.
