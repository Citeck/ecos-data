package ru.citeck.ecos.data.sql.inmem

import ru.citeck.ecos.data.sql.test.content.ChunkedUploadContractTest

/**
 * Re-triggers the shared [ChunkedUploadContractTest] under this module's own package so surefire's
 * `dependenciesToScan` + package `<includes>` (see `pom.xml`) pick it up on the in-memory backend.
 * The suite itself is fully backend-neutral (via [ru.citeck.ecos.data.sql.test.records.DbRecordsTestBase]) -
 * no overrides are needed here, unlike the `UploadSessionServiceContractTest`/
 * `ContentServiceRegisterContractTest` runners, which need a backend-specific data source/factory.
 */
class InMemChunkedUploadContractTest : ChunkedUploadContractTest()
