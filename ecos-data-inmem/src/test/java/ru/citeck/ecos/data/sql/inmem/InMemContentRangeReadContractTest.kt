package ru.citeck.ecos.data.sql.inmem

import ru.citeck.ecos.data.sql.test.content.ContentRangeReadContractTest

/**
 * Re-triggers the shared [ContentRangeReadContractTest] under this module's own package so
 * surefire's `dependenciesToScan` + package `<includes>` (see `pom.xml`) pick it up on the
 * in-memory backend. The suite itself is fully backend-neutral (via
 * [ru.citeck.ecos.data.sql.test.records.DbRecordsTestBase]) - no overrides are needed here.
 */
class InMemContentRangeReadContractTest : ContentRangeReadContractTest()
