package ru.citeck.ecos.data.sql.test.records

import ru.citeck.ecos.test.commons.EcosWebAppApiMock

/**
 * SPI for [DbRecordsTestBackend]. Each storage backend publishes one implementation via
 * `META-INF/services/ru.citeck.ecos.data.sql.test.records.DbRecordsTestBackendFactory` and is
 * keyed by [id] (e.g. `pg`, `inmem`). [DbRecordsTestBackends] resolves the active factory by the
 * `ecos.data.test.backend` system property.
 */
interface DbRecordsTestBackendFactory {

    /** Stable backend key, matched against the `ecos.data.test.backend` system property. */
    val id: String

    /**
     * Build a backend instance. [webAppApi] is the suite's shared mock web-app api (the PG backend
     * needs it for the XA-managed datasource; the in-mem backend ignores it).
     */
    fun create(webAppApi: EcosWebAppApiMock): DbRecordsTestBackend
}
