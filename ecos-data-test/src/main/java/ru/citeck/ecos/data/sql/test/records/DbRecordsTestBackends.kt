package ru.citeck.ecos.data.sql.test.records

import ru.citeck.ecos.test.commons.EcosWebAppApiMock
import java.util.ServiceLoader

/**
 * Resolves the active [DbRecordsTestBackendFactory] from the `ecos.data.test.backend` system
 * property (default [DEFAULT_BACKEND] = `inmem`), using the standard [ServiceLoader] SPI.
 *
 * The default is the Docker-free in-memory backend, so a consumer that just instantiates
 * [DataMockFactory] gets a working records environment with no setup. The in-repo runs are
 * unaffected because each sets the property explicitly: `ecos-data-sql-pg` → `pg`,
 * `ecos-data-inmem` → `inmem`.
 */
object DbRecordsTestBackends {

    const val BACKEND_PROP = "ecos.data.test.backend"
    const val DEFAULT_BACKEND = "inmem"

    fun create(webAppApi: EcosWebAppApiMock): DbRecordsTestBackend {
        val backendId = System.getProperty(BACKEND_PROP, DEFAULT_BACKEND)
        val factories = ServiceLoader.load(DbRecordsTestBackendFactory::class.java).toList()
        val factory = factories.find { it.id == backendId }
            ?: error(
                "No DbRecordsTestBackendFactory found for '$BACKEND_PROP=$backendId'. " +
                    "Available: ${factories.map { it.id }}"
            )
        return factory.create(webAppApi)
    }
}
