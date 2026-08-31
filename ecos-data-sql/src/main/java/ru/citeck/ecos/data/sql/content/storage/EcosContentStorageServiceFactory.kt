package ru.citeck.ecos.data.sql.content.storage

import ru.citeck.ecos.data.sql.context.DbSchemaContext

/**
 * Creates the [EcosContentStorageService] of a schema.
 *
 * The built-in service routes LOCAL storage refs to the schema's own table and everything else to
 * the owning application over the web api, which is what an application wants; supplying a factory
 * is for a backend that has to answer content calls itself - an in-memory test backend, or an
 * application that stores content somewhere the built-in routing can't reach.
 *
 * Called by [DbSchemaContext] while it is being constructed, so [schemaCtx] is not fully initialized
 * yet: an implementation may keep the reference, but must not read anything from it before the
 * context is handed out.
 */
fun interface EcosContentStorageServiceFactory {

    fun create(schemaCtx: DbSchemaContext): EcosContentStorageService
}
