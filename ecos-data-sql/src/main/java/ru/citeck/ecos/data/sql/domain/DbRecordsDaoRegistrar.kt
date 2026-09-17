package ru.citeck.ecos.data.sql.domain

import ru.citeck.ecos.records3.record.dao.RecordsDao

/**
 * How this library publishes a records DAO it built itself, without depending on the records
 * service to do it.
 *
 * `ecos-data` cannot register a DAO on its own: wiring one up takes a `RecordsServiceFactory` and a
 * `RecordsService`, and a [DbDomainFactory] is given neither - every DAO it builds is handed back
 * to the caller, who registers it. That is fine for the DAOs an application asks for by name, and
 * no use at all for the ones this library owns: the administrator's view of `ed_batch_task` exists
 * per **schema**, and a schema appears when something first touches it, long after any application
 * finished listing the sources it wanted.
 *
 * So the composition root supplies this instead - one line in a Spring autoconfiguration - and
 * [DbDomainFactory.setRecordsDaoRegistrar] calls it whenever a schema of that factory comes into
 * existence. The DAO is already wired by the time it arrives here; an implementation has only to
 * hand it to the records service.
 *
 * An implementation must tolerate being called from schema creation, which can happen on any
 * thread and inside a transaction. Anything it throws is logged and swallowed by the caller: an
 * admin view that failed to publish must not take the schema it describes down with it.
 */
fun interface DbRecordsDaoRegistrar {

    fun register(dao: RecordsDao)
}
