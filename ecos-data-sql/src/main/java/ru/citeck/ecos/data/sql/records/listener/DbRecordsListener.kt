package ru.citeck.ecos.data.sql.records.listener

import ru.citeck.ecos.model.lib.status.dto.StatusDef
import ru.citeck.ecos.model.lib.type.dto.TypeInfo

abstract class DbRecordsListenerAdapter : DbRecordsListener {
    override fun onChanged(event: DbRecordChangedEvent) {}
    override fun onCreated(event: DbRecordCreatedEvent) {}
    override fun onDeleted(event: DbRecordDeletedEvent) {}
    override fun onDraftStatusChanged(event: DbRecordDraftStatusChangedEvent) {}
    override fun onStatusChanged(event: DbRecordStatusChangedEvent) {}
}

interface DbRecordsListener {

    fun onChanged(event: DbRecordChangedEvent)

    fun onCreated(event: DbRecordCreatedEvent)

    fun onDeleted(event: DbRecordDeletedEvent)

    fun onDraftStatusChanged(event: DbRecordDraftStatusChangedEvent)

    fun onStatusChanged(event: DbRecordStatusChangedEvent)
}

class DbRecordDraftStatusChangedEvent(
    val record: Any,
    val typeDef: TypeInfo,
    val before: Boolean,
    val after: Boolean
)

class DbRecordStatusChangedEvent(
    val record: Any,
    val typeDef: TypeInfo,
    val before: StatusDef,
    val after: StatusDef
)

class DbRecordDeletedEvent(
    val record: Any,
    val typeDef: TypeInfo
)

class DbRecordCreatedEvent(
    val record: Any,
    val typeDef: TypeInfo
)

class DbRecordChangedEvent(
    val record: Any,
    val typeDef: TypeInfo,
    val before: Map<String, Any?>,
    val after: Map<String, Any?>
)
