package ru.citeck.ecos.data.sql.records.listener

import ru.citeck.ecos.model.lib.status.dto.StatusDef
import ru.citeck.ecos.model.lib.type.dto.TypeInfo

interface DbRecordsListener {

    fun onChanged(event: DbRecordChangedEvent)

    fun onCreated(event: DbRecordCreatedEvent)

    fun onDeleted(event: DbRecordDeletedEvent)

    fun onStatusChanged(event: DbRecordStatusChangedEvent)
}

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
