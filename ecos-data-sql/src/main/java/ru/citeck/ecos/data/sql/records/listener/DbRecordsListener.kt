package ru.citeck.ecos.data.sql.records.listener

import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.status.dto.StatusDef

interface DbRecordsListener {

    fun onChanged(event: DbRecordChangedEvent)

    fun onCreated(event: DbRecordCreatedEvent)

    fun onDeleted(event: DbRecordDeletedEvent)

    fun onStatusChanged(event: DbRecordStatusChangedEvent)
}

class DbRecordStatusChangedEvent(
    val record: Any,
    val before: StatusDef,
    val after: StatusDef
)

class DbRecordDeletedEvent(
    val record: Any
)

class DbRecordCreatedEvent(
    val record: Any
)

class DbRecordChangedEvent(
    val record: Any,
    val attsDef: List<AttributeDef>,
    val before: Map<String, Any?>,
    val after: Map<String, Any?>
)
