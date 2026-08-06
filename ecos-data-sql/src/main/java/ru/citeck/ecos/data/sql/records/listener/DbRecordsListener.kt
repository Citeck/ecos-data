package ru.citeck.ecos.data.sql.records.listener

import ru.citeck.ecos.data.sql.records.assocs.DbAssocRefsDiff
import ru.citeck.ecos.model.lib.aspect.dto.AspectInfo
import ru.citeck.ecos.model.lib.status.dto.StatusDef
import ru.citeck.ecos.model.lib.type.dto.TypeInfo
import ru.citeck.ecos.webapp.api.entity.EntityRef

abstract class DbRecordsListenerAdapter : DbRecordsListener {
    override fun onChanged(event: DbRecordChangedEvent) {}
    override fun onCreated(event: DbRecordCreatedEvent) {}
    override fun onDeleted(event: DbRecordDeletedEvent) {}
    override fun onRecordRefChangedEvent(event: DbRecordRefChangedEvent) {}
    override fun onParentChanged(event: DbRecordParentChangedEvent) {}
    override fun onContentChanged(event: DbRecordContentChangedEvent) {}
    override fun onDraftStatusChanged(event: DbRecordDraftStatusChangedEvent) {}
    override fun onStatusChanged(event: DbRecordStatusChangedEvent) {}
    override fun onTypeChanged(event: DbRecordTypeChangedEvent) {}
}

interface DbRecordsListener {

    fun onChanged(event: DbRecordChangedEvent)

    fun onCreated(event: DbRecordCreatedEvent)

    fun onDeleted(event: DbRecordDeletedEvent)

    fun onRecordRefChangedEvent(event: DbRecordRefChangedEvent)

    fun onParentChanged(event: DbRecordParentChangedEvent)

    fun onDraftStatusChanged(event: DbRecordDraftStatusChangedEvent)

    fun onContentChanged(event: DbRecordContentChangedEvent)

    fun onStatusChanged(event: DbRecordStatusChangedEvent)

    /**
     * Declared with a default empty body, so direct implementations of this interface
     * (e.g. DbRecordsEcosDefaultEventsEmitter in ecos-webapp-commons) stay compilable
     * without changes. The default body is not a JVM default method (the build doesn't
     * use -Xjvm-default), so such implementations must be recompiled against this
     * version of ecos-data.
     */
    fun onTypeChanged(event: DbRecordTypeChangedEvent) {}
}

/**
 * Emitted when the record type was changed by
 * [ru.citeck.ecos.data.sql.records.DbRecordsControlAtts.UPDATE_TYPE] control attribute.
 * [typeDef] and [aspects] describe the state after the change, as in other events,
 * and [before] is the type of the record before the change.
 */
class DbRecordTypeChangedEvent(
    val localRef: EntityRef,
    val globalRef: EntityRef,
    val isDraft: Boolean,
    val record: Any,
    val typeDef: TypeInfo,
    val aspects: List<AspectInfo>,
    val before: TypeInfo
)

class DbRecordRefChangedEvent(
    val before: EntityRef,
    val after: EntityRef,
    val localRef: EntityRef,
    val globalRef: EntityRef,
    val isDraft: Boolean,
    val record: Any,
    val typeDef: TypeInfo,
    val aspects: List<AspectInfo>
)

class DbRecordDraftStatusChangedEvent(
    val localRef: EntityRef,
    val globalRef: EntityRef,
    val isDraft: Boolean,
    val record: Any,
    val typeDef: TypeInfo,
    val aspects: List<AspectInfo>,
    val before: Boolean,
    val after: Boolean
)

class DbRecordParentChangedEvent(
    val localRef: EntityRef,
    val globalRef: EntityRef,
    val isDraft: Boolean,
    val record: Any,
    val typeDef: TypeInfo,
    val aspects: List<AspectInfo>,
    val parentBefore: EntityRef,
    val parentAfter: EntityRef,
    val parentAttBefore: String,
    val parentAttAfter: String
)

class DbRecordStatusChangedEvent(
    val localRef: EntityRef,
    val globalRef: EntityRef,
    val isDraft: Boolean,
    val record: Any,
    val typeDef: TypeInfo,
    val aspects: List<AspectInfo>,
    val before: StatusDef,
    val after: StatusDef
)

class DbRecordDeletedEvent(
    val localRef: EntityRef,
    val globalRef: EntityRef,
    val isDraft: Boolean,
    val record: Any,
    val typeDef: TypeInfo,
    val aspects: List<AspectInfo>
)

class DbRecordCreatedEvent(
    val localRef: EntityRef,
    val globalRef: EntityRef,
    val isDraft: Boolean,
    val record: Any,
    val typeDef: TypeInfo,
    val aspects: List<AspectInfo>
)

class DbRecordChangedEvent(
    val localRef: EntityRef,
    val globalRef: EntityRef,
    val isDraft: Boolean,
    val record: Any,
    val typeDef: TypeInfo,
    val aspects: List<AspectInfo>,
    val before: Map<String, Any?>,
    val after: Map<String, Any?>,
    val assocs: List<DbAssocRefsDiff>,
    val systemBefore: Map<String, Any?>,
    val systemAfter: Map<String, Any?>,
    val systemAssocs: List<DbAssocRefsDiff>
) {

    fun isSystemAttsChanged(): Boolean {
        return systemBefore.isNotEmpty() || systemAfter.isNotEmpty() || systemAssocs.isNotEmpty()
    }

    fun isNonSystemAttsChanged(): Boolean {
        return before.isNotEmpty() || after.isNotEmpty() || assocs.isNotEmpty()
    }
}

class DbRecordContentChangedEvent(
    val localRef: EntityRef,
    val globalRef: EntityRef,
    val isDraft: Boolean,
    val record: Any,
    val typeDef: TypeInfo,
    val aspects: List<AspectInfo>,
    val before: Any?,
    val after: Any?,
    val attsBefore: Map<String, Any?>,
    val attsAfter: Map<String, Any?>
)
