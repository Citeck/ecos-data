package ru.citeck.ecos.data.sql.records

/**
 * Special control attributes that can be passed during record mutation
 * to trigger specific behaviors instead of (or in addition to) a regular save.
 * These attributes are consumed by [ru.citeck.ecos.data.sql.records.dao.mutate.DbRecordsMutateDao]
 * and are never persisted as record data.
 */
object DbRecordsControlAtts {

    /**
     * Disable audit field updates (created/creator/modified/modifier) for this mutation.
     * System context only.
     */
    const val DISABLE_AUDIT = "__disableAudit"

    /**
     * Disable record lifecycle event emission (created, changed, deleted, statusChanged) for this mutation.
     * System context only.
     */
    const val DISABLE_EVENTS = "__disableEvents"

    /**
     * Trigger a permissions recalculation for the record instead of a regular save.
     * Admin/system context only.
     */
    const val UPDATE_PERMISSIONS = "__updatePermissions"

    /**
     * Move the record (and its child records) to the specified workspace.
     * Admin/system context only.
     */
    const val UPDATE_WORKSPACE = "__updateWorkspace"

    /**
     * Change the record's external ID (extId) to the specified value.
     * Admin/system context only. Must be explicitly enabled in DAO config via [allowRecordIdUpdate].
     */
    const val UPDATE_ID = "__updateId"

    /**
     * Recalculate computed/calculated attributes for the record.
     * Admin/system context only.
     */
    const val UPDATE_CALCULATED_ATTS = "__updateCalculatedAtts"

    /**
     * Change the type of an existing record to the specified one.
     * Allowed only when the new type has the same sourceId as the current one,
     * so the record stays in the same records DAO.
     * Requires write permissions for the record.
     *
     * This is a standalone operation: all other attributes of the mutation are ignored
     * except [ru.citeck.ecos.model.lib.status.constants.StatusConstants.ATT_STATUS],
     * [DISABLE_AUDIT] and [DISABLE_EVENTS].
     *
     * The status attribute passed with a blank value resets the record status. This is
     * required to change type to the type without statuses in its model.
     *
     * When [DISABLE_AUDIT] is set, audit attributes (_created, _creator, _modified,
     * _modifier, _statusModified) passed by the same mutation are applied to the record:
     * they are processed before the type updating and their values are saved by it.
     */
    const val UPDATE_TYPE = "__updateType"

    /**
     * Atomically increment the specified counter attribute(s).
     * Accepts a single attribute name or a list of attribute names.
     */
    const val UPDATE_COUNTER_ATT = "__updateCounterAtt"

    /**
     * Supply the full set of record attributes as a JSON object.
     * When set to `true`, all existing attributes not present in the mutation payload
     * are treated as explicitly removed (set to null).
     * When set to a JSON object, its contents are used as the complete attribute map.
     */
    const val FULL_ATTS = "__fullAtts"
}
