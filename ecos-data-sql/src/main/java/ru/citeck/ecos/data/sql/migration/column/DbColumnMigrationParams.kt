package ru.citeck.ecos.data.sql.migration.column

import ru.citeck.ecos.commons.data.ObjectData
import ru.citeck.ecos.data.sql.columnmeta.DbColumnSemanticType
import ru.citeck.ecos.data.sql.columnmeta.DbConversionClass
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType

/**
 * The typed view of a `column-migration` task's `__params`.
 *
 * Everything the handler needs is written down here at the moment the transition is decided, rather
 * than re-derived when the task runs. The model may have changed again by then - that is precisely
 * the "repeated type change before the transfer finished" case - and a task must keep
 * doing the job it was queued for, into the column it was queued for.
 */
data class DbColumnMigrationParams(
    val attId: String,
    val backupColumn: String,
    val targetColumn: String,
    val sourceType: DbColumnSemanticType,
    val sourceMultiple: Boolean,
    val targetType: AttributeType,
    val targetMultiple: Boolean,
    /**
     * Whether the model calls the target attribute a **child** association
     * ([ru.citeck.ecos.data.sql.records.DbRecordsUtils.isChildAssocAttribute]) - the value
     * `ed_associations.__child` gets for every link a transfer into an assoc-like type creates.
     *
     * Written down here with everything else for the reason this class exists: the flag lives in
     * `AttributeDef.config`, which nothing downstream of the transition can reach, and the model may
     * have changed again by the time the task runs. Always false where the target is not an
     * association at all, and never true for `PERSON`/`AUTHORITY`/`AUTHORITY_GROUP`, which are not
     * child associations by definition.
     */
    val targetChild: Boolean,
    /**
     * Whether the model asks for an index on the target column.
     *
     * Recorded here because the transition deliberately adds the column *without* it - building a
     * btree means a full heap scan and would happen inside a user's mutation - and nothing else
     * ever looks at that column again: after the transition it matches the model, so it never
     * appears among the missing columns. The handler builds the index in its `onFinish`, and this
     * is the only place the intention survives in between.
     */
    val targetIndexEnabled: Boolean,
    val conversionClass: DbConversionClass,
    /**
     * The `ed_column_meta` row of the backup column. Associations backed up for this transition are
     * keyed by it, because one attribute can have several backups and each needs its own
     * snapshot of links.
     */
    val backupColumnMetaId: Long,
    /**
     * The `ed_column_meta` row of the backup column that was **restored** into the attribute's place
     *, or [NO_RESTORED_COLUMN] for an ordinary transition, where nothing came back.
     *
     * A field of its own rather than a second meaning for [backupColumnMetaId], because the two
     * name opposite ends of this transition: [backupColumnMetaId] is where the links of an
     * attribute *leaving* the association group are parked, and this is where the links of the
     * column *coming back* were parked the last time it left. `ASSOC -> TEXT` with an older TEXT
     * backup to restore is both at once, and keying the departure on the restored row would park
     * the association links under the text backup - where no later restore of the assoc column
     * would ever look for them.
     */
    val restoredColumnMetaId: Long = NO_RESTORED_COLUMN
) {

    companion object {

        const val HANDLER_TYPE = "column-migration"

        /**
         * [restoredColumnMetaId] of a transition that restored nothing.
         */
        const val NO_RESTORED_COLUMN = -1L

        private const val ATT_ID = "attId"
        private const val BACKUP_COLUMN = "backupColumn"
        private const val TARGET_COLUMN = "targetColumn"
        private const val SOURCE_TYPE = "sourceType"
        private const val SOURCE_MULTIPLE = "sourceMultiple"
        private const val TARGET_TYPE = "targetType"
        private const val TARGET_MULTIPLE = "targetMultiple"
        private const val TARGET_CHILD = "targetChild"
        private const val TARGET_INDEX_ENABLED = "targetIndexEnabled"
        private const val CONVERSION_CLASS = "conversionClass"
        private const val BACKUP_COLUMN_META_ID = "backupColumnMetaId"
        private const val RESTORED_COLUMN_META_ID = "restoredColumnMetaId"

        @JvmStatic
        fun from(data: ObjectData): DbColumnMigrationParams {
            return DbColumnMigrationParams(
                attId = data.get(ATT_ID).asText(),
                backupColumn = data.get(BACKUP_COLUMN).asText(),
                targetColumn = data.get(TARGET_COLUMN).asText(),
                // parse() throws on an unrecognised form on purpose: a task whose params cannot be
                // read is not something to guess at - it would migrate a column in the wrong
                // direction - and the engine routes the failure into the retry bookkeeping where an
                // administrator can see it.
                sourceType = DbColumnSemanticType.parse(data.get(SOURCE_TYPE).asText()),
                sourceMultiple = data.get(SOURCE_MULTIPLE).asBoolean(),
                targetType = AttributeType.valueOf(data.get(TARGET_TYPE).asText()),
                targetMultiple = data.get(TARGET_MULTIPLE).asBoolean(),
                // absent from every task queued before an arrival filled `ed_associations`;
                // false is what such a task would have created anyway, and it is the only answer
                // that cannot turn an ordinary association into a child one behind the user's back
                targetChild = data.get(TARGET_CHILD).asBoolean(false),
                targetIndexEnabled = data.get(TARGET_INDEX_ENABLED).asBoolean(),
                conversionClass = DbConversionClass.valueOf(data.get(CONVERSION_CLASS).asText()),
                backupColumnMetaId = data.get(BACKUP_COLUMN_META_ID).asLong(),
                // absent from every task queued before the restore existed, and absent from
                // every ordinary transition since - both of which restored nothing
                restoredColumnMetaId = data.get(RESTORED_COLUMN_META_ID).asLong(NO_RESTORED_COLUMN)
            )
        }
    }

    fun toObjectData(): ObjectData {
        val data = ObjectData.create()
        data.set(ATT_ID, attId)
        data.set(BACKUP_COLUMN, backupColumn)
        data.set(TARGET_COLUMN, targetColumn)
        data.set(SOURCE_TYPE, sourceType.asString())
        data.set(SOURCE_MULTIPLE, sourceMultiple)
        data.set(TARGET_TYPE, targetType.name)
        data.set(TARGET_MULTIPLE, targetMultiple)
        data.set(TARGET_CHILD, targetChild)
        data.set(TARGET_INDEX_ENABLED, targetIndexEnabled)
        data.set(CONVERSION_CLASS, conversionClass.name)
        data.set(BACKUP_COLUMN_META_ID, backupColumnMetaId)
        data.set(RESTORED_COLUMN_META_ID, restoredColumnMetaId)
        return data
    }
}
