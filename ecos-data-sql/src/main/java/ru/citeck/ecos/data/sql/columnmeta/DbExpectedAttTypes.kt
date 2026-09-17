package ru.citeck.ecos.data.sql.columnmeta

/**
 * What the type model says about the columns a save is about to touch, in the terms the schema
 * layer needs and cannot work out for itself.
 *
 * [attTypes] is keyed by column name, which for a model-driven column is the attribute id
 * ([ru.citeck.ecos.data.sql.ecostype.DbEcosModelService] names the column after the attribute).
 *
 * [conflicts] lists the columns that must not be migrated at all because two types sharing this
 * table declare the same attribute id with different types. There is no correct target in that
 * case - either choice corrupts the records of the other type - so the column is frozen and the
 * disagreement is reported. Empty means "nothing is frozen".
 *
 * [groupingMayBeOverBroad] is true when the storage boundary that produced [conflicts] was found
 * with a blank resolved `sourceId` (see
 * [ru.citeck.ecos.data.sql.ecostype.DbEcosModelService.getTableAttTypes]'s KDoc) - meaning the
 * group of types [conflicts] was computed over may be wider than the types that really share this
 * table. Kept as its own signal, not folded into the list values: [conflicts] is the only
 * machine-readable description of a frozen column that plans 4 and 5 will consume, and it is
 * documented as a list of type ids - appending prose to it would corrupt that contract for every
 * future reader that is not the one log line this flag is meant for.
 */
class DbExpectedAttTypes(
    val attTypes: Map<String, DbColumnSemanticType>,
    /**
     * The columns whose attribute is a **child** association
     * ([ru.citeck.ecos.data.sql.records.DbRecordsUtils.isChildAssocAttribute]), by column name.
     *
     * Here rather than derived downstream because it cannot be derived downstream: `child` lives in
     * [ru.citeck.ecos.model.lib.attributes.dto.AttributeDef.config], which the schema layer never
     * sees - [attTypes] keeps only the [ru.citeck.ecos.model.lib.attributes.dto.AttributeType], and
     * `ed_column_meta` records only that. A transfer into `G_ASSOC` has to
     * write `ed_associations.__child` with what `DbRecordsMutateDao` would have written, and its own
     * only alternative would be to guess `false` - which is a link the model calls a child stored as
     * one that is not, so a cascade delete and every parent/child query answer differently for it.
     *
     * A set rather than a flag on [attTypes]'s value type, because that value is
     * [DbColumnSemanticType], which is **persisted** in `ed_column_meta.__att_type`: widening it
     * would change a stored form for a fact the registry has no column for.
     *
     * **No default**, for the same reason
     * [ru.citeck.ecos.data.sql.migration.column.DbColumnMigrationParams.targetChild] has none: this
     * is not optional information, it is one of the two facts the migration path cannot recover for
     * itself, and a default is how a construction site forgets it silently. One already had - the
     * commit that added this field left `DbEcosModelService.getTableAttTypes` unpopulated, which
     * compiled precisely because the default was here. Every site must now say what it means.
     */
    val childAtts: Set<String>,
    /**
     * Resolved lazily: working the conflicts out means walking the type's descendants and their
     * aspects, and this object is built on every mutation while the answer is only ever needed on
     * the rare path where a column type actually has to change.
     */
    private val conflictsProvider: () -> ConflictsInfo = { ConflictsInfo(emptyMap(), false) }
) {
    companion object {
        @JvmField
        val EMPTY = DbExpectedAttTypes(emptyMap(), emptySet())
    }

    /**
     * [conflicts] together with [groupingMayBeOverBroad] - resolved as one unit because both come
     * out of the same walk in [ru.citeck.ecos.data.sql.ecostype.DbEcosModelService.getTableAttTypes].
     */
    data class ConflictsInfo(
        val conflicts: Map<String, List<String>>,
        val groupingMayBeOverBroad: Boolean
    )

    private val conflictsInfo: ConflictsInfo by lazy(LazyThreadSafetyMode.NONE) { conflictsProvider() }

    val conflicts: Map<String, List<String>>
        get() = conflictsInfo.conflicts

    val groupingMayBeOverBroad: Boolean
        get() = conflictsInfo.groupingMayBeOverBroad
}
