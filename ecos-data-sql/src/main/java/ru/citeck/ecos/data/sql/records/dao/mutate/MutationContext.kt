package ru.citeck.ecos.data.sql.records.dao.mutate

import ru.citeck.ecos.data.sql.columnmeta.DbColumnSemanticType
import ru.citeck.ecos.data.sql.columnmeta.DbExpectedAttTypes
import ru.citeck.ecos.data.sql.ecostype.EcosAttColumnDef
import ru.citeck.ecos.data.sql.records.DbRecordsUtils
import ru.citeck.ecos.data.sql.records.dao.atts.DbAssocAttValuesContainer
import ru.citeck.ecos.data.sql.repo.entity.DbEntity
import ru.citeck.ecos.model.lib.type.dto.TypeInfo
import ru.citeck.ecos.records3.record.atts.dto.LocalRecordAtts
import java.util.HashSet
import java.util.LinkedHashMap
import kotlin.collections.set

class MutationContext(
    val record: LocalRecordAtts,
    val typeInfo: TypeInfo,
    val disableEvents: Boolean,
    val entityToMutate: DbEntity,
    val typeAttColumns: MutableList<EcosAttColumnDef>,
    /**
     * Resolves the columns frozen by a configuration conflict between the types that share this
     * table, together with whether that table's storage-boundary grouping may itself be
     * over-broad. A provider rather than a value: the answer depends on the table rather than on
     * the record, and it is only ever consulted when a column type actually has to change.
     */
    val frozenColumnsProvider: () -> DbExpectedAttTypes.ConflictsInfo,
    val currentUser: String,
    val currentUserRefId: Long,
    val isRunAsSystemOrAdmin: Boolean,
    val isNewEntity: Boolean,
    val extIdFromAtts: String,
    val computeContext: MutationComputeContext
) {

    val knownColumnIds = HashSet<String>()
    val typeAttColumnsByAtt: MutableMap<String, EcosAttColumnDef> = LinkedHashMap()
    val typeColumns = typeAttColumns.map { it.column }.toMutableList()
    val typeColumnNames = typeColumns.map { it.name }.toMutableSet()
    val typeAspects = typeInfo.aspects.map { it.ref }.toSet()
    val allAssocsValues: MutableMap<String, DbAssocAttValuesContainer> = LinkedHashMap()
    val postMutationActions: MutableList<(DbEntity) -> DbEntity> = ArrayList()

    private val attSemanticTypes: MutableMap<String, DbColumnSemanticType> = LinkedHashMap()

    /**
     * The column names of this mutation's **child** association attributes - the one thing about an
     * association the schema layer cannot work out from an
     * [ru.citeck.ecos.model.lib.attributes.dto.AttributeType] alone, and which a transfer into
     * `G_ASSOC` has to write into `ed_associations.__child`. See [DbExpectedAttTypes.childAtts].
     */
    private val childAtts: MutableSet<String> = LinkedHashSet()

    init {
        typeAttColumns.forEach {
            typeAttColumnsByAtt[it.attribute.id] = it
            attSemanticTypes[it.column.name] = DbColumnSemanticType.Model(it.attribute.type)
            if (DbRecordsUtils.isChildAssocAttribute(it.attribute)) {
                childAtts.add(it.column.name)
            }
        }
    }

    fun runPostMutationActions(entity: DbEntity): DbEntity {
        var result = entity
        for (action in postMutationActions) {
            result = action.invoke(result)
        }
        return result
    }

    /**
     * What the schema layer needs to know about this mutation's columns beyond their physical
     * shape.
     */
    fun getExpectedAttTypes(): DbExpectedAttTypes {
        return DbExpectedAttTypes(attSemanticTypes, childAtts, frozenColumnsProvider)
    }

    fun addTypeAttColumn(column: EcosAttColumnDef) {
        if (!knownColumnIds.add(column.attribute.id)) {
            return
        }
        typeAttColumns.add(column)
        typeAttColumnsByAtt[column.attribute.id] = column
        typeColumns.add(column.column)
        typeColumnNames.add(column.column.name)
        attSemanticTypes[column.column.name] = DbColumnSemanticType.Model(column.attribute.type)
        if (DbRecordsUtils.isChildAssocAttribute(column.attribute)) {
            childAtts.add(column.column.name)
        }
    }
}
