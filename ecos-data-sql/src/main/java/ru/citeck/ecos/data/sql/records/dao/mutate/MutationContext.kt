package ru.citeck.ecos.data.sql.records.dao.mutate

import ru.citeck.ecos.data.sql.ecostype.EcosAttColumnDef
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

    init {
        typeAttColumns.forEach { typeAttColumnsByAtt[it.attribute.id] = it }
    }

    fun runPostMutationAtts(entity: DbEntity): DbEntity {
        var result = entity
        for (action in postMutationActions) {
            result = action.invoke(result)
        }
        return result
    }

    fun addTypeAttColumn(column: EcosAttColumnDef) {
        if (!knownColumnIds.add(column.attribute.id)) {
            return
        }
        typeAttColumns.add(column)
        typeAttColumnsByAtt[column.attribute.id] = column
        typeColumns.add(column.column)
        typeColumnNames.add(column.column.name)
    }
}
