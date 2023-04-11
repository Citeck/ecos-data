package ru.citeck.ecos.data.sql.ecostype

import mu.KotlinLogging
import ru.citeck.ecos.data.sql.dto.DbColumnDef
import ru.citeck.ecos.data.sql.dto.DbColumnIndexDef
import ru.citeck.ecos.data.sql.dto.DbColumnType
import ru.citeck.ecos.model.lib.ModelServiceFactory
import ru.citeck.ecos.model.lib.aspect.dto.AspectInfo
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType
import ru.citeck.ecos.model.lib.attributes.dto.computed.ComputedAttStoringType
import ru.citeck.ecos.model.lib.attributes.dto.computed.ComputedAttType
import ru.citeck.ecos.model.lib.type.dto.TypeInfo
import ru.citeck.ecos.model.lib.utils.ModelUtils
import ru.citeck.ecos.webapp.api.entity.EntityRef

class DbEcosModelService(modelServices: ModelServiceFactory) {

    companion object {
        const val TYPE_ID_TEMP_FILE = "temp-file"

        private val log = KotlinLogging.logger {}

        private val VALID_COLUMN_NAME = "[\\w-_:]+".toRegex()
    }

    private val typesRepo = modelServices.typesRepo
    private val aspectsRepo = modelServices.aspectsRepo

    fun isSubType(typeId: String?, ofTypeId: String?): Boolean {
        if (typeId == ofTypeId) {
            return true
        }
        if (typeId.isNullOrBlank() || ofTypeId.isNullOrBlank()) {
            return false
        }
        if (ofTypeId == "base") {
            return true
        }
        var typeInfo = typesRepo.getTypeInfo(ModelUtils.getTypeRef(typeId))
        var iterations = 30
        while (typeInfo != null && typeInfo.id != "base" && --iterations > 0) {
            if (typeInfo.parentRef.getLocalId() == ofTypeId) {
                return true
            }
            typeInfo = typesRepo.getTypeInfo(typeInfo.parentRef)
        }
        return false
    }

    fun getAspectsForAtts(attributes: Set<String>): List<EntityRef> {
        return aspectsRepo.getAspectsForAtts(attributes)
    }

    fun getAllChildrenIds(typeId: String, result: MutableCollection<String>) {
        val children = typesRepo.getChildren(ModelUtils.getTypeRef(typeId))
        for (child in children) {
            result.add(child.getLocalId())
            getAllChildrenIds(child.getLocalId(), result)
        }
    }

    fun getTypeInfoNotNull(typeId: String): TypeInfo {
        return getTypeInfo(typeId) ?: error("TypeInfo is not found for id '$typeId'")
    }

    fun getTypeInfo(typeId: String): TypeInfo? {
        if (typeId.isBlank()) {
            return null
        }
        return typesRepo.getTypeInfo(ModelUtils.getTypeRef(typeId))
    }

    fun getAspectsInfo(aspectRefs: Collection<EntityRef>): List<AspectInfo> {
        return aspectRefs.map { aspectsRepo.getAspectInfo(it) ?: AspectInfo.EMPTY }
    }

    fun getAllAttributesForAspects(aspectRefs: Collection<EntityRef>): List<AttributeDef> {
        return getAttributesForAspects(aspectRefs, true)
    }

    fun getAttributesForAspects(aspectRefs: Collection<EntityRef>, includeSystem: Boolean): List<AttributeDef> {
        val attributes = ArrayList<AttributeDef>(32)
        for (aspectRef in aspectRefs) {
            val aspectInfo = aspectsRepo.getAspectInfo(aspectRef)
            if (aspectInfo != null) {
                attributes.addAll(aspectInfo.attributes)
                if (includeSystem) {
                    attributes.addAll(aspectInfo.systemAttributes)
                }
            }
        }
        return attributes
    }

    fun getColumnsForAspects(aspectRefs: Collection<EntityRef>): List<EcosAttColumnDef> {
        val columns = ArrayList<EcosAttColumnDef>(32)
        for (aspectRef in aspectRefs) {
            val aspectInfo = aspectsRepo.getAspectInfo(aspectRef)
            if (aspectInfo != null) {
                columns.addAll(mapAttsToColumns(aspectInfo.attributes, false))
                columns.addAll(mapAttsToColumns(aspectInfo.systemAttributes, true))
            }
        }
        return columns
    }

    fun getColumnsForTypes(typesInfo: List<TypeInfo>): List<EcosAttColumnDef> {

        val processedTypes = hashSetOf<String>()
        val columnsById = LinkedHashMap<String, EcosAttColumnDef>()

        typesInfo.forEach { typeInfo ->

            if (processedTypes.add(typeInfo.id)) {

                val model = typeInfo.model
                val columns = ArrayList<EcosAttColumnDef>(
                    model.attributes.size + model.systemAttributes.size
                )
                columns.addAll(mapAttsToColumns(model.attributes, false))
                columns.addAll(mapAttsToColumns(model.systemAttributes, true))

                columns.forEach {
                    val currentColumn = columnsById[it.column.name]
                    if (currentColumn != null) {
                        if (it.column.type != currentColumn.column.type) {
                            error(
                                "Columns type doesn't match. " +
                                    "Current column: $currentColumn New column: $it"
                            )
                        }
                        if (it.systemAtt != currentColumn.systemAtt) {
                            error(
                                "System attribute flag doesn't match. " +
                                    "Current column: $currentColumn New column: $it"
                            )
                        }
                    }
                    columnsById[it.column.name] = it
                }
            }
        }

        return columnsById.values.toList()
    }

    private fun mapAttsToColumns(atts: List<AttributeDef>, system: Boolean): List<EcosAttColumnDef> {
        return atts.mapNotNull {
            mapAttToColumn(it)?.let { columnDef ->
                EcosAttColumnDef(columnDef, it, system)
            }
        }
    }

    private fun mapAttToColumn(attribute: AttributeDef): DbColumnDef? {

        if (!VALID_COLUMN_NAME.matches(attribute.id)) {
            log.debug { "Attribute id '${attribute.id}' is not a valid column name and will be skipped" }
            return null
        }
        if (attribute.id.startsWith("_")) {
            log.debug { "Attribute id '${attribute.id}' starts with '_', but it is reserved system prefix" }
            return null
        }
        if (attribute.computed.type != ComputedAttType.NONE &&
            attribute.computed.storingType == ComputedAttStoringType.NONE
        ) {
            // computed attributes without storingType won't be stored in DB
            return null
        }

        val columnType = when (attribute.type) {
            AttributeType.ASSOC -> DbColumnType.LONG
            AttributeType.PERSON -> DbColumnType.LONG
            AttributeType.AUTHORITY_GROUP -> DbColumnType.LONG
            AttributeType.AUTHORITY -> DbColumnType.LONG
            AttributeType.TEXT -> DbColumnType.TEXT
            AttributeType.MLTEXT -> DbColumnType.TEXT
            AttributeType.NUMBER -> DbColumnType.DOUBLE
            AttributeType.BOOLEAN -> DbColumnType.BOOLEAN
            AttributeType.DATE -> DbColumnType.DATE
            AttributeType.DATETIME -> DbColumnType.DATETIME
            AttributeType.CONTENT -> DbColumnType.LONG
            AttributeType.JSON -> DbColumnType.JSON
            AttributeType.BINARY -> DbColumnType.BINARY
        }
        val multiple = attribute.type != AttributeType.CONTENT && attribute.multiple
        val index = DbColumnIndexDef(attribute.index.enabled)

        return DbColumnDef(attribute.id, columnType, multiple, emptyList(), index)
    }
}
