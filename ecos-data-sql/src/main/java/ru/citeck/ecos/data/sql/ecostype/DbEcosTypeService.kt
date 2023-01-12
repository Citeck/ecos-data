package ru.citeck.ecos.data.sql.ecostype

import mu.KotlinLogging
import ru.citeck.ecos.data.sql.dto.DbColumnDef
import ru.citeck.ecos.data.sql.dto.DbColumnIndexDef
import ru.citeck.ecos.data.sql.dto.DbColumnType
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType
import ru.citeck.ecos.model.lib.attributes.dto.computed.ComputedAttStoringType
import ru.citeck.ecos.model.lib.attributes.dto.computed.ComputedAttType
import ru.citeck.ecos.model.lib.type.dto.TypeInfo
import ru.citeck.ecos.model.lib.type.repo.TypesRepo
import ru.citeck.ecos.model.lib.type.service.utils.TypeUtils

class DbEcosTypeService(private val typesRepo: TypesRepo) {

    companion object {
        const val TYPE_ID_TEMP_FILE = "temp-file"

        private val log = KotlinLogging.logger {}

        private val VALID_COLUMN_NAME = "[\\w-_:]+".toRegex()
    }

    fun getTypeInfoNotNull(typeId: String): TypeInfo {
        return getTypeInfo(typeId) ?: error("TypeInfo is not found for id '$typeId'")
    }

    fun getTypeInfo(typeId: String): TypeInfo? {
        if (typeId.isBlank()) {
            return null
        }
        return typesRepo.getTypeInfo(TypeUtils.getTypeRef(typeId))
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
