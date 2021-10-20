package ru.citeck.ecos.data.sql.ecostype

import mu.KotlinLogging
import ru.citeck.ecos.data.sql.dto.DbColumnDef
import ru.citeck.ecos.data.sql.dto.DbColumnType
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType
import ru.citeck.ecos.model.lib.type.dto.TypeInfo
import ru.citeck.ecos.model.lib.type.repo.TypesRepo
import ru.citeck.ecos.model.lib.type.service.utils.TypeUtils

class DbEcosTypeService(private val typesRepo: TypesRepo) {

    companion object {
        private val log = KotlinLogging.logger {}

        private val VALID_COLUMN_NAME = "[\\w-_:]+".toRegex()
    }

    fun getTypeInfo(typeId: String): TypeInfo? {
        return typesRepo.getTypeInfo(TypeUtils.getTypeRef(typeId))
    }

    fun getColumnsForTypes(typesInfo: List<TypeInfo>): List<EcosAttColumnDef> {

        val processedTypes = hashSetOf<String>()
        val columnsById = LinkedHashMap<String, EcosAttColumnDef>()

        typesInfo.forEach { typeInfo ->

            if (processedTypes.add(typeInfo.id)) {
                val columns = typeInfo.model.attributes.mapNotNull {
                    mapAttToColumn(it)?.let { columnDef ->
                        EcosAttColumnDef(columnDef, it)
                    }
                }
                columns.forEach {
                    val currentColumn = columnsById[it.column.name]
                    if (currentColumn != null && it.column.type != currentColumn.column.type) {
                        error(
                            "Columns type doesn't match. " +
                                "Current column: $currentColumn New column: $it"
                        )
                    }
                    columnsById[it.column.name] = it
                }
            }
        }

        return columnsById.values.toList()
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

        return DbColumnDef(attribute.id, columnType, multiple, emptyList())
    }
}
