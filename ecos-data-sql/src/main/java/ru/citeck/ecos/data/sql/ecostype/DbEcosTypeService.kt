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

    fun getColumnsForTypes(typesInfo: List<TypeInfo>): List<DbColumnDef> {

        val processedTypes = hashSetOf<String>()
        val columnsById = LinkedHashMap<String, DbColumnDef>()

        typesInfo.forEach { typeInfo ->

            if (processedTypes.add(typeInfo.id)) {
                val columns = typeInfo.model.attributes.mapNotNull { mapAttToColumn(it) }
                columns.forEach { columnsById[it.name] = it }
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
            AttributeType.ASSOC -> DbColumnType.TEXT
            AttributeType.PERSON -> DbColumnType.TEXT
            AttributeType.AUTHORITY_GROUP -> DbColumnType.TEXT
            AttributeType.AUTHORITY -> DbColumnType.TEXT
            AttributeType.TEXT -> DbColumnType.TEXT
            AttributeType.MLTEXT -> DbColumnType.TEXT
            AttributeType.NUMBER -> DbColumnType.DOUBLE
            AttributeType.BOOLEAN -> DbColumnType.BOOLEAN
            AttributeType.DATE -> DbColumnType.DATE
            AttributeType.DATETIME -> DbColumnType.DATETIME
            AttributeType.CONTENT -> DbColumnType.TEXT
            AttributeType.JSON -> DbColumnType.JSON
            AttributeType.BINARY -> DbColumnType.BINARY
        }

        return DbColumnDef(attribute.id, columnType, attribute.multiple, emptyList())
    }
}
