package ru.citeck.ecos.data.sql.ecostype

import mu.KotlinLogging
import ru.citeck.ecos.commons.data.MLText
import ru.citeck.ecos.data.sql.dto.DbColumnDef
import ru.citeck.ecos.data.sql.dto.DbColumnType
import ru.citeck.ecos.data.sql.repo.entity.DbEntity
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType
import ru.citeck.ecos.records2.RecordRef
import ru.citeck.ecos.records2.meta.RecordsTemplateService
import ru.citeck.ecos.records3.RecordsServiceFactory

class DbEcosTypeService(
    private val ecosTypeRepo: DbEcosTypeRepo,
    recordsServices: RecordsServiceFactory
) {

    companion object {
        private val log = KotlinLogging.logger {}

        private val VALID_COLUMN_NAME = "[\\w-_:]+".toRegex()
    }

    private val templateService = RecordsTemplateService(recordsServices)
    private val recordsService = recordsServices.recordsServiceV1

    fun fillComputedAtts(sourceId: String, dbRecSrcEntity: DbEntity): Boolean {

        val typeInfo = ecosTypeRepo.getTypeInfo(dbRecSrcEntity.type) ?: return false

        var changed = false
        val newName = getDisplayName(RecordRef.create(sourceId, dbRecSrcEntity.extId), typeInfo)
        if (dbRecSrcEntity.name != newName) {
            dbRecSrcEntity.name = newName
            changed = true
        }
        return changed
    }

    private fun getDisplayName(recordRef: RecordRef, typeInfo: DbEcosTypeInfo): MLText {

        if (!MLText.isEmpty(typeInfo.dispNameTemplate)) {
            return templateService.resolve(typeInfo.dispNameTemplate, recordRef)
        }

        val nameAtt = typeInfo.attributes.find { it.id == "name" }
        if (nameAtt != null) {
            if (nameAtt.type == AttributeType.TEXT) {
                val name = recordsService.getAtt(recordRef, "name").asText()
                if (name.isNotBlank()) {
                    return MLText(name)
                }
            } else if (nameAtt.type == AttributeType.MLTEXT) {
                val name = recordsService.getAtt(recordRef, "name?json")
                if (name.isObject()) {
                    val mlName = name.getAs(MLText::class.java)
                    if (mlName != null && !MLText.isEmpty(mlName)) {
                        return mlName
                    }
                }
            }
        }
        if (!MLText.isEmpty(typeInfo.name)) {
            return typeInfo.name
        }
        return MLText(typeInfo.id)
    }

    fun getColumnsForTypes(typesInfo: List<DbEcosTypeInfo>): List<DbColumnDef> {

        val processedTypes = hashSetOf<String>()
        val columnsById = LinkedHashMap<String, DbColumnDef>()

        typesInfo.forEach { typeInfo ->
            if (processedTypes.add(typeInfo.id)) {
                val columns = typeInfo.attributes.mapNotNull { mapAttToColumn(it) }
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
        if (attribute.id.startsWith("__")) {
            log.debug { "Attribute id '${attribute.id}' starts with '__', but it is reserved system prefix" }
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
            AttributeType.DATE -> DbColumnType.DATETIME
            AttributeType.DATETIME -> DbColumnType.DATETIME
            AttributeType.CONTENT -> DbColumnType.TEXT
            AttributeType.JSON -> DbColumnType.JSON
            AttributeType.BINARY -> DbColumnType.BINARY
        }

        return DbColumnDef(attribute.id, columnType, attribute.multiple, emptyList())
    }
}
