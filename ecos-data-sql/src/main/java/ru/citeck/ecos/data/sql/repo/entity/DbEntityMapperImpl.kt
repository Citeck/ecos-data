package ru.citeck.ecos.data.sql.repo.entity

import ru.citeck.beans.BeanUtils
import ru.citeck.ecos.commons.data.MLText
import ru.citeck.ecos.commons.json.Json
import ru.citeck.ecos.data.sql.dto.DbColumnDef
import ru.citeck.ecos.data.sql.dto.DbColumnType
import ru.citeck.ecos.data.sql.dto.DbIndexDef
import ru.citeck.ecos.data.sql.repo.entity.annotation.ColumnType
import ru.citeck.ecos.data.sql.repo.entity.annotation.Constraints
import ru.citeck.ecos.data.sql.repo.entity.annotation.Indexes
import ru.citeck.ecos.data.sql.repo.entity.legacy.DbLegacyEntity
import ru.citeck.ecos.data.sql.repo.entity.legacy.DbLegacyTypes
import ru.citeck.ecos.data.sql.service.DbDataService
import ru.citeck.ecos.data.sql.type.DbTypesConverter
import java.lang.reflect.Array
import java.net.URI
import java.sql.Timestamp
import java.time.Instant
import java.time.LocalDate
import java.time.ZoneOffset
import kotlin.collections.ArrayList
import kotlin.collections.LinkedHashMap
import kotlin.reflect.KClass

class DbEntityMapperImpl<T : Any>(
    private val entityType: KClass<T>,
    private val converter: DbTypesConverter
) : DbEntityMapper<T> {

    companion object {
        private const val ATTRIBUTES_FIELD = "attributes"
        private const val EXT_ID_FIELD = "extId"

        private val CAMEL_REGEX = "(?<=[a-zA-Z])[A-Z]".toRegex()
    }

    private val columns: List<DbEntityColumn> = getColumnsImpl(entityType)
    private val columnsByColumnName: Map<String, DbEntityColumn> = columns.associateBy { it.columnDef.name }
    private val indexes: List<DbIndexDef> = getIndexesImpl(entityType)
    private val legacyConverters: List<Pair<Int, DbEntityMapper<DbLegacyEntity<T>>>> = getLegacyConverters(entityType)

    private val hasAttributesField = hasField(entityType, ATTRIBUTES_FIELD)

    override fun getEntityIndexes(): List<DbIndexDef> {
        return indexes
    }

    override fun getEntityColumnByColumnName(name: String?): DbEntityColumn? {
        name ?: return null
        return columnsByColumnName[name]
    }

    override fun getEntityColumns(): List<DbEntityColumn> {
        return columns
    }

    override fun getExtId(entity: T): String {
        return BeanUtils.getProperty(entity, EXT_ID_FIELD) as? String ?: ""
    }

    override fun convertToEntity(data: Map<String, Any?>): T {
        return convertToEntity(data, DbDataService.NEW_TABLE_SCHEMA_VERSION)
    }

    override fun convertToEntity(data: Map<String, Any?>, schemaVersion: Int): T {

        if (schemaVersion != DbDataService.NEW_TABLE_SCHEMA_VERSION && legacyConverters.isNotEmpty()) {
            for (converter in legacyConverters) {
                if (converter.first >= schemaVersion) {
                    return converter.second.convertToEntity(data, schemaVersion).getAsEntity()
                }
            }
        }

        val entityColumns = columns.associateBy { it.columnDef.name }

        val entityAtts = LinkedHashMap<String, Any?>()
        val additionalAtts = LinkedHashMap<String, Any?>()
        data.forEach { (k, v) ->
            val entityField = entityColumns[k]
            if (entityField == null) {
                additionalAtts[k] = convertAdditionalAttValue(v)
            } else {
                entityAtts[entityField.fieldName] = converter.convert(v, entityField.fieldType)
                    ?: entityField.defaultValue
            }
        }
        if (entityAtts.containsKey(DbEntity.REF_ID) && entityAtts[DbEntity.REF_ID] == null) {
            // protection against NullPointer for legacy records
            entityAtts[DbEntity.REF_ID] = DbEntity.NEW_REC_ID
        }
        val entity = Json.mapper.convert(entityAtts, entityType.java) ?: error("Conversion error")
        if (hasAttributesField) {
            BeanUtils.setProperty(entity, ATTRIBUTES_FIELD, additionalAtts)
        }
        return entity
    }

    private fun convertAdditionalAttValue(value: Any?): Any? {
        if (value == null) {
            return null
        }
        if (value is ByteArray) {
            return value
        } else if (value::class.java.isArray) {
            val arraySize = Array.getLength(value)
            val list = ArrayList<Any?>(arraySize)
            for (i in 0 until arraySize) {
                list.add(convertAdditionalAttValue(Array.get(value, i)))
            }
            return list
        } else if (value is LocalDate) {
            return value.atStartOfDay(ZoneOffset.UTC).toInstant()
        } else if (value is java.sql.Date) {
            return value.toLocalDate().atStartOfDay(ZoneOffset.UTC).toInstant()
        } else if (value is Timestamp) {
            return value.toInstant()
        }
        return value
    }

    override fun convertToMap(entity: T): Map<String, Any?> {

        val rawAtts = LinkedHashMap<String, Any?>()

        if (hasAttributesField) {
            @Suppress("UNCHECKED_CAST")
            val attributes = BeanUtils.getProperty(entity, ATTRIBUTES_FIELD) as? Map<String, Any?>
            if (attributes != null) {
                rawAtts.putAll(attributes)
            }
        }

        columns.forEach {
            rawAtts[it.columnDef.name] = it.getter.invoke(entity) ?: it.defaultValue
        }

        return rawAtts
    }

    private fun hasField(type: KClass<*>, fieldName: String): Boolean {
        val properties = BeanUtils.getProperties(type.java)
        return properties.any { it.getName() == fieldName }
    }

    private fun getColumnsImpl(type: KClass<*>): List<DbEntityColumn> {

        val properties = BeanUtils.getProperties(type.java)
        val defaultValue = type.java.getDeclaredConstructor().newInstance()

        return properties.mapNotNull { prop ->

            if (prop.getWriteMethod() == null ||
                prop.getReadMethod() == null ||
                prop.getName() == ATTRIBUTES_FIELD ||
                prop.getName().startsWith("legacy")
            ) {

                null
            } else {

                val field = type.java.getDeclaredField(prop.getName())
                val constraints = field.getAnnotation(Constraints::class.java)?.value?.toList() ?: emptyList()
                val explicitColumnType = field.getAnnotation(ColumnType::class.java)?.value

                val fieldType = explicitColumnType ?: when {
                    prop.getName() == "id" -> DbColumnType.BIGSERIAL
                    prop.getPropClass().kotlin == Long::class -> DbColumnType.LONG
                    prop.getPropClass().kotlin == String::class -> DbColumnType.TEXT
                    prop.getPropClass().kotlin == MLText::class -> DbColumnType.TEXT
                    prop.getPropClass().kotlin == Double::class -> DbColumnType.DOUBLE
                    prop.getPropClass().kotlin == Int::class -> DbColumnType.INT
                    prop.getPropClass().kotlin == Boolean::class -> DbColumnType.BOOLEAN
                    prop.getPropClass().kotlin == Instant::class -> DbColumnType.DATETIME
                    prop.getPropClass().kotlin == ByteArray::class -> DbColumnType.BINARY
                    prop.getPropClass().kotlin == URI::class -> DbColumnType.TEXT
                    else -> error("Unknown type: ${prop.getPropClass()}")
                }

                val columnName = if (prop.getName() == "id") {
                    prop.getName()
                } else {
                    val snakeName = CAMEL_REGEX.replace(prop.getName()) { "_${it.value}" }.lowercase()
                    "__$snakeName"
                }

                DbEntityColumn(
                    prop.getName(),
                    prop.getPropClass().kotlin,
                    prop.getReadMethod()!!.invoke(defaultValue),
                    DbColumnDef.create {
                        withName(columnName)
                        withType(fieldType)
                        withConstraints(constraints)
                    }
                ) { obj -> prop.getReadMethod()!!.invoke(obj) }
            }
        }
    }

    private fun getIndexesImpl(type: KClass<*>): List<DbIndexDef> {

        val indexes = type.java.getAnnotation(Indexes::class.java)
        if (indexes == null || indexes.value.isEmpty()) {
            return emptyList()
        }
        return indexes.value.map {
            DbIndexDef.create()
                .withColumns(it.columns.toList())
                .withUnique(it.unique)
                .withCaseInsensitive(it.caseInsensitive)
                .build()
        }
    }

    private fun getLegacyConverters(type: KClass<*>): List<Pair<Int, DbEntityMapper<DbLegacyEntity<T>>>> {

        val legacyTypes = type.java.getAnnotation(DbLegacyTypes::class.java) ?: return emptyList()

        val typesList = legacyTypes.value.map {
            val ver = it.java.getDeclaredField(DbLegacyEntity.MAX_SCHEMA_VERSION_FIELD_NAME).get(null) as Int
            ver to it
        }.sortedBy { it.first }

        return typesList.map {
            @Suppress("UNCHECKED_CAST")
            it.first to DbEntityMapperImpl(it.second, converter) as DbEntityMapper<DbLegacyEntity<T>>
        }
    }
}
