package ru.citeck.ecos.data.sql.repo.entity

import org.apache.commons.beanutils.PropertyUtils
import ru.citeck.ecos.commons.data.MLText
import ru.citeck.ecos.commons.json.Json
import ru.citeck.ecos.data.sql.dto.DbColumnDef
import ru.citeck.ecos.data.sql.dto.DbColumnType
import ru.citeck.ecos.data.sql.dto.DbIndexDef
import ru.citeck.ecos.data.sql.repo.entity.annotation.ColumnType
import ru.citeck.ecos.data.sql.repo.entity.annotation.Constraints
import ru.citeck.ecos.data.sql.repo.entity.annotation.Indexes
import ru.citeck.ecos.data.sql.type.DbTypesConverter
import java.lang.reflect.Array
import java.sql.Date
import java.sql.Timestamp
import java.time.Instant
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
    private val indexes: List<DbIndexDef> = getIndexesImpl(entityType)

    private val hasAttributesField = hasField(entityType, ATTRIBUTES_FIELD)

    override fun getEntityIndexes(): List<DbIndexDef> {
        return indexes
    }

    override fun getEntityColumns(): List<DbEntityColumn> {
        return columns
    }

    override fun getExtId(entity: T): String {
        return PropertyUtils.getProperty(entity, EXT_ID_FIELD) as? String ?: ""
    }

    override fun convertToEntity(data: Map<String, Any?>): T {

        val entityColumns = columns.associateBy { it.columnDef.name }

        val entityAtts = LinkedHashMap<String, Any?>()
        val additionalAtts = LinkedHashMap<String, Any?>()
        data.forEach { (k, v) ->
            val entityField = entityColumns[k]
            if (entityField == null) {
                additionalAtts[k] = convertAdditionalAttValue(v)
            } else {
                entityAtts[entityField.fieldName] = converter.convert(v, entityField.fieldType)
            }
        }
        val entity = Json.mapper.convert(entityAtts, entityType.java) ?: error("Conversion error")
        if (hasAttributesField) {
            PropertyUtils.setProperty(entity, ATTRIBUTES_FIELD, additionalAtts)
        }
        return entity
    }

    private fun convertAdditionalAttValue(value: Any?): Any? {
        if (value == null) {
            return null
        }
        if (value::class.java.isArray) {
            val arraySize = Array.getLength(value)
            val list = ArrayList<Any?>(arraySize)
            for (i in 0 until arraySize) {
                list.add(convertAdditionalAttValue(Array.get(value, i)))
            }
            return list
        } else if (value is Date) {
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
            val attributes = PropertyUtils.getProperty(entity, ATTRIBUTES_FIELD) as? Map<String, Any?>
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
        val descriptors = PropertyUtils.getPropertyDescriptors(type.java)
        return descriptors.any { it.name == fieldName }
    }

    private fun getColumnsImpl(type: KClass<*>): List<DbEntityColumn> {

        val descriptors = PropertyUtils.getPropertyDescriptors(type.java)
        val defaultValue = type.java.getDeclaredConstructor().newInstance()

        return descriptors.mapNotNull { prop ->

            if (prop.writeMethod == null || prop.readMethod == null || prop.name == ATTRIBUTES_FIELD) {

                null
            } else {

                val field = type.java.getDeclaredField(prop.name)
                val constraints = field.getAnnotation(Constraints::class.java)?.value?.toList() ?: emptyList()
                val explicitColumnType = field.getAnnotation(ColumnType::class.java)?.value

                val fieldType = explicitColumnType ?: when {
                    prop.name == "id" -> DbColumnType.BIGSERIAL
                    prop.propertyType.kotlin == Long::class -> DbColumnType.LONG
                    prop.propertyType.kotlin == String::class -> DbColumnType.TEXT
                    prop.propertyType.kotlin == MLText::class -> DbColumnType.TEXT
                    prop.propertyType.kotlin == Double::class -> DbColumnType.DOUBLE
                    prop.propertyType.kotlin == Int::class -> DbColumnType.INT
                    prop.propertyType.kotlin == Boolean::class -> DbColumnType.BOOLEAN
                    prop.propertyType.kotlin == Instant::class -> DbColumnType.DATETIME
                    else -> error("Unknown type: ${prop.propertyType}")
                }

                val columnName = if (prop.name == "id") {
                    prop.name
                } else {
                    val snakeName = CAMEL_REGEX.replace(prop.name) { "_${it.value}" }.lowercase()
                    "__$snakeName"
                }

                DbEntityColumn(
                    prop.name,
                    prop.propertyType.kotlin,
                    prop.readMethod.invoke(defaultValue),
                    DbColumnDef(columnName, fieldType, false, constraints)
                ) { obj -> prop.readMethod.invoke(obj) }
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
                .build()
        }
    }
}
