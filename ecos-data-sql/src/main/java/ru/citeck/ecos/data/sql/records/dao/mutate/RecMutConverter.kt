package ru.citeck.ecos.data.sql.records.dao.mutate

import ru.citeck.ecos.commons.data.DataValue
import ru.citeck.ecos.commons.json.Json
import ru.citeck.ecos.data.sql.dto.DbColumnType
import ru.citeck.ecos.webapp.api.entity.EntityRef

class RecMutConverter {

    fun convert(
        rawValue: DataValue,
        multiple: Boolean,
        columnType: DbColumnType
    ): Any? {

        return if (columnType == DbColumnType.JSON) {
            val converted = convertToClass(rawValue, multiple, DataValue::class.java)
            if (converted == null || converted is DataValue && converted.isNull()) {
                return null
            }
            Json.mapper.toString(converted)
        } else {
            convertToClass(rawValue, multiple, columnType.type.java)
        }
    }

    fun convertToClass(rawValue: DataValue, multiple: Boolean, javaType: Class<*>): Any? {

        val value = if (multiple) {
            if (!rawValue.isArray()) {
                val arr = DataValue.createArr()
                if (!rawValue.isNull()) {
                    arr.add(rawValue)
                }
                arr
            } else {
                rawValue
            }
        } else {
            if (rawValue.isArray()) {
                if (rawValue.size() == 0) {
                    DataValue.NULL
                } else {
                    rawValue[0]
                }
            } else {
                rawValue
            }
        }

        return if (multiple) {
            val result = ArrayList<Any?>(value.size())
            for (element in value) {
                val convertedElement = convertToClass(element, false, javaType)
                if (!isNull(convertedElement)) {
                    result.add(convertToClass(element, false, javaType))
                }
            }
            result
        } else if (value.isObject() && javaType == String::class.java) {
            Json.mapper.toString(value)
        } else {
            Json.mapper.convert(value, javaType)
        }
    }

    private fun isNull(value: Any?): Boolean {
        return value == null ||
            value is DataValue && value.isNull() ||
            value is EntityRef && value.isEmpty()
    }
}
