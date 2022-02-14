package ru.citeck.ecos.data.sql.records.dao.mutate

import ru.citeck.ecos.commons.data.DataValue
import ru.citeck.ecos.commons.json.Json
import ru.citeck.ecos.data.sql.dto.DbColumnType

class RecMutConverter {

    fun convert(
        rawValue: DataValue,
        multiple: Boolean,
        columnType: DbColumnType
    ): Any? {

        return if (columnType == DbColumnType.JSON) {
            val converted = convertToClass(rawValue, multiple, DataValue::class.java)
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
                    rawValue.get(0)
                }
            } else {
                rawValue
            }
        }

        if (multiple) {
            val result = ArrayList<Any?>(value.size())
            for (element in value) {
                result.add(convertToClass(element, false, javaType))
            }
            return result
        }

        return if (value.isObject() && javaType == String::class.java) {
            Json.mapper.toString(value)
        } else {
            Json.mapper.convert(value, javaType)
        }
    }
}
