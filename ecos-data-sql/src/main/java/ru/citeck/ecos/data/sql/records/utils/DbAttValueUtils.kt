package ru.citeck.ecos.data.sql.records.utils

import ru.citeck.ecos.commons.data.DataValue
import ru.citeck.ecos.webapp.api.entity.EntityRef

object DbAttValueUtils {

    fun anyToSetOfStrings(value: Any?): Set<String> {
        return collectStringValues(value, LinkedHashSet())
    }

    fun <T : MutableCollection<String>> collectStringValues(value: Any?, result: T): T {
        if (value is Collection<*>) {
            for (elem in value) {
                collectStringValues(elem, result)
            }
        } else if (value is String) {
            if (value.isNotBlank()) {
                result.add(value)
            }
        } else if (value is EntityRef) {
            if (value.isNotEmpty()) {
                result.add(value.toString())
            }
        } else if (value is DataValue) {
            if (value.isTextual()) {
                val textVal = value.asText()
                if (textVal.isNotBlank()) {
                    result.add(textVal)
                }
            } else if (value.isArray()) {
                for (elem in value) {
                    collectStringValues(elem, result)
                }
            }
        }
        return result
    }

    fun anyToSetOfLongs(value: Any?): Set<Long> {
        return collectLongValues(value, LinkedHashSet())
    }

    fun collectLongValues(value: Any?): ArrayList<Long> {
        return collectLongValues(value, ArrayList())
    }

    fun <T : MutableCollection<Long>> collectLongValues(value: Any?, result: T): T {
        forEachLongValue(value) { result.add(it) }
        return result
    }

    fun forEachLongValue(value: Any?, action: (Long) -> Unit) {
        if (value is DataValue) {
            if (value.isNumber()) {
                action.invoke(value.asLong(-1))
            } else if (value.isArray()) {
                value.forEach { forEachLongValue(it, action) }
            }
        } else if (value is Long) {
            action.invoke(value)
        } else if (value is Collection<*>) {
            value.forEach { forEachLongValue(it, action) }
        }
    }
}
