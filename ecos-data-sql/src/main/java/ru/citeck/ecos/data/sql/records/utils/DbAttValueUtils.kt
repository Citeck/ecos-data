package ru.citeck.ecos.data.sql.records.utils

object DbAttValueUtils {

    fun collectLongValues(value: Any?): List<Long> {
        val values = ArrayList<Long>()
        forEachLongValue(value) { values.add(it) }
        return values
    }

    fun forEachLongValue(value: Any?, action: (Long) -> Unit) {
        if (value is Long) {
            action.invoke(value)
        } else if (value is Collection<*>) {
            value.forEach { forEachLongValue(it, action) }
        }
    }
}
