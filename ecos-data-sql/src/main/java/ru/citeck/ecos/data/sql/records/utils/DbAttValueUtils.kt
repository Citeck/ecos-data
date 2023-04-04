package ru.citeck.ecos.data.sql.records.utils

object DbAttValueUtils {

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
        if (value is Long) {
            action.invoke(value)
        } else if (value is Collection<*>) {
            value.forEach { forEachLongValue(it, action) }
        }
    }
}
