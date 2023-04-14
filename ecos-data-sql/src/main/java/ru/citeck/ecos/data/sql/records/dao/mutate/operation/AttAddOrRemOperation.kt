package ru.citeck.ecos.data.sql.records.dao.mutate.operation

import ru.citeck.ecos.commons.data.DataValue

class AttAddOrRemOperation(
    private val att: String,
    private val add: Boolean,
    value: DataValue,
) : AttValueOperation {

    private val operationValues: List<Any> = if (value.isNull()) {
        emptyList()
    } else if (value.isArray()) {
        val opValues = ArrayList<Any>()
        value.forEach { dv ->
            dv.asJavaObj()?.let { opValues.add(it) }
        }
        opValues
    } else {
        value.asJavaObj()?.let { listOf(it) } ?: emptyList()
    }

    override fun invoke(value: Any?): Any? {
        if (operationValues.isEmpty()) {
            return value
        }
        if (value == null) {
            return if (add) {
                this.operationValues
            } else {
                null
            }
        }
        if (value is AttValuesContainer<*>) {
            @Suppress("UNCHECKED_CAST")
            value as AttValuesContainer<Any>
            if (add) {
                value.addAll(this.operationValues)
            } else {
                value.removeAll(this.operationValues)
            }
            return value
        }
        if (value is Collection<*>) {
            val newValue = ArrayList(value)
            if (add) {
                newValue.addAll(operationValues.filter { !newValue.contains(it) })
            } else {
                newValue.removeAll(operationValues.toSet())
            }
            return newValue
        }
        if (!add) {
            if ((value is String || value is Long) && operationValues.any { it == value }) {
                return null
            }
        } else {
            val newValue = ArrayList<Any?>()
            newValue.add(value)
            newValue.addAll(operationValues.filter { it != value })
            return newValue
        }
        return value
    }

    override fun getAttName() = att

    fun isAdd(): Boolean {
        return add
    }
}
