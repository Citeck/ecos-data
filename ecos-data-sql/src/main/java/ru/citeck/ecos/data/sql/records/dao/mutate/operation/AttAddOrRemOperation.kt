package ru.citeck.ecos.data.sql.records.dao.mutate.operation

import ru.citeck.ecos.commons.data.DataValue

class AttAddOrRemOperation(
    private val att: String,
    private val add: Boolean,
    private val exclusive: Boolean,
    value: DataValue,
) : AttValueOperation {

    private val operationValues: List<Any?> = if (value.isNull()) {
        emptyList()
    } else if (value.isArray()) {
        val opValues = ArrayList<Any?>()
        value.forEach { opValues.add(it.asJavaObj()) }
        opValues
    } else {
        listOf(value.asJavaObj())
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
        if (value is Collection<*>) {
            val newValue = ArrayList(value)
            if (add) {
                if (exclusive) {
                    newValue.addAll(operationValues.filter { !newValue.contains(it) })
                } else {
                    newValue.addAll(operationValues)
                }
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
            if (exclusive) {
                newValue.addAll(operationValues.filter { it != value })
            } else {
                newValue.addAll(operationValues)
            }
            return newValue
        }
        return value
    }

    override fun getAttName() = att

    fun isAdd(): Boolean {
        return add
    }
}
