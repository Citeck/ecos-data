package ru.citeck.ecos.data.sql.records.dao.mutate.operation

import ru.citeck.ecos.commons.data.ObjectData

class RecMutAttOperationsHandler {

    fun extractAttValueOperations(attributes: ObjectData): List<AttValueOperation> {
        val operations = ArrayList<AttValueOperation>()
        val operationsNames = hashSetOf<String>()
        attributes.forEach { name, value ->
            if (name.startsWith(OperationType.ATT_ADD.prefix)) {
                operations.add(
                    AttAddOrRemOperation(
                        name.replaceFirst(OperationType.ATT_ADD.prefix, ""),
                        add = true,
                        exclusive = true,
                        value = value
                    )
                )
                operationsNames.add(name)
            } else if (name.startsWith(OperationType.ATT_REMOVE.prefix)) {
                operations.add(
                    AttAddOrRemOperation(
                        name.replaceFirst(OperationType.ATT_REMOVE.prefix, ""),
                        add = false,
                        exclusive = true,
                        value = value
                    )
                )
                operationsNames.add(name)
            }
        }
        if (operationsNames.isNotEmpty()) {
            operationsNames.forEach { attributes.remove(it) }
        }
        return operations
    }
}
