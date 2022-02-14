package ru.citeck.ecos.data.sql.records.dao.mutate.operation

enum class OperationType(val prefix: String) {
    ATT_ADD("att_add_"),
    ATT_REMOVE("att_rem_")
}
