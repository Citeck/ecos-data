package ru.citeck.ecos.data.sql.service.aggregation

data class AggregateFunc(
    val alias: String,
    val func: String,
    val field: String
) {
    override fun toString(): String {
        return "$func($field) AS $alias"
    }
}
