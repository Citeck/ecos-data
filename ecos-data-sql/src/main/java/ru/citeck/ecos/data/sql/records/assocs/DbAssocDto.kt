package ru.citeck.ecos.data.sql.records.assocs

data class DbAssocDto(
    val sourceId: Long,
    val attribute: String,
    val targetId: Long,
    val child: Boolean
)
