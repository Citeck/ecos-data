package ru.citeck.ecos.data.sql.service.assocs

class AssocTableJoin(
    val attId: Long,
    val attribute: String,
    val srcColumn: String,
    val target: Boolean
)
