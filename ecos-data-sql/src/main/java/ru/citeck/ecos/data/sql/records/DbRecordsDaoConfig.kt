package ru.citeck.ecos.data.sql.records

data class DbRecordsDaoConfig(
    val insertable: Boolean,
    val updatable: Boolean,
    val deletable: Boolean,
    val queryMaxItems: Int = 5000
)
