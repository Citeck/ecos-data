package ru.citeck.ecos.data.sql.records

import ru.citeck.ecos.data.sql.dto.DbTableRef

data class DbRecordsDaoConfig(
    val tableRef: DbTableRef,
    val insertable: Boolean,
    val updatable: Boolean,
    val deletable: Boolean,
    val authEnabled: Boolean,
    val queryMaxItems: Int = 5000
)
