package ru.citeck.ecos.data.sql.records

import ru.citeck.ecos.data.sql.dto.DbTableRef

data class DbRecordsDaoConfig(
    val tableRef: DbTableRef,
    val insertable: Boolean,
    val mutable: Boolean,
    val authEnabled: Boolean
)

