package ru.citeck.ecos.data.sql.records

import ru.citeck.ecos.records2.RecordRef

data class DbRecordsDaoConfig(
    val typeRef: RecordRef,
    val insertable: Boolean,
    val updatable: Boolean,
    val deletable: Boolean,
    val queryMaxItems: Int = 5000
)
