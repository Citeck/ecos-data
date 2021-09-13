package ru.citeck.ecos.data.sql.records.listener

import ru.citeck.ecos.records3.record.atts.value.RecordAttValueCtx

class MutationEvent(
    val recordBefore: RecordAttValueCtx,
    val recordAfter: RecordAttValueCtx,
    val isNewRecord: Boolean
)
