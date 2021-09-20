package ru.citeck.ecos.data.sql.records.listener

import ru.citeck.ecos.records3.record.atts.value.AttValueCtx

class MutationEvent(
    val recordBefore: AttValueCtx,
    val recordAfter: AttValueCtx,
    val isNewRecord: Boolean
)
