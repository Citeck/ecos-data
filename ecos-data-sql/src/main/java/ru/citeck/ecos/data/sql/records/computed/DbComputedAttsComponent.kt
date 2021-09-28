package ru.citeck.ecos.data.sql.records.computed

import ru.citeck.ecos.commons.data.ObjectData
import ru.citeck.ecos.records2.RecordRef

interface DbComputedAttsComponent {

    fun computeAttsToStore(value: Any, isNewRecord: Boolean, typeRef: RecordRef): ObjectData
}
