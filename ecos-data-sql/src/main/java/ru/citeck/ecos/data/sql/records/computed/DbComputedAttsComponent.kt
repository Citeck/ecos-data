package ru.citeck.ecos.data.sql.records.computed

import ru.citeck.ecos.commons.data.MLText
import ru.citeck.ecos.commons.data.ObjectData
import ru.citeck.ecos.model.lib.attributes.computed.ComputeRes
import ru.citeck.ecos.model.lib.attributes.dto.AttOptionValue
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType
import ru.citeck.ecos.model.lib.attributes.dto.computed.ComputedAttDef
import ru.citeck.ecos.webapp.api.entity.EntityRef

interface DbComputedAttsComponent {

    fun getAttOptions(record: Any, config: ObjectData): List<AttOptionValue>

    fun computeAttsToStore(
        value: Any,
        isNewRecord: Boolean,
        typeRef: EntityRef,
        preCalculatedAtts: Map<String, Any?> = emptyMap()
    ): ObjectData

    fun computeDisplayName(value: Any, typeRef: EntityRef): MLText

    fun computeAtt(value: Any, attId: String, attType: AttributeType, computed: ComputedAttDef): ComputeRes
}
