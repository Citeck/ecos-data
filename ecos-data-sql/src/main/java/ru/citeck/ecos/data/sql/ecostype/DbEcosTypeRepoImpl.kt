package ru.citeck.ecos.data.sql.ecostype

import ru.citeck.ecos.commons.data.MLText
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.status.dto.StatusDef
import ru.citeck.ecos.records2.RecordRef
import ru.citeck.ecos.records3.RecordsService
import ru.citeck.ecos.records3.record.atts.schema.annotation.AttName
import ru.citeck.ecos.records3.record.request.RequestContext

class DbEcosTypeRepoImpl(private val recordsService: RecordsService) : DbEcosTypeRepo {

    override fun getTypeInfo(typeId: String): DbEcosTypeInfo {

        val typeRef = RecordRef.create("emodel", "rtype", typeId)

        val typeAtts = RequestContext.getCurrentNotNull()
            .getMap<String, EcosTypeModelAtts>("${DbEcosTypeService::class.simpleName}-type-atts")
            .computeIfAbsent(typeRef.id) { recordsService.getAtts(typeRef, EcosTypeModelAtts::class.java) }

        return DbEcosTypeInfo(
            typeRef.id,
            typeAtts.name ?: MLText.EMPTY,
            typeAtts.dispNameTemplate ?: MLText.EMPTY,
            typeAtts.numTemplateRef ?: RecordRef.EMPTY,
            typeAtts.attributes,
            typeAtts.statuses
        )
    }

    class EcosTypeModelAtts(
        val name: MLText?,
        val dispNameTemplate: MLText?,
        val numTemplateRef: RecordRef?,
        @AttName("model.attributes[]")
        val attributes: List<AttributeDef>,
        @AttName("model.statuses[]")
        val statuses: List<StatusDef>
    )
}
