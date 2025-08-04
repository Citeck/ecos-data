package ru.citeck.ecos.data.sql.records.dao.atts

import ru.citeck.ecos.data.sql.repo.find.DbFindPage
import ru.citeck.ecos.records2.RecordConstants
import ru.citeck.ecos.records3.record.atts.schema.SchemaAtt
import ru.citeck.ecos.records3.record.atts.schema.resolver.AttContext
import ru.citeck.ecos.records3.record.atts.value.AttValue
import ru.citeck.ecos.records3.record.atts.value.impl.InnerAttValue
import ru.citeck.ecos.webapp.api.entity.EntityRef

class DbPathByAssocValue(
    private val record: DbRecord
) : AttValue {

    override fun getAtt(name: String): Any {

        val sourceRef = if (name == RecordConstants.ATT_PARENT) {
            record.getAtt(RecordConstants.ATT_PARENT) as? EntityRef
        } else {
            record.ctx.assocsService.getSourceAssocs(record.entity.refId, name, DbFindPage.FIRST)
                .entities
                .firstOrNull()
                ?.sourceId
                ?.let { record.ctx.recordRefService.getEntityRefById(it) }
        }
        if (EntityRef.isEmpty(sourceRef)) {
            return listOf(record)
        }

        val attsToLoad = AttContext.getCurrentSchemaAtt()

        val schemaAtt = SchemaAtt.create()
            .withName(DbRecord.ATT_PATH_BY_ASSOC)
            .withInner(
                SchemaAtt.create()
                    .withName(name)
                    .withMultiple(true)
                    .withInner(attsToLoad.inner)
            ).build()

        val attToLoadStr = record.ctx.schemaWriter.write(schemaAtt)
        val atts = record.ctx.recordsService.getAtt(sourceRef, attToLoadStr)

        val result = ArrayList<Any>()
        if (atts.isArray()) {
            atts.forEach { element ->
                result.add(InnerAttValue(element.asJson()))
            }
        }
        result.add(record)
        return result
    }
}
