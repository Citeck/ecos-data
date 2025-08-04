package ru.citeck.ecos.data.sql.records.dao.atts

import ru.citeck.ecos.data.sql.repo.find.DbFindPage
import ru.citeck.ecos.records2.RecordConstants
import ru.citeck.ecos.records3.record.atts.schema.SchemaAtt
import ru.citeck.ecos.records3.record.atts.schema.resolver.AttContext
import ru.citeck.ecos.records3.record.atts.value.AttValue
import ru.citeck.ecos.records3.record.atts.value.impl.InnerAttValue
import ru.citeck.ecos.webapp.api.entity.EntityRef

class DbPathByAssocValue private constructor(
    private val record: DbRecord
) : AttValue {

    companion object {
        /**
         * Protection against recursion
         */
        private val pathNodesInProgress = ThreadLocal.withInitial { HashSet<EntityRef>() }

        fun getValue(record: DbRecord): DbPathByAssocValue? {
            return if (pathNodesInProgress.get().contains(record.id)) {
                null
            } else {
                DbPathByAssocValue(record)
            }
        }
    }

    override fun getAtt(name: String): Any? {
        val nodesInProgress = pathNodesInProgress.get()
        return if (nodesInProgress.add(record.id)) {
            try {
                getPathByAssoc(name)
            } finally {
                nodesInProgress.remove(record.id)
            }
        } else {
            null
        }
    }

    private fun getPathByAssoc(assocName: String): Any {

        val sourceRef = if (assocName == RecordConstants.ATT_PARENT) {
            record.getAtt(RecordConstants.ATT_PARENT) as? EntityRef
        } else {
            record.ctx.assocsService.getSourceAssocs(record.entity.refId, assocName, DbFindPage.FIRST)
                .entities
                .firstOrNull()
                ?.sourceId
                ?.let { record.ctx.recordRefService.getEntityRefById(it) }
        }
        if (EntityRef.isEmpty(sourceRef)) {
            return listOf(record)
        }

        val innerAttsToLoad = AttContext.getCurrentSchemaAtt()

        val schemaAtt = SchemaAtt.create()
            .withName(DbRecord.ATT_PATH_BY_ASSOC)
            .withInner(
                SchemaAtt.create()
                    .withName(assocName)
                    .withMultiple(true)
                    .withInner(innerAttsToLoad.inner)
            ).build()

        val attsToLoad = record.ctx.schemaWriter.writeToMap(listOf(schemaAtt))

        val atts = record.ctx.recordsService.getAtts(listOf(sourceRef), attsToLoad, true)
            .first()[DbRecord.ATT_PATH_BY_ASSOC][assocName]

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
