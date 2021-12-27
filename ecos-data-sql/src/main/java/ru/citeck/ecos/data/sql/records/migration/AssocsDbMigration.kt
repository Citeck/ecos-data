package ru.citeck.ecos.data.sql.records.migration

import mu.KotlinLogging
import ru.citeck.ecos.data.sql.dto.DbColumnConstraint
import ru.citeck.ecos.data.sql.dto.DbColumnDef
import ru.citeck.ecos.data.sql.dto.DbColumnType
import ru.citeck.ecos.data.sql.records.DbRecordsUtils
import ru.citeck.ecos.data.sql.records.refs.DbRecordRefService
import ru.citeck.ecos.data.sql.repo.entity.DbEntity
import ru.citeck.ecos.data.sql.service.migration.DbMigration
import ru.citeck.ecos.data.sql.service.migration.DbMigrationService
import ru.citeck.ecos.model.lib.type.dto.TypeInfo
import ru.citeck.ecos.records2.RecordRef
import ru.citeck.ecos.records2.predicate.model.VoidPredicate

class AssocsDbMigration(
    private val dbRecordRefService: DbRecordRefService
) : DbMigration<DbEntity, AssocsDbMigration.AssocsDbMigrationConfig> {

    companion object {
        const val TYPE = "assocs-str-to-long"

        private val log = KotlinLogging.logger {}
    }

    override fun run(service: DbMigrationService<DbEntity>, mock: Boolean, config: AssocsDbMigrationConfig) {

        if (mock) {
            error("mock mode is not supported")
        }

        log.info { "Associations migration started" }

        val assocAttributes = config.typeInfo.model.attributes.filter {
            DbRecordsUtils.isAssocLikeAttribute(it)
        }

        if (assocAttributes.isEmpty()) {
            log.info { "Association attributes doesn't found in type '${config.typeInfo.id}'" }
        } else {
            log.info { "Association attributes: ${assocAttributes.joinToString { it.id }}" }
        }

        val tempColumnPostfix = "__assoc_bigint"

        service.schemaDao.addColumns(
            assocAttributes.map {
                DbColumnDef.create()
                    .withName(it.id + tempColumnPostfix)
                    .withType(DbColumnType.LONG)
                    .withMultiple(it.multiple)
                    .build()
            }
        )
        service.dataService.resetColumnsCache()

        val entities = service.dataService.findAll(VoidPredicate.INSTANCE, true)

        log.info { "Found ${entities.size} entities" }

        var refIdColumnWasAdded = false
        if (entities.isNotEmpty() && service.schemaDao.getColumns().none { it.name == DbEntity.REF_ID }) {
            // add __ref_id column manually to avoid error with not-null constraint
            service.schemaDao.addColumns(
                listOf(
                    DbColumnDef.create {
                        withName(DbEntity.REF_ID)
                        withType(DbColumnType.LONG)
                    }
                )
            )
            service.dataService.resetColumnsCache()
            refIdColumnWasAdded = true
        }

        for (entity in entities) {
            var changed = false
            for (attribute in assocAttributes) {
                if (entity.attributes.containsKey(attribute.id)) {
                    val mappedValue = mapAssocValue(entity.attributes[attribute.id])
                    val newValue = if (mappedValue == null && attribute.multiple) {
                        emptyList<Long>()
                    } else {
                        mappedValue
                    }
                    entity.attributes[attribute.id + tempColumnPostfix] = newValue
                    changed = true
                }
            }
            if (entity.refId == DbEntity.NEW_REC_ID) {
                val entityRef = RecordRef.create(
                    config.appName,
                    config.sourceId,
                    entity.extId
                )
                entity.refId = dbRecordRefService.getOrCreateIdByRecordRefs(listOf(entityRef))[0]
                changed = true
            }
            if (changed) {
                service.dataService.save(entity)
            }
        }

        for (attribute in assocAttributes) {

            service.dataSource.updateSchema(
                "ALTER TABLE ${service.dataService.getTableRef().fullName} " +
                    "DROP COLUMN \"${attribute.id}\""
            )
            service.dataSource.updateSchema(
                "ALTER TABLE ${service.dataService.getTableRef().fullName} " +
                    "RENAME COLUMN \"${attribute.id}$tempColumnPostfix\" TO \"${attribute.id}\""
            )
        }

        if (refIdColumnWasAdded) {
            service.schemaDao.setColumnConstraints(
                DbEntity.REF_ID,
                listOf(DbColumnConstraint.NOT_NULL)
            )
        }
    }

    private fun mapAssocValue(value: Any?): Any? {

        if (value == null || value is Long) {
            return value
        }
        if (value is List<*>) {
            val result = ArrayList<Any?>()
            for (subValue in value) {
                result.add(mapAssocValue(subValue))
            }
            return result
        }
        if (value is String) {
            val ref = RecordRef.valueOf(value)
            return if (RecordRef.isEmpty(ref)) {
                null
            } else {
                dbRecordRefService.getOrCreateIdByRecordRefs(listOf(ref))[0]
            }
        }
        error("unknown assoc type: ${value::class.java}")
    }

    override fun getType() = TYPE

    data class AssocsDbMigrationConfig(
        val typeInfo: TypeInfo,
        val sourceId: String,
        val appName: String
    )
}
