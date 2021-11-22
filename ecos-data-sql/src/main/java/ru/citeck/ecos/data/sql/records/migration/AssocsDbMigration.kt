package ru.citeck.ecos.data.sql.records.migration

import mu.KotlinLogging
import ru.citeck.ecos.data.sql.dto.DbColumnDef
import ru.citeck.ecos.data.sql.dto.DbColumnType
import ru.citeck.ecos.data.sql.records.DbRecordsUtils
import ru.citeck.ecos.data.sql.records.refs.DbRecordRefService
import ru.citeck.ecos.data.sql.repo.entity.DbEntity
import ru.citeck.ecos.data.sql.service.migration.DbMigration
import ru.citeck.ecos.data.sql.service.migration.DbMigrationService
import ru.citeck.ecos.model.lib.type.dto.TypeInfo
import ru.citeck.ecos.records2.RecordRef

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
            return
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

        log.info { "Association attributes: ${assocAttributes.joinToString { it.id }}" }

        val entities = service.dataService.findAll()

        log.info { "Found ${entities.size} entities" }

        for (entity in entities) {
            var changed = false
            for (attribute in assocAttributes) {
                if (entity.attributes.containsKey(attribute.id)) {
                    entity.attributes[attribute.id + tempColumnPostfix] = mapAssocValue(entity.attributes[attribute.id])
                    changed = true
                }
            }
            if (changed) {
                service.dataService.save(entity)
            }
        }

        for (attribute in assocAttributes) {
            service.dataSource.updateSchema(
                "ALTER TABLE ${service.dataService.getTableRef().fullName} " +
                    "RENAME COLUMN \"${attribute.id}\" TO \"${attribute.id}__assoc_str\""
            )
            service.dataSource.updateSchema(
                "ALTER TABLE ${service.dataService.getTableRef().fullName} " +
                    "RENAME COLUMN \"${attribute.id}$tempColumnPostfix\" TO \"${attribute.id}\""
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
                value
            } else {
                dbRecordRefService.getOrCreateIdByRecordRefs(listOf(ref))[0]
            }
        }
        return value
    }

    override fun getType() = TYPE

    data class AssocsDbMigrationConfig(
        val typeInfo: TypeInfo
    )
}
